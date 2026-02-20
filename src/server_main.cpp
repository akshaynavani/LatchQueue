// latchqueue_server: runs the broker with the Asio TCP data plane and the
// optional gRPC control plane until SIGINT/SIGTERM.

#include <atomic>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

#include "config.hpp"
#include "latchqueue/broker.hpp"
#include "latchqueue/net/delivery_pump.hpp"
#include "latchqueue/net/tcp_server.hpp"

#ifdef LQ_WITH_GRPC
#include "latchqueue/rpc/grpc_service.hpp"
#endif

namespace {

std::atomic<bool> g_stop{false};

extern "C" void on_signal(int) { g_stop.store(true); }

void log_stats(const latchqueue::Broker& broker, std::uint64_t& last_published,
               double interval_s) {
  const auto s = broker.stats();
  const double rate = static_cast<double>(s.total_published - last_published) / interval_s;
  last_published = s.total_published;

  std::ostringstream os;
  os << std::fixed << std::setprecision(0) << "[stats] published=" << s.total_published
     << " delivered=" << s.total_delivered << " dropped=" << s.total_dropped
     << " rate=" << rate << "/s topics=" << s.topics.size();
  for (const auto& t : s.topics) {
    for (const auto& sub : t.subscriptions) {
      os << "\n        " << t.name << "/" << sub.name << " depth=" << sub.depth << "/"
         << sub.capacity << " p50=" << sub.latency.p50_ns << "ns p99=" << sub.latency.p99_ns
         << "ns p99.9=" << sub.latency.p999_ns << "ns";
    }
  }
  std::cout << os.str() << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  int exit_code = 0;
  auto cfg = latchqueue::load_config(argc, argv, exit_code);
  if (!cfg) return exit_code;

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
#ifdef SIGPIPE
  std::signal(SIGPIPE, SIG_IGN);
#endif

  latchqueue::BrokerOptions bopts;
  bopts.default_queue_capacity = cfg->queue_capacity;
  bopts.max_topics = cfg->max_topics;
  latchqueue::Broker broker(bopts);

  // Preload topics.
  std::stringstream topics(cfg->preload_topics);
  for (std::string name; std::getline(topics, name, ',');) {
    if (name.empty()) continue;
    if (broker.create_topic(name)) {
      std::cout << "[init] created topic '" << name << "'\n";
    } else {
      std::cerr << "[init] could not create topic '" << name << "'\n";
    }
  }

  latchqueue::net::DeliveryPumpOptions popts;
  popts.threads = cfg->pump_threads;
  latchqueue::net::DeliveryPump pump(popts);
  pump.start();

  latchqueue::net::TcpServerOptions topts;
  topts.host = cfg->tcp_host;
  topts.port = cfg->tcp_port;
  topts.io_threads = cfg->io_threads;
  latchqueue::net::TcpServer tcp(broker, pump, topts);
  try {
    tcp.start();
  } catch (const std::exception& e) {
    std::cerr << "[tcp] failed to start: " << e.what() << "\n";
    return 1;
  }
  std::cout << "[tcp] listening on " << cfg->tcp_host << ":" << tcp.port() << " ("
            << cfg->io_threads << " io threads, " << cfg->pump_threads << " pump threads)\n";

#ifdef LQ_WITH_GRPC
  std::unique_ptr<latchqueue::rpc::GrpcServer> grpc;
  if (cfg->grpc_enabled) {
    grpc = std::make_unique<latchqueue::rpc::GrpcServer>(broker, cfg->grpc_address);
    if (!grpc->start()) {
      std::cerr << "[grpc] failed to listen on " << cfg->grpc_address << "\n";
      return 1;
    }
    std::cout << "[grpc] listening on " << cfg->grpc_address << "\n";
  }
#else
  if (cfg->grpc_enabled) {
    std::cout << "[grpc] not compiled in (configure with -DLQ_BUILD_GRPC=ON)\n";
  }
#endif

  std::uint64_t last_published = 0;
  auto next_stats = std::chrono::steady_clock::now() + std::chrono::seconds(cfg->stats_interval_s);
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (cfg->stats_interval_s != 0 && std::chrono::steady_clock::now() >= next_stats) {
      log_stats(broker, last_published, cfg->stats_interval_s);
      next_stats += std::chrono::seconds(cfg->stats_interval_s);
    }
  }

  std::cout << "[main] shutting down\n";
#ifdef LQ_WITH_GRPC
  if (grpc) grpc->stop();
#endif
  tcp.stop();
  pump.stop();
  log_stats(broker, last_published, 1.0);
  return 0;
}
