// End-to-end benchmark over the binary TCP protocol.
//
//   tcp_bench [--host H --port P] [--messages N] [--payload BYTES]
//
// Without --port, an in-process server is started on an ephemeral loopback
// port. One client publishes fire-and-forget batches while another
// subscribes; reports publish throughput and publish->receive latency.
// Latency uses the broker's monotonic clock, so it is only meaningful when
// client and server share a host.

#include <atomic>
#include <memory>
#include <thread>

#include "bench_util.hpp"
#include "latchqueue/broker.hpp"
#include "latchqueue/client/tcp_client.hpp"
#include "latchqueue/clock.hpp"
#include "latchqueue/net/delivery_pump.hpp"
#include "latchqueue/net/tcp_server.hpp"

using namespace latchqueue;
using namespace std::chrono_literals;

int main(int argc, char** argv) {
  const auto messages = static_cast<std::uint64_t>(bench::arg_int(argc, argv, "--messages", 1'000'000));
  const auto payload_size = static_cast<std::size_t>(bench::arg_int(argc, argv, "--payload", 64));
  std::string host = bench::arg_str(argc, argv, "--host", "127.0.0.1");
  auto port = static_cast<std::uint16_t>(bench::arg_int(argc, argv, "--port", 0));

  std::unique_ptr<Broker> broker;
  std::unique_ptr<net::DeliveryPump> pump;
  std::unique_ptr<net::TcpServer> server;
  if (port == 0) {
    BrokerOptions bopts;
    bopts.default_queue_capacity = 1 << 18;
    broker = std::make_unique<Broker>(bopts);
    pump = std::make_unique<net::DeliveryPump>(net::DeliveryPumpOptions{});
    pump->start();
    net::TcpServerOptions sopts;
    sopts.host = "127.0.0.1";
    sopts.port = 0;
    sopts.io_threads = 2;
    server = std::make_unique<net::TcpServer>(*broker, *pump, sopts);
    server->start();
    port = server->port();
    std::cout << "started in-process server on port " << port << "\n";
  }

  client::TcpClient sub, pub;
  sub.connect(host, port, "bench-sub");
  pub.connect(host, port, "bench-pub");
  const TopicId id = pub.create_topic("tcp-bench");
  // Blocking subscription so the benchmark measures delivery, not drops.
  sub.subscribe(id, "bench", 1 << 18, /*policy=*/1);

  LatencyHistogram latency;
  std::atomic<std::uint64_t> received{0};
  std::thread receiver([&] {
    while (received.load(std::memory_order_relaxed) < messages) {
      auto m = sub.receive(5000ms);
      if (!m) {
        std::cerr << "receiver timed out after " << received.load() << " messages\n";
        return;
      }
      latency.record(now_ns() - m->publish_ts_ns);
      received.fetch_add(1, std::memory_order_relaxed);
    }
  });

  const std::string payload(std::min(payload_size, kMaxPayload), 'x');
  const auto start = now_ns();
  for (std::uint64_t i = 0; i < messages; ++i) pub.publish(id, payload);
  pub.flush();
  pub.ping();  // server has processed every publish once this returns
  const double pub_secs = static_cast<double>(now_ns() - start) / 1e9;
  receiver.join();
  const double total_secs = static_cast<double>(now_ns() - start) / 1e9;

  bench::print_throughput("tcp publish", messages, pub_secs);
  bench::print_throughput("tcp end-to-end", received.load(), total_secs);
  bench::print_latency("tcp publish->receive", latency);

  if (server) server->stop();
  if (pump) pump->stop();
  return 0;
}
