#include "config.hpp"

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace latchqueue {

namespace {

constexpr const char* kUsage = R"(latchqueue_server - lock-free low-latency message broker

Usage: latchqueue_server [options]

  --tcp-host HOST          bind address for the binary TCP protocol  [LQ_TCP_HOST, 0.0.0.0]
  --tcp-port PORT          TCP data-plane port                       [LQ_TCP_PORT, 7400]
  --grpc-address ADDR      gRPC listen address host:port             [LQ_GRPC_ADDRESS, 0.0.0.0:7401]
  --no-grpc                disable the gRPC control plane            [LQ_GRPC_ENABLED=0]
  --io-threads N           Asio io_context threads                   [LQ_IO_THREADS, 2]
  --pump-threads N         delivery pump threads                     [LQ_PUMP_THREADS, 1]
  --queue-capacity N       default per-subscription queue slots      [LQ_QUEUE_CAPACITY, 65536]
  --max-topics N           topic limit                               [LQ_MAX_TOPICS, 4096]
  --stats-interval SEC     periodic stats log, 0 = off               [LQ_STATS_INTERVAL, 10]
  --topics a,b,c           topics to create at startup               [LQ_TOPICS]
  -h, --help               show this help
)";

unsigned long long parse_uint(std::string_view name, const std::string& v) {
  try {
    std::size_t idx = 0;
    const auto n = std::stoull(v, &idx);
    if (idx != v.size()) throw std::invalid_argument("trailing characters");
    return n;
  } catch (const std::exception&) {
    throw std::invalid_argument(std::string(name) + ": expected a non-negative integer, got '" +
                                v + "'");
  }
}

bool parse_bool(const std::string& v) {
  return !(v == "0" || v == "false" || v == "no" || v == "off");
}

const char* env(const char* name) {
  const char* v = std::getenv(name);
  return (v && *v) ? v : nullptr;
}

void apply_env(ServerConfig& c) {
  if (auto v = env("LQ_TCP_HOST")) c.tcp_host = v;
  if (auto v = env("LQ_TCP_PORT")) c.tcp_port = static_cast<std::uint16_t>(parse_uint("LQ_TCP_PORT", v));
  if (auto v = env("LQ_GRPC_ADDRESS")) c.grpc_address = v;
  if (auto v = env("LQ_GRPC_ENABLED")) c.grpc_enabled = parse_bool(v);
  if (auto v = env("LQ_IO_THREADS")) c.io_threads = parse_uint("LQ_IO_THREADS", v);
  if (auto v = env("LQ_PUMP_THREADS")) c.pump_threads = parse_uint("LQ_PUMP_THREADS", v);
  if (auto v = env("LQ_QUEUE_CAPACITY")) c.queue_capacity = parse_uint("LQ_QUEUE_CAPACITY", v);
  if (auto v = env("LQ_MAX_TOPICS")) c.max_topics = parse_uint("LQ_MAX_TOPICS", v);
  if (auto v = env("LQ_STATS_INTERVAL")) c.stats_interval_s = static_cast<unsigned>(parse_uint("LQ_STATS_INTERVAL", v));
  if (auto v = env("LQ_TOPICS")) c.preload_topics = v;
}

}  // namespace

std::optional<ServerConfig> load_config(int argc, char** argv, int& exit_code) {
  ServerConfig c;
  try {
    apply_env(c);
    for (int i = 1; i < argc; ++i) {
      const std::string_view arg = argv[i];
      auto next = [&]() -> std::string {
        if (i + 1 >= argc) throw std::invalid_argument(std::string(arg) + " requires a value");
        return argv[++i];
      };
      if (arg == "-h" || arg == "--help") {
        std::cout << kUsage;
        exit_code = 0;
        return std::nullopt;
      } else if (arg == "--tcp-host") {
        c.tcp_host = next();
      } else if (arg == "--tcp-port") {
        const auto p = parse_uint(arg, next());
        if (p > 65535) throw std::invalid_argument("--tcp-port out of range");
        c.tcp_port = static_cast<std::uint16_t>(p);
      } else if (arg == "--grpc-address") {
        c.grpc_address = next();
      } else if (arg == "--no-grpc") {
        c.grpc_enabled = false;
      } else if (arg == "--io-threads") {
        c.io_threads = parse_uint(arg, next());
      } else if (arg == "--pump-threads") {
        c.pump_threads = parse_uint(arg, next());
      } else if (arg == "--queue-capacity") {
        c.queue_capacity = parse_uint(arg, next());
      } else if (arg == "--max-topics") {
        c.max_topics = parse_uint(arg, next());
      } else if (arg == "--stats-interval") {
        c.stats_interval_s = static_cast<unsigned>(parse_uint(arg, next()));
      } else if (arg == "--topics") {
        c.preload_topics = next();
      } else {
        throw std::invalid_argument("unknown option: " + std::string(arg));
      }
    }
    if (c.io_threads == 0 || c.pump_threads == 0) {
      throw std::invalid_argument("thread counts must be >= 1");
    }
    if (c.queue_capacity < 2) throw std::invalid_argument("queue capacity must be >= 2");
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n\n" << kUsage;
    exit_code = 2;
    return std::nullopt;
  }
  return c;
}

}  // namespace latchqueue
