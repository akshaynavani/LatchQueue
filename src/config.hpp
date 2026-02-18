#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace latchqueue {

struct ServerConfig {
  std::string tcp_host = "0.0.0.0";
  std::uint16_t tcp_port = 7400;
  std::string grpc_address = "0.0.0.0:7401";
  bool grpc_enabled = true;
  std::size_t io_threads = 2;
  std::size_t pump_threads = 1;
  std::size_t queue_capacity = 1 << 16;
  std::size_t max_topics = 4096;
  unsigned stats_interval_s = 10;  // 0 disables periodic stats logging
  std::string preload_topics;      // comma-separated topic names
};

// Builds config from defaults, then LQ_* environment variables, then CLI
// flags (highest precedence). On --help or a parse error, prints usage and
// returns nullopt; `exit_code` is set accordingly.
std::optional<ServerConfig> load_config(int argc, char** argv, int& exit_code);

}  // namespace latchqueue
