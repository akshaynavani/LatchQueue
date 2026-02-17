#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio.hpp>

#include "latchqueue/broker.hpp"
#include "latchqueue/net/delivery_pump.hpp"
#include "latchqueue/net/session.hpp"

namespace latchqueue::net {

struct TcpServerOptions {
  std::string host = "0.0.0.0";
  std::uint16_t port = 7400;  // 0 = pick an ephemeral port
  std::size_t io_threads = 2;
};

// Accepts client connections and runs their sessions on a pool of threads
// sharing one io_context. Each accepted socket gets its own strand.
class TcpServer {
 public:
  TcpServer(Broker& broker, DeliveryPump& pump, TcpServerOptions options = {});
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  void start();
  void stop();

  // Valid after start(); useful when options.port == 0.
  std::uint16_t port() const noexcept { return bound_port_; }
  std::size_t session_count() const;

 private:
  void do_accept();

  Broker& broker_;
  DeliveryPump& pump_;
  const TcpServerOptions options_;

  boost::asio::io_context io_;
  std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_;
  boost::asio::ip::tcp::acceptor acceptor_;
  std::vector<std::thread> threads_;
  std::uint16_t bound_port_ = 0;
  std::atomic<bool> running_{false};

  mutable std::mutex sessions_mutex_;
  std::unordered_map<std::uint64_t, std::weak_ptr<Session>> sessions_;
  std::uint64_t next_session_id_ = 1;
};

}  // namespace latchqueue::net
