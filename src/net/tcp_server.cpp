#include "latchqueue/net/tcp_server.hpp"

#include <iostream>

namespace latchqueue::net {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

TcpServer::TcpServer(Broker& broker, DeliveryPump& pump, TcpServerOptions options)
    : broker_(broker), pump_(pump), options_(std::move(options)), acceptor_(io_) {}

TcpServer::~TcpServer() { stop(); }

void TcpServer::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) return;

  const tcp::endpoint endpoint(asio::ip::make_address(options_.host), options_.port);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(asio::socket_base::max_listen_connections);
  bound_port_ = acceptor_.local_endpoint().port();

  work_.emplace(asio::make_work_guard(io_));
  do_accept();

  const std::size_t n = options_.io_threads == 0 ? 1 : options_.io_threads;
  threads_.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    threads_.emplace_back([this] { io_.run(); });
  }
}

void TcpServer::do_accept() {
  acceptor_.async_accept(asio::make_strand(io_), [this](boost::system::error_code ec,
                                                         tcp::socket socket) {
    if (ec) {
      if (ec != asio::error::operation_aborted) {
        std::cerr << "[tcp] accept error: " << ec.message() << "\n";
      }
      if (!running_.load(std::memory_order_acquire)) return;
    } else {
      std::shared_ptr<Session> session;
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        const std::uint64_t id = next_session_id_++;
        session = std::make_shared<Session>(std::move(socket), broker_, pump_, id);
        // Opportunistically prune dead sessions.
        for (auto it = sessions_.begin(); it != sessions_.end();) {
          it = it->second.expired() ? sessions_.erase(it) : std::next(it);
        }
        sessions_.emplace(id, session);
      }
      session->start();
    }
    if (running_.load(std::memory_order_acquire)) do_accept();
  });
}

std::size_t TcpServer::session_count() const {
  std::lock_guard<std::mutex> lock(sessions_mutex_);
  std::size_t n = 0;
  for (const auto& [_, w] : sessions_) {
    if (auto s = w.lock(); s && !s->closed()) ++n;
  }
  return n;
}

void TcpServer::stop() {
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) return;

  asio::post(io_, [this] {
    boost::system::error_code ec;
    acceptor_.close(ec);
  });
  {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    for (auto& [_, w] : sessions_) {
      if (auto s = w.lock()) s->close();
    }
    sessions_.clear();
  }
  work_.reset();
  // Let in-flight handlers (socket closes) drain, then stop the loop.
  for (auto& t : threads_) {
    if (t.joinable()) t.join();
  }
  threads_.clear();
}

}  // namespace latchqueue::net
