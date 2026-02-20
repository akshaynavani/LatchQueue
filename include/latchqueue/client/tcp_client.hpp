#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/asio.hpp>

#include "latchqueue/net/protocol.hpp"

namespace latchqueue::client {

class LatchQueueError : public std::runtime_error {
 public:
  LatchQueueError(net::ErrorCode code, const std::string& what)
      : std::runtime_error(what), code_(code) {}
  net::ErrorCode code() const noexcept { return code_; }

 private:
  net::ErrorCode code_;
};

struct ReceivedMessage {
  std::uint32_t subscription = 0;
  TopicId topic = 0;
  std::uint64_t sequence = 0;
  std::uint64_t publish_ts_ns = 0;
  std::string payload;
};

// Minimal synchronous client for the binary TCP protocol. Not thread-safe:
// use one client per thread. Request/response calls block until the matching
// ACK/ERROR arrives; MESSAGE frames that arrive meanwhile are buffered and
// returned by receive().
class TcpClient {
 public:
  TcpClient();
  ~TcpClient();

  TcpClient(const TcpClient&) = delete;
  TcpClient& operator=(const TcpClient&) = delete;

  void connect(const std::string& host, std::uint16_t port,
               std::string_view client_name = "latchqueue-cpp");
  void close();
  bool connected() const noexcept { return socket_.is_open(); }

  TopicId create_topic(std::string_view name);

  // Publishes and waits for the broker's ACK; returns the sequence number.
  std::uint64_t publish_sync(TopicId topic, std::string_view payload);

  // Fire-and-forget publish into a local buffer; sent on flush() or when the
  // buffer exceeds `auto_flush_bytes`. This is the high-throughput path.
  void publish(TopicId topic, std::string_view payload);
  void flush();
  void set_auto_flush_bytes(std::size_t n) noexcept { auto_flush_bytes_ = n; }

  // policy: 0 = drop newest when full, 1 = block publishers.
  std::uint32_t subscribe(TopicId topic, std::string_view subscription_name,
                          std::uint32_t capacity = 0, std::uint8_t policy = 0);
  void unsubscribe(std::uint32_t handle);

  // Round-trip time of a PING, in nanoseconds.
  std::uint64_t ping();

  // Returns the next delivered message. timeout < 0 blocks indefinitely,
  // timeout == 0 polls without waiting.
  std::optional<ReceivedMessage> receive(
      std::chrono::milliseconds timeout = std::chrono::milliseconds(-1));

 private:
  std::uint32_t next_request_id() noexcept { return ++request_id_; }

  // Sends `frame` (after flushing pending publishes) and waits for the
  // response with `req`. Returns the ACK value or throws.
  std::uint64_t request(std::string&& frame, std::uint32_t req);

  // Reads one complete frame from the socket into body_buf_.
  net::FrameView read_frame();
  void buffer_message(const net::FrameView& f);
  [[noreturn]] void throw_error(const net::FrameView& f);

  boost::asio::io_context io_;
  boost::asio::ip::tcp::socket socket_;
  std::uint32_t request_id_ = 0;
  std::string out_buf_;
  std::size_t auto_flush_bytes_ = 64 * 1024;
  std::string body_buf_;
  std::deque<ReceivedMessage> inbox_;
};

}  // namespace latchqueue::client
