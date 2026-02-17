#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <boost/asio.hpp>

#include "latchqueue/broker.hpp"
#include "latchqueue/net/delivery_pump.hpp"
#include "latchqueue/net/protocol.hpp"

namespace latchqueue::net {

// One client connection. The socket is bound to a strand, so every read
// handler, request handler and write completion for this session runs
// serially without locks. send() may be called from any thread (delivery
// pump threads use it); it appends to a double-buffered outbound queue and
// posts a write onto the strand when none is in flight.
class Session : public std::enable_shared_from_this<Session> {
 public:
  using tcp = boost::asio::ip::tcp;

  Session(tcp::socket socket, Broker& broker, DeliveryPump& pump, std::uint64_t id);
  ~Session();

  void start();
  void close();

  // Thread-safe. Takes ownership of one or more encoded frames.
  void send(std::string&& frames);

  bool closed() const noexcept { return closed_.load(std::memory_order_acquire); }
  std::size_t backlog_bytes() const noexcept {
    return backlog_.load(std::memory_order_relaxed);
  }
  std::uint64_t id() const noexcept { return id_; }

 private:
  void do_read_length();
  void do_read_body(std::uint32_t len);
  void handle_frame(const FrameView& frame);

  void on_hello(const FrameView& f);
  void on_create_topic(const FrameView& f);
  void on_publish(const FrameView& f);
  void on_subscribe(const FrameView& f);
  void on_unsubscribe(const FrameView& f);

  void reply_ack(std::uint32_t req, std::uint64_t value);
  void reply_error(std::uint32_t req, ErrorCode code, std::string_view message = {});

  std::shared_ptr<Topic> resolve_topic(TopicId id);

  void do_write();
  void shutdown_on_strand();

  tcp::socket socket_;
  Broker& broker_;
  DeliveryPump& pump_;
  const std::uint64_t id_;

  // Read state (strand only).
  std::array<unsigned char, kLengthPrefix> len_buf_{};
  std::string body_buf_;
  std::unordered_map<TopicId, std::shared_ptr<Topic>> topic_cache_;
  std::unordered_map<std::uint32_t, std::shared_ptr<Binding>> bindings_;
  std::uint32_t next_handle_ = 1;

  // Outbound state.
  std::mutex out_mutex_;
  std::string pending_;   // guarded by out_mutex_
  std::string inflight_;  // owned by the strand while writing_
  bool writing_ = false;  // guarded by out_mutex_
  std::atomic<std::size_t> backlog_{0};

  std::atomic<bool> closed_{false};
};

}  // namespace latchqueue::net
