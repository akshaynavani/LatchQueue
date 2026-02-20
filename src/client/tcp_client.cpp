#include "latchqueue/client/tcp_client.hpp"

#include <thread>

#include "latchqueue/clock.hpp"

namespace latchqueue::client {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using namespace latchqueue::net;

TcpClient::TcpClient() : socket_(io_) {}

TcpClient::~TcpClient() {
  try {
    if (connected()) flush();
  } catch (...) {
  }
  close();
}

void TcpClient::connect(const std::string& host, std::uint16_t port,
                        std::string_view client_name) {
  tcp::resolver resolver(io_);
  asio::connect(socket_, resolver.resolve(host, std::to_string(port)));
  socket_.set_option(tcp::no_delay(true));

  std::string frame;
  const auto req = next_request_id();
  encode_hello(frame, req, client_name);
  request(std::move(frame), req);
}

void TcpClient::close() {
  boost::system::error_code ec;
  socket_.shutdown(tcp::socket::shutdown_both, ec);
  socket_.close(ec);
}

TopicId TcpClient::create_topic(std::string_view name) {
  std::string frame;
  const auto req = next_request_id();
  encode_create_topic(frame, req, name);
  return static_cast<TopicId>(request(std::move(frame), req));
}

std::uint64_t TcpClient::publish_sync(TopicId topic, std::string_view payload) {
  std::string frame;
  const auto req = next_request_id();
  encode_publish(frame, req, topic, /*want_ack=*/true, payload);
  return request(std::move(frame), req);
}

void TcpClient::publish(TopicId topic, std::string_view payload) {
  encode_publish(out_buf_, 0, topic, /*want_ack=*/false, payload);
  if (out_buf_.size() >= auto_flush_bytes_) flush();
}

void TcpClient::flush() {
  if (out_buf_.empty()) return;
  asio::write(socket_, asio::buffer(out_buf_));
  out_buf_.clear();
}

std::uint32_t TcpClient::subscribe(TopicId topic, std::string_view subscription_name,
                                   std::uint32_t capacity, std::uint8_t policy) {
  std::string frame;
  const auto req = next_request_id();
  encode_subscribe(frame, req, topic, policy, capacity, subscription_name);
  return static_cast<std::uint32_t>(request(std::move(frame), req));
}

void TcpClient::unsubscribe(std::uint32_t handle) {
  std::string frame;
  const auto req = next_request_id();
  encode_unsubscribe(frame, req, handle);
  request(std::move(frame), req);
}

std::uint64_t TcpClient::ping() {
  std::string frame;
  const auto req = next_request_id();
  encode_ping(frame, req);
  flush();
  const auto start = now_ns();
  asio::write(socket_, asio::buffer(frame));
  for (;;) {
    const FrameView f = read_frame();
    if (f.type == FrameType::kPong && f.request_id == req) return now_ns() - start;
    if (f.type == FrameType::kMessage) {
      buffer_message(f);
    } else if (f.type == FrameType::kError && f.request_id == req) {
      throw_error(f);
    }
  }
}

std::optional<ReceivedMessage> TcpClient::receive(std::chrono::milliseconds timeout) {
  flush();
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    if (!inbox_.empty()) {
      ReceivedMessage m = std::move(inbox_.front());
      inbox_.pop_front();
      return m;
    }
    if (timeout.count() >= 0 && socket_.available() == 0) {
      if (std::chrono::steady_clock::now() >= deadline) return std::nullopt;
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      continue;
    }
    const FrameView f = read_frame();
    if (f.type == FrameType::kMessage) {
      buffer_message(f);
    } else if (f.type == FrameType::kError) {
      // Errors for fire-and-forget publishes (request_id 0) surface here.
      throw_error(f);
    }
    // Stray ACK/PONG frames are ignored.
  }
}

std::uint64_t TcpClient::request(std::string&& frame, std::uint32_t req) {
  out_buf_.append(frame);
  flush();
  for (;;) {
    const FrameView f = read_frame();
    if (f.type == FrameType::kMessage) {
      buffer_message(f);
      continue;
    }
    if (f.request_id != req) {
      if (f.type == FrameType::kError) throw_error(f);  // async publish error
      continue;
    }
    if (f.type == FrameType::kError) throw_error(f);
    if (f.type == FrameType::kAck) {
      auto v = decode_ack(f.body);
      if (!v) throw LatchQueueError(ErrorCode::kMalformed, "malformed ACK");
      return *v;
    }
  }
}

FrameView TcpClient::read_frame() {
  unsigned char len_buf[kLengthPrefix];
  asio::read(socket_, asio::buffer(len_buf));
  const std::uint32_t len = decode_length(len_buf);
  if (len < kFrameHeader || len > kMaxFrameSize) {
    close();
    throw LatchQueueError(ErrorCode::kMalformed, "invalid frame length from server");
  }
  body_buf_.resize(len);
  asio::read(socket_, asio::buffer(body_buf_.data(), len));
  auto f = decode_frame(body_buf_);
  if (!f) throw LatchQueueError(ErrorCode::kMalformed, "malformed frame from server");
  return *f;
}

void TcpClient::buffer_message(const FrameView& f) {
  auto body = decode_message(f.body);
  if (!body) return;
  ReceivedMessage m;
  m.subscription = f.request_id;
  m.topic = body->topic;
  m.sequence = body->sequence;
  m.publish_ts_ns = body->publish_ts_ns;
  m.payload.assign(body->payload.data(), body->payload.size());
  inbox_.push_back(std::move(m));
}

void TcpClient::throw_error(const FrameView& f) {
  auto e = decode_error(f.body);
  if (!e) throw LatchQueueError(ErrorCode::kMalformed, "malformed ERROR frame");
  throw LatchQueueError(e->code, std::string(to_string(e->code)) + ": " +
                                     std::string(e->message));
}

}  // namespace latchqueue::client
