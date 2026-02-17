#include "latchqueue/net/session.hpp"

namespace latchqueue::net {

namespace asio = boost::asio;

Session::Session(tcp::socket socket, Broker& broker, DeliveryPump& pump, std::uint64_t id)
    : socket_(std::move(socket)), broker_(broker), pump_(pump), id_(id) {
  boost::system::error_code ec;
  socket_.set_option(tcp::no_delay(true), ec);
}

Session::~Session() {
  for (auto& [_, b] : bindings_) b->active.store(false, std::memory_order_release);
}

void Session::start() {
  asio::post(socket_.get_executor(), [self = shared_from_this()] { self->do_read_length(); });
}

void Session::close() {
  bool expected = false;
  if (!closed_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return;
  asio::post(socket_.get_executor(), [self = shared_from_this()] { self->shutdown_on_strand(); });
}

void Session::shutdown_on_strand() {
  for (auto& [_, b] : bindings_) b->active.store(false, std::memory_order_release);
  bindings_.clear();
  boost::system::error_code ec;
  socket_.shutdown(tcp::socket::shutdown_both, ec);
  socket_.close(ec);
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

void Session::do_read_length() {
  asio::async_read(socket_, asio::buffer(len_buf_),
                   [self = shared_from_this()](boost::system::error_code ec, std::size_t) {
                     if (ec) {
                       self->close();
                       return;
                     }
                     const std::uint32_t len = decode_length(self->len_buf_.data());
                     if (len < kFrameHeader || len > kMaxFrameSize) {
                       self->close();
                       return;
                     }
                     self->do_read_body(len);
                   });
}

void Session::do_read_body(std::uint32_t len) {
  body_buf_.resize(len);
  asio::async_read(socket_, asio::buffer(body_buf_.data(), len),
                   [self = shared_from_this()](boost::system::error_code ec, std::size_t) {
                     if (ec) {
                       self->close();
                       return;
                     }
                     if (auto frame = decode_frame(self->body_buf_)) {
                       self->handle_frame(*frame);
                     } else {
                       self->reply_error(0, ErrorCode::kMalformed);
                     }
                     if (!self->closed()) self->do_read_length();
                   });
}

void Session::handle_frame(const FrameView& f) {
  switch (f.type) {
    case FrameType::kHello: on_hello(f); break;
    case FrameType::kCreateTopic: on_create_topic(f); break;
    case FrameType::kPublish: on_publish(f); break;
    case FrameType::kSubscribe: on_subscribe(f); break;
    case FrameType::kUnsubscribe: on_unsubscribe(f); break;
    case FrameType::kPing: {
      std::string out;
      encode_pong(out, f.request_id);
      send(std::move(out));
      break;
    }
    default:
      reply_error(f.request_id, ErrorCode::kUnsupported, to_string(f.type));
      break;
  }
}

// ---------------------------------------------------------------------------
// Request handlers (strand)
// ---------------------------------------------------------------------------

void Session::on_hello(const FrameView& f) {
  Reader r(f.body);
  std::uint16_t version;
  if (!r.u16(version)) return reply_error(f.request_id, ErrorCode::kMalformed);
  if (version != kProtocolVersion) {
    return reply_error(f.request_id, ErrorCode::kUnsupported, "protocol version mismatch");
  }
  reply_ack(f.request_id, kProtocolVersion);
}

void Session::on_create_topic(const FrameView& f) {
  Reader r(f.body);
  std::string_view name;
  if (!r.str(name)) return reply_error(f.request_id, ErrorCode::kMalformed);
  const std::string name_str(name);
  if (!Broker::valid_topic_name(name_str)) {
    return reply_error(f.request_id, ErrorCode::kInvalidName);
  }
  auto topic = broker_.create_topic(name_str);
  if (!topic) return reply_error(f.request_id, ErrorCode::kTooManyTopics);
  topic_cache_[topic->id()] = topic;
  reply_ack(f.request_id, topic->id());
}

std::shared_ptr<Topic> Session::resolve_topic(TopicId id) {
  if (auto it = topic_cache_.find(id); it != topic_cache_.end()) return it->second;
  auto topic = broker_.get_topic(id);
  if (topic) topic_cache_.emplace(id, topic);
  return topic;
}

void Session::on_publish(const FrameView& f) {
  auto body = decode_publish(f.body);
  if (!body) return reply_error(f.request_id, ErrorCode::kMalformed);
  if (body->payload.size() > kMaxPayload) {
    return reply_error(f.request_id, ErrorCode::kPayloadTooLarge);
  }
  auto topic = resolve_topic(body->topic);
  if (!topic) return reply_error(f.request_id, ErrorCode::kUnknownTopic);
  const PublishResult res = topic->publish(body->payload);
  if (body->want_ack) reply_ack(f.request_id, res.sequence);
}

void Session::on_subscribe(const FrameView& f) {
  auto body = decode_subscribe(f.body);
  if (!body) return reply_error(f.request_id, ErrorCode::kMalformed);
  auto topic = resolve_topic(body->topic);
  if (!topic) return reply_error(f.request_id, ErrorCode::kUnknownTopic);
  if (body->policy > static_cast<std::uint8_t>(OverflowPolicy::kBlock)) {
    return reply_error(f.request_id, ErrorCode::kMalformed, "bad overflow policy");
  }
  const std::string name = body->name.empty() ? "default" : std::string(body->name);
  auto sub = topic->subscribe(name, body->capacity, static_cast<OverflowPolicy>(body->policy));

  const std::uint32_t handle = next_handle_++;
  auto binding = std::make_shared<Binding>(handle, std::move(sub), weak_from_this());
  bindings_.emplace(handle, binding);
  // ACK first so the client knows the handle before MESSAGE frames arrive;
  // both go through the same ordered outbound buffer.
  reply_ack(f.request_id, handle);
  pump_.add(std::move(binding));
}

void Session::on_unsubscribe(const FrameView& f) {
  Reader r(f.body);
  std::uint32_t handle;
  if (!r.u32(handle)) return reply_error(f.request_id, ErrorCode::kMalformed);
  auto it = bindings_.find(handle);
  if (it == bindings_.end()) return reply_error(f.request_id, ErrorCode::kUnknownSubscription);
  // Detach this consumer only; the subscription (consumer group) lives on.
  it->second->active.store(false, std::memory_order_release);
  bindings_.erase(it);
  reply_ack(f.request_id, handle);
}

void Session::reply_ack(std::uint32_t req, std::uint64_t value) {
  std::string out;
  encode_ack(out, req, value);
  send(std::move(out));
}

void Session::reply_error(std::uint32_t req, ErrorCode code, std::string_view message) {
  std::string out;
  encode_error(out, req, code, message.empty() ? to_string(code) : message);
  send(std::move(out));
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

void Session::send(std::string&& frames) {
  if (frames.empty() || closed()) return;
  {
    std::lock_guard<std::mutex> lock(out_mutex_);
    backlog_.fetch_add(frames.size(), std::memory_order_relaxed);
    if (pending_.empty()) {
      pending_.swap(frames);  // avoid a copy for the common single-batch case
    } else {
      pending_.append(frames);
    }
    if (writing_) return;
    writing_ = true;
  }
  asio::post(socket_.get_executor(), [self = shared_from_this()] { self->do_write(); });
}

void Session::do_write() {
  {
    std::lock_guard<std::mutex> lock(out_mutex_);
    if (pending_.empty() || closed()) {
      writing_ = false;
      return;
    }
    inflight_.swap(pending_);
    pending_.clear();
  }
  asio::async_write(socket_, asio::buffer(inflight_),
                    [self = shared_from_this()](boost::system::error_code ec, std::size_t n) {
                      self->backlog_.fetch_sub(n, std::memory_order_relaxed);
                      if (ec) {
                        self->close();
                        return;
                      }
                      self->do_write();
                    });
}

}  // namespace latchqueue::net
