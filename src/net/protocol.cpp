#include "latchqueue/net/protocol.hpp"

namespace latchqueue::net {

const char* to_string(FrameType t) noexcept {
  switch (t) {
    case FrameType::kHello: return "HELLO";
    case FrameType::kCreateTopic: return "CREATE_TOPIC";
    case FrameType::kPublish: return "PUBLISH";
    case FrameType::kSubscribe: return "SUBSCRIBE";
    case FrameType::kUnsubscribe: return "UNSUBSCRIBE";
    case FrameType::kPing: return "PING";
    case FrameType::kAck: return "ACK";
    case FrameType::kError: return "ERROR";
    case FrameType::kMessage: return "MESSAGE";
    case FrameType::kPong: return "PONG";
  }
  return "UNKNOWN";
}

const char* to_string(ErrorCode c) noexcept {
  switch (c) {
    case ErrorCode::kOk: return "ok";
    case ErrorCode::kMalformed: return "malformed frame";
    case ErrorCode::kUnknownTopic: return "unknown topic";
    case ErrorCode::kInvalidName: return "invalid name";
    case ErrorCode::kTooManyTopics: return "too many topics";
    case ErrorCode::kPayloadTooLarge: return "payload too large";
    case ErrorCode::kUnknownSubscription: return "unknown subscription";
    case ErrorCode::kUnsupported: return "unsupported";
  }
  return "unknown error";
}

std::size_t begin_frame(std::string& out, FrameType type, std::uint32_t request_id) {
  const std::size_t start = out.size();
  Writer w(out);
  w.u32(0);  // placeholder length
  w.u8(static_cast<std::uint8_t>(type));
  w.u32(request_id);
  return start;
}

void finish_frame(std::string& out, std::size_t frame_start) {
  const auto len = static_cast<std::uint32_t>(out.size() - frame_start - kLengthPrefix);
  for (std::size_t i = 0; i < kLengthPrefix; ++i) {
    out[frame_start + i] = static_cast<char>((len >> (8 * i)) & 0xFF);
  }
}

void encode_hello(std::string& out, std::uint32_t req, std::string_view client_name) {
  const auto start = begin_frame(out, FrameType::kHello, req);
  Writer w(out);
  w.u16(kProtocolVersion);
  w.str(client_name);
  finish_frame(out, start);
}

void encode_create_topic(std::string& out, std::uint32_t req, std::string_view name) {
  const auto start = begin_frame(out, FrameType::kCreateTopic, req);
  Writer(out).str(name);
  finish_frame(out, start);
}

void encode_publish(std::string& out, std::uint32_t req, TopicId topic, bool want_ack,
                    std::string_view payload) {
  const auto start = begin_frame(out, FrameType::kPublish, req);
  Writer w(out);
  w.u32(topic);
  w.u8(want_ack ? 1 : 0);
  w.bytes(payload);
  finish_frame(out, start);
}

void encode_subscribe(std::string& out, std::uint32_t req, TopicId topic, std::uint8_t policy,
                      std::uint32_t capacity, std::string_view sub_name) {
  const auto start = begin_frame(out, FrameType::kSubscribe, req);
  Writer w(out);
  w.u32(topic);
  w.u8(policy);
  w.u32(capacity);
  w.str(sub_name);
  finish_frame(out, start);
}

void encode_unsubscribe(std::string& out, std::uint32_t req, std::uint32_t handle) {
  const auto start = begin_frame(out, FrameType::kUnsubscribe, req);
  Writer(out).u32(handle);
  finish_frame(out, start);
}

void encode_ping(std::string& out, std::uint32_t req) {
  finish_frame(out, begin_frame(out, FrameType::kPing, req));
}

void encode_ack(std::string& out, std::uint32_t req, std::uint64_t value) {
  const auto start = begin_frame(out, FrameType::kAck, req);
  Writer(out).u64(value);
  finish_frame(out, start);
}

void encode_error(std::string& out, std::uint32_t req, ErrorCode code, std::string_view message) {
  const auto start = begin_frame(out, FrameType::kError, req);
  Writer w(out);
  w.u16(static_cast<std::uint16_t>(code));
  w.str(message);
  finish_frame(out, start);
}

void encode_message(std::string& out, std::uint32_t sub_handle, const Message& msg) {
  const auto start = begin_frame(out, FrameType::kMessage, sub_handle);
  Writer w(out);
  w.u32(msg.topic);
  w.u64(msg.header.sequence);
  w.u64(msg.header.publish_ts_ns);
  w.bytes(msg.view());
  finish_frame(out, start);
}

void encode_pong(std::string& out, std::uint32_t req) {
  finish_frame(out, begin_frame(out, FrameType::kPong, req));
}

std::uint32_t decode_length(const unsigned char* p) noexcept {
  return static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
         (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
}

std::optional<FrameView> decode_frame(std::string_view buf) {
  Reader r(buf);
  std::uint8_t type;
  std::uint32_t req;
  if (!r.u8(type) || !r.u32(req)) return std::nullopt;
  return FrameView{static_cast<FrameType>(type), req, r.rest()};
}

std::optional<PublishBody> decode_publish(std::string_view body) {
  Reader r(body);
  PublishBody b{};
  std::uint8_t ack;
  if (!r.u32(b.topic) || !r.u8(ack)) return std::nullopt;
  b.want_ack = ack != 0;
  b.payload = r.rest();
  return b;
}

std::optional<SubscribeBody> decode_subscribe(std::string_view body) {
  Reader r(body);
  SubscribeBody b{};
  if (!r.u32(b.topic) || !r.u8(b.policy) || !r.u32(b.capacity) || !r.str(b.name)) {
    return std::nullopt;
  }
  return b;
}

std::optional<MessageBody> decode_message(std::string_view body) {
  Reader r(body);
  MessageBody b{};
  if (!r.u32(b.topic) || !r.u64(b.sequence) || !r.u64(b.publish_ts_ns)) return std::nullopt;
  b.payload = r.rest();
  return b;
}

std::optional<ErrorBody> decode_error(std::string_view body) {
  Reader r(body);
  std::uint16_t code;
  ErrorBody b{};
  if (!r.u16(code) || !r.str(b.message)) return std::nullopt;
  b.code = static_cast<ErrorCode>(code);
  return b;
}

std::optional<std::uint64_t> decode_ack(std::string_view body) {
  Reader r(body);
  std::uint64_t v;
  if (!r.u64(v)) return std::nullopt;
  return v;
}

}  // namespace latchqueue::net
