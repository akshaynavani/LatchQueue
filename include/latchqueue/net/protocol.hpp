#pragma once

// LatchQueue binary wire protocol (little-endian).
//
//   +-----------+---------+---------------+------------------+
//   | u32 len   | u8 type | u32 request_id| body (len-5 B)   |
//   +-----------+---------+---------------+------------------+
//
// `len` counts everything after itself (type + request_id + body).
//
// Client -> server
//   HELLO        body: u16 version, str client_name
//   CREATE_TOPIC body: str name                         -> ACK(value=topic_id)
//   PUBLISH      body: u32 topic_id, u8 want_ack, bytes -> ACK(value=sequence) if want_ack
//   SUBSCRIBE    body: u32 topic_id, u8 policy, u32 capacity, str sub_name
//                                                       -> ACK(value=sub_handle)
//   UNSUBSCRIBE  body: u32 sub_handle                   -> ACK(value=sub_handle)
//   PING         body: (empty)                          -> PONG
//
// Server -> client
//   ACK          body: u64 value
//   ERROR        body: u16 code, str message
//   MESSAGE      request_id = sub_handle;
//                body: u32 topic_id, u64 sequence, u64 publish_ts_ns, bytes
//   PONG         body: (empty)
//
// `str` = u16 length + bytes. `bytes` = remainder of the frame.

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

#include "latchqueue/message.hpp"

namespace latchqueue::net {

inline constexpr std::uint16_t kProtocolVersion = 1;
inline constexpr std::size_t kLengthPrefix = 4;
inline constexpr std::size_t kFrameHeader = 1 + 4;  // type + request_id
inline constexpr std::uint32_t kMaxFrameSize = 64 * 1024;

enum class FrameType : std::uint8_t {
  kHello = 1,
  kCreateTopic = 2,
  kPublish = 3,
  kSubscribe = 4,
  kUnsubscribe = 5,
  kPing = 6,

  kAck = 64,
  kError = 65,
  kMessage = 66,
  kPong = 67,
};

enum class ErrorCode : std::uint16_t {
  kOk = 0,
  kMalformed = 1,
  kUnknownTopic = 2,
  kInvalidName = 3,
  kTooManyTopics = 4,
  kPayloadTooLarge = 5,
  kUnknownSubscription = 6,
  kUnsupported = 7,
};

const char* to_string(FrameType t) noexcept;
const char* to_string(ErrorCode c) noexcept;

// ---------------------------------------------------------------------------
// Byte-level helpers
// ---------------------------------------------------------------------------

class Writer {
 public:
  explicit Writer(std::string& out) : out_(out) {}

  void u8(std::uint8_t v) { out_.push_back(static_cast<char>(v)); }
  void u16(std::uint16_t v) { put_le(v); }
  void u32(std::uint32_t v) { put_le(v); }
  void u64(std::uint64_t v) { put_le(v); }
  void str(std::string_view s) {
    u16(static_cast<std::uint16_t>(s.size()));
    out_.append(s.data(), s.size());
  }
  void bytes(std::string_view s) { out_.append(s.data(), s.size()); }

 private:
  template <typename U>
  void put_le(U v) {
    char buf[sizeof(U)];
    for (std::size_t i = 0; i < sizeof(U); ++i) {
      buf[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
    }
    out_.append(buf, sizeof(U));
  }

  std::string& out_;
};

class Reader {
 public:
  explicit Reader(std::string_view in) : in_(in) {}

  bool u8(std::uint8_t& v) { return get_le(v); }
  bool u16(std::uint16_t& v) { return get_le(v); }
  bool u32(std::uint32_t& v) { return get_le(v); }
  bool u64(std::uint64_t& v) { return get_le(v); }
  bool str(std::string_view& s) {
    std::uint16_t n;
    if (!u16(n) || in_.size() - pos_ < n) return false;
    s = in_.substr(pos_, n);
    pos_ += n;
    return true;
  }
  std::string_view rest() {
    auto r = in_.substr(pos_);
    pos_ = in_.size();
    return r;
  }
  std::size_t remaining() const noexcept { return in_.size() - pos_; }

 private:
  template <typename U>
  bool get_le(U& v) {
    if (in_.size() - pos_ < sizeof(U)) return false;
    U r = 0;
    for (std::size_t i = 0; i < sizeof(U); ++i) {
      r |= static_cast<U>(static_cast<unsigned char>(in_[pos_ + i])) << (8 * i);
    }
    v = r;
    pos_ += sizeof(U);
    return true;
  }

  std::string_view in_;
  std::size_t pos_ = 0;
};

// ---------------------------------------------------------------------------
// Frame building. Each encoder appends one complete frame (with length
// prefix) to `out`, so many frames can be batched into a single write.
// ---------------------------------------------------------------------------

// Starts a frame and returns the offset of its length prefix; call
// finish_frame() once the body has been written.
std::size_t begin_frame(std::string& out, FrameType type, std::uint32_t request_id);
void finish_frame(std::string& out, std::size_t frame_start);

void encode_hello(std::string& out, std::uint32_t req, std::string_view client_name);
void encode_create_topic(std::string& out, std::uint32_t req, std::string_view name);
void encode_publish(std::string& out, std::uint32_t req, TopicId topic, bool want_ack,
                    std::string_view payload);
void encode_subscribe(std::string& out, std::uint32_t req, TopicId topic, std::uint8_t policy,
                      std::uint32_t capacity, std::string_view sub_name);
void encode_unsubscribe(std::string& out, std::uint32_t req, std::uint32_t handle);
void encode_ping(std::string& out, std::uint32_t req);

void encode_ack(std::string& out, std::uint32_t req, std::uint64_t value);
void encode_error(std::string& out, std::uint32_t req, ErrorCode code, std::string_view message);
void encode_message(std::string& out, std::uint32_t sub_handle, const Message& msg);
void encode_pong(std::string& out, std::uint32_t req);

// A decoded frame view; `body` points into the caller's buffer.
struct FrameView {
  FrameType type;
  std::uint32_t request_id;
  std::string_view body;
};

// Decodes a frame from `buf` (which starts *after* the length prefix and is
// exactly `len` bytes). Returns nullopt if too short.
std::optional<FrameView> decode_frame(std::string_view buf);

// Reads the u32 length prefix from 4 bytes.
std::uint32_t decode_length(const unsigned char* p) noexcept;

// Decoded bodies --------------------------------------------------------------

struct PublishBody {
  TopicId topic;
  bool want_ack;
  std::string_view payload;
};
struct SubscribeBody {
  TopicId topic;
  std::uint8_t policy;
  std::uint32_t capacity;
  std::string_view name;
};
struct MessageBody {
  TopicId topic;
  std::uint64_t sequence;
  std::uint64_t publish_ts_ns;
  std::string_view payload;
};
struct ErrorBody {
  ErrorCode code;
  std::string_view message;
};

std::optional<PublishBody> decode_publish(std::string_view body);
std::optional<SubscribeBody> decode_subscribe(std::string_view body);
std::optional<MessageBody> decode_message(std::string_view body);
std::optional<ErrorBody> decode_error(std::string_view body);
std::optional<std::uint64_t> decode_ack(std::string_view body);

}  // namespace latchqueue::net
