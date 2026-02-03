#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace latchqueue {

// Maximum payload carried inline. Message is sized to exactly four 64-byte
// cache lines (24B header + 232B payload) so copies are a few vector moves
// and the queue never allocates.
inline constexpr std::size_t kMaxPayload = 232;

using TopicId = std::uint32_t;

struct MessageHeader {
  std::uint64_t sequence = 0;      // per-topic, assigned by the broker
  std::uint64_t publish_ts_ns = 0; // monotonic timestamp at publish
};

// Trivially copyable message record stored by value in the queues.
struct Message {
  MessageHeader header;
  TopicId topic = 0;
  std::uint16_t size = 0;
  std::uint16_t flags = 0;
  char payload[kMaxPayload];

  // Copies at most kMaxPayload bytes; returns false if `data` was truncated.
  bool set_payload(const void* data, std::size_t len) noexcept {
    const std::size_t n = std::min(len, kMaxPayload);
    std::memcpy(payload, data, n);
    size = static_cast<std::uint16_t>(n);
    return n == len;
  }

  bool set_payload(std::string_view sv) noexcept { return set_payload(sv.data(), sv.size()); }

  std::string_view view() const noexcept { return {payload, size}; }
};

static_assert(std::is_trivially_copyable_v<Message>, "Message must be trivially copyable");
static_assert(sizeof(Message) == 256, "unexpected Message layout");

}  // namespace latchqueue
