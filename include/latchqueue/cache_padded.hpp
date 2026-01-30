#pragma once

#include <utility>

#include "latchqueue/cpu.hpp"

namespace latchqueue {

// Wraps a value so that it occupies its own cache line(s). Used for the head
// and tail counters of the ring buffers so producers and consumers never
// invalidate each other's lines.
template <typename T>
struct alignas(kCacheLine) CachePadded {
  T value;

  CachePadded() = default;
  template <typename... Args>
  explicit CachePadded(Args&&... args) : value(std::forward<Args>(args)...) {}

  T& operator*() noexcept { return value; }
  const T& operator*() const noexcept { return value; }
  T* operator->() noexcept { return &value; }
  const T* operator->() const noexcept { return &value; }

 private:
  // Ensure trailing padding even when sizeof(T) is an exact multiple.
  char pad_[kCacheLine - (sizeof(T) % kCacheLine == 0 ? kCacheLine : sizeof(T) % kCacheLine)]{};
};

}  // namespace latchqueue
