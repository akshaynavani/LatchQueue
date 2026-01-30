#pragma once

#include <chrono>
#include <cstdint>

namespace latchqueue {

// Monotonic nanoseconds since an unspecified epoch. steady_clock is backed by
// clock_gettime(CLOCK_MONOTONIC) / mach_absolute_time and costs ~20ns.
inline std::uint64_t now_ns() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

}  // namespace latchqueue
