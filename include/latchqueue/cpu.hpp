#pragma once

#include <cstddef>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
#include <immintrin.h>
#endif

namespace latchqueue {

// Size used for padding to avoid false sharing. 64 bytes covers x86 and most
// ARM cores; Apple M-series use 128-byte lines for some prefetch pairs, so we
// pad to 128 on aarch64 to be safe.
#if defined(__aarch64__) || defined(_M_ARM64)
inline constexpr std::size_t kCacheLine = 128;
#else
inline constexpr std::size_t kCacheLine = 64;
#endif

// Hint to the CPU that we are in a spin-wait loop.
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
  _mm_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#else
  // Fall back to a compiler barrier.
  asm volatile("" ::: "memory");
#endif
}

constexpr bool is_power_of_two(std::size_t v) noexcept {
  return v != 0 && (v & (v - 1)) == 0;
}

constexpr std::size_t next_power_of_two(std::size_t v) noexcept {
  if (v <= 1) return 1;
  --v;
  for (std::size_t shift = 1; shift < sizeof(std::size_t) * 8; shift <<= 1) {
    v |= v >> shift;
  }
  return v + 1;
}

}  // namespace latchqueue
