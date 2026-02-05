#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <vector>

namespace latchqueue {

// Lock-free log-linear latency histogram (HDR-like).
//
// Values are bucketed by their most significant bit (the "magnitude") and then
// split linearly into kSubBuckets within each magnitude, giving roughly 6%
// relative precision across the full 64-bit range with 1 KiB of counters per
// 128 buckets. Recording is a single relaxed fetch_add.
class LatencyHistogram {
 public:
  static constexpr unsigned kSubBucketBits = 4;
  static constexpr unsigned kSubBuckets = 1u << kSubBucketBits;
  static constexpr unsigned kMagnitudes = 64;
  static constexpr unsigned kBuckets = kMagnitudes * kSubBuckets;

  void record(std::uint64_t value_ns) noexcept;

  std::uint64_t count() const noexcept;
  std::uint64_t max() const noexcept { return max_.load(std::memory_order_relaxed); }
  std::uint64_t min() const noexcept;
  double mean() const noexcept;

  // q in [0, 1]. Returns the upper bound of the bucket containing the
  // q-quantile, i.e. a conservative estimate.
  std::uint64_t percentile(double q) const noexcept;

  void reset() noexcept;

  // Combine another histogram into this one (not atomic as a whole).
  void merge(const LatencyHistogram& other) noexcept;

  static unsigned bucket_index(std::uint64_t value) noexcept;
  static std::uint64_t bucket_upper_bound(unsigned index) noexcept;

 private:
  std::array<std::atomic<std::uint64_t>, kBuckets> buckets_{};
  std::atomic<std::uint64_t> total_{0};
  std::atomic<std::uint64_t> sum_{0};
  std::atomic<std::uint64_t> max_{0};
  std::atomic<std::uint64_t> min_{~std::uint64_t{0}};
};

}  // namespace latchqueue
