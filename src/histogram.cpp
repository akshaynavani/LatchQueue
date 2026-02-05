#include "latchqueue/histogram.hpp"

#include <algorithm>
#include <cmath>

namespace latchqueue {

namespace {

inline unsigned msb_index(std::uint64_t v) noexcept {
#if defined(__GNUC__) || defined(__clang__)
  return 63u - static_cast<unsigned>(__builtin_clzll(v));
#else
  unsigned r = 0;
  while (v >>= 1) ++r;
  return r;
#endif
}

}  // namespace

unsigned LatencyHistogram::bucket_index(std::uint64_t value) noexcept {
  // Values below kSubBuckets get one bucket each (magnitude 0 region).
  if (value < kSubBuckets) return static_cast<unsigned>(value);
  const unsigned msb = msb_index(value);
  const unsigned shift = msb - kSubBucketBits;
  const unsigned sub = static_cast<unsigned>((value >> shift) & (kSubBuckets - 1));
  // magnitude for msb = kSubBucketBits starts right after the linear region.
  const unsigned magnitude = msb - kSubBucketBits + 1;
  const unsigned idx = magnitude * kSubBuckets + sub;
  return std::min(idx, kBuckets - 1);
}

std::uint64_t LatencyHistogram::bucket_upper_bound(unsigned index) noexcept {
  if (index < kSubBuckets) return index;
  const unsigned magnitude = index / kSubBuckets;
  const unsigned sub = index % kSubBuckets;
  const unsigned shift = magnitude - 1;  // == msb - kSubBucketBits
  if (shift + kSubBucketBits >= 64) return ~std::uint64_t{0};
  const std::uint64_t base = (std::uint64_t{kSubBuckets} | sub) << shift;
  return base + ((std::uint64_t{1} << shift) - 1);
}

void LatencyHistogram::record(std::uint64_t value_ns) noexcept {
  buckets_[bucket_index(value_ns)].fetch_add(1, std::memory_order_relaxed);
  total_.fetch_add(1, std::memory_order_relaxed);
  sum_.fetch_add(value_ns, std::memory_order_relaxed);

  std::uint64_t cur = max_.load(std::memory_order_relaxed);
  while (value_ns > cur &&
         !max_.compare_exchange_weak(cur, value_ns, std::memory_order_relaxed)) {
  }
  cur = min_.load(std::memory_order_relaxed);
  while (value_ns < cur &&
         !min_.compare_exchange_weak(cur, value_ns, std::memory_order_relaxed)) {
  }
}

std::uint64_t LatencyHistogram::count() const noexcept {
  return total_.load(std::memory_order_relaxed);
}

std::uint64_t LatencyHistogram::min() const noexcept {
  const std::uint64_t m = min_.load(std::memory_order_relaxed);
  return m == ~std::uint64_t{0} ? 0 : m;
}

double LatencyHistogram::mean() const noexcept {
  const std::uint64_t n = count();
  return n == 0 ? 0.0 : static_cast<double>(sum_.load(std::memory_order_relaxed)) / n;
}

std::uint64_t LatencyHistogram::percentile(double q) const noexcept {
  const std::uint64_t n = count();
  if (n == 0) return 0;
  q = std::clamp(q, 0.0, 1.0);
  const auto target = static_cast<std::uint64_t>(std::ceil(q * static_cast<double>(n)));
  std::uint64_t seen = 0;
  for (unsigned i = 0; i < kBuckets; ++i) {
    seen += buckets_[i].load(std::memory_order_relaxed);
    if (seen >= target && seen > 0) {
      return std::min(bucket_upper_bound(i), max());
    }
  }
  return max();
}

void LatencyHistogram::reset() noexcept {
  for (auto& b : buckets_) b.store(0, std::memory_order_relaxed);
  total_.store(0, std::memory_order_relaxed);
  sum_.store(0, std::memory_order_relaxed);
  max_.store(0, std::memory_order_relaxed);
  min_.store(~std::uint64_t{0}, std::memory_order_relaxed);
}

void LatencyHistogram::merge(const LatencyHistogram& other) noexcept {
  for (unsigned i = 0; i < kBuckets; ++i) {
    const auto v = other.buckets_[i].load(std::memory_order_relaxed);
    if (v) buckets_[i].fetch_add(v, std::memory_order_relaxed);
  }
  total_.fetch_add(other.total_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  sum_.fetch_add(other.sum_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  const auto omax = other.max_.load(std::memory_order_relaxed);
  std::uint64_t cur = max_.load(std::memory_order_relaxed);
  while (omax > cur && !max_.compare_exchange_weak(cur, omax, std::memory_order_relaxed)) {
  }
  const auto omin = other.min_.load(std::memory_order_relaxed);
  cur = min_.load(std::memory_order_relaxed);
  while (omin < cur && !min_.compare_exchange_weak(cur, omin, std::memory_order_relaxed)) {
  }
}

}  // namespace latchqueue
