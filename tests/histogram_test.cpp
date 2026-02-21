#include "latchqueue/histogram.hpp"

#include <gtest/gtest.h>

using latchqueue::LatencyHistogram;

TEST(LatencyHistogram, EmptyReturnsZero) {
  LatencyHistogram h;
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.percentile(0.99), 0u);
  EXPECT_EQ(h.min(), 0u);
}

TEST(LatencyHistogram, BucketBoundsContainValue) {
  for (std::uint64_t v : {0ull, 1ull, 15ull, 16ull, 17ull, 100ull, 1000ull, 123456ull,
                          987654321ull, 1ull << 40}) {
    const unsigned idx = LatencyHistogram::bucket_index(v);
    EXPECT_GE(LatencyHistogram::bucket_upper_bound(idx), v) << v;
    if (idx > 0) EXPECT_LT(LatencyHistogram::bucket_upper_bound(idx - 1), v) << v;
  }
}

TEST(LatencyHistogram, PercentilesWithinRelativeError) {
  LatencyHistogram h;
  for (std::uint64_t v = 1; v <= 10'000; ++v) h.record(v);
  EXPECT_EQ(h.count(), 10'000u);
  EXPECT_EQ(h.min(), 1u);
  EXPECT_EQ(h.max(), 10'000u);
  EXPECT_NEAR(h.mean(), 5000.5, 0.01);
  const auto p50 = static_cast<double>(h.percentile(0.5));
  const auto p99 = static_cast<double>(h.percentile(0.99));
  EXPECT_NEAR(p50, 5000, 5000 * 0.07);
  EXPECT_NEAR(p99, 9900, 9900 * 0.07);
  EXPECT_EQ(h.percentile(1.0), 10'000u);
}

TEST(LatencyHistogram, MergeAndReset) {
  LatencyHistogram a, b;
  a.record(10);
  b.record(1000);
  a.merge(b);
  EXPECT_EQ(a.count(), 2u);
  EXPECT_EQ(a.max(), 1000u);
  EXPECT_EQ(a.min(), 10u);
  a.reset();
  EXPECT_EQ(a.count(), 0u);
}
