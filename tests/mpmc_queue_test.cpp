#include "latchqueue/mpmc_queue.hpp"

#include <atomic>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using latchqueue::MpmcQueue;

TEST(MpmcQueue, RoundsCapacityUpToPowerOfTwo) {
  MpmcQueue<int> q(100);
  EXPECT_EQ(q.capacity(), 128u);
  MpmcQueue<int> tiny(0);
  EXPECT_EQ(tiny.capacity(), 2u);
}

TEST(MpmcQueue, FifoSingleThread) {
  MpmcQueue<int> q(8);
  for (int i = 0; i < 8; ++i) ASSERT_TRUE(q.try_push(i));
  EXPECT_FALSE(q.try_push(99)) << "queue should be full";
  EXPECT_EQ(q.size_approx(), 8u);
  for (int i = 0; i < 8; ++i) {
    int v = -1;
    ASSERT_TRUE(q.try_pop(v));
    EXPECT_EQ(v, i);
  }
  int v;
  EXPECT_FALSE(q.try_pop(v));
}

TEST(MpmcQueue, WrapsAroundManyLaps) {
  MpmcQueue<std::uint64_t> q(4);
  for (std::uint64_t i = 0; i < 10'000; ++i) {
    ASSERT_TRUE(q.try_push(i));
    std::uint64_t out;
    ASSERT_TRUE(q.try_pop(out));
    ASSERT_EQ(out, i);
  }
}

TEST(MpmcQueue, MoveOnlyElementsAreDestroyed) {
  auto counter = std::make_shared<int>(0);
  {
    MpmcQueue<std::shared_ptr<int>> q(4);
    q.try_push(counter);
    q.try_push(counter);
    EXPECT_EQ(counter.use_count(), 3);
  }
  EXPECT_EQ(counter.use_count(), 1) << "destructor must release remaining elements";
}

TEST(MpmcQueue, PopBulk) {
  MpmcQueue<int> q(16);
  for (int i = 0; i < 10; ++i) q.try_push(i);
  std::vector<int> out;
  EXPECT_EQ(q.try_pop_bulk(std::back_inserter(out), 4), 4u);
  EXPECT_EQ(out, (std::vector<int>{0, 1, 2, 3}));
}

// Every value pushed by any producer must be popped exactly once.
TEST(MpmcQueue, MultiProducerMultiConsumerNoLossNoDuplication) {
  constexpr int kProducers = 4;
  constexpr int kConsumers = 4;
  constexpr std::uint64_t kPerProducer = 200'000;
  MpmcQueue<std::uint64_t> q(1024);

  std::vector<std::atomic<std::uint8_t>> seen(kProducers * kPerProducer);
  std::atomic<std::uint64_t> consumed{0};
  std::atomic<std::uint64_t> sum{0};

  std::vector<std::thread> threads;
  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&, p] {
      for (std::uint64_t i = 0; i < kPerProducer; ++i) q.push(p * kPerProducer + i);
    });
  }
  for (int c = 0; c < kConsumers; ++c) {
    threads.emplace_back([&] {
      std::uint64_t v;
      while (consumed.load(std::memory_order_relaxed) < kProducers * kPerProducer) {
        if (q.try_pop(v)) {
          seen[v].fetch_add(1, std::memory_order_relaxed);
          sum.fetch_add(v, std::memory_order_relaxed);
          consumed.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& t : threads) t.join();

  const std::uint64_t n = kProducers * kPerProducer;
  EXPECT_EQ(consumed.load(), n);
  EXPECT_EQ(sum.load(), n * (n - 1) / 2);
  for (std::uint64_t i = 0; i < n; ++i) ASSERT_EQ(seen[i].load(), 1) << "value " << i;
}

// Within one producer, values must come out in the order they went in.
TEST(MpmcQueue, PerProducerOrderingPreservedWithSingleConsumer) {
  constexpr int kProducers = 3;
  constexpr std::uint64_t kPerProducer = 100'000;
  MpmcQueue<std::uint64_t, latchqueue::BusySpinWait> q(256);

  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (std::uint64_t i = 0; i < kPerProducer; ++i) q.push((std::uint64_t(p) << 32) | i);
    });
  }
  std::vector<std::uint64_t> last(kProducers, ~std::uint64_t{0});
  for (std::uint64_t n = 0; n < kProducers * kPerProducer; ++n) {
    std::uint64_t v;
    q.pop(v);
    const auto p = v >> 32;
    const auto i = v & 0xFFFFFFFF;
    if (last[p] != ~std::uint64_t{0}) ASSERT_EQ(i, last[p] + 1);
    last[p] = i;
  }
  for (auto& t : producers) t.join();
}

TEST(MpmcQueue, BlockingPopWakesWithSpinPark) {
  MpmcQueue<int, latchqueue::SpinParkWait> q(4);
  std::thread consumer([&] {
    int v = 0;
    q.pop(v);
    EXPECT_EQ(v, 42);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  q.push(42);
  consumer.join();
}

TEST(MpmcQueue, PopForTimesOut) {
  MpmcQueue<int> q(4);
  int v;
  EXPECT_FALSE(q.pop_for(v, 64));
}
