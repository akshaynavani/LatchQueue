#include "latchqueue/spsc_queue.hpp"

#include <string>
#include <thread>

#include <gtest/gtest.h>

using latchqueue::SpscQueue;

TEST(SpscQueue, FullAndEmpty) {
  SpscQueue<int> q(4);
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(q.try_push(i));
  EXPECT_FALSE(q.try_push(4));
  int v;
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(q.try_pop(v));
    EXPECT_EQ(v, i);
  }
  EXPECT_FALSE(q.try_pop(v));
}

TEST(SpscQueue, FrontAndPopFront) {
  SpscQueue<std::string> q(4);
  EXPECT_EQ(q.front(), nullptr);
  q.try_push(std::string("hello"));
  ASSERT_NE(q.front(), nullptr);
  EXPECT_EQ(*q.front(), "hello");
  q.pop_front();
  EXPECT_EQ(q.front(), nullptr);
}

TEST(SpscQueue, OrderedAcrossThreads) {
  constexpr std::uint64_t kCount = 1'000'000;
  SpscQueue<std::uint64_t> q(1024);
  std::thread producer([&] {
    for (std::uint64_t i = 0; i < kCount; ++i) {
      while (!q.try_push(i)) latchqueue::cpu_relax();
    }
  });
  std::uint64_t expected = 0, v;
  while (expected < kCount) {
    if (q.try_pop(v)) {
      ASSERT_EQ(v, expected);
      ++expected;
    }
  }
  producer.join();
  EXPECT_EQ(q.size_approx(), 0u);
}

TEST(SpscQueue, DestructorReleasesRemaining) {
  auto token = std::make_shared<int>(1);
  {
    SpscQueue<std::shared_ptr<int>> q(8);
    q.try_push(token);
    q.try_push(token);
  }
  EXPECT_EQ(token.use_count(), 1);
}
