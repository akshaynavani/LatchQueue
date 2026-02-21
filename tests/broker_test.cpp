#include "latchqueue/broker.hpp"

#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace latchqueue;

TEST(Broker, CreateTopicIsIdempotent) {
  Broker broker;
  auto a = broker.create_topic("orders");
  auto b = broker.create_topic("orders");
  ASSERT_NE(a, nullptr);
  EXPECT_EQ(a, b);
  EXPECT_EQ(broker.get_topic(a->id()), a);
  EXPECT_EQ(broker.get_topic("orders"), a);
  EXPECT_EQ(broker.list_topics().size(), 1u);
}

TEST(Broker, RejectsInvalidNamesAndEnforcesLimit) {
  BrokerOptions opts;
  opts.max_topics = 2;
  Broker broker(opts);
  EXPECT_EQ(broker.create_topic(""), nullptr);
  EXPECT_EQ(broker.create_topic("has space"), nullptr);
  EXPECT_NE(broker.create_topic("a"), nullptr);
  EXPECT_NE(broker.create_topic("md/equities.us"), nullptr);
  EXPECT_EQ(broker.create_topic("c"), nullptr);
}

TEST(Topic, FanOutToEverySubscription) {
  Broker broker;
  auto t = broker.create_topic("ticks");
  auto s1 = t->subscribe("risk");
  auto s2 = t->subscribe("audit");

  auto r = t->publish("hello");
  EXPECT_EQ(r.sequence, 1u);
  EXPECT_EQ(r.delivered_to, 2u);

  Message m;
  ASSERT_TRUE(s1->try_poll(m));
  EXPECT_EQ(m.view(), "hello");
  ASSERT_TRUE(s2->try_poll(m));
  EXPECT_EQ(m.header.sequence, 1u);
  EXPECT_FALSE(s1->try_poll(m));
}

TEST(Topic, SameSubscriptionNameIsSharedConsumerGroup) {
  Broker broker;
  auto t = broker.create_topic("jobs");
  auto a = t->subscribe("workers");
  auto b = t->subscribe("workers");
  EXPECT_EQ(a, b);
  EXPECT_EQ(t->subscriptions()->size(), 1u);
}

TEST(Topic, DropNewestCountsDrops) {
  Broker broker;
  auto t = broker.create_topic("small");
  auto s = t->subscribe("s", 2, OverflowPolicy::kDropNewest);
  t->publish("1");
  t->publish("2");
  auto r = t->publish("3");
  EXPECT_EQ(r.dropped_by, 1u);
  const auto st = s->stats();
  EXPECT_EQ(st.enqueued, 2u);
  EXPECT_EQ(st.dropped, 1u);
}

TEST(Topic, TruncatesOversizedPayload) {
  Broker broker;
  auto t = broker.create_topic("big");
  auto s = t->subscribe("s");
  const std::string big(kMaxPayload + 10, 'x');
  EXPECT_TRUE(t->publish(big).truncated);
  Message m;
  ASSERT_TRUE(s->try_poll(m));
  EXPECT_EQ(m.size, kMaxPayload);
}

TEST(Topic, UnsubscribeClosesAndUnblocksPublisher) {
  Broker broker;
  auto t = broker.create_topic("bp");
  auto s = t->subscribe("slow", 2, OverflowPolicy::kBlock);
  t->publish("1");
  t->publish("2");
  std::thread publisher([&] { t->publish("3"); });  // blocks: queue full
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_TRUE(t->unsubscribe("slow"));
  publisher.join();
  EXPECT_TRUE(s->closed());
  EXPECT_EQ(t->subscriptions()->size(), 0u);
}

TEST(Topic, ConcurrentPublishersAndCompetingConsumers) {
  Broker broker;
  auto t = broker.create_topic("load");
  auto s = t->subscribe("group", 1 << 12, OverflowPolicy::kBlock);
  constexpr int kPublishers = 4, kConsumers = 3, kPer = 50'000;
  std::atomic<int> received{0};
  std::atomic<bool> done{false};

  std::vector<std::thread> threads;
  for (int c = 0; c < kConsumers; ++c) {
    threads.emplace_back([&] {
      Message m;
      while (!done.load() || s->depth() > 0) {
        if (s->try_poll(m)) received.fetch_add(1);
      }
    });
  }
  std::vector<std::thread> pubs;
  for (int p = 0; p < kPublishers; ++p) {
    pubs.emplace_back([&] {
      for (int i = 0; i < kPer; ++i) t->publish("x");
    });
  }
  for (auto& p : pubs) p.join();
  done.store(true);
  for (auto& th : threads) th.join();

  EXPECT_EQ(received.load(), kPublishers * kPer);
  EXPECT_EQ(t->published(), static_cast<std::uint64_t>(kPublishers * kPer));
  const auto st = broker.stats();
  EXPECT_EQ(st.total_delivered, static_cast<std::uint64_t>(kPublishers * kPer));
  EXPECT_EQ(st.total_dropped, 0u);
}

TEST(Broker, DeleteTopicClosesSubscriptions) {
  Broker broker;
  auto t = broker.create_topic("gone");
  auto s = t->subscribe("s");
  EXPECT_TRUE(broker.delete_topic("gone"));
  EXPECT_TRUE(s->closed());
  EXPECT_EQ(broker.get_topic("gone"), nullptr);
  EXPECT_FALSE(broker.delete_topic("gone"));
}
