#include <chrono>
#include <set>
#include <string>

#include <gtest/gtest.h>

#include "latchqueue/broker.hpp"
#include "latchqueue/client/tcp_client.hpp"
#include "latchqueue/net/delivery_pump.hpp"
#include "latchqueue/net/tcp_server.hpp"

using namespace latchqueue;
using namespace std::chrono_literals;

class TcpIntegration : public ::testing::Test {
 protected:
  void SetUp() override {
    pump_.start();
    net::TcpServerOptions opts;
    opts.host = "127.0.0.1";
    opts.port = 0;  // ephemeral
    opts.io_threads = 2;
    server_ = std::make_unique<net::TcpServer>(broker_, pump_, opts);
    server_->start();
  }

  void TearDown() override {
    server_->stop();
    pump_.stop();
  }

  std::unique_ptr<client::TcpClient> connect() {
    auto c = std::make_unique<client::TcpClient>();
    c->connect("127.0.0.1", server_->port(), "test");
    return c;
  }

  Broker broker_;
  net::DeliveryPump pump_{net::DeliveryPumpOptions{}};
  std::unique_ptr<net::TcpServer> server_;
};

TEST_F(TcpIntegration, CreatePublishSubscribe) {
  auto sub = connect();
  auto pub = connect();

  const TopicId id = pub->create_topic("prices");
  EXPECT_EQ(sub->create_topic("prices"), id);
  const auto handle = sub->subscribe(id, "ui");

  EXPECT_EQ(pub->publish_sync(id, "first"), 1u);
  for (int i = 0; i < 99; ++i) pub->publish(id, "m" + std::to_string(i));
  pub->flush();

  auto m = sub->receive(2000ms);
  ASSERT_TRUE(m);
  EXPECT_EQ(m->subscription, handle);
  EXPECT_EQ(m->payload, "first");
  EXPECT_EQ(m->sequence, 1u);

  std::uint64_t last = m->sequence;
  for (int i = 0; i < 99; ++i) {
    auto next = sub->receive(2000ms);
    ASSERT_TRUE(next) << "missing message " << i;
    EXPECT_GT(next->sequence, last);
    last = next->sequence;
  }
}

TEST_F(TcpIntegration, CompetingConsumersSplitMessages) {
  auto pub = connect();
  auto c1 = connect();
  auto c2 = connect();
  const TopicId id = pub->create_topic("work");
  c1->subscribe(id, "pool");
  c2->subscribe(id, "pool");

  constexpr int kCount = 1000;
  for (int i = 0; i < kCount; ++i) pub->publish(id, std::to_string(i));
  pub->flush();

  std::set<std::uint64_t> seqs;
  int got = 0;
  while (got < kCount) {
    bool progressed = false;
    for (auto* c : {c1.get(), c2.get()}) {
      if (auto m = c->receive(0ms)) {
        EXPECT_TRUE(seqs.insert(m->sequence).second) << "duplicate delivery";
        ++got;
        progressed = true;
      }
    }
    if (!progressed) {
      // Wait a little for the pump, but bail out if nothing arrives.
      auto m = c1->receive(500ms);
      if (!m) m = c2->receive(500ms);
      if (!m) break;
      EXPECT_TRUE(seqs.insert(m->sequence).second);
      ++got;
    }
  }
  EXPECT_EQ(got, kCount);
}

TEST_F(TcpIntegration, ErrorsAreReported) {
  auto c = connect();
  EXPECT_THROW(c->publish_sync(999, "x"), client::LatchQueueError);
  EXPECT_THROW(c->create_topic("bad name"), client::LatchQueueError);
  const TopicId id = c->create_topic("ok");
  EXPECT_THROW(c->publish_sync(id, std::string(kMaxPayload + 1, 'x')), client::LatchQueueError);
  EXPECT_THROW(c->unsubscribe(12345), client::LatchQueueError);
}

TEST_F(TcpIntegration, PingMeasuresRoundTrip) {
  auto c = connect();
  EXPECT_GT(c->ping(), 0u);
}
