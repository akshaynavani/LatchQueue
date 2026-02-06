#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace latchqueue {

struct LatencySummary {
  std::uint64_t count = 0;
  double mean_ns = 0;
  std::uint64_t min_ns = 0;
  std::uint64_t p50_ns = 0;
  std::uint64_t p99_ns = 0;
  std::uint64_t p999_ns = 0;
  std::uint64_t max_ns = 0;
};

struct SubscriptionStats {
  std::string name;
  std::uint64_t enqueued = 0;
  std::uint64_t delivered = 0;
  std::uint64_t dropped = 0;
  std::uint64_t depth = 0;
  std::uint64_t capacity = 0;
  LatencySummary latency;  // publish -> consumer dequeue
};

struct TopicStats {
  std::string name;
  std::uint32_t id = 0;
  std::uint64_t published = 0;
  std::vector<SubscriptionStats> subscriptions;
};

struct BrokerStats {
  std::uint64_t uptime_ns = 0;
  std::uint64_t total_published = 0;
  std::uint64_t total_delivered = 0;
  std::uint64_t total_dropped = 0;
  std::vector<TopicStats> topics;
};

}  // namespace latchqueue
