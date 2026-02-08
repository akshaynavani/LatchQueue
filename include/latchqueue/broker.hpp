#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "latchqueue/stats.hpp"
#include "latchqueue/topic.hpp"

namespace latchqueue {

struct BrokerOptions {
  std::size_t default_queue_capacity = 1 << 16;  // per subscription
  std::size_t max_topics = 4096;
};

// Owns all topics. Lookups take a shared lock, which only happens when a
// client first resolves a topic name/id; the publish/consume hot path holds a
// shared_ptr<Topic> and never touches the registry.
class Broker {
 public:
  explicit Broker(BrokerOptions options = {});

  // Creates a topic or returns the existing one with the same name. Returns
  // nullptr if max_topics would be exceeded or the name is invalid.
  std::shared_ptr<Topic> create_topic(const std::string& name);

  std::shared_ptr<Topic> get_topic(const std::string& name) const;
  std::shared_ptr<Topic> get_topic(TopicId id) const;
  std::vector<std::shared_ptr<Topic>> list_topics() const;

  // Removes a topic and closes all of its subscriptions.
  bool delete_topic(const std::string& name);

  BrokerStats stats() const;
  const BrokerOptions& options() const noexcept { return options_; }

  static bool valid_topic_name(const std::string& name) noexcept;

 private:
  const BrokerOptions options_;
  const std::uint64_t started_ns_;

  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Topic>> by_name_;
  std::unordered_map<TopicId, std::shared_ptr<Topic>> by_id_;
  TopicId next_id_ = 1;
};

}  // namespace latchqueue
