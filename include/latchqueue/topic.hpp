#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "latchqueue/cache_padded.hpp"
#include "latchqueue/message.hpp"
#include "latchqueue/stats.hpp"
#include "latchqueue/subscription.hpp"

namespace latchqueue {

struct PublishResult {
  std::uint64_t sequence = 0;
  std::uint32_t delivered_to = 0;  // subscriptions that accepted the message
  std::uint32_t dropped_by = 0;    // subscriptions that dropped it (full)
  bool truncated = false;          // payload exceeded kMaxPayload
};

// A topic fans each published message out to all of its subscriptions.
//
// The subscription list is copy-on-write: publishers take an atomic snapshot
// (shared_ptr load) and iterate it without locks, while the rare
// subscribe/unsubscribe path builds a new vector under a mutex and swaps it in.
class Topic {
 public:
  using SubscriptionList = std::vector<std::shared_ptr<Subscription>>;

  Topic(TopicId id, std::string name, std::size_t default_capacity);

  TopicId id() const noexcept { return id_; }
  const std::string& name() const noexcept { return name_; }

  PublishResult publish(std::string_view payload);
  PublishResult publish(const void* data, std::size_t len);

  // Returns the subscription with `name`, creating it if needed. A capacity of
  // 0 uses the topic's default.
  std::shared_ptr<Subscription> subscribe(const std::string& name, std::size_t capacity = 0,
                                          OverflowPolicy policy = OverflowPolicy::kDropNewest);

  // Removes and closes a subscription. Returns false if it did not exist.
  bool unsubscribe(const std::string& name);

  std::shared_ptr<Subscription> find_subscription(const std::string& name) const;
  std::shared_ptr<const SubscriptionList> subscriptions() const;

  std::uint64_t published() const noexcept {
    return published_->load(std::memory_order_relaxed);
  }

  TopicStats stats() const;

 private:
  const TopicId id_;
  const std::string name_;
  const std::size_t default_capacity_;

  CachePadded<std::atomic<std::uint64_t>> next_sequence_{1};
  CachePadded<std::atomic<std::uint64_t>> published_{0};

  // Accessed with std::atomic_load / std::atomic_store (C++17).
  std::shared_ptr<const SubscriptionList> subs_;
  std::mutex subs_write_mutex_;
};

}  // namespace latchqueue
