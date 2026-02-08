#include "latchqueue/topic.hpp"

#include <algorithm>

#include "latchqueue/clock.hpp"

namespace latchqueue {

Topic::Topic(TopicId id, std::string name, std::size_t default_capacity)
    : id_(id),
      name_(std::move(name)),
      default_capacity_(default_capacity),
      subs_(std::make_shared<const SubscriptionList>()) {}

PublishResult Topic::publish(std::string_view payload) {
  return publish(payload.data(), payload.size());
}

PublishResult Topic::publish(const void* data, std::size_t len) {
  PublishResult result;

  Message msg;
  msg.topic = id_;
  msg.flags = 0;
  result.truncated = !msg.set_payload(data, len);
  msg.header.sequence = next_sequence_->fetch_add(1, std::memory_order_relaxed);
  msg.header.publish_ts_ns = now_ns();
  result.sequence = msg.header.sequence;

  const auto snapshot = std::atomic_load_explicit(&subs_, std::memory_order_acquire);
  for (const auto& sub : *snapshot) {
    if (sub->offer(msg)) {
      ++result.delivered_to;
    } else {
      ++result.dropped_by;
    }
  }
  published_->fetch_add(1, std::memory_order_relaxed);
  return result;
}

std::shared_ptr<Subscription> Topic::subscribe(const std::string& name, std::size_t capacity,
                                               OverflowPolicy policy) {
  std::lock_guard<std::mutex> lock(subs_write_mutex_);
  const auto current = std::atomic_load_explicit(&subs_, std::memory_order_acquire);
  for (const auto& sub : *current) {
    if (sub->name() == name) return sub;
  }
  auto sub = std::make_shared<Subscription>(name, capacity ? capacity : default_capacity_, policy);
  auto next = std::make_shared<SubscriptionList>(*current);
  next->push_back(sub);
  std::atomic_store_explicit(&subs_, std::shared_ptr<const SubscriptionList>(std::move(next)),
                             std::memory_order_release);
  return sub;
}

bool Topic::unsubscribe(const std::string& name) {
  std::lock_guard<std::mutex> lock(subs_write_mutex_);
  const auto current = std::atomic_load_explicit(&subs_, std::memory_order_acquire);
  auto next = std::make_shared<SubscriptionList>();
  next->reserve(current->size());
  std::shared_ptr<Subscription> removed;
  for (const auto& sub : *current) {
    if (sub->name() == name) {
      removed = sub;
    } else {
      next->push_back(sub);
    }
  }
  if (!removed) return false;
  std::atomic_store_explicit(&subs_, std::shared_ptr<const SubscriptionList>(std::move(next)),
                             std::memory_order_release);
  removed->close();
  return true;
}

std::shared_ptr<Subscription> Topic::find_subscription(const std::string& name) const {
  const auto current = std::atomic_load_explicit(&subs_, std::memory_order_acquire);
  for (const auto& sub : *current) {
    if (sub->name() == name) return sub;
  }
  return nullptr;
}

std::shared_ptr<const Topic::SubscriptionList> Topic::subscriptions() const {
  return std::atomic_load_explicit(&subs_, std::memory_order_acquire);
}

TopicStats Topic::stats() const {
  TopicStats s;
  s.name = name_;
  s.id = id_;
  s.published = published();
  for (const auto& sub : *subscriptions()) {
    s.subscriptions.push_back(sub->stats());
  }
  return s;
}

}  // namespace latchqueue
