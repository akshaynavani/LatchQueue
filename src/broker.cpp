#include "latchqueue/broker.hpp"

#include <algorithm>
#include <cctype>
#include <mutex>

#include "latchqueue/clock.hpp"

namespace latchqueue {

Broker::Broker(BrokerOptions options) : options_(options), started_ns_(now_ns()) {}

bool Broker::valid_topic_name(const std::string& name) noexcept {
  if (name.empty() || name.size() > 128) return false;
  return std::all_of(name.begin(), name.end(), [](unsigned char c) {
    return std::isalnum(c) || c == '.' || c == '_' || c == '-' || c == '/';
  });
}

std::shared_ptr<Topic> Broker::create_topic(const std::string& name) {
  if (!valid_topic_name(name)) return nullptr;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (auto it = by_name_.find(name); it != by_name_.end()) return it->second;
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (auto it = by_name_.find(name); it != by_name_.end()) return it->second;
  if (by_name_.size() >= options_.max_topics) return nullptr;
  const TopicId id = next_id_++;
  auto topic = std::make_shared<Topic>(id, name, options_.default_queue_capacity);
  by_name_.emplace(name, topic);
  by_id_.emplace(id, topic);
  return topic;
}

std::shared_ptr<Topic> Broker::get_topic(const std::string& name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = by_name_.find(name);
  return it == by_name_.end() ? nullptr : it->second;
}

std::shared_ptr<Topic> Broker::get_topic(TopicId id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = by_id_.find(id);
  return it == by_id_.end() ? nullptr : it->second;
}

std::vector<std::shared_ptr<Topic>> Broker::list_topics() const {
  std::vector<std::shared_ptr<Topic>> out;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    out.reserve(by_name_.size());
    for (const auto& [_, topic] : by_name_) out.push_back(topic);
  }
  std::sort(out.begin(), out.end(),
            [](const auto& a, const auto& b) { return a->id() < b->id(); });
  return out;
}

bool Broker::delete_topic(const std::string& name) {
  std::shared_ptr<Topic> topic;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = by_name_.find(name);
    if (it == by_name_.end()) return false;
    topic = it->second;
    by_id_.erase(topic->id());
    by_name_.erase(it);
  }
  for (const auto& sub : *topic->subscriptions()) {
    topic->unsubscribe(sub->name());
  }
  return true;
}

BrokerStats Broker::stats() const {
  BrokerStats s;
  s.uptime_ns = now_ns() - started_ns_;
  for (const auto& topic : list_topics()) {
    TopicStats ts = topic->stats();
    s.total_published += ts.published;
    for (const auto& sub : ts.subscriptions) {
      s.total_delivered += sub.delivered;
      s.total_dropped += sub.dropped;
    }
    s.topics.push_back(std::move(ts));
  }
  return s;
}

}  // namespace latchqueue
