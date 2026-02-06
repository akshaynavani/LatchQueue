#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "latchqueue/cache_padded.hpp"
#include "latchqueue/clock.hpp"
#include "latchqueue/histogram.hpp"
#include "latchqueue/message.hpp"
#include "latchqueue/mpmc_queue.hpp"
#include "latchqueue/stats.hpp"
#include "latchqueue/wait_strategy.hpp"

namespace latchqueue {

// What a publisher does when a subscription's queue is full.
enum class OverflowPolicy : std::uint8_t {
  kDropNewest = 0,  // count the drop and move on; publishers never block
  kBlock = 1,       // back-pressure: publisher waits for space
};

// A named subscription on a topic. All consumers attached to the same
// subscription compete for its messages (work-queue / consumer-group
// semantics); distinct subscriptions each receive every message (fan-out).
class Subscription {
 public:
  using Queue = MpmcQueue<Message, SpinParkWait>;

  Subscription(std::string name, std::size_t capacity, OverflowPolicy policy)
      : name_(std::move(name)), policy_(policy), queue_(capacity) {}

  const std::string& name() const noexcept { return name_; }
  OverflowPolicy policy() const noexcept { return policy_; }

  // Called by Topic::publish on the publisher's thread.
  bool offer(const Message& msg) {
    if (policy_ == OverflowPolicy::kBlock) {
      queue_.push(msg);
    } else if (!queue_.try_push(msg)) {
      dropped_->fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    enqueued_->fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  // Non-blocking receive.
  bool try_poll(Message& out) noexcept {
    if (!queue_.try_pop(out)) return false;
    on_delivered(out);
    return true;
  }

  // Receive, waiting for at most `max_wait_iterations` rounds of the wait
  // strategy. Lets delivery loops observe shutdown flags.
  bool poll(Message& out, std::uint32_t max_wait_iterations = 2048) {
    if (!queue_.pop_for(out, max_wait_iterations)) return false;
    on_delivered(out);
    return true;
  }

  // Mark the subscription closed; delivery loops should stop polling.
  void close() noexcept { closed_.store(true, std::memory_order_release); }
  bool closed() const noexcept { return closed_.load(std::memory_order_acquire); }

  SubscriptionStats stats() const {
    SubscriptionStats s;
    s.name = name_;
    s.enqueued = enqueued_->load(std::memory_order_relaxed);
    s.delivered = delivered_->load(std::memory_order_relaxed);
    s.dropped = dropped_->load(std::memory_order_relaxed);
    s.depth = queue_.size_approx();
    s.capacity = queue_.capacity();
    s.latency.count = latency_.count();
    s.latency.mean_ns = latency_.mean();
    s.latency.min_ns = latency_.min();
    s.latency.p50_ns = latency_.percentile(0.50);
    s.latency.p99_ns = latency_.percentile(0.99);
    s.latency.p999_ns = latency_.percentile(0.999);
    s.latency.max_ns = latency_.max();
    return s;
  }

  const LatencyHistogram& latency() const noexcept { return latency_; }

 private:
  void on_delivered(const Message& msg) noexcept {
    delivered_->fetch_add(1, std::memory_order_relaxed);
    const std::uint64_t now = now_ns();
    if (now >= msg.header.publish_ts_ns) {
      latency_.record(now - msg.header.publish_ts_ns);
    }
  }

  const std::string name_;
  const OverflowPolicy policy_;
  Queue queue_;

  CachePadded<std::atomic<std::uint64_t>> enqueued_{0};
  CachePadded<std::atomic<std::uint64_t>> delivered_{0};
  CachePadded<std::atomic<std::uint64_t>> dropped_{0};
  std::atomic<bool> closed_{false};
  LatencyHistogram latency_;
};

}  // namespace latchqueue
