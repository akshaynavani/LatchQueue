#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "latchqueue/subscription.hpp"

namespace latchqueue::net {

class Session;

// Connects one subscription to one client session. Several bindings (from
// different sessions) may share a Subscription; they then compete for its
// messages through the lock-free MPMC queue.
struct Binding {
  Binding(std::uint32_t h, std::shared_ptr<Subscription> s, std::weak_ptr<Session> sess)
      : handle(h), subscription(std::move(s)), session(std::move(sess)) {}

  const std::uint32_t handle;
  const std::shared_ptr<Subscription> subscription;
  const std::weak_ptr<Session> session;
  std::atomic<bool> active{true};
};

struct DeliveryPumpOptions {
  std::size_t threads = 1;
  std::size_t batch_size = 256;                   // max messages per write batch
  std::size_t max_session_backlog = 8 << 20;      // bytes queued before we pause
};

// Pool of threads that drain subscription queues into client sessions.
//
// Each binding is owned by exactly one pump thread (round-robin), so a
// binding's frames are always encoded in sequence order. Messages are batched
// into a single buffer per session write to amortise syscalls. If a session's
// outbound backlog exceeds max_session_backlog the pump stops draining that
// binding, leaving messages in the subscription queue so the subscription's
// overflow policy (drop or block) provides back-pressure.
class DeliveryPump {
 public:
  explicit DeliveryPump(DeliveryPumpOptions options = {});
  ~DeliveryPump();

  DeliveryPump(const DeliveryPump&) = delete;
  DeliveryPump& operator=(const DeliveryPump&) = delete;

  void start();
  void stop();

  void add(std::shared_ptr<Binding> binding);

  std::uint64_t frames_sent() const noexcept {
    return frames_sent_.load(std::memory_order_relaxed);
  }

 private:
  struct Worker {
    std::mutex mutex;
    std::vector<std::shared_ptr<Binding>> incoming;
    std::atomic<bool> has_incoming{false};
    std::thread thread;
  };

  void run(Worker& worker);

  const DeliveryPumpOptions options_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::atomic<std::size_t> next_worker_{0};
  std::atomic<bool> running_{false};
  std::atomic<std::uint64_t> frames_sent_{0};
};

}  // namespace latchqueue::net
