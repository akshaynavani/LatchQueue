#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

#include "latchqueue/cpu.hpp"

namespace latchqueue {

// A wait strategy decides what a blocked producer/consumer does while the
// queue is full/empty. Every strategy exposes:
//   void wait(std::uint32_t& iteration)   -- called once per failed attempt
//   void notify()                         -- called after a successful op
// `iteration` is caller-owned so strategies stay stateless per waiter.

// Lowest latency, burns a core. Use with pinned threads.
struct BusySpinWait {
  void wait(std::uint32_t& /*iteration*/) noexcept { cpu_relax(); }
  void notify() noexcept {}
};

// Spin for a while, then yield the time slice. Good default.
struct SpinYieldWait {
  static constexpr std::uint32_t kSpinLimit = 128;

  void wait(std::uint32_t& iteration) noexcept {
    if (iteration < kSpinLimit) {
      cpu_relax();
    } else {
      std::this_thread::yield();
    }
    ++iteration;
  }
  void notify() noexcept {}
};

// Spin, then yield, then park on a condition variable. notify() is cheap when
// nobody is parked (one relaxed load), so the hot path stays lock-free.
class SpinParkWait {
 public:
  static constexpr std::uint32_t kSpinLimit = 256;
  static constexpr std::uint32_t kYieldLimit = 512;

  void wait(std::uint32_t& iteration) {
    if (iteration < kSpinLimit) {
      cpu_relax();
    } else if (iteration < kYieldLimit) {
      std::this_thread::yield();
    } else {
      std::unique_lock<std::mutex> lock(mutex_);
      parked_.fetch_add(1, std::memory_order_seq_cst);
      // Bounded park: guards against a lost wakeup between the caller's
      // failed attempt and our increment of parked_.
      cv_.wait_for(lock, std::chrono::microseconds(200));
      parked_.fetch_sub(1, std::memory_order_relaxed);
    }
    ++iteration;
  }

  void notify() {
    if (parked_.load(std::memory_order_seq_cst) != 0) {
      std::lock_guard<std::mutex> lock(mutex_);
      cv_.notify_all();
    }
  }

 private:
  std::atomic<std::uint32_t> parked_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
};

}  // namespace latchqueue
