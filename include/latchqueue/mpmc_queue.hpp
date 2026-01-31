#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "latchqueue/cache_padded.hpp"
#include "latchqueue/cpu.hpp"
#include "latchqueue/wait_strategy.hpp"

namespace latchqueue {

// Bounded multi-producer / multi-consumer queue.
//
// Algorithm: Dmitry Vyukov's bounded MPMC ring. Every slot carries a sequence
// number that encodes whose turn it is:
//   seq == pos           -> slot is free for the producer that claims `pos`
//   seq == pos + 1       -> slot holds data for the consumer that claims `pos`
//   seq == pos + cap     -> slot was consumed, free for the next lap
// A producer claims a position with a CAS on `tail_`, writes the value and
// then publishes it with a release store of `seq = pos + 1`. Consumers mirror
// this on `head_`. Each slot is touched by exactly one producer and one
// consumer per lap, so contention is limited to the head/tail counters.
//
// Capacity is rounded up to a power of two so index = pos & mask.
template <typename T, typename Wait = SpinYieldWait>
class MpmcQueue {
  static_assert(std::is_nothrow_move_constructible_v<T>,
                "MpmcQueue requires nothrow-move-constructible elements");
  static_assert(std::is_default_constructible_v<T>,
                "MpmcQueue requires default-constructible elements (pop takes T&)");

 public:
  explicit MpmcQueue(std::size_t capacity)
      : capacity_(next_power_of_two(capacity < 2 ? 2 : capacity)),
        mask_(capacity_ - 1),
        slots_(static_cast<Slot*>(::operator new[](sizeof(Slot) * capacity_,
                                                   std::align_val_t{alignof(Slot)}))) {
    for (std::size_t i = 0; i < capacity_; ++i) {
      new (&slots_[i]) Slot();
      slots_[i].seq.store(i, std::memory_order_relaxed);
    }
    std::atomic_thread_fence(std::memory_order_release);
  }

  ~MpmcQueue() {
    // Destroy any elements still in the queue.
    T tmp;
    while (try_pop(tmp)) {
    }
    for (std::size_t i = 0; i < capacity_; ++i) {
      slots_[i].~Slot();
    }
    ::operator delete[](slots_, std::align_val_t{alignof(Slot)});
  }

  MpmcQueue(const MpmcQueue&) = delete;
  MpmcQueue& operator=(const MpmcQueue&) = delete;

  // Non-blocking push. Returns false if the queue is full.
  template <typename U>
  bool try_push(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
    std::size_t pos;
    Slot* slot = claim_push_slot(pos);
    if (slot == nullptr) return false;
    publish(slot, pos, std::forward<U>(value));
    return true;
  }

  // Non-blocking pop. Returns false if the queue is empty.
  bool try_pop(T& out) noexcept {
    std::size_t pos = head_->load(std::memory_order_relaxed);
    for (;;) {
      Slot& slot = slots_[pos & mask_];
      const std::size_t seq = slot.seq.load(std::memory_order_acquire);
      const auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
      if (diff == 0) {
        if (head_->compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          T* ptr = slot.ptr();
          out = std::move(*ptr);
          ptr->~T();
          slot.seq.store(pos + capacity_, std::memory_order_release);
          not_full_.notify();
          return true;
        }
        // CAS failure reloaded `pos`; retry.
      } else if (diff < 0) {
        return false;  // empty
      } else {
        pos = head_->load(std::memory_order_relaxed);
      }
    }
  }

  // Blocking push: waits according to the wait strategy while full.
  template <typename U>
  void push(U&& value) {
    std::uint32_t iteration = 0;
    std::size_t pos;
    Slot* slot;
    while ((slot = claim_push_slot(pos)) == nullptr) {
      not_full_.wait(iteration);
    }
    publish(slot, pos, std::forward<U>(value));
  }

  // Blocking pop: waits according to the wait strategy while empty.
  void pop(T& out) {
    std::uint32_t iteration = 0;
    while (!try_pop(out)) {
      not_empty_.wait(iteration);
    }
  }

  // Pop with a bounded number of wait iterations; used by delivery loops that
  // also need to observe shutdown flags.
  bool pop_for(T& out, std::uint32_t max_iterations) {
    std::uint32_t iteration = 0;
    while (!try_pop(out)) {
      if (iteration >= max_iterations) return false;
      not_empty_.wait(iteration);
    }
    return true;
  }

  // Drain up to `max` elements into `out` (an output iterator). Returns the
  // number popped. Batching amortises the per-message notify cost.
  template <typename OutIt>
  std::size_t try_pop_bulk(OutIt out, std::size_t max) noexcept {
    std::size_t n = 0;
    T tmp;
    while (n < max && try_pop(tmp)) {
      *out++ = std::move(tmp);
      ++n;
    }
    return n;
  }

  // Approximate size; exact only when the queue is quiescent.
  std::size_t size_approx() const noexcept {
    const std::size_t tail = tail_->load(std::memory_order_relaxed);
    const std::size_t head = head_->load(std::memory_order_relaxed);
    return tail >= head ? tail - head : 0;
  }

  bool empty_approx() const noexcept { return size_approx() == 0; }
  std::size_t capacity() const noexcept { return capacity_; }

 private:
  struct alignas(kCacheLine) Slot {
    std::atomic<std::size_t> seq{0};
    alignas(T) unsigned char storage[sizeof(T)];

    T* ptr() noexcept { return std::launder(reinterpret_cast<T*>(storage)); }
  };

  // Claims a slot for writing; returns nullptr if the queue is full.
  Slot* claim_push_slot(std::size_t& pos) noexcept {
    pos = tail_->load(std::memory_order_relaxed);
    for (;;) {
      Slot& slot = slots_[pos & mask_];
      const std::size_t seq = slot.seq.load(std::memory_order_acquire);
      const auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
      if (diff == 0) {
        if (tail_->compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          return &slot;
        }
      } else if (diff < 0) {
        return nullptr;  // full
      } else {
        pos = tail_->load(std::memory_order_relaxed);
      }
    }
  }

  template <typename U>
  void publish(Slot* slot, std::size_t pos, U&& value) noexcept(
      std::is_nothrow_constructible_v<T, U&&>) {
    new (slot->storage) T(std::forward<U>(value));
    slot->seq.store(pos + 1, std::memory_order_release);
    not_empty_.notify();
  }

  const std::size_t capacity_;
  const std::size_t mask_;
  Slot* slots_;

  CachePadded<std::atomic<std::size_t>> head_{0};
  CachePadded<std::atomic<std::size_t>> tail_{0};
  Wait not_empty_;
  Wait not_full_;
};

}  // namespace latchqueue
