#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "latchqueue/cache_padded.hpp"
#include "latchqueue/cpu.hpp"

namespace latchqueue {

// Bounded single-producer / single-consumer ring (Lamport queue).
//
// Each side keeps a private cached copy of the other side's index, so in the
// common case a push or pop touches only its own cache line. The shared index
// is reloaded only when the cached value says the queue looks full/empty.
template <typename T>
class SpscQueue {
  static_assert(std::is_nothrow_move_constructible_v<T>,
                "SpscQueue requires nothrow-move-constructible elements");

 public:
  explicit SpscQueue(std::size_t capacity)
      : capacity_(next_power_of_two(capacity < 2 ? 2 : capacity)),
        mask_(capacity_ - 1),
        buffer_(static_cast<T*>(::operator new[](sizeof(T) * capacity_,
                                                 std::align_val_t{alignof(T)}))) {}

  ~SpscQueue() {
    std::size_t head = head_->load(std::memory_order_relaxed);
    const std::size_t tail = tail_->load(std::memory_order_relaxed);
    for (; head != tail; ++head) {
      buffer_[head & mask_].~T();
    }
    ::operator delete[](buffer_, std::align_val_t{alignof(T)});
  }

  SpscQueue(const SpscQueue&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;

  // Producer side only.
  template <typename U>
  bool try_push(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
    const std::size_t tail = tail_->load(std::memory_order_relaxed);
    if (tail - producer_cached_head_->value >= capacity_) {
      producer_cached_head_->value = head_->load(std::memory_order_acquire);
      if (tail - producer_cached_head_->value >= capacity_) return false;
    }
    new (&buffer_[tail & mask_]) T(std::forward<U>(value));
    tail_->store(tail + 1, std::memory_order_release);
    return true;
  }

  // Consumer side only.
  bool try_pop(T& out) noexcept {
    const std::size_t head = head_->load(std::memory_order_relaxed);
    if (head == consumer_cached_tail_->value) {
      consumer_cached_tail_->value = tail_->load(std::memory_order_acquire);
      if (head == consumer_cached_tail_->value) return false;
    }
    T* slot = &buffer_[head & mask_];
    out = std::move(*slot);
    slot->~T();
    head_->store(head + 1, std::memory_order_release);
    return true;
  }

  // Consumer side only: peek at the front element without removing it.
  T* front() noexcept {
    const std::size_t head = head_->load(std::memory_order_relaxed);
    if (head == consumer_cached_tail_->value) {
      consumer_cached_tail_->value = tail_->load(std::memory_order_acquire);
      if (head == consumer_cached_tail_->value) return nullptr;
    }
    return &buffer_[head & mask_];
  }

  // Consumer side only: drop the element returned by front().
  void pop_front() noexcept {
    const std::size_t head = head_->load(std::memory_order_relaxed);
    buffer_[head & mask_].~T();
    head_->store(head + 1, std::memory_order_release);
  }

  std::size_t size_approx() const noexcept {
    const std::size_t tail = tail_->load(std::memory_order_acquire);
    const std::size_t head = head_->load(std::memory_order_acquire);
    return tail - head;
  }

  std::size_t capacity() const noexcept { return capacity_; }

 private:
  struct Cached {
    std::size_t value = 0;
  };

  const std::size_t capacity_;
  const std::size_t mask_;
  T* buffer_;

  CachePadded<std::atomic<std::size_t>> head_{0};
  CachePadded<Cached> consumer_cached_tail_{};
  CachePadded<std::atomic<std::size_t>> tail_{0};
  CachePadded<Cached> producer_cached_head_{};
};

}  // namespace latchqueue
