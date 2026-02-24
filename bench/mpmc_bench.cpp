// Raw MPMC queue benchmark.
//
//   mpmc_bench [--producers P] [--consumers C] [--messages N] [--capacity K]
//              [--pin] [--latency]
//
// Throughput mode pushes N 8-byte values through the queue. Latency mode
// (--latency) runs a 1:1 ping-pong over two queues and records the one-way
// enqueue->dequeue latency, which isolates the queue's handoff cost.

#include <algorithm>
#include <atomic>
#include <utility>
#include <thread>
#include <vector>

#include "bench_util.hpp"
#include "latchqueue/clock.hpp"
#include "latchqueue/mpmc_queue.hpp"

using namespace latchqueue;

namespace {

void throughput(int producers, int consumers, std::uint64_t total, std::size_t capacity, bool pin) {
  MpmcQueue<std::uint64_t, BusySpinWait> q(capacity);
  const std::uint64_t per_producer = total / producers;
  const std::uint64_t expected = per_producer * producers;
  std::atomic<std::uint64_t> consumed{0};
  std::atomic<int> ready{0};
  std::atomic<bool> go{false};

  std::vector<std::thread> threads;
  for (int p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      if (pin) bench::pin_to_cpu(p);
      ready.fetch_add(1);
      while (!go.load(std::memory_order_acquire)) cpu_relax();
      for (std::uint64_t i = 0; i < per_producer; ++i) q.push(i);
    });
  }
  for (int c = 0; c < consumers; ++c) {
    threads.emplace_back([&, c] {
      if (pin) bench::pin_to_cpu(producers + c);
      ready.fetch_add(1);
      while (!go.load(std::memory_order_acquire)) cpu_relax();
      std::uint64_t v, local = 0;
      while (consumed.load(std::memory_order_relaxed) < expected) {
        if (q.try_pop(v)) {
          if (++local == 1024) {
            consumed.fetch_add(local, std::memory_order_relaxed);
            local = 0;
          }
        } else if (local) {
          consumed.fetch_add(local, std::memory_order_relaxed);
          local = 0;
        }
      }
    });
  }
  while (ready.load() != producers + consumers) cpu_relax();
  const auto start = now_ns();
  go.store(true, std::memory_order_release);
  for (auto& t : threads) t.join();
  const double secs = static_cast<double>(now_ns() - start) / 1e9;

  std::string label = "mpmc " + std::to_string(producers) + "P/" + std::to_string(consumers) + "C";
  bench::print_throughput(label.c_str(), expected, secs);
}

void latency(std::uint64_t rounds, bool pin) {
  MpmcQueue<std::uint64_t, BusySpinWait> ping(1024), pong(1024);
  LatencyHistogram one_way;

  std::thread echo([&] {
    if (pin) bench::pin_to_cpu(1);
    std::uint64_t ts;
    for (std::uint64_t i = 0; i < rounds; ++i) {
      ping.pop(ts);
      one_way.record(now_ns() - ts);
      pong.push(ts);
    }
  });
  if (pin) bench::pin_to_cpu(0);
  std::uint64_t ts;
  for (std::uint64_t i = 0; i < rounds; ++i) {
    ping.push(now_ns());
    pong.pop(ts);
  }
  echo.join();
  bench::print_latency("mpmc one-way handoff", one_way);
}

}  // namespace

int main(int argc, char** argv) {
  const int producers = static_cast<int>(bench::arg_int(argc, argv, "--producers", 2));
  const int consumers = static_cast<int>(bench::arg_int(argc, argv, "--consumers", 2));
  const auto messages = static_cast<std::uint64_t>(bench::arg_int(argc, argv, "--messages", 20'000'000));
  const auto capacity = static_cast<std::size_t>(bench::arg_int(argc, argv, "--capacity", 1 << 16));
  const bool pin = bench::arg_flag(argc, argv, "--pin");

  if (bench::arg_flag(argc, argv, "--latency")) {
    latency(std::min<std::uint64_t>(messages, 2'000'000), pin);
    return 0;
  }
  // --sweep runs a fixed set of producer/consumer shapes.
  if (bench::arg_flag(argc, argv, "--sweep")) {
    const std::pair<int, int> shapes[] = {{1, 1}, {2, 2}, {4, 4}, {4, 1}, {1, 4}};
    for (const auto& [p, c] : shapes) {
      throughput(p, c, messages, capacity, pin);
    }
  } else {
    throughput(producers, consumers, messages, capacity, pin);
  }
  latency(1'000'000, pin);
  return 0;
}
