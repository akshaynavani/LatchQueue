// In-process broker benchmark: publishers call Topic::publish, consumers poll
// a shared subscription. Measures broker throughput (message construction,
// sequencing, fan-out, 256-byte copies) and publish->consume latency.
//
//   broker_bench [--publishers P] [--consumers C] [--messages N] [--fanout F]
//                [--payload BYTES] [--pin]

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "bench_util.hpp"
#include "latchqueue/broker.hpp"
#include "latchqueue/clock.hpp"

using namespace latchqueue;

int main(int argc, char** argv) {
  const int publishers = static_cast<int>(bench::arg_int(argc, argv, "--publishers", 2));
  const int consumers = static_cast<int>(bench::arg_int(argc, argv, "--consumers", 2));
  const int fanout = static_cast<int>(bench::arg_int(argc, argv, "--fanout", 1));
  const auto messages = static_cast<std::uint64_t>(bench::arg_int(argc, argv, "--messages", 5'000'000));
  const auto payload_size = static_cast<std::size_t>(bench::arg_int(argc, argv, "--payload", 64));
  const bool pin = bench::arg_flag(argc, argv, "--pin");

  BrokerOptions opts;
  opts.default_queue_capacity = 1 << 16;
  Broker broker(opts);
  auto topic = broker.create_topic("bench");

  std::vector<std::shared_ptr<Subscription>> subs;
  for (int f = 0; f < fanout; ++f) {
    subs.push_back(topic->subscribe("sub" + std::to_string(f), 0, OverflowPolicy::kBlock));
  }

  const std::string payload(std::min(payload_size, kMaxPayload), 'p');
  const std::uint64_t per_pub = messages / publishers;
  const std::uint64_t total = per_pub * publishers;
  std::atomic<bool> go{false};
  std::vector<std::atomic<std::uint64_t>> consumed(fanout);

  std::vector<std::thread> threads;
  int cpu = 0;
  for (int f = 0; f < fanout; ++f) {
    for (int c = 0; c < consumers; ++c) {
      threads.emplace_back([&, f, mycpu = cpu++] {
        if (pin) bench::pin_to_cpu(mycpu);
        while (!go.load(std::memory_order_acquire)) cpu_relax();
        Message m;
        while (consumed[f].load(std::memory_order_relaxed) < total) {
          if (subs[f]->try_poll(m)) consumed[f].fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
  }
  for (int p = 0; p < publishers; ++p) {
    threads.emplace_back([&, mycpu = cpu++] {
      if (pin) bench::pin_to_cpu(mycpu);
      while (!go.load(std::memory_order_acquire)) cpu_relax();
      for (std::uint64_t i = 0; i < per_pub; ++i) topic->publish(payload);
    });
  }

  const auto start = now_ns();
  go.store(true, std::memory_order_release);
  for (auto& t : threads) t.join();
  const double secs = static_cast<double>(now_ns() - start) / 1e9;

  std::cout << "broker: " << publishers << " publishers, " << consumers
            << " consumers/sub, fanout " << fanout << ", payload " << payload.size() << "B\n";
  bench::print_throughput("publish (to broker)", total, secs);
  bench::print_throughput("deliveries (all subs)", total * fanout, secs);
  for (const auto& s : subs) {
    bench::print_latency(("latency " + s->name()).c_str(), s->latency());
  }
  return 0;
}
