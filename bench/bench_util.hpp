#pragma once

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#include "latchqueue/histogram.hpp"

namespace bench {

// Best-effort CPU pinning (Linux only); pinned threads give far more stable
// tail latency numbers.
inline void pin_to_cpu(int cpu) {
#if defined(__linux__)
  if (cpu < 0) return;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(static_cast<unsigned>(cpu) % std::thread::hardware_concurrency(), &set);
  pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
  (void)cpu;
#endif
}

inline long arg_int(int argc, char** argv, const char* name, long def) {
  for (int i = 1; i + 1 < argc; ++i) {
    if (std::strcmp(argv[i], name) == 0) return std::strtol(argv[i + 1], nullptr, 10);
  }
  return def;
}

inline bool arg_flag(int argc, char** argv, const char* name) {
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], name) == 0) return true;
  }
  return false;
}

inline std::string arg_str(int argc, char** argv, const char* name, const char* def) {
  for (int i = 1; i + 1 < argc; ++i) {
    if (std::strcmp(argv[i], name) == 0) return argv[i + 1];
  }
  return def;
}

inline void print_latency(const char* label, const latchqueue::LatencyHistogram& h) {
  std::cout << std::left << std::setw(28) << label << std::right << " n=" << h.count()
            << "  min=" << h.min() << "ns  p50=" << h.percentile(0.50)
            << "ns  p99=" << h.percentile(0.99) << "ns  p99.9=" << h.percentile(0.999)
            << "ns  max=" << h.max() << "ns\n";
}

inline void print_throughput(const char* label, std::uint64_t messages, double seconds) {
  std::cout << std::left << std::setw(28) << label << std::right << std::fixed
            << std::setprecision(2) << " " << (messages / seconds) / 1e6 << " M msg/s  ("
            << messages << " msgs in " << std::setprecision(3) << seconds << " s)\n";
}

}  // namespace bench
