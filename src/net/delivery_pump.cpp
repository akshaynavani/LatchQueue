#include "latchqueue/net/delivery_pump.hpp"

#include <chrono>
#include <string>

#include "latchqueue/cpu.hpp"
#include "latchqueue/net/protocol.hpp"
#include "latchqueue/net/session.hpp"

namespace latchqueue::net {

DeliveryPump::DeliveryPump(DeliveryPumpOptions options) : options_(options) {
  const std::size_t n = options_.threads == 0 ? 1 : options_.threads;
  workers_.reserve(n);
  for (std::size_t i = 0; i < n; ++i) workers_.push_back(std::make_unique<Worker>());
}

DeliveryPump::~DeliveryPump() { stop(); }

void DeliveryPump::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) return;
  for (auto& w : workers_) {
    Worker* worker = w.get();
    worker->thread = std::thread([this, worker] { run(*worker); });
  }
}

void DeliveryPump::stop() {
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) return;
  for (auto& w : workers_) {
    if (w->thread.joinable()) w->thread.join();
  }
}

void DeliveryPump::add(std::shared_ptr<Binding> binding) {
  const std::size_t idx =
      next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
  Worker& w = *workers_[idx];
  {
    std::lock_guard<std::mutex> lock(w.mutex);
    w.incoming.push_back(std::move(binding));
  }
  w.has_incoming.store(true, std::memory_order_release);
}

void DeliveryPump::run(Worker& worker) {
  std::vector<std::shared_ptr<Binding>> local;
  std::string batch;
  batch.reserve(options_.batch_size * 64);
  Message msg;
  std::uint32_t idle_rounds = 0;

  while (running_.load(std::memory_order_acquire)) {
    if (worker.has_incoming.exchange(false, std::memory_order_acq_rel)) {
      std::lock_guard<std::mutex> lock(worker.mutex);
      for (auto& b : worker.incoming) local.push_back(std::move(b));
      worker.incoming.clear();
    }

    bool did_work = false;
    for (auto it = local.begin(); it != local.end();) {
      Binding& b = **it;
      auto session = b.session.lock();
      if (!b.active.load(std::memory_order_acquire) || !session || session->closed() ||
          b.subscription->closed()) {
        it = local.erase(it);
        continue;
      }
      if (session->backlog_bytes() > options_.max_session_backlog) {
        ++it;
        continue;
      }

      std::size_t n = 0;
      while (n < options_.batch_size && b.subscription->try_poll(msg)) {
        encode_message(batch, b.handle, msg);
        ++n;
      }
      if (n > 0) {
        session->send(std::move(batch));
        batch = std::string();
        batch.reserve(options_.batch_size * 64);
        frames_sent_.fetch_add(n, std::memory_order_relaxed);
        did_work = true;
      }
      ++it;
    }

    if (did_work) {
      idle_rounds = 0;
    } else if (idle_rounds < 256) {
      ++idle_rounds;
      cpu_relax();
    } else if (idle_rounds < 1024) {
      ++idle_rounds;
      std::this_thread::yield();
    } else {
      // Fully idle: back off so an idle broker doesn't burn a core.
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  }
}

}  // namespace latchqueue::net
