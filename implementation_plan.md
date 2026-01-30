# LatchQueue — Implementation Plan

**LatchQueue** is a low-latency message queue (broker) written in C++17. Its hot path is
a set of lock-free queues. A Boost.Asio binary TCP protocol is the data plane, and a gRPC
service handles control and management (plus a convenience publish/subscribe API).

Target: 2M+ messages/second on the in-process path, with sub-microsecond enqueue/dequeue
latency, using bounded lock-free MPMC ring buffers.

## Key design decisions

| Decision | Choice | Rationale |
|---|---|---|
| Queue algorithm | Bounded MPMC ring buffer with per-slot sequence numbers (Vyukov style) | No locks or CAS loops on shared head and tail for each element after claiming a slot. Fixed memory, cache-friendly. |
| Secondary queue | SPSC ring (Lamport with cached indices) | Lets each session buffer outbound frames without contention. |
| False sharing | `alignas(64)` padded head/tail/slots | Producers and consumers don't fight over cache lines. |
| Waiting strategy | Pluggable: busy-spin, spin-then-yield, spin-then-park (C++20-free futex emulation with condvar fallback) | Callers choose between latency and CPU use. |
| Message payload | Fixed-capacity inline buffer (`Message` with up to 240 bytes inline), so it is trivially copyable | No allocation on the hot path. |
| Data plane | Boost.Asio, length-prefixed binary frames, one `io_context` per thread | Low overhead, async, no HTTP/2 framing cost. |
| Control plane | gRPC (`latchqueue.v1.Broker`): CreateTopic, ListTopics, Publish, Subscribe (server stream), GetStats | Easy to use from any language, and good for tooling. |
| Fan-out model | Each topic has N subscription queues. A publish copies into every subscription's MPMC queue. Consumers in one subscription compete (work-queue semantics). | This is the familiar pub/sub plus consumer-group model. |
| Metrics | Lock-free counters plus an HDR-style log-bucketed latency histogram | Cheap to record on the hot path. |
| Build | CMake ≥ 3.16, `find_package(Boost)`, `find_package(gRPC)`/`Protobuf`, GoogleTest via FetchContent | Standard toolchain. |

## Phases

### Phase 1: Core lock-free primitives and build skeleton
Deliverables:
- CMake project with options for building the tests, benchmarks and gRPC parts.
- `CachePadded<T>`, CPU relax/pause helper.
- `MpmcQueue<T>`: bounded, power-of-two capacity, `try_push`/`try_pop`, plus blocking `push`/`pop` built on a wait strategy.
- `SpscQueue<T>`.
- Wait strategies (`BusySpin`, `SpinYield`, `SpinPark`).
- `Message` POD with header (topic id, sequence, timestamp, size) and inline payload.

Files: `CMakeLists.txt`, `include/latchqueue/{cache_padded.hpp,cpu.hpp,mpmc_queue.hpp,spsc_queue.hpp,wait_strategy.hpp,message.hpp,clock.hpp}`

### Phase 2: Broker core (topics, subscriptions, metrics)
Deliverables:
- `Subscription`: owns an MPMC queue and tracks delivered and dropped counts.
- `Topic`: an atomic sequence and a copy-on-write subscription list, with fan-out publish.
- `Broker`: topic registry (with a shared mutex used only off the hot path), create/get/list.
- Overflow policies: `DropNewest`, `Block`.
- `LatencyHistogram` and `Stats` snapshot.

Files: `include/latchqueue/{subscription.hpp,topic.hpp,broker.hpp,histogram.hpp,stats.hpp}`, `src/{topic.cpp,broker.cpp,histogram.cpp}`

### Phase 3: Boost.Asio TCP data plane
Deliverables:
- Wire protocol: `[u32 len][u8 type][payload]` frames for HELLO, CREATE, PUBLISH, SUBSCRIBE, MESSAGE, ACK, ERROR.
- `Session`: async read loop, frame decoding, and a write queue for outbound frames.
- `TcpServer`: acceptor plus an `io_context` thread pool.
- `Delivery pump`: drains subscription queues into sessions.

Files: `include/latchqueue/net/{protocol.hpp,session.hpp,tcp_server.hpp}`, `src/net/{protocol.cpp,session.cpp,tcp_server.cpp}`

### Phase 4: gRPC service and server binary
Deliverables:
- `proto/latchqueue.proto` (`Broker` service).
- `GrpcService` implementation that bridges to `Broker`. Subscribe is a server-streaming RPC that pops from a subscription.
- `latchqueue_server` main: config through CLI flags and env vars, starts the TCP and gRPC servers, and shuts down cleanly on SIGINT/SIGTERM.

Files: `proto/latchqueue.proto`, `include/latchqueue/rpc/grpc_service.hpp`, `src/rpc/grpc_service.cpp`, `src/server_main.cpp`, `src/config.{hpp,cpp}`

### Phase 5: Client, tests and benchmarks
Deliverables:
- Synchronous Asio TCP client library and a `lq_cli` tool (pub/sub).
- GoogleTest unit tests: MPMC correctness (multi-producer/multi-consumer sums), SPSC ordering, broker fan-out, protocol encode/decode.
- Benchmarks: MPMC throughput (msgs/s) and latency percentiles (p50/p99/p99.9). Also an end-to-end TCP benchmark.
- An `.env.example`.

Files: `include/latchqueue/client/tcp_client.hpp`, `src/client/tcp_client.cpp`, `tools/lq_cli.cpp`, `tests/*.cpp`, `bench/*.cpp`

## Out of scope
Persistence/replication, authentication and TLS. These are listed in the README as future work.
