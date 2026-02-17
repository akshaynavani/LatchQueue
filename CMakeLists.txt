cmake_minimum_required(VERSION 3.16)
project(LatchQueue VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(LQ_BUILD_TESTS "Build unit tests" ON)
option(LQ_BUILD_BENCH "Build benchmarks" ON)
option(LQ_BUILD_GRPC  "Build the gRPC control plane" ON)
option(LQ_NATIVE      "Compile with -march=native" OFF)

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Header-only lock-free primitives
# ---------------------------------------------------------------------------
add_library(latchqueue_primitives INTERFACE)
target_include_directories(latchqueue_primitives INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries(latchqueue_primitives INTERFACE Threads::Threads)
target_compile_options(latchqueue_primitives INTERFACE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>)
if(LQ_NATIVE)
  target_compile_options(latchqueue_primitives INTERFACE -march=native)
endif()

# ---------------------------------------------------------------------------
# Broker core
# ---------------------------------------------------------------------------
add_library(latchqueue_core
  src/histogram.cpp
  src/topic.cpp
  src/broker.cpp)
target_link_libraries(latchqueue_core PUBLIC latchqueue_primitives)

# ---------------------------------------------------------------------------
# Boost.Asio TCP data plane
# ---------------------------------------------------------------------------
find_package(Boost 1.74 REQUIRED COMPONENTS system)

add_library(latchqueue_net
  src/net/protocol.cpp
  src/net/delivery_pump.cpp
  src/net/session.cpp
  src/net/tcp_server.cpp)
target_link_libraries(latchqueue_net PUBLIC latchqueue_core Boost::system)
target_compile_definitions(latchqueue_net PUBLIC BOOST_ASIO_NO_DEPRECATED)
