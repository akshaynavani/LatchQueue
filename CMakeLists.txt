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
# Asio and Boost.System are header-only since Boost 1.69.
find_package(Boost 1.74 REQUIRED)

# Wire protocol codec, shared by the server and the client.
add_library(latchqueue_protocol src/net/protocol.cpp)
target_link_libraries(latchqueue_protocol PUBLIC latchqueue_primitives)

add_library(latchqueue_net
  src/net/delivery_pump.cpp
  src/net/session.cpp
  src/net/tcp_server.cpp)
target_link_libraries(latchqueue_net PUBLIC latchqueue_core latchqueue_protocol Boost::headers)
target_compile_definitions(latchqueue_net PUBLIC BOOST_ASIO_NO_DEPRECATED)

# ---------------------------------------------------------------------------
# gRPC control plane
# ---------------------------------------------------------------------------
if(LQ_BUILD_GRPC)
  find_package(Protobuf CONFIG REQUIRED)
  find_package(gRPC CONFIG REQUIRED)

  set(LQ_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/proto/latchqueue.proto)
  set(LQ_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
  file(MAKE_DIRECTORY ${LQ_GEN_DIR})
  set(LQ_GEN_SRCS
    ${LQ_GEN_DIR}/latchqueue.pb.cc
    ${LQ_GEN_DIR}/latchqueue.pb.h
    ${LQ_GEN_DIR}/latchqueue.grpc.pb.cc
    ${LQ_GEN_DIR}/latchqueue.grpc.pb.h)

  add_custom_command(
    OUTPUT ${LQ_GEN_SRCS}
    COMMAND $<TARGET_FILE:protobuf::protoc>
      --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
      --cpp_out=${LQ_GEN_DIR}
      --grpc_out=${LQ_GEN_DIR}
      --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
      ${LQ_PROTO}
    DEPENDS ${LQ_PROTO}
    COMMENT "Generating gRPC sources from latchqueue.proto")

  add_library(latchqueue_rpc ${LQ_GEN_SRCS} src/rpc/grpc_service.cpp)
  target_include_directories(latchqueue_rpc PUBLIC ${LQ_GEN_DIR})
  target_link_libraries(latchqueue_rpc PUBLIC latchqueue_core gRPC::grpc++ protobuf::libprotobuf)
endif()

# ---------------------------------------------------------------------------
# Server binary
# ---------------------------------------------------------------------------
add_executable(latchqueue_server src/server_main.cpp src/config.cpp)
target_include_directories(latchqueue_server PRIVATE src)
target_link_libraries(latchqueue_server PRIVATE latchqueue_net)
if(LQ_BUILD_GRPC)
  target_link_libraries(latchqueue_server PRIVATE latchqueue_rpc)
  target_compile_definitions(latchqueue_server PRIVATE LQ_WITH_GRPC=1)
endif()

# ---------------------------------------------------------------------------
# Client library and CLI
# ---------------------------------------------------------------------------
add_library(latchqueue_client src/client/tcp_client.cpp)
target_link_libraries(latchqueue_client PUBLIC latchqueue_protocol Boost::headers)

add_executable(lq_cli tools/lq_cli.cpp)
target_link_libraries(lq_cli PRIVATE latchqueue_client)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(LQ_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()
  include(GoogleTest)

  add_executable(latchqueue_tests
    tests/mpmc_queue_test.cpp
    tests/spsc_queue_test.cpp
    tests/histogram_test.cpp
    tests/broker_test.cpp
    tests/protocol_test.cpp)
  target_link_libraries(latchqueue_tests PRIVATE latchqueue_core latchqueue_protocol GTest::gtest_main)
  gtest_discover_tests(latchqueue_tests)

  add_executable(latchqueue_integration_tests tests/tcp_integration_test.cpp)
  target_link_libraries(latchqueue_integration_tests PRIVATE
    latchqueue_net latchqueue_client GTest::gtest_main)
  gtest_discover_tests(latchqueue_integration_tests)
endif()

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
if(LQ_BUILD_BENCH)
  add_executable(mpmc_bench bench/mpmc_bench.cpp)
  target_link_libraries(mpmc_bench PRIVATE latchqueue_core)

  add_executable(broker_bench bench/broker_bench.cpp)
  target_link_libraries(broker_bench PRIVATE latchqueue_core)

  add_executable(tcp_bench bench/tcp_bench.cpp)
  target_link_libraries(tcp_bench PRIVATE latchqueue_net latchqueue_client)
endif()
