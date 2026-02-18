#pragma once

#include <atomic>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "latchqueue.grpc.pb.h"
#include "latchqueue/broker.hpp"

namespace latchqueue::rpc {

// Synchronous gRPC implementation of latchqueue.v1.Broker. Unary calls go
// straight to the in-process Broker; Subscribe runs a polling loop on the
// gRPC worker thread that serves the stream.
class GrpcService final : public v1::Broker::Service {
 public:
  explicit GrpcService(latchqueue::Broker& broker) : broker_(broker) {}

  void begin_shutdown() noexcept { shutting_down_.store(true, std::memory_order_release); }

  grpc::Status CreateTopic(grpc::ServerContext*, const v1::CreateTopicRequest*,
                           v1::TopicInfo*) override;
  grpc::Status DeleteTopic(grpc::ServerContext*, const v1::DeleteTopicRequest*,
                           v1::DeleteTopicResponse*) override;
  grpc::Status ListTopics(grpc::ServerContext*, const v1::ListTopicsRequest*,
                          v1::ListTopicsResponse*) override;
  grpc::Status Publish(grpc::ServerContext*, const v1::PublishRequest*,
                       v1::PublishResponse*) override;
  grpc::Status PublishStream(grpc::ServerContext*, grpc::ServerReader<v1::PublishRequest>*,
                             v1::PublishStreamSummary*) override;
  grpc::Status Subscribe(grpc::ServerContext*, const v1::SubscribeRequest*,
                         grpc::ServerWriter<v1::Delivery>*) override;
  grpc::Status Unsubscribe(grpc::ServerContext*, const v1::UnsubscribeRequest*,
                           v1::UnsubscribeResponse*) override;
  grpc::Status GetStats(grpc::ServerContext*, const v1::GetStatsRequest*,
                        v1::StatsResponse*) override;

 private:
  latchqueue::Broker& broker_;
  std::atomic<bool> shutting_down_{false};
};

// Owns the grpc::Server lifecycle.
class GrpcServer {
 public:
  GrpcServer(latchqueue::Broker& broker, std::string address);
  ~GrpcServer();

  bool start();
  void stop();
  const std::string& address() const noexcept { return address_; }

 private:
  std::string address_;
  GrpcService service_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace latchqueue::rpc
