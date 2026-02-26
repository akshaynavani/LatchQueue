#include "latchqueue/rpc/grpc_service.hpp"

#include <chrono>

namespace latchqueue::rpc {

namespace {

void fill_topic_info(const Topic& topic, v1::TopicInfo* out) {
  out->set_name(topic.name());
  out->set_id(topic.id());
  out->set_published(topic.published());
  for (const auto& sub : *topic.subscriptions()) out->add_subscriptions(sub->name());
}

void fill_latency(const LatencySummary& in, v1::LatencySummary* out) {
  out->set_count(in.count);
  out->set_mean_ns(in.mean_ns);
  out->set_min_ns(in.min_ns);
  out->set_p50_ns(in.p50_ns);
  out->set_p99_ns(in.p99_ns);
  out->set_p999_ns(in.p999_ns);
  out->set_max_ns(in.max_ns);
}

}  // namespace

grpc::Status GrpcService::CreateTopic(grpc::ServerContext*, const v1::CreateTopicRequest* req,
                                      v1::TopicInfo* resp) {
  if (!latchqueue::Broker::valid_topic_name(req->name())) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "topic names are 1-128 chars of [A-Za-z0-9._/-]"};
  }
  auto topic = broker_.create_topic(req->name());
  if (!topic) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "topic limit reached"};
  fill_topic_info(*topic, resp);
  return grpc::Status::OK;
}

grpc::Status GrpcService::DeleteTopic(grpc::ServerContext*, const v1::DeleteTopicRequest* req,
                                      v1::DeleteTopicResponse* resp) {
  resp->set_deleted(broker_.delete_topic(req->name()));
  return grpc::Status::OK;
}

grpc::Status GrpcService::ListTopics(grpc::ServerContext*, const v1::ListTopicsRequest*,
                                     v1::ListTopicsResponse* resp) {
  for (const auto& topic : broker_.list_topics()) fill_topic_info(*topic, resp->add_topics());
  return grpc::Status::OK;
}

grpc::Status GrpcService::Publish(grpc::ServerContext*, const v1::PublishRequest* req,
                                  v1::PublishResponse* resp) {
  auto topic = broker_.get_topic(req->topic());
  if (!topic) return {grpc::StatusCode::NOT_FOUND, "unknown topic: " + req->topic()};
  if (req->payload().size() > kMaxPayload) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "payload exceeds " + std::to_string(kMaxPayload) + " bytes"};
  }
  const PublishResult r = topic->publish(req->payload());
  resp->set_sequence(r.sequence);
  resp->set_delivered_to(r.delivered_to);
  resp->set_dropped_by(r.dropped_by);
  return grpc::Status::OK;
}

grpc::Status GrpcService::PublishStream(grpc::ServerContext*,
                                        grpc::ServerReader<v1::PublishRequest>* reader,
                                        v1::PublishStreamSummary* summary) {
  v1::PublishRequest req;
  // Cache the last topic so a stream to a single topic skips the registry.
  std::shared_ptr<Topic> topic;
  std::uint64_t accepted = 0, rejected = 0, last_seq = 0;
  while (reader->Read(&req)) {
    if (!topic || topic->name() != req.topic()) topic = broker_.get_topic(req.topic());
    if (!topic || req.payload().size() > kMaxPayload) {
      ++rejected;
      continue;
    }
    last_seq = topic->publish(req.payload()).sequence;
    ++accepted;
  }
  summary->set_accepted(accepted);
  summary->set_rejected(rejected);
  summary->set_last_sequence(last_seq);
  return grpc::Status::OK;
}

grpc::Status GrpcService::Subscribe(grpc::ServerContext* ctx, const v1::SubscribeRequest* req,
                                    grpc::ServerWriter<v1::Delivery>* writer) {
  auto topic = broker_.get_topic(req->topic());
  if (!topic) return {grpc::StatusCode::NOT_FOUND, "unknown topic: " + req->topic()};

  const std::string name = req->subscription().empty() ? "default" : req->subscription();
  const auto policy =
      req->policy() == v1::BLOCK ? OverflowPolicy::kBlock : OverflowPolicy::kDropNewest;
  auto sub = topic->subscribe(name, req->capacity(), policy);

  const std::uint64_t limit = req->max_messages();
  std::uint64_t sent = 0;
  Message msg;
  v1::Delivery delivery;
  grpc::WriteOptions batched;
  batched.set_buffer_hint();

  while (!ctx->IsCancelled() && !shutting_down_.load(std::memory_order_acquire) &&
         !sub->closed()) {
    if (!sub->poll(msg)) continue;  // bounded wait, then re-check cancellation
    delivery.set_topic_id(msg.topic);
    delivery.set_sequence(msg.header.sequence);
    delivery.set_publish_ts_ns(msg.header.publish_ts_ns);
    delivery.mutable_payload()->assign(msg.payload, msg.size);
    // Hint gRPC to coalesce writes while the queue still has backlog.
    const bool more = sub->depth() > 0;
    if (!writer->Write(delivery, more ? batched : grpc::WriteOptions())) break;
    if (limit != 0 && ++sent >= limit) break;
  }
  return grpc::Status::OK;
}

grpc::Status GrpcService::Unsubscribe(grpc::ServerContext*, const v1::UnsubscribeRequest* req,
                                      v1::UnsubscribeResponse* resp) {
  auto topic = broker_.get_topic(req->topic());
  if (!topic) return {grpc::StatusCode::NOT_FOUND, "unknown topic: " + req->topic()};
  resp->set_removed(topic->unsubscribe(req->subscription()));
  return grpc::Status::OK;
}

grpc::Status GrpcService::GetStats(grpc::ServerContext*, const v1::GetStatsRequest*,
                                   v1::StatsResponse* resp) {
  const BrokerStats s = broker_.stats();
  resp->set_uptime_ns(s.uptime_ns);
  resp->set_total_published(s.total_published);
  resp->set_total_delivered(s.total_delivered);
  resp->set_total_dropped(s.total_dropped);
  for (const auto& t : s.topics) {
    auto* ts = resp->add_topics();
    ts->set_name(t.name);
    ts->set_id(t.id);
    ts->set_published(t.published);
    for (const auto& sub : t.subscriptions) {
      auto* ss = ts->add_subscriptions();
      ss->set_name(sub.name);
      ss->set_enqueued(sub.enqueued);
      ss->set_delivered(sub.delivered);
      ss->set_dropped(sub.dropped);
      ss->set_depth(sub.depth);
      ss->set_capacity(sub.capacity);
      fill_latency(sub.latency, ss->mutable_latency());
    }
  }
  return grpc::Status::OK;
}

// ---------------------------------------------------------------------------

GrpcServer::GrpcServer(latchqueue::Broker& broker, std::string address)
    : address_(std::move(address)), service_(broker) {}

GrpcServer::~GrpcServer() { stop(); }

bool GrpcServer::start() {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  server_ = builder.BuildAndStart();
  return server_ != nullptr;
}

void GrpcServer::stop() {
  if (!server_) return;
  service_.begin_shutdown();
  // Give streaming Subscribe calls a moment to observe the flag and finish.
  server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
  server_->Wait();
  server_.reset();
}

}  // namespace latchqueue::rpc
