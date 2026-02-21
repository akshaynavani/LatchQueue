#include "latchqueue/net/protocol.hpp"

#include <gtest/gtest.h>

using namespace latchqueue;
using namespace latchqueue::net;

namespace {

// Strips and validates the length prefix, returning the decoded frame.
FrameView parse(const std::string& wire, std::size_t offset = 0) {
  const auto len = decode_length(reinterpret_cast<const unsigned char*>(wire.data() + offset));
  EXPECT_LE(offset + kLengthPrefix + len, wire.size());
  auto f = decode_frame(std::string_view(wire).substr(offset + kLengthPrefix, len));
  EXPECT_TRUE(f.has_value());
  return *f;
}

}  // namespace

TEST(Protocol, PublishRoundTrip) {
  std::string wire;
  encode_publish(wire, 7, 42, true, "payload-bytes");
  const auto f = parse(wire);
  EXPECT_EQ(f.type, FrameType::kPublish);
  EXPECT_EQ(f.request_id, 7u);
  auto b = decode_publish(f.body);
  ASSERT_TRUE(b);
  EXPECT_EQ(b->topic, 42u);
  EXPECT_TRUE(b->want_ack);
  EXPECT_EQ(b->payload, "payload-bytes");
}

TEST(Protocol, SubscribeRoundTrip) {
  std::string wire;
  encode_subscribe(wire, 1, 3, 1, 4096, "risk-engine");
  auto b = decode_subscribe(parse(wire).body);
  ASSERT_TRUE(b);
  EXPECT_EQ(b->topic, 3u);
  EXPECT_EQ(b->policy, 1u);
  EXPECT_EQ(b->capacity, 4096u);
  EXPECT_EQ(b->name, "risk-engine");
}

TEST(Protocol, MessageRoundTrip) {
  Message m;
  m.topic = 9;
  m.header.sequence = 123456789012ull;
  m.header.publish_ts_ns = 555;
  m.set_payload("abc");
  std::string wire;
  encode_message(wire, 4, m);
  const auto f = parse(wire);
  EXPECT_EQ(f.type, FrameType::kMessage);
  EXPECT_EQ(f.request_id, 4u);
  auto b = decode_message(f.body);
  ASSERT_TRUE(b);
  EXPECT_EQ(b->topic, 9u);
  EXPECT_EQ(b->sequence, 123456789012ull);
  EXPECT_EQ(b->publish_ts_ns, 555u);
  EXPECT_EQ(b->payload, "abc");
}

TEST(Protocol, AckAndErrorRoundTrip) {
  std::string wire;
  encode_ack(wire, 5, 0xDEADBEEFCAFEull);
  encode_error(wire, 6, ErrorCode::kUnknownTopic, "nope");

  const auto ack = parse(wire);
  EXPECT_EQ(ack.type, FrameType::kAck);
  EXPECT_EQ(*decode_ack(ack.body), 0xDEADBEEFCAFEull);

  const std::size_t second = kLengthPrefix + decode_length(
                                 reinterpret_cast<const unsigned char*>(wire.data()));
  const auto err = parse(wire, second);
  EXPECT_EQ(err.type, FrameType::kError);
  auto e = decode_error(err.body);
  ASSERT_TRUE(e);
  EXPECT_EQ(e->code, ErrorCode::kUnknownTopic);
  EXPECT_EQ(e->message, "nope");
}

TEST(Protocol, TruncatedBodiesAreRejected) {
  EXPECT_FALSE(decode_frame("ab").has_value());
  EXPECT_FALSE(decode_publish(std::string_view("\x01\x00", 2)).has_value());
  EXPECT_FALSE(decode_subscribe(std::string_view("\x01\x00\x00\x00\x00", 5)).has_value());
  EXPECT_FALSE(decode_ack("1234").has_value());
}

TEST(Protocol, LittleEndianEncoding) {
  std::string out;
  Writer w(out);
  w.u32(0x01020304);
  ASSERT_EQ(out.size(), 4u);
  EXPECT_EQ(static_cast<unsigned char>(out[0]), 0x04);
  EXPECT_EQ(static_cast<unsigned char>(out[3]), 0x01);
}
