// lq_cli: small command-line client for the LatchQueue TCP protocol.
//
//   lq_cli [--host H] [--port P] create <topic>
//   lq_cli [--host H] [--port P] pub <topic> <message> [--count N] [--sync]
//   lq_cli [--host H] [--port P] sub <topic> [--group NAME] [--count N] [--block]
//   lq_cli [--host H] [--port P] ping [--count N]

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "latchqueue/client/tcp_client.hpp"
#include "latchqueue/clock.hpp"

namespace {

void usage() {
  std::cerr << "usage: lq_cli [--host H] [--port P] <command> [args]\n"
               "  create <topic>\n"
               "  pub <topic> <message> [--count N] [--sync]\n"
               "  sub <topic> [--group NAME] [--count N] [--block]\n"
               "  ping [--count N]\n"
               "env: LQ_HOST, LQ_TCP_PORT\n";
}

struct Args {
  std::string host = "127.0.0.1";
  std::uint16_t port = 7400;
  std::vector<std::string> positional;
  std::string group = "cli";
  std::uint64_t count = 1;
  bool count_set = false;
  bool sync = false;
  bool block = false;
};

Args parse(int argc, char** argv) {
  Args a;
  if (const char* h = std::getenv("LQ_HOST")) a.host = h;
  if (const char* p = std::getenv("LQ_TCP_PORT")) a.port = static_cast<std::uint16_t>(std::atoi(p));
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        usage();
        std::exit(2);
      }
      return argv[++i];
    };
    if (arg == "--host") a.host = next();
    else if (arg == "--port") a.port = static_cast<std::uint16_t>(std::stoi(next()));
    else if (arg == "--group") a.group = next();
    else if (arg == "--count") { a.count = std::stoull(next()); a.count_set = true; }
    else if (arg == "--sync") a.sync = true;
    else if (arg == "--block") a.block = true;
    else if (arg == "-h" || arg == "--help") { usage(); std::exit(0); }
    else a.positional.push_back(arg);
  }
  return a;
}

}  // namespace

int main(int argc, char** argv) {
  Args a = parse(argc, argv);
  if (a.positional.empty()) {
    usage();
    return 2;
  }
  const std::string& cmd = a.positional[0];

  try {
    latchqueue::client::TcpClient client;
    client.connect(a.host, a.port, "lq_cli");

    if (cmd == "create" && a.positional.size() == 2) {
      const auto id = client.create_topic(a.positional[1]);
      std::cout << "topic '" << a.positional[1] << "' id=" << id << "\n";

    } else if (cmd == "pub" && a.positional.size() == 3) {
      const auto id = client.create_topic(a.positional[1]);
      const std::string& msg = a.positional[2];
      const auto start = latchqueue::now_ns();
      for (std::uint64_t i = 0; i < a.count; ++i) {
        if (a.sync) {
          const auto seq = client.publish_sync(id, msg);
          if (a.count == 1) std::cout << "published seq=" << seq << "\n";
        } else {
          client.publish(id, msg);
        }
      }
      client.flush();
      client.ping();  // round-trip ensures the server has processed everything
      const double secs = static_cast<double>(latchqueue::now_ns() - start) / 1e9;
      std::cout << "published " << a.count << " message(s) in " << secs << "s ("
                << static_cast<std::uint64_t>(a.count / std::max(secs, 1e-9)) << " msg/s)\n";

    } else if (cmd == "sub" && a.positional.size() == 2) {
      const auto id = client.create_topic(a.positional[1]);
      const auto handle = client.subscribe(id, a.group, 0, a.block ? 1 : 0);
      std::cerr << "subscribed to '" << a.positional[1] << "' as '" << a.group
                << "' (handle " << handle << ")\n";
      const std::uint64_t limit = a.count_set ? a.count : 0;
      for (std::uint64_t n = 0; limit == 0 || n < limit; ++n) {
        auto m = client.receive();
        if (!m) break;
        const auto lat = latchqueue::now_ns() - m->publish_ts_ns;
        std::cout << "[" << m->sequence << "] " << m->payload << "  (" << lat << " ns)\n";
      }

    } else if (cmd == "ping") {
      std::vector<std::uint64_t> rtts;
      for (std::uint64_t i = 0; i < a.count; ++i) rtts.push_back(client.ping());
      std::sort(rtts.begin(), rtts.end());
      std::cout << "ping x" << rtts.size() << ": min=" << rtts.front()
                << "ns p50=" << rtts[rtts.size() / 2] << "ns max=" << rtts.back() << "ns\n";

    } else {
      usage();
      return 2;
    }
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
