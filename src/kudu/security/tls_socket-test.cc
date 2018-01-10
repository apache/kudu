// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/security/tls_handshake.h"

#include <algorithm>
#include <pthread.h>
#include <sched.h>
#include <sys/uio.h>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace security {

const MonoDelta kTimeout = MonoDelta::FromSeconds(10);

// Size is big enough to not fit into output socket buffer of default size
// (controlled by setsockopt() with SO_SNDBUF).
constexpr size_t kEchoChunkSize = 32 * 1024 * 1024;

class TlsSocketTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(client_tls_.Init());
  }

 protected:
  void ConnectClient(const Sockaddr& addr, unique_ptr<Socket>* sock);
  TlsContext client_tls_;
};

Status DoNegotiationSide(Socket* sock, TlsHandshake* tls, const char* side) {
  tls->set_verification_mode(TlsVerificationMode::VERIFY_NONE);

  bool done = false;
  string received;
  while (!done) {
    string to_send;
    Status s = tls->Continue(received, &to_send);
    if (s.ok()) {
      done = true;
    } else if (!s.IsIncomplete()) {
      RETURN_NOT_OK_PREPEND(s, "unexpected tls error");
    }
    if (!to_send.empty()) {
      size_t nwritten;
      auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(10);
      RETURN_NOT_OK_PREPEND(sock->BlockingWrite(
          reinterpret_cast<const uint8_t*>(to_send.data()),
          to_send.size(), &nwritten, deadline),
                            "error sending");
    }

    if (!done) {
      uint8_t buf[1024];
      int32_t n = 0;
      RETURN_NOT_OK_PREPEND(sock->Recv(buf, arraysize(buf), &n),
                            "error receiving");
      received = string(reinterpret_cast<char*>(&buf[0]), n);
    }
  }
  LOG(INFO) << side << ": negotiation complete";
  return Status::OK();
}

void TlsSocketTest::ConnectClient(const Sockaddr& addr, unique_ptr<Socket>* sock) {
  unique_ptr<Socket> client_sock(new Socket());
  ASSERT_OK(client_sock->Init(0));
  ASSERT_OK(client_sock->Connect(addr));

  TlsHandshake client;
  ASSERT_OK(client_tls_.InitiateHandshake(TlsHandshakeType::CLIENT, &client));
  ASSERT_OK(DoNegotiationSide(client_sock.get(), &client, "client"));
  ASSERT_OK(client.Finish(&client_sock));
  *sock = std::move(client_sock);
}

class EchoServer {
 public:
  EchoServer()
      : pthread_sync_(1) {
  }
  ~EchoServer() {
    Stop();
    Join();
  }

  void Start() {
    ASSERT_OK(server_tls_.Init());
    ASSERT_OK(server_tls_.GenerateSelfSignedCertAndKey());
    ASSERT_OK(listen_addr_.ParseString("127.0.0.1", 0));
    ASSERT_OK(listener_.Init(0));
    ASSERT_OK(listener_.BindAndListen(listen_addr_, /*listen_queue_size=*/10));
    ASSERT_OK(listener_.GetSocketAddress(&listen_addr_));

    thread_ = thread([&] {
        pthread_ = pthread_self();
        pthread_sync_.CountDown();
        unique_ptr<Socket> sock(new Socket());
        Sockaddr remote;
        CHECK_OK(listener_.Accept(sock.get(), &remote, /*flags=*/0));

        TlsHandshake server;
        CHECK_OK(server_tls_.InitiateHandshake(TlsHandshakeType::SERVER, &server));
        CHECK_OK(DoNegotiationSide(sock.get(), &server, "server"));
        CHECK_OK(server.Finish(&sock));

        CHECK_OK(sock->SetRecvTimeout(kTimeout));
        unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
        // An "echo" loop for kEchoChunkSize byte buffers.
        while (!stop_) {
          size_t n;
          Status s = sock->BlockingRecv(buf.get(), kEchoChunkSize, &n, MonoTime::Now() + kTimeout);
          if (!s.ok()) {
            CHECK(stop_) << "unexpected error reading: " << s.ToString();
          }

          LOG(INFO) << "server echoing " << n << " bytes";
          size_t written;
          s = sock->BlockingWrite(buf.get(), n, &written, MonoTime::Now() + kTimeout);
          if (!s.ok()) {
            CHECK(stop_) << "unexpected error writing: " << s.ToString();
          }
          if (slow_read_) {
            SleepFor(MonoDelta::FromMilliseconds(10));
          }
        }
      });
  }

  void EnableSlowRead() {
    slow_read_ = true;
  }

  const Sockaddr& listen_addr() const {
    return listen_addr_;
  }

  bool stopped() const {
    return stop_;
  }

  void Stop() {
    stop_ = true;
  }
  void Join() {
    thread_.join();
  }

  const pthread_t& pthread() {
    pthread_sync_.Wait();
    return pthread_;
  }

 private:
  TlsContext server_tls_;
  Socket listener_;
  Sockaddr listen_addr_;
  thread thread_;
  pthread_t pthread_;
  CountDownLatch pthread_sync_;
  std::atomic<bool> stop_ { false };

  bool slow_read_ = false;
};

void handler(int /* signal */) {}

// Test for failures to handle EINTR during TLS connection
// negotiation and data send/receive.
TEST_F(TlsSocketTest, TestTlsSocketInterrupted) {
  // Set up a no-op signal handler for SIGUSR2.
  struct sigaction sa, sa_old;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = &handler;
  sigaction(SIGUSR2, &sa, &sa_old);
  SCOPED_CLEANUP({ sigaction(SIGUSR2, &sa_old, nullptr); });

  EchoServer server;
  NO_FATALS(server.Start());

  // Start a thread to send signals to the server thread.
  thread killer([&]() {
      while (!server.stopped()) {
        PCHECK(pthread_kill(server.pthread(), SIGUSR2) == 0);
        SleepFor(MonoDelta::FromMicroseconds(rand() % 10));
      }
    });
  SCOPED_CLEANUP({ killer.join(); });

  unique_ptr<Socket> client_sock;
  NO_FATALS(ConnectClient(server.listen_addr(), &client_sock));

  unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
  for (int i = 0; i < 10; i++) {
    SleepFor(MonoDelta::FromMilliseconds(1));
    size_t nwritten;
    ASSERT_OK(client_sock->BlockingWrite(buf.get(), kEchoChunkSize, &nwritten,
        MonoTime::Now() + kTimeout));
    size_t n;
    ASSERT_OK(client_sock->BlockingRecv(buf.get(), kEchoChunkSize, &n,
        MonoTime::Now() + kTimeout));
  }
  server.Stop();
  ASSERT_OK(client_sock->Close());
  LOG(INFO) << "client done";
}

// Return an iovec containing the same data as the buffer 'buf' with the length 'len',
// but split into random-sized chunks. The chunks are sized randomly between 1 and
// 'max_chunk_size' bytes.
vector<struct iovec> ChunkIOVec(Random* rng, uint8_t* buf, int len, int max_chunk_size) {
  vector<struct iovec> ret;
  uint8_t* p = buf;
  int rem = len;
  while (rem > 0) {
    int len = rng->Uniform(max_chunk_size) + 1;
    len = std::min(len, rem);
    ret.push_back({p, static_cast<size_t>(len)});
    p += len;
    rem -= len;
  }
  return ret;
}

// Regression test for KUDU-2218, a bug in which Writev would improperly handle
// partial writes in non-blocking mode.
TEST_F(TlsSocketTest, TestNonBlockingWritev) {
  Random rng(GetRandomSeed32());

  EchoServer server;
  server.EnableSlowRead();
  NO_FATALS(server.Start());

  unique_ptr<Socket> client_sock;
  NO_FATALS(ConnectClient(server.listen_addr(), &client_sock));

  unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
  unique_ptr<uint8_t[]> rbuf(new uint8_t[kEchoChunkSize]);
  RandomString(buf.get(), kEchoChunkSize, &rng);

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(client_sock->SetNonBlocking(true));

    // Prepare an IOV with the input data split into a bunch of randomly-sized
    // chunks.
    vector<struct iovec> iov = ChunkIOVec(&rng, buf.get(), kEchoChunkSize, 1024 * 1024);

    // Loop calling writev until the iov is exhausted
    int rem = kEchoChunkSize;
    while (rem > 0) {
      CHECK(!iov.empty()) << rem;
      int32_t n;
      Status s = client_sock->Writev(&iov[0], iov.size(), &n);
      if (Socket::IsTemporarySocketError(s.posix_code())) {
        sched_yield();
        continue;
      }
      ASSERT_OK(s);
      rem -= n;
      ASSERT_GE(n, 0);
      while (n > 0) {
        if (n < iov[0].iov_len) {
          iov[0].iov_len -= n;
          iov[0].iov_base = reinterpret_cast<uint8_t*>(iov[0].iov_base) + n;
          n = 0;
        } else {
          n -= iov[0].iov_len;
          iov.erase(iov.begin());
        }
      }
    }
    LOG(INFO) << "client waiting";

    size_t n;
    ASSERT_OK(client_sock->SetNonBlocking(false));
    ASSERT_OK(client_sock->BlockingRecv(rbuf.get(), kEchoChunkSize, &n,
        MonoTime::Now() + kTimeout));
    LOG(INFO) << "client got response";

    ASSERT_EQ(0, memcmp(buf.get(), rbuf.get(), kEchoChunkSize));
  }

  server.Stop();
  ASSERT_OK(client_sock->Close());
}

} // namespace security
} // namespace kudu
