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

#include <pthread.h>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::thread;
using std::unique_ptr;


namespace kudu {
namespace security {


class TlsSocketTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    ASSERT_OK(client_tls_.Init());
    ASSERT_OK(server_tls_.Init());
    ASSERT_OK(server_tls_.GenerateSelfSignedCertAndKey());
  }

 protected:
  TlsContext client_tls_;
  TlsContext server_tls_;
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

void handler(int /* signal */) {}

// Test for failures to handle EINTR during TLS connection
// negotiation and data send/receive.
TEST_F(TlsSocketTest, TestTlsSocketInterrupted) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  Sockaddr listen_addr;
  ASSERT_OK(listen_addr.ParseString("127.0.0.1", 0));
  Socket listener;
  ASSERT_OK(listener.Init(0));
  ASSERT_OK(listener.BindAndListen(listen_addr, /*listen_queue_size=*/10));
  ASSERT_OK(listener.GetSocketAddress(&listen_addr));

  // Set up a no-op signal handler for SIGUSR2.
  struct sigaction sa, sa_old;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = &handler;
  sigaction(SIGUSR2, &sa, &sa_old);
  auto sig_cleanup = MakeScopedCleanup([&]() { sigaction(SIGUSR2, &sa_old, nullptr); });

  // Size is big enough to not fit into output socket buffer of default size
  // (controlled by setsockopt() with SO_SNDBUF).
  constexpr size_t kSize = 32 * 1024 * 1024;

  pthread_t server_tid;
  CountDownLatch server_tid_sync(1);
  std::atomic<bool> stop { false };
  thread server([&] {
      server_tid = pthread_self();
      server_tid_sync.CountDown();
      unique_ptr<Socket> sock(new Socket());
      Sockaddr remote;
      CHECK_OK(listener.Accept(sock.get(), &remote, /*flags=*/0));

      TlsHandshake server;
      CHECK_OK(server_tls_.InitiateHandshake(TlsHandshakeType::SERVER, &server));
      CHECK_OK(DoNegotiationSide(sock.get(), &server, "server"));
      CHECK_OK(server.Finish(&sock));

      CHECK_OK(sock->SetRecvTimeout(kTimeout));
      unique_ptr<uint8_t[]> buf(new uint8_t[kSize]);
      // An "echo" loop for kSize byte buffers.
      while (!stop) {
        size_t n;
        Status s = sock->BlockingRecv(buf.get(), kSize, &n, MonoTime::Now() + kTimeout);
        if (s.ok()) {
          size_t written;
          s = sock->BlockingWrite(buf.get(), n, &written, MonoTime::Now() + kTimeout);
        }
        if (!s.ok()) {
          CHECK(stop) << "unexpected error: " << s.ToString();
        }
      }
    });
  auto server_cleanup = MakeScopedCleanup([&]() { server.join(); });

  // Start a thread to send signals to the server thread.
  thread killer([&]() {
    server_tid_sync.Wait();
    while (!stop) {
      PCHECK(pthread_kill(server_tid, SIGUSR2) == 0);
      SleepFor(MonoDelta::FromMicroseconds(rand() % 10));
    }
  });
  auto killer_cleanup = MakeScopedCleanup([&]() { killer.join(); });

  unique_ptr<Socket> client_sock(new Socket());
  ASSERT_OK(client_sock->Init(0));
  ASSERT_OK(client_sock->Connect(listen_addr));

  TlsHandshake client;
  ASSERT_OK(client_tls_.InitiateHandshake(TlsHandshakeType::CLIENT, &client));
  ASSERT_OK(DoNegotiationSide(client_sock.get(), &client, "client"));
  ASSERT_OK(client.Finish(&client_sock));

  unique_ptr<uint8_t[]> buf(new uint8_t[kSize]);
  for (int i = 0; i < 10; i++) {
    SleepFor(MonoDelta::FromMilliseconds(1));
    size_t nwritten;
    ASSERT_OK(client_sock->BlockingWrite(buf.get(), kSize, &nwritten,
        MonoTime::Now() + kTimeout));
    size_t n;
    ASSERT_OK(client_sock->BlockingRecv(buf.get(), kSize, &n,
        MonoTime::Now() + kTimeout));
  }
  stop = true;
  ASSERT_OK(client_sock->Close());

  LOG(INFO) << "client done";
}

} // namespace security
} // namespace kudu
