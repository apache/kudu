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

#include "kudu/util/net/socket.h"

#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {

constexpr size_t kEchoChunkSize = 32 * 1024 * 1024;

class SocketTest : public KuduTest {
 protected:
  Socket listener_;
  Sockaddr listen_addr_;

  void BindAndListen(const string& addr_str) {
    Sockaddr address;
    ASSERT_OK(address.ParseString(addr_str, 0));
    BindAndListen(address);
  }
  void BindAndListenUnix(const string& path) {
    Sockaddr address;
    ASSERT_OK(address.ParseUnixDomainPath(path));
    BindAndListen(address);
  }

  void BindAndListen(const Sockaddr& address) {
    CHECK_OK(listener_.Init(address.family(), 0));
    CHECK_OK(listener_.BindAndListen(address, 0));
    CHECK_OK(listener_.GetSocketAddress(&listen_addr_));
  }

  Socket ConnectToListeningServer() {
    Socket client;
    CHECK_OK(client.Init(listen_addr_.family(), 0));
    CHECK_OK(client.Connect(listen_addr_));
    CHECK_OK(client.SetRecvTimeout(MonoDelta::FromMilliseconds(100)));
    return client;
  }

  void DoTestServerDisconnects(bool accept, const std::string &message) {
    NO_FATALS(BindAndListen("0.0.0.0:0"));
    std::thread t([&]{
      if (accept) {
        Sockaddr new_addr;
        Socket sock;
        CHECK_OK(listener_.Accept(&sock, &new_addr, 0));
        CHECK_OK(sock.Close());
      } else {
        SleepFor(MonoDelta::FromMilliseconds(200));
        CHECK_OK(listener_.Close());
      }
    });

    Socket client = ConnectToListeningServer();
    int n;
    std::unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
    Status s = client.Recv(buf.get(), kEchoChunkSize, &n);

    ASSERT_TRUE(!s.ok());
    ASSERT_TRUE(s.IsNetworkError());
    ASSERT_STR_MATCHES(s.message().ToString(), message);

    t.join();
  }

  void DoUnixSocketTest(const string& path) {
    const string kData = "hello world over a socket";

    NO_FATALS(BindAndListenUnix(path));
    std::thread t(
        [&]{
          Sockaddr new_addr;
          Socket sock;
          CHECK_OK(listener_.Accept(&sock, &new_addr, 0));

          // Test GetPeerAddress from server side.
          Sockaddr peer_addr;
          CHECK_OK(sock.GetPeerAddress(&peer_addr));
          CHECK_EQ("unix:<unnamed>", peer_addr.ToString());

          size_t n_written;
          CHECK_OK(sock.BlockingWrite(
              reinterpret_cast<const uint8_t*>(kData.data()), kData.size(), &n_written,
              MonoTime::Now() + MonoDelta::FromSeconds(10)));
          CHECK_OK(sock.Close());
        });

    Socket client = ConnectToListeningServer();

    // Test GetPeerAddress from client side.
    Sockaddr peer_addr;
    ASSERT_OK(client.GetPeerAddress(&peer_addr));
    EXPECT_EQ("unix:" + path, peer_addr.ToString());

    size_t n;
    char buf[kData.size()];
    ASSERT_OK(client.BlockingRecv(reinterpret_cast<uint8_t*>(buf), kData.size(), &n,
                                  MonoTime::Now() + MonoDelta::FromSeconds(5)));
    t.join();
    ASSERT_OK(client.Close());

    ASSERT_EQ(n, kData.size());
    ASSERT_EQ(string(buf, n), kData);
  }
};

TEST_F(SocketTest, TestRecvReset) {
  DoTestServerDisconnects(false, "recv error from 127.0.0.1:[0-9]+: "
                          "Resource temporarily unavailable");
}

TEST_F(SocketTest, TestRecvEOF) {
  DoTestServerDisconnects(true, "recv got EOF from 127.0.0.1:[0-9]+");
}

TEST_F(SocketTest, TestUnixSocketAbstractNamespace) {
  DoUnixSocketTest(strings::Substitute("@kudu-test-pid-$0", getpid()));
}
TEST_F(SocketTest, TestUnixSocketFilesystemPath) {
  // Use a path in /tmp/ instead of the normal GetTestPath approach because
  // unix domain socket paths are limited in length. The test directory
  // may be too long.
  string path = strings::Substitute("/tmp/kudu-test-pid-$0", getpid());
  SCOPED_CLEANUP({
      unlink(path.c_str());
    });
  DoUnixSocketTest(path);
}

} // namespace kudu
