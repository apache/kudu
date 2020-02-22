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

#pragma once

#include <cstdint>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class Subprocess;

namespace clock {

// This structure represents a set of properties for the 'server' configuration
// directive in the chrony.conf file. All the fields of the structure except
// for the 'address' directly map into corresponding options of the 'server'
// configuration directive (see 'man chrony.conf' for details).
//
// NOTE: the default values for the most configuration options are different
//       from the defaults used in chronyd's config file (chrony.conf)
struct MiniChronydServerOptions {
  // Hostname or IP address of the server.
  //
  // Default: ""
  std::string address;

  // Port number where server listens for NTP requests.
  //
  // Default: 123
  uint16_t port = 123;

  // The minimum interval between requests sent to the server as a power of 2
  // in seconds.
  //
  // Default: -3
  int8_t minpoll = -3;

  // The maximum interval between requests sent to the server as a power of 2
  // in seconds.
  //
  // Default: 2
  int8_t maxpoll = 2;

  // With this option enabled, the interval between the first four requests sent
  // to the server is much less than interval specified by the 'minpoll' option,
  // which allows chronyd to make the first update of the clock shortly after
  // it has started.
  //
  // Default: true
  bool iburst = true;

  // With this option enabled, chronyd will shorten the interval between up to
  // four requests to 2 seconds or less when it cannot get a good measurement
  // from the server.
  //
  // Default: true
  bool burst = true;

  // This option specifies a correction (in seconds) which will be applied to
  // offsets measured with this source. In test scenarios, this is useful for
  // fine-tuning the true time offsets as seen by a client in NTP responses
  // sent from the server. For example, if running multiple chronyd NTP servers
  // that use the local clock as a reference clock, it's possible to get precise
  // distribution of the true time offsets observed by a client.
  //
  // Default: 0.0
  double offset = 0.0;

  // Returns a string representation of the options suitable for debug printing.
  std::string ToString() const;
};

// Options to run MiniChronyd with.
struct MiniChronydOptions {
  // There might be multiple mini_chronyd run by the same test.
  //
  // Default: 0
  size_t index = 0;

  // Directory under which to store all chronyd-related data.
  //
  // Default: "", which auto-generates a unique path for this chronyd.
  // The default may only be used from a gtest unit test.
  std::string data_root;

  // IP address or path Unix domain local socket file used to listen to command
  // packets (issued by chronyc).
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: "", which auto-generates a unique path to Unix domain socket for
  // this chronyd. The default may only be used from a gtest unit test.
  std::string bindcmdaddress;

  // IP address to bind the NTP server to listen and respond to client requests.
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: 127.0.0.1
  std::string bindaddress = "127.0.0.1";

  // Port of the NTP server to listen to and serve requests from clients.
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: 10123 (10000 + the default NTP port).
  uint16_t port = 10123;

  // File to store PID of the chronyd process.
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: "", which auto-generates a unique pid file path for this chronyd.
  // The default may only be used from a gtest unit test.
  std::string pidfile;

  // Whether to run in the 'local reference mode', using the local clock of the
  // machine as a reference clock. With 'local' set to 'true', chronyd is able
  // to operate as NTP server synchronised to real time (from the viewpoint of
  // clients polling it), even if it was never synchronised or the last update
  // of the clock happened a long time ago.
  //
  // Default: true
  bool local = true;

  // Stratum of the server to report when running in local reference mode.
  // This directly maps to chronyd's 'stratum' option of the 'local'
  // configuration directive.
  //
  // Default: 10, which is default when running in local reference mode
  uint8_t local_stratum = 10;

  // NTP servers to use as reference servers for chronyd instance.
  //
  // Default: {} (an empty container)
  std::vector<MiniChronydServerOptions> servers;

  // Returns a string representation of the options suitable for debug printing.
  std::string ToString() const;
};

// MiniChronyd is a wrapper around chronyd NTP daemon running in server-only
// mode (i.e. it doesn't drive the system clock), allowing manual setting of the
// reference true time. MiniChronyd is used in tests as a reference NTP servers
// for the built-in NTP client.
class MiniChronyd {
 public:
  // Structure to represent relevant information from output by
  // 'chronyc serverstats'.
  struct ServerStats {
    int64_t cmd_packets_received;
    int64_t ntp_packets_received;
  };

  // Check that NTP servers with the specified endpoints are seen as a good
  // enough synchronisation source by the reference NTP client (chronyd itself).
  // The client will wait for no more than the specified timeout in seconds
  // for the set reference servers to become a good NTP source.
  // This method returns Status::OK() if the servers look like a good source
  // for clock synchronisation via NTP, even if the offset of the client's clock
  // from the reference clock provided by NTP server(s) is huge.
  static Status CheckNtpSource(const std::vector<HostPort>& servers,
                               int timeout_sec = 3)
      WARN_UNUSED_RESULT;

  // Create a new MiniChronyd with the provided options, or with defaults
  // if the 'options' argument is omitted.
  explicit MiniChronyd(MiniChronydOptions options = {});

  ~MiniChronyd();

  // Return the options which the underlying chronyd is given to start with.
  const MiniChronydOptions& options() const;

  // Get the PID of the chronyd process.
  pid_t pid() const;

  // Get the IP address and port at which the underlying NTP server is listening
  // for incoming requests. Should be called only when NTP server is started.
  HostPort address() const;

  // Start the mini chronyd in server-only mode.
  Status Start() WARN_UNUSED_RESULT;

  // Stop the mini chronyd.
  Status Stop() WARN_UNUSED_RESULT;

  // Sends SIGSTOP signal to the underlying chronyd.
  Status Pause() WARN_UNUSED_RESULT;

  // Sends SIGCONT signal to the underlying chronyd.
  Status Resume() WARN_UNUSED_RESULT;

  // Get NTP server statistics as output by 'chronyc serverstats'.
  Status GetServerStats(ServerStats* stats) const;

  // Manually set the reference time for the underlying chronyd
  // with the precision of 1 second. The input is number of seconds
  // from the beginning of the Epoch.
  Status SetTime(time_t time) WARN_UNUSED_RESULT;

 private:
  friend class MiniChronydTest;

  // Find absolute path to chronyc (chrony's CLI tool),
  // storing the result path in 'path' output parameter.
  static Status GetChronycPath(std::string* path);

  // Find absolute path to chronyd (chrony NTP implementation),
  // storing the result path in 'path' output parameter.
  static Status GetChronydPath(std::string* path);

  // Get absolute path to an executable from the chrony bundle.
  static Status GetPath(const std::string& path_suffix, std::string* abs_path);

  // Correct the ownership of the target path to be compliant with chrony's
  // security constraints.
  static Status CorrectOwnership(const std::string& path);

  // A shortcut to options_.bindcmdaddress: returns the command address
  // for the underlying chronyd, by default that's the absolute path
  // to a Unix socket.
  std::string cmdaddress() const { return options_.bindcmdaddress; }

  // Return absolute path to chronyd's configuration file.
  std::string config_file_path() const;

  // Create a chrony.conf file with server-only mode settings and other options
  // corresponding to MiniChronydOptions in the data root of Kudu mini cluster.
  Status CreateConf() WARN_UNUSED_RESULT;

  // Run chronyc command with arguments as specified by 'args', targeting this
  // chronyd instance.
  Status RunChronyCmd(const std::vector<std::string>& args,
                      std::string* out_stdout = nullptr,
                      std::string* out_stderr = nullptr) const WARN_UNUSED_RESULT;

  MiniChronydOptions options_;
  std::string cmd_socket_dir_;
  std::unique_ptr<Subprocess> process_;
};

} // namespace clock
} // namespace kudu
