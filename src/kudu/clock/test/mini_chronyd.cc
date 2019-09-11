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

#include "kudu/clock/test/mini_chronyd.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"
#include "kudu/util/user.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::SkipEmpty;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace clock {

string MiniChronydServerOptions::ToString() const {
  return Substitute(
      "{address: $0,"
      " port: $1,"
      " minpoll: $2,"
      " maxpoll: $3,"
      " iburst: $4,"
      " burst: $5,"
      " offset: $6}",
      address, port, minpoll, maxpoll, iburst, burst, offset);
}

string MiniChronydOptions::ToString() const {
  string servers_str = "[";
  for (const auto& s : servers) {
    servers_str += " " + s.ToString() + ",";
  }
  servers_str += "]";
  return Substitute(
      "{index: $0,"
      " data_root: $1,"
      " bindcmdaddress: $2,"
      " bindaddress: $3,"
      " port: $4,"
      " pidfile: $5,"
      " local: $6,"
      " local_stratum: $7,"
      " servers: $8}",
      index, data_root, bindcmdaddress, bindaddress, port, pidfile,
      local, local_stratum, servers_str);
}

// Check that the specified servers are seen as good enough synchronisation
// source by the reference NTP client (chronyd itself).
Status MiniChronyd::CheckNtpSource(const vector<HostPort>& servers,
                                   int timeout_sec) {
  // The configuration template for chronyd to make it print the offset of the
  // system clock from the NTP time of the specified servers. The NTP client
  // has to latch on the specified NTP servers (i.e. deem them to be a reliable
  // NTP clock source) before printing the offset. In case of successful
  // latching, the offset is printed and chronyd exits with status 0. Otherwise,
  // if the set of servers doesn't appear to be a reliable NTP clock source or
  // other error happens, chronyd exits with non-zero status and prints warning
  // "No suitable source for synchronisation".
  //
  // The shorter polling time intervals allows for faster querying of NTP
  // servers, and using 'initial burst' mode allows for quicker exchange of NTP
  // packets. Also, it's desirable to use the same NTP protocol version that is
  // used by the Kudu built-in NTP client. Some more details:
  //   * 'minpoll' parameter is set to the smallest possible that is supported
  //      by chrony (-6, i.e. 1/64 second)
  //   * 'maxpoll' parameter is set to be at least 2 times longer interval
  //      than 'minpoll' with some margin to accumulate at least 4 samples
  //      with current setting of 'minpoll' interval
  //   * 'iburst' option allows to send first 4 NTP packets in burst.
  //   * 'version' is set to 3 to enforce using NTP v3 since the current
  //     implementation of the Kudu built-in NTP client uses NTPv3 (chrony
  //     supports NTP v4 and would use newer version of protocol otherwise)
  static const string kConfigTemplate =
      "server $0 port $1 maxpoll -1 minpoll -6 iburst version 3\n";

  if (servers.empty()) {
    return Status::InvalidArgument("empty set of NTP server endpoints");
  }
  string cfg;
  for (const auto& hp : servers) {
    cfg += Substitute(kConfigTemplate, hp.host(), hp.port());
  }
  string chronyd_bin;
  RETURN_NOT_OK(MiniChronyd::GetChronydPath(&chronyd_bin));
  const vector<string> cmd_and_args = {
    chronyd_bin,
    "-Q",                               // client-only mode without setting time
    "-f", "/dev/stdin",                 // read config file from stdin
    "-t", std::to_string(timeout_sec),  // timeout for clock synchronisation
  };
  string out_stdout;
  string out_stderr;
  RETURN_NOT_OK_PREPEND(
      Subprocess::Call(cmd_and_args, cfg, &out_stdout, &out_stderr),
      Substitute("failed measure clock offset from reference NTP servers: "
                 "stdout{$0} stderr{$1}",
                 out_stdout, out_stderr));
  return Status::OK();
}

MiniChronyd::MiniChronyd(MiniChronydOptions options)
    : options_(std::move(options)) {
  if (options_.data_root.empty()) {
    options_.data_root = JoinPathSegments(
        GetTestDataDirectory(), Substitute("chrony.$0", options_.index));
  }
  if (options_.pidfile.empty()) {
    options_.pidfile = JoinPathSegments(options_.data_root, "chronyd.pid");
  }
}

MiniChronyd::~MiniChronyd() {
  if (process_) {
    WARN_NOT_OK(Stop(), "unable to stop MiniChronyd");
  }
}

const MiniChronydOptions& MiniChronyd::options() const {
  CHECK(process_) << "must start the chronyd process first";
  return options_;
}

pid_t MiniChronyd::pid() const {
  CHECK(process_) << "must start the chronyd process first";
  return process_->pid();
}

Status MiniChronyd::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, "starting chronyd");
  CHECK(!process_);
  VLOG(1) << "starting chronyd: " << options_.ToString();

  if (!Env::Default()->FileExists(options_.data_root)) {
    VLOG(1) << "creating chronyd configuration file";
    RETURN_NOT_OK(Env::Default()->CreateDir(options_.data_root));
    // The chronyd's implementation puts strict requirements on the ownership
    // of the directories where the runtime data is stored. In some environments
    // (e.g., macOS), the group owner of the newly created directory might be
    // different from the user account's GID.
    RETURN_NOT_OK(CorrectOwnership(options_.data_root));
    RETURN_NOT_OK(CreateConf());
  }

  // Start the chronyd in server-only mode, not detaching from terminal
  // since the Subprocess needs to have the process running in foreground
  // to be able to control it.
  string server_bin;
  RETURN_NOT_OK(GetChronydPath(&server_bin));
  string username;
  RETURN_NOT_OK(GetLoggedInUser(&username));

  process_.reset(new Subprocess({
      server_bin,
      "-f", config_file_path(),
      "-x", // do not drive the system clock (server-only mode)
      "-d", // do not daemonize; print logs into standard out
  }));
  RETURN_NOT_OK(process_->Start());

  static const auto kTimeout = MonoDelta::FromSeconds(1);
  const auto deadline = MonoTime::Now() + kTimeout;
  for (auto i = 0; ; ++i) {
    auto s = GetServerStats(nullptr);
    if (s.ok()) {
      break;
    }
    if (deadline < MonoTime::Now()) {
      return Status::TimedOut(Substitute("failed to contact chronyd in $0",
                                         kTimeout.ToString()));
    }
    SleepFor(MonoDelta::FromMilliseconds(i * 2));
  }
  return Status::OK();
}

Status MiniChronyd::Stop() {
  if (!process_) {
    return Status::OK();
  }
  VLOG(1) << "stopping chronyd";
  unique_ptr<Subprocess> proc = std::move(process_);
  return proc->KillAndWait(SIGTERM);
}

Status MiniChronyd::GetServerStats(ServerStats* stats) const {
  static const string kNtpPacketsKey = "NTP packets received";
  static const string kCmdPacketsKey = "Command packets received";

  string out;
  RETURN_NOT_OK(RunChronyCmd({ "serverstats" }, &out));
  if (stats) {
    ServerStats result;
    bool ntp_packets_key_found = false;
    bool cmd_packets_key_found = false;
    for (StringPiece sp : Split(out, "\n", SkipEmpty())) {
      vector<string> kv = Split(sp, ":", SkipEmpty());
      if (kv.size() != 2) {
        return Status::Corruption(
            Substitute("'$0': unexpected line in serverstats", sp));
      }
      for (auto& str : kv) {
        StripWhiteSpace(&str);
      }
      if (kv[0] == kNtpPacketsKey) {
        int64_t val;
        if (!safe_strto64(kv[1], &val)) {
          return Status::Corruption(
              Substitute("$0: unexpected value for '$1' in serverstats",
                         kv[1], kNtpPacketsKey));
        }
        result.ntp_packets_received = val;
        ntp_packets_key_found = true;
      } else if (kv[0] == kCmdPacketsKey) {
        int64_t val;
        if (!safe_strto64(kv[1], &val)) {
          return Status::Corruption(
              Substitute("$0: unexpected value for '$1' in serverstats",
                         kv[1], kCmdPacketsKey));
        }
        result.cmd_packets_received = val;
        cmd_packets_key_found = true;
      }
    }
    if (!(ntp_packets_key_found && cmd_packets_key_found)) {
      return Status::Corruption("'$0': unexpected serverstats output", out);
    }
    *stats = result;
  }
  return Status::OK();
}

Status MiniChronyd::SetTime(time_t time) {
  char buf[kFastToBufferSize];
  char* time_to_set = FastTimeToBuffer(time, buf);
  return RunChronyCmd({ "settime", time_to_set });
}

// Find absolute path to chronyc (chrony's CLI tool),
// storing the result path in 'path' output parameter.
Status MiniChronyd::GetChronycPath(string* path) {
  return GetPath("chronyc", path);
}

// Find absolute path to chronyd (chrony NTP implementation),
// storing the result path in 'path' output parameter.
Status MiniChronyd::GetChronydPath(string* path) {
  return GetPath("chronyd", path);
}

Status MiniChronyd::GetPath(const string& path_suffix, string* abs_path) {
  Env* env = Env::Default();
  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  auto path = Substitute("$0/$1", DirName(exe), path_suffix);
  string result;
  RETURN_NOT_OK(env->Canonicalize(path, &result));
  if (env->FileExists(result)) {
    *abs_path = std::move(result);
    return Status::OK();
  }
  return Status::NotFound(Substitute("$0: no such file", result));
}

Status MiniChronyd::CorrectOwnership(const string& path) {
  const uid_t uid = getuid();
  const gid_t gid = getgid();
  if (chown(path.c_str(), uid, gid) == -1) {
    int err = errno;
    return Status::IOError(Substitute("chown($0, $1, $2)", path, uid, gid),
                           ErrnoToString(err), err);
  }
  return Status::OK();
}

string MiniChronyd::config_file_path() const {
  return JoinPathSegments(options_.data_root, "chrony.conf");
}

// Creates a chronyd.conf file according to the provided options. The multitude
// of overriden parameters is because it's necessary to run multiple chronyd
// instances on the same node, so all the 'defaults' should be customized
// to avoid conflicts.
Status MiniChronyd::CreateConf() {
  static const string kFileTemplateCommon = R"(
# Override the default user chronyd NTP server starts because the compiled-in
# default 'root' is not suitable when running chronyd in the context of the Kudu
# testing framework. It's also be possible to override this parameter in the
# chronyd's command line, but doing so in the configuration file is cleaner.
user $0

# The IP address to bind the NTP server socket to. By default, chronyd tries
# to bind to all available IP addresses, which is not desirable in case of a
# shared environment where Kudu tests are usually run.
bindaddress $1

# In this case, it's an absolute path to Unix domain socket file that is used
# by the chronyc CLI tool to send commands to chronyd server.
bindcmdaddress $2

# Override the default NTP port 123 since it's necessary to (1) run multiple
# chronyd servers at the same IP address and (2) specify non-privileged port,
# so chronyd is able to bind to the port even if the chronyd process is not run
# with super-user privileges.
port $3

# Absolute path where to store the PID of chronyd process once it's started.
pidfile $4

# The daemon is controlled only via Unix domain socket (see 'bindcmdaddress'),
# no INET network control port is needed. The command control via INET addresses
# is very limited: no need for that if the control via Unix domain socket
# is already enabled.
cmdport 0

# NTP clients from all addresses are allowed to access the NTP server that is
# serving requests as specified by the 'bindaddress' and 'port' directives.
allow all

# Allow setting the time manually using the cronyc CLI utility.
manual
)";

  static const string kFileTemplateLocal = R"(
# Use the local clock as the clock source (usually it's a high precision
# oscillator or a NTP server), and report the stratum as configured.
local stratum $0
)";

  static const string kFileTemplateServers = R"(
# The set of NTP servers to synchronize with.
$0
)";

  if (options_.bindcmdaddress.empty()) {
    // The path to Unix domain socket file cannot be longer than ~100 bytes,
    // so it's necessary to create a directory with shorter absolute path.
    // TODO(aserbin): use some synthetic mount point instead?
    string dir;
    RETURN_NOT_OK(Env::Default()->GetTestDirectory(&dir));
    dir += Substitute("/$0.$1", Env::Default()->NowMicros(), getpid());
    const auto s = Env::Default()->CreateDir(dir);
    if (!s.IsAlreadyPresent() && !s.ok()) {
      return s;
    }
    RETURN_NOT_OK(CorrectOwnership(dir));
    options_.bindcmdaddress = Substitute("$0/chronyd.$1.sock",
                                         dir, options_.index);
  }
  string username;
  RETURN_NOT_OK(GetLoggedInUser(&username));
  auto contents = Substitute(kFileTemplateCommon,
                             username,
                             options_.bindaddress,
                             options_.bindcmdaddress,
                             options_.port,
                             options_.pidfile);
  if (options_.local) {
    contents += Substitute(kFileTemplateLocal, options_.local_stratum);
  }
  if (!options_.servers.empty()) {
    string servers_str;
    for (const auto& server : options_.servers) {
      auto str = Substitute(
          "server $0 port $1 minpoll $2 maxpoll $3 offset $4",
          server.address, server.port, server.minpoll, server.maxpoll, server.offset);
      if (server.iburst) {
        str += " iburst";
      }
      if (server.burst) {
        str += " burst";
      }
      str += "\n";
      servers_str += str;
    }
    contents += Substitute(kFileTemplateServers, servers_str);
  }
  return WriteStringToFile(Env::Default(), contents, config_file_path());
}

Status MiniChronyd::RunChronyCmd(const vector<string>& args,
                                 string* out_stdout,
                                 string* out_stderr) const {
  string chronyc_bin;
  RETURN_NOT_OK(GetChronycPath(&chronyc_bin));
  vector<string> cmd_and_args = { chronyc_bin, "-h", cmdaddress(), };
  std::copy(args.begin(), args.end(), std::back_inserter(cmd_and_args));
  return Subprocess::Call(cmd_and_args, "", out_stdout, out_stderr);
}

} // namespace clock
} // namespace kudu
