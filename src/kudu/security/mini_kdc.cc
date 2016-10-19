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

#include "kudu/security/mini_kdc.h"

#include <csignal>
#include <stdlib.h>

#include <limits>
#include <map>
#include <memory>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::map;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {

string MiniKdcOptions::ToString() const {
  return strings::Substitute("{ realm: $0, port: $1, data_root: $2 }", realm, port, data_root);
}

MiniKdc::MiniKdc()
    : MiniKdc(MiniKdcOptions()) {
}

MiniKdc::MiniKdc(const MiniKdcOptions& options)
    : options_(options) {
  if (options_.realm.empty()) {
    options_.realm = "KRBTEST.COM";
  }
  if (options_.data_root.empty()) {
    options_.data_root = JoinPathSegments(GetTestDataDirectory(), "krb5kdc");
  }
}

MiniKdc::~MiniKdc() {
  if (kdc_process_) {
    WARN_NOT_OK(Stop(), "Unable to stop MiniKdc");
  }
}

map<string, string> MiniKdc::GetEnvVars() const {
  return {
    {"KRB5_CONFIG", JoinPathSegments(options_.data_root, "krb5.conf")},
    {"KRB5_KDC_PROFILE", JoinPathSegments(options_.data_root, "kdc.conf")},
    {"KRB5CCNAME", "DIR:" + JoinPathSegments(options_.data_root, "krb5cc")}
  };
}

vector<string> MiniKdc::MakeArgv(const vector<string>& in_argv) {
  vector<string> real_argv = { "env" };
  for (const auto& p : GetEnvVars()) {
    real_argv.push_back(Substitute("$0=$1", p.first, p.second));
  }
  for (const string& a : in_argv) {
    real_argv.push_back(a);
  }
  return real_argv;
}

namespace {
// Attempts to find the path to the specified Kerberos binary, storing it in 'path'.
Status GetBinaryPath(const string& binary,
                     const vector<string>& search,
                     string* path) {
  string p;

  // First, check specified locations which are sometimes not on the PATH.
  // This is necessary to check first so that the system Heimdal kerberos
  // binaries won't be found first on OS X.
  for (const auto& location : search) {
    p = JoinPathSegments(location, binary);
    if (Env::Default()->FileExists(p)) {
      *path = p;
      return Status::OK();
    }
  }

  // Next check if the binary is on the PATH.
  Status s = Subprocess::Call({ "which", binary }, "", &p);
  if (s.ok()) {
    StripTrailingNewline(&p);
    *path = p;
    return Status::OK();
  }

  return Status::NotFound("Unable to find binary", binary);
}

// Attempts to find the path to the specified Kerberos binary, storing it in 'path'.
Status GetBinaryPath(const string& binary, string* path) {
  static const vector<string> kCommonLocations = {
    "/usr/local/opt/krb5/sbin", // Homebrew
    "/usr/local/opt/krb5/bin", // Homebrew
    "/opt/local/sbin", // Macports
    "/opt/local/bin", // Macports
    "/usr/lib/mit/sbin", // SLES
    "/usr/sbin", // Linux
  };
  return GetBinaryPath(binary, kCommonLocations, path);
}
} // namespace


Status MiniKdc::Start() {
  CHECK(!kdc_process_);
  VLOG(1) << "Starting Kerberos KDC: " << options_.ToString();

  if (!Env::Default()->FileExists(options_.data_root)) {
    RETURN_NOT_OK(Env::Default()->CreateDir(options_.data_root));

    RETURN_NOT_OK(CreateKdcConf());
    RETURN_NOT_OK(CreateKrb5Conf());

    // Create the KDC database using the kdb5_util tool.
    string kdb5_util_bin;
    RETURN_NOT_OK(GetBinaryPath("kdb5_util", &kdb5_util_bin));

    RETURN_NOT_OK(Subprocess::Call(MakeArgv({
        kdb5_util_bin, "create",
        "-s", // Stash the master password.
        "-P", "masterpw", // Set a password.
        "-W", // Use weak entropy (since we don't need real security).
    })));
  }

  // Start the Kerberos KDC.
  string krb5kdc_bin;
  RETURN_NOT_OK(GetBinaryPath("krb5kdc", &krb5kdc_bin));

  kdc_process_.reset(new Subprocess(
      "env", MakeArgv({
      krb5kdc_bin,
      "-n", // Do not daemonize.
  })));

  RETURN_NOT_OK(kdc_process_->Start());

  // If we asked for an ephemeral port, grab the actual ports and
  // rewrite the configuration so that clients can connect.
  if (options_.port == 0) {
    RETURN_NOT_OK(WaitForKdcPorts());
    RETURN_NOT_OK(CreateKrb5Conf());
    RETURN_NOT_OK(CreateKdcConf());
  }

  return Status::OK();
}

Status MiniKdc::Stop() {
  CHECK(kdc_process_);
  VLOG(1) << "Stopping KDC";
  unique_ptr<Subprocess> proc(kdc_process_.release());
  RETURN_NOT_OK(proc->Kill(SIGKILL));
  RETURN_NOT_OK(proc->Wait());

  return Status::OK();
}

// Creates a kdc.conf file according to the provided options.
Status MiniKdc::CreateKdcConf() const {
  static const string kFileTemplate = R"(
[kdcdefaults]
kdc_ports = ""
kdc_tcp_ports = $2

[realms]
$1 = {
        acl_file = $0/kadm5.acl
        admin_keytab = $0/kadm5.keytab
        database_name = $0/principal
        key_stash_file = $0/.k5.$1
        max_renewable_life = 7d 0h 0m 0s
}
  )";
  string file_contents = strings::Substitute(kFileTemplate, options_.data_root,
                                             options_.realm, options_.port);
  return WriteStringToFile(Env::Default(), file_contents,
                           JoinPathSegments(options_.data_root, "kdc.conf"));
}

// Creates a krb5.conf file according to the provided options.
Status MiniKdc::CreateKrb5Conf() const {
  static const string kFileTemplate = R"(
[logging]
    kdc = STDERR

[libdefaults]
    default_realm = $1
    dns_lookup_kdc = false
    dns_lookup_realm = false
    forwardable = true
    renew_lifetime = 7d
    ticket_lifetime = 24h

    # In miniclusters, we start daemons on local loopback IPs that
    # have no reverse DNS entries. So, disable reverse DNS.
    rdns = false

    # The server side will start its GSSAPI server using the local FQDN.
    # However, in tests, we connect to it via a non-matching loopback IP.
    # This enables us to connect despite that mismatch.
    ignore_acceptor_hostname = true

    # The KDC is configured to only use TCP, so the client should not prefer UDP.
    udp_preference_limit = 0

[realms]
    $1 = {
        kdc = 127.0.0.1:$0
    }
  )";
  string file_contents = strings::Substitute(kFileTemplate, options_.port, options_.realm);
  return WriteStringToFile(Env::Default(), file_contents,
                           JoinPathSegments(options_.data_root, "krb5.conf"));
}

Status MiniKdc::WaitForKdcPorts() {
  // We have to use 'lsof' to figure out which ports the KDC bound to if we
  // requested ephemeral ones. The KDC doesn't log the bound port or expose it
  // in any other fashion, and re-implementing lsof involves parsing a lot of
  // files in /proc/. So, requiring lsof for tests and parsing its output seems
  // more straight-forward. We call lsof in a loop in case the kdc is slow to
  // bind to the ports.

  string lsof;
  RETURN_NOT_OK(GetBinaryPath("lsof", {"/sbin"}, &lsof));

  vector<string> cmd = {
    lsof, "-wbn", "-Fn",
    "-p", std::to_string(kdc_process_->pid()),
    "-a", "-i", "4TCP"};

  string lsof_out;
  for (int i = 1; ; i++) {
    lsof_out.clear();
    Status s = Subprocess::Call(cmd, "", &lsof_out);

    if (s.ok()) {
      StripTrailingNewline(&lsof_out);
      break;
    } else if (i > 10) {
      return s;
    }

    SleepFor(MonoDelta::FromMilliseconds(i * i));
  }

  // The '-Fn' flag gets lsof to output something like:
  //   p19730
  //   n*:41254
  // The first line is the pid, which we already know. The second has the
  // bind address and port.
  vector<string> lines = strings::Split(lsof_out, "\n");
  int32_t port = -1;
  if (lines.size() != 2 ||
      lines[1].substr(0, 3) != "n*:" ||
      !safe_strto32(lines[1].substr(3), &port) ||
      port <= 0) {
    return Status::RuntimeError("unexpected lsof output", lsof_out);
  }
  CHECK(port > 0 && port < std::numeric_limits<uint16_t>::max())
      << "parsed invalid port: " << port;
  options_.port = port;
  VLOG(1) << "Determined bound KDC port: " << options_.port;
  return Status::OK();
}

Status MiniKdc::CreateUserPrincipal(const string& username) {
  string kadmin;
  RETURN_NOT_OK(GetBinaryPath("kadmin.local", &kadmin));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({
          kadmin, "-q", Substitute("add_principal -pw $0 $0", username)})));
  return Status::OK();
}

Status MiniKdc::CreateServiceKeytab(const string& spn,
                                    string* path) {
  string kt_path = spn;
  StripString(&kt_path, "/", '_');
  kt_path = JoinPathSegments(options_.data_root, kt_path) + ".keytab";

  string kadmin;
  RETURN_NOT_OK(GetBinaryPath("kadmin.local", &kadmin));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({
          kadmin, "-q", Substitute("add_principal -randkey $0", spn)})));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({
          kadmin, "-q", Substitute("ktadd -k $0 $1", kt_path, spn)})));
  *path = kt_path;
  return Status::OK();
}

Status MiniKdc::Kinit(const string& username) {
  string kinit;
  RETURN_NOT_OK(GetBinaryPath("kinit", &kinit));
  Subprocess::Call(MakeArgv({ kinit, username }), username);
  return Status::OK();
}

Status MiniKdc::Kdestroy() {
  string kdestroy;
  RETURN_NOT_OK(GetBinaryPath("kdestroy", &kdestroy));
  return Subprocess::Call(MakeArgv({ kdestroy, "-A" }));
}

Status MiniKdc::Klist(string* output) {
  string klist;
  RETURN_NOT_OK(GetBinaryPath("klist", &klist));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({ klist, "-A" }), "", output));
  return Status::OK();
}

Status MiniKdc::KlistKeytab(const string& keytab_path, string* output) {
  string klist;
  RETURN_NOT_OK(GetBinaryPath("klist", &klist));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({ klist, "-k", keytab_path }), "", output));
  return Status::OK();
}

Status MiniKdc::SetKrb5Environment() const {
  if (!kdc_process_) {
    return Status::IllegalState("KDC not started");
  }
  for (const auto& p : GetEnvVars()) {
    CHECK_ERR(setenv(p.first.c_str(), p.second.c_str(), 1 /*overwrite*/));
  }

  return Status::OK();
}

} // namespace kudu
