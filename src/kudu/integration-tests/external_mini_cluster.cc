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

#include "kudu/integration-tests/external_mini_cluster.h"

#include <algorithm>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <unordered_set>

#include "kudu/client/client.h"
#include "kudu/client/master_rpc.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/master/master.proxy.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/async_util.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using kudu::client::internal::ConnectToClusterRpc;
using kudu::master::ListTablesRequestPB;
using kudu::master::ListTablesResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::rpc::RpcController;
using kudu::server::ServerStatusPB;
using kudu::tserver::ListTabletsRequestPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::TabletServerServiceProxy;
using rapidjson::Value;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using strings::Substitute;

typedef ListTabletsResponsePB::StatusAndSchemaPB StatusAndSchemaPB;

namespace kudu {

static const char* const kMasterBinaryName = "kudu-master";
static const char* const kTabletServerBinaryName = "kudu-tserver";
static const char* const kWildcardIpAddr = "0.0.0.0";
static const char* const kLoopbackIpAddr = "127.0.0.1";
static double kProcessStartTimeoutSeconds = 30.0;
static double kTabletServerRegistrationTimeoutSeconds = 15.0;
static double kMasterCatalogManagerTimeoutSeconds = 60.0;

#if defined(__APPLE__)
static ExternalMiniClusterOptions::BindMode kBindMode =
    ExternalMiniClusterOptions::BindMode::LOOPBACK;
#else
static ExternalMiniClusterOptions::BindMode kBindMode =
    ExternalMiniClusterOptions::BindMode::UNIQUE_LOOPBACK;
#endif

ExternalMiniClusterOptions::ExternalMiniClusterOptions()
    : num_masters(1),
      num_tablet_servers(1),
      bind_mode(kBindMode),
      enable_kerberos(false),
      logtostderr(true) {
}

ExternalMiniClusterOptions::~ExternalMiniClusterOptions() {
}

ExternalMiniCluster::ExternalMiniCluster()
  : opts_(ExternalMiniClusterOptions()) {
}

ExternalMiniCluster::ExternalMiniCluster(ExternalMiniClusterOptions opts)
  : opts_(std::move(opts)) {
}

ExternalMiniCluster::~ExternalMiniCluster() {
  Shutdown();
}

Status ExternalMiniCluster::DeduceBinRoot(std::string* ret) {
  string exe;
  RETURN_NOT_OK(Env::Default()->GetExecutablePath(&exe));
  *ret = DirName(exe);
  return Status::OK();
}

Status ExternalMiniCluster::HandleOptions() {
  daemon_bin_path_ = opts_.daemon_bin_path;
  if (daemon_bin_path_.empty()) {
    RETURN_NOT_OK(DeduceBinRoot(&daemon_bin_path_));
  }

  data_root_ = opts_.data_root;
  if (data_root_.empty()) {
    // If they don't specify a data root, use the current gtest directory.
    data_root_ = JoinPathSegments(GetTestDataDirectory(), "minicluster-data");
  }

  return Status::OK();
}

Status ExternalMiniCluster::Start() {
  CHECK(masters_.empty()) << "Masters are not empty (size: " << masters_.size()
      << "). Maybe you meant Restart()?";
  CHECK(tablet_servers_.empty()) << "Tablet servers are not empty (size: "
      << tablet_servers_.size() << "). Maybe you meant Restart()?";
  RETURN_NOT_OK(HandleOptions());

  RETURN_NOT_OK_PREPEND(rpc::MessengerBuilder("minicluster-messenger")
                        .set_num_reactors(1)
                        .set_max_negotiation_threads(1)
                        .Build(&messenger_),
                        "Failed to start Messenger for minicluster");

  Status s = Env::Default()->CreateDir(data_root_);
  if (!s.ok() && !s.IsAlreadyPresent()) {
    RETURN_NOT_OK_PREPEND(s, "Could not create root dir " + data_root_);
  }

  if (opts_.enable_kerberos) {
    kdc_.reset(new MiniKdc(opts_.mini_kdc_options));
    RETURN_NOT_OK(kdc_->Start());
    RETURN_NOT_OK_PREPEND(kdc_->CreateUserPrincipal("test-admin"),
                          "could not create admin principal");
    RETURN_NOT_OK_PREPEND(kdc_->CreateUserPrincipal("test-user"),
                          "could not create user principal");
    RETURN_NOT_OK_PREPEND(kdc_->CreateUserPrincipal("joe-interloper"),
                          "could not create unauthorized principal");

    RETURN_NOT_OK_PREPEND(kdc_->Kinit("test-admin"),
                          "could not kinit as admin");
    RETURN_NOT_OK_PREPEND(kdc_->SetKrb5Environment(),
                          "could not set krb5 client env");
  }

  if (opts_.num_masters != 1) {
    RETURN_NOT_OK_PREPEND(StartDistributedMasters(),
                          "Failed to add distributed masters");
  } else {
    RETURN_NOT_OK_PREPEND(StartSingleMaster(),
                          Substitute("Failed to start a single Master"));
  }

  for (int i = 1; i <= opts_.num_tablet_servers; i++) {
    RETURN_NOT_OK_PREPEND(AddTabletServer(),
                          Substitute("Failed starting tablet server $0", i));
  }
  RETURN_NOT_OK(WaitForTabletServerCount(
                  opts_.num_tablet_servers,
                  MonoDelta::FromSeconds(kTabletServerRegistrationTimeoutSeconds)));

  return Status::OK();
}


void ExternalMiniCluster::ShutdownNodes(ClusterNodes nodes) {
  if (nodes == ClusterNodes::ALL || nodes == ClusterNodes::TS_ONLY) {
    for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
      ts->Shutdown();
    }
  }
  if (nodes == ClusterNodes::ALL || nodes == ClusterNodes::MASTERS_ONLY) {
    for (const scoped_refptr<ExternalMaster>& master : masters_) {
      if (master) {
        master->Shutdown();
      }
    }
  }
}

Status ExternalMiniCluster::Restart() {
  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    if (master && master->IsShutdown()) {
      RETURN_NOT_OK_PREPEND(master->Restart(), "Cannot restart master bound at: " +
                                               master->bound_rpc_hostport().ToString());
    }
  }

  for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
    if (ts->IsShutdown()) {
      RETURN_NOT_OK_PREPEND(ts->Restart(), "Cannot restart tablet server bound at: " +
                                           ts->bound_rpc_hostport().ToString());
    }
  }

  RETURN_NOT_OK(WaitForTabletServerCount(
      tablet_servers_.size(),
      MonoDelta::FromSeconds(kTabletServerRegistrationTimeoutSeconds)));

  return Status::OK();
}

void ExternalMiniCluster::SetDaemonBinPath(string daemon_bin_path) {
  daemon_bin_path_ = std::move(daemon_bin_path);
  for (auto& master : masters_) {
    master->SetExePath(GetBinaryPath(kMasterBinaryName));
  }
  for (auto& ts : tablet_servers_) {
    ts->SetExePath(GetBinaryPath(kTabletServerBinaryName));
  }
}

string ExternalMiniCluster::GetBinaryPath(const string& binary) const {
  CHECK(!daemon_bin_path_.empty());
  return JoinPathSegments(daemon_bin_path_, binary);
}

string ExternalMiniCluster::GetDataPath(const string& daemon_id) const {
  CHECK(!data_root_.empty());
  return JoinPathSegments(JoinPathSegments(data_root_, daemon_id), "data");
}

string ExternalMiniCluster::GetLogPath(const string& daemon_id) const {
  CHECK(!data_root_.empty());
  return JoinPathSegments(JoinPathSegments(data_root_, daemon_id), "logs");
}

namespace {
vector<string> SubstituteInFlags(const vector<string>& orig_flags,
                                 int index) {
  string str_index = strings::Substitute("$0", index);
  vector<string> ret;
  for (const string& orig : orig_flags) {
    ret.push_back(StringReplace(orig, "${index}", str_index, true));
  }
  return ret;
}

} // anonymous namespace

Status ExternalMiniCluster::StartSingleMaster() {
  string daemon_id = "master-0";

  ExternalDaemonOptions opts(opts_.logtostderr);
  opts.messenger = messenger_;
  opts.exe = GetBinaryPath(kMasterBinaryName);
  opts.data_dir = GetDataPath(daemon_id);
  opts.log_dir = GetLogPath(daemon_id);
  opts.extra_flags = SubstituteInFlags(opts_.extra_master_flags, 0);
  scoped_refptr<ExternalMaster> master = new ExternalMaster(opts);
  if (opts_.enable_kerberos) {
    RETURN_NOT_OK_PREPEND(master->EnableKerberos(kdc_.get(), Substitute("$0", kLoopbackIpAddr)),
                          "could not enable Kerberos");
  }

  RETURN_NOT_OK(master->Start());
  masters_.push_back(master);
  return Status::OK();
}

Status ExternalMiniCluster::StartDistributedMasters() {
  int num_masters = opts_.num_masters;

  if (opts_.master_rpc_ports.size() != num_masters) {
    LOG(FATAL) << num_masters << " masters requested, but only " <<
        opts_.master_rpc_ports.size() << " ports specified in 'master_rpc_ports'";
  }

  vector<string> peer_addrs;
  for (int i = 0; i < num_masters; i++) {
    string addr = Substitute("$0:$1", kLoopbackIpAddr, opts_.master_rpc_ports[i]);
    peer_addrs.push_back(addr);
  }
  vector<string> flags = opts_.extra_master_flags;
  flags.push_back("--master_addresses=" + JoinStrings(peer_addrs, ","));
  string exe = GetBinaryPath(kMasterBinaryName);

  // Start the masters.
  for (int i = 0; i < num_masters; i++) {
    string daemon_id = Substitute("master-$0", i);

    ExternalDaemonOptions opts(opts_.logtostderr);
    opts.messenger = messenger_;
    opts.exe = exe;
    opts.data_dir = GetDataPath(daemon_id);
    opts.log_dir = GetLogPath(daemon_id);
    opts.extra_flags = SubstituteInFlags(flags, i);

    scoped_refptr<ExternalMaster> peer = new ExternalMaster(opts, peer_addrs[i]);
    if (opts_.enable_kerberos) {
      RETURN_NOT_OK_PREPEND(peer->EnableKerberos(kdc_.get(), Substitute("$0", kLoopbackIpAddr)),
                            "could not enable Kerberos");
    }
    RETURN_NOT_OK_PREPEND(peer->Start(),
                          Substitute("Unable to start Master at index $0", i));
    masters_.push_back(peer);
  }

  return Status::OK();
}

string ExternalMiniCluster::GetBindIpForTabletServer(int index) const {
  string bind_ip;
  if (opts_.bind_mode == ExternalMiniClusterOptions::UNIQUE_LOOPBACK) {
    pid_t p = getpid();
    CHECK_LE(p, MathLimits<uint16_t>::kMax) << "Cannot run on systems with >16-bit pid";
    bind_ip = Substitute("127.$0.$1.$2", p >> 8, p & 0xff, index);
  } else if (opts_.bind_mode == ExternalMiniClusterOptions::WILDCARD) {
    bind_ip = Substitute("$0", kWildcardIpAddr);
  } else {
    bind_ip = Substitute("$0", kLoopbackIpAddr);
  }
  return bind_ip;
}

Status ExternalMiniCluster::AddTabletServer() {
  CHECK(leader_master() != nullptr)
      << "Must have started at least 1 master before adding tablet servers";

  int idx = tablet_servers_.size();
  string daemon_id = Substitute("ts-$0", idx);

  vector<HostPort> master_hostports;
  for (int i = 0; i < num_masters(); i++) {
    master_hostports.push_back(DCHECK_NOTNULL(master(i))->bound_rpc_hostport());
  }
  string bind_host = GetBindIpForTabletServer(idx);

  ExternalDaemonOptions opts(opts_.logtostderr);
  opts.messenger = messenger_;
  opts.exe = GetBinaryPath(kTabletServerBinaryName);
  opts.data_dir = GetDataPath(daemon_id);
  opts.log_dir = GetLogPath(daemon_id);
  opts.extra_flags = SubstituteInFlags(opts_.extra_tserver_flags, idx);

  scoped_refptr<ExternalTabletServer> ts =
      new ExternalTabletServer(opts, bind_host, master_hostports);
  if (opts_.enable_kerberos) {
    RETURN_NOT_OK_PREPEND(ts->EnableKerberos(kdc_.get(), bind_host),
                          "could not enable Kerberos");
  }

  RETURN_NOT_OK(ts->Start());
  tablet_servers_.push_back(ts);
  return Status::OK();
}

Status ExternalMiniCluster::WaitForTabletServerCount(int count, const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;

  unordered_set<int> masters_to_search;
  for (int i = 0; i < masters_.size(); i++) {
    if (!masters_[i]->IsShutdown()) {
      masters_to_search.insert(i);
    }
  }

  while (true) {
    MonoDelta remaining = deadline - MonoTime::Now();
    if (remaining.ToSeconds() < 0) {
      return Status::TimedOut(Substitute(
          "Timed out waiting for $0 TS(s) to register with all masters", count));
    }

    for (auto iter = masters_to_search.begin(); iter != masters_to_search.end();) {
      master::ListTabletServersRequestPB req;
      master::ListTabletServersResponsePB resp;
      rpc::RpcController rpc;
      rpc.set_timeout(remaining);
      RETURN_NOT_OK_PREPEND(master_proxy(*iter)->ListTabletServers(req, &resp, &rpc),
                            "ListTabletServers RPC failed");
      // ListTabletServers() may return servers that are no longer online.
      // Do a second step of verification to verify that the descs that we got
      // are aligned (same uuid/seqno) with the TSs that we have in the cluster.
      int match_count = 0;
      for (const master::ListTabletServersResponsePB_Entry& e : resp.servers()) {
        for (const scoped_refptr<ExternalTabletServer>& ets : tablet_servers_) {
          if (ets->instance_id().permanent_uuid() == e.instance_id().permanent_uuid() &&
              ets->instance_id().instance_seqno() == e.instance_id().instance_seqno()) {
            match_count++;
            break;
          }
        }
      }
      if (match_count == count) {
        // This master has returned the correct set of tservers.
        iter = masters_to_search.erase(iter);
      } else {
        iter++;
      }
    }

    if (masters_to_search.empty()) {
      // All masters have returned the correct set of tservers.
      LOG(INFO) << count << " TS(s) registered with all masters";
      return Status::OK();
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
}

void ExternalMiniCluster::AssertNoCrashes() {
  vector<ExternalDaemon*> daemons = this->daemons();
  int num_crashes = 0;
  for (ExternalDaemon* d : daemons) {
    if (d->IsShutdown()) continue;
    if (!d->IsProcessAlive()) {
      LOG(ERROR) << "Process with UUID " << d->uuid() << " has crashed";
      num_crashes++;
    }
  }
  ASSERT_EQ(0, num_crashes) << "At least one process crashed";
}

Status ExternalMiniCluster::WaitForTabletsRunning(ExternalTabletServer* ts,
                                                  int min_tablet_count,
                                                  const MonoDelta& timeout) {
  TabletServerServiceProxy proxy(messenger_, ts->bound_rpc_addr());
  ListTabletsRequestPB req;
  ListTabletsResponsePB resp;

  MonoTime deadline = MonoTime::Now() + timeout;
  while (MonoTime::Now() < deadline) {
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.ListTablets(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }

    bool all_running = true;
    for (const StatusAndSchemaPB& status : resp.status_and_schema()) {
      if (status.tablet_status().state() != tablet::RUNNING) {
        all_running = false;
      }
    }

    // We're done if:
    // 1. All the tablets are running, and
    // 2. We've observed as many tablets as we had expected or more.
    if (all_running && resp.status_and_schema_size() >= min_tablet_count) {
      return Status::OK();
    }

    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  return Status::TimedOut(SecureDebugString(resp));
}

Status ExternalMiniCluster::GetLeaderMasterIndex(int* idx) {
  scoped_refptr<ConnectToClusterRpc> rpc;
  Synchronizer sync;
  vector<Sockaddr> addrs;
  Sockaddr leader_master_addr;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);

  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    addrs.push_back(master->bound_rpc_addr());
  }
  const auto& cb = [&](const Status& status,
                       const Sockaddr& leader_master,
                       const master::ConnectToMasterResponsePB& resp) {
    if (status.ok()) {
      leader_master_addr = leader_master;
    }
    sync.StatusCB(status);
  };
  rpc.reset(new ConnectToClusterRpc(cb,
                                    std::move(addrs),
                                    deadline,
                                    MonoDelta::FromSeconds(5),
                                    messenger_));
  rpc->SendRpc();
  RETURN_NOT_OK(sync.Wait());
  bool found = false;
  for (int i = 0; i < masters_.size(); i++) {
    if (masters_[i]->bound_rpc_hostport().port() == leader_master_addr.port()) {
      found = true;
      *idx = i;
      break;
    }
  }
  if (!found) {
    // There is never a situation where this should happen, so it's
    // better to exit with a FATAL log message right away vs. return a
    // Status::IllegalState().
    LOG(FATAL) << "Leader master is not in masters_";
  }
  return Status::OK();
}

ExternalTabletServer* ExternalMiniCluster::tablet_server_by_uuid(const std::string& uuid) const {
  for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
    if (ts->instance_id().permanent_uuid() == uuid) {
      return ts.get();
    }
  }
  return nullptr;
}

int ExternalMiniCluster::tablet_server_index_by_uuid(const std::string& uuid) const {
  for (int i = 0; i < tablet_servers_.size(); i++) {
    if (tablet_servers_[i]->uuid() == uuid) {
      return i;
    }
  }
  return -1;
}

vector<ExternalDaemon*> ExternalMiniCluster::daemons() const {
  vector<ExternalDaemon*> results;
  for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
    results.push_back(ts.get());
  }
  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    results.push_back(master.get());
  }
  return results;
}

std::shared_ptr<rpc::Messenger> ExternalMiniCluster::messenger() const {
  return messenger_;
}

std::shared_ptr<MasterServiceProxy> ExternalMiniCluster::master_proxy() const {
  CHECK_EQ(masters_.size(), 1);
  return master_proxy(0);
}

std::shared_ptr<MasterServiceProxy> ExternalMiniCluster::master_proxy(int idx) const {
  CHECK_LT(idx, masters_.size());
  return std::shared_ptr<MasterServiceProxy>(
      new MasterServiceProxy(messenger_, CHECK_NOTNULL(master(idx))->bound_rpc_addr()));
}

Status ExternalMiniCluster::CreateClient(client::KuduClientBuilder* builder,
                                         client::sp::shared_ptr<client::KuduClient>* client) const {
  client::KuduClientBuilder defaults;
  if (builder == nullptr) {
    builder = &defaults;
  }

  CHECK(!masters_.empty());
  builder->clear_master_server_addrs();
  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    builder->add_master_server_addr(master->bound_rpc_hostport().ToString());
  }
  return builder->Build(client);
}

Status ExternalMiniCluster::SetFlag(ExternalDaemon* daemon,
                                    const string& flag,
                                    const string& value) {
  server::GenericServiceProxy proxy(messenger_, daemon->bound_rpc_addr());

  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(30));
  server::SetFlagRequestPB req;
  server::SetFlagResponsePB resp;
  req.set_flag(flag);
  req.set_value(value);
  req.set_force(true);
  RETURN_NOT_OK_PREPEND(proxy.SetFlag(req, &resp, &controller),
                        "rpc failed");
  if (resp.result() != server::SetFlagResponsePB::SUCCESS) {
    return Status::RemoteError("failed to set flag",
                               SecureShortDebugString(resp));
  }
  return Status::OK();
}

//------------------------------------------------------------
// ExternalDaemon
//------------------------------------------------------------

ExternalDaemon::ExternalDaemon(ExternalDaemonOptions opts)
    : messenger_(std::move(opts.messenger)),
      data_dir_(std::move(opts.data_dir)),
      log_dir_(std::move(opts.log_dir)),
      logtostderr_(opts.logtostderr),
      exe_(std::move(opts.exe)),
      extra_flags_(std::move(opts.extra_flags)) {}

ExternalDaemon::~ExternalDaemon() {
}

Status ExternalDaemon::EnableKerberos(MiniKdc* kdc, const string& bind_host) {
  string spn = "kudu/" + bind_host;
  string ktpath;
  RETURN_NOT_OK_PREPEND(kdc->CreateServiceKeytab(spn, &ktpath),
                        "could not create keytab");
  extra_env_ = kdc->GetEnvVars();
  extra_flags_.push_back(Substitute("--keytab_file=$0", ktpath));
  extra_flags_.push_back(Substitute("--principal=$0", spn));
  extra_flags_.push_back("--rpc_authentication=required");
  extra_flags_.push_back("--superuser_acl=test-admin");
  extra_flags_.push_back("--user_acl=test-user");
  return Status::OK();
}

Status ExternalDaemon::StartProcess(const vector<string>& user_flags) {
  CHECK(!process_);

  vector<string> argv;

  // First the exe for argv[0].
  argv.push_back(exe_);

  // Then all the flags coming from the minicluster framework.
  argv.insert(argv.end(), user_flags.begin(), user_flags.end());

  // Disable fsync to dramatically speed up runtime. This is safe as no tests
  // rely on forcefully cutting power to a machine or equivalent.
  argv.push_back("--never_fsync");

  // Generate smaller RSA keys -- generating a 1024-bit key is faster
  // than generating the default 2048-bit key, and we don't care about
  // strong encryption in tests. Setting it lower (e.g. 512 bits) results
  // in OpenSSL errors RSA_sign:digest too big for rsa key:rsa_sign.c:122
  // since we are using strong/high TLS v1.2 cipher suites, so the minimum
  // size of TLS-related RSA key is 768 bits (due to the usage of
  // the ECDHE-RSA-AES256-GCM-SHA384 suite). However, to work with Java
  // client it's necessary to have at least 1024 bits for certificate RSA key
  // due to Java security policies.
  argv.push_back("--ipki_server_key_size=1024");

  // Disable minidumps by default since many tests purposely inject faults.
  argv.push_back("--enable_minidumps=false");

  // Disable log redaction.
  argv.push_back("--redact=flag");

  // Enable metrics logging.
  argv.push_back("--metrics_log_interval_ms=1000");

  if (logtostderr_) {
    // Ensure that logging goes to the test output and doesn't get buffered.
    argv.push_back("--logtostderr");
    argv.push_back("--logbuflevel=-1");
  }

  // Even if we are logging to stderr, metrics logs and minidumps end up being
  // written based on -log_dir. So, we have to set that too.
  argv.push_back("--log_dir=" + log_dir_);
  RETURN_NOT_OK(env_util::CreateDirsRecursively(Env::Default(), log_dir_));

  // Tell the server to dump its port information so we can pick it up.
  string info_path = JoinPathSegments(data_dir_, "info.pb");
  argv.push_back("--server_dump_info_path=" + info_path);
  argv.push_back("--server_dump_info_format=pb");

  // We use ephemeral ports in many tests. They don't work for production, but are OK
  // in unit tests.
  argv.push_back("--rpc_server_allow_ephemeral_ports");

  // Allow unsafe and experimental flags from tests, since we often use
  // fault injection, etc.
  argv.push_back("--unlock_experimental_flags");
  argv.push_back("--unlock_unsafe_flags");

  // Then the "extra flags" passed into the ctor (from the ExternalMiniCluster
  // options struct). These come at the end so they can override things like
  // web port or RPC bind address if necessary.
  argv.insert(argv.end(), extra_flags_.begin(), extra_flags_.end());

  // A previous instance of the daemon may have run in the same directory. So, remove
  // the previous info file if it's there.
  ignore_result(Env::Default()->DeleteFile(info_path));

  gscoped_ptr<Subprocess> p(new Subprocess(argv));
  p->ShareParentStdout(false);
  p->SetEnvVars(extra_env_);
  string env_str;
  JoinMapKeysAndValues(extra_env_, "=", ",", &env_str);
  LOG(INFO) << "Running " << exe_ << "\n" << JoinStrings(argv, "\n")
            << " with env {" << env_str << "}";
  RETURN_NOT_OK_PREPEND(p->Start(),
                        Substitute("Failed to start subprocess $0", exe_));

  // The process is now starting -- wait for the bound port info to show up.
  Stopwatch sw;
  sw.start();
  bool success = false;
  while (sw.elapsed().wall_seconds() < kProcessStartTimeoutSeconds) {
    if (Env::Default()->FileExists(info_path)) {
      success = true;
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
    int wait_status;
    Status s = p->WaitNoBlock(&wait_status);
    if (s.IsTimedOut()) {
      // The process is still running.
      continue;
    }

    // If the process exited with expected exit status we need to still swap() the process
    // and exit as if it had succeeded.
    if (WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == fault_injection::kExitStatus) {
      process_.swap(p);
      return Status::OK();
    }

    RETURN_NOT_OK_PREPEND(s, Substitute("Failed waiting on $0", exe_));
    string exit_info;
    RETURN_NOT_OK(p->GetExitStatus(nullptr, &exit_info));
    return Status::RuntimeError(exit_info);
  }

  if (!success) {
    ignore_result(p->Kill(SIGKILL));
    return Status::TimedOut(
        Substitute("Timed out after $0s waiting for process ($1) to write info file ($2)",
                   kProcessStartTimeoutSeconds, exe_, info_path));
  }

  status_.reset(new ServerStatusPB());
  RETURN_NOT_OK_PREPEND(pb_util::ReadPBFromPath(Env::Default(), info_path, status_.get()),
                        "Failed to read info file from " + info_path);
  LOG(INFO) << "Started " << exe_ << " as pid " << p->pid();
  VLOG(1) << exe_ << " instance information:\n" << SecureDebugString(*status_);

  process_.swap(p);
  return Status::OK();
}

void ExternalDaemon::SetExePath(string exe) {
  CHECK(IsShutdown()) << "Call Shutdown() before changing the executable path";
  exe_ = std::move(exe);
}

Status ExternalDaemon::Pause() {
  if (!process_) return Status::OK();
  VLOG(1) << "Pausing " << exe_ << " with pid " << process_->pid();
  paused_ = true;
  return process_->Kill(SIGSTOP);
}

Status ExternalDaemon::Resume() {
  if (!process_) return Status::OK();
  VLOG(1) << "Resuming " << exe_ << " with pid " << process_->pid();
  paused_ = false;
  return process_->Kill(SIGCONT);
}

bool ExternalDaemon::IsShutdown() const {
  return process_.get() == nullptr;
}

bool ExternalDaemon::IsProcessAlive() const {
  if (IsShutdown()) {
    return false;
  }
  Status s = process_->WaitNoBlock();
  // If the non-blocking Wait "times out", that means the process
  // is running.
  return s.IsTimedOut();
}

Status ExternalDaemon::WaitForInjectedCrash(const MonoDelta& timeout) const {
  return WaitForCrash(timeout, [](int status) {
      return WIFEXITED(status) && WEXITSTATUS(status) == fault_injection::kExitStatus;
    }, "fault injection");
}

Status ExternalDaemon::WaitForFatal(const MonoDelta& timeout) const {
  return WaitForCrash(timeout, [](int status) {
      return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
    }, "FATAL crash");
}


Status ExternalDaemon::WaitForCrash(const MonoDelta& timeout,
                                    const std::function<bool(int)>& wait_status_predicate,
                                    const char* crash_type_str) const {
  CHECK(process_) << "process not started";
  MonoTime deadline = MonoTime::Now() + timeout;

  int i = 1;
  while (IsProcessAlive() && MonoTime::Now() < deadline) {
    int sleep_ms = std::min(i++ * 10, 200);
    SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
  }

  if (IsProcessAlive()) {
    return Status::TimedOut(Substitute("Process did not crash within $0",
                                       timeout.ToString()));
  }

  // If the process has exited, make sure it exited with the expected status.
  int wait_status;
  RETURN_NOT_OK_PREPEND(process_->WaitNoBlock(&wait_status),
                        "could not get wait status");

  if (!wait_status_predicate(wait_status)) {
    string info_str;
    RETURN_NOT_OK_PREPEND(process_->GetExitStatus(nullptr, &info_str),
                          "could not get description of exit");
    return Status::Aborted(
        Substitute("process exited, but not due to a $0: $1", crash_type_str, info_str));
  }
  return Status::OK();
}

pid_t ExternalDaemon::pid() const {
  return process_->pid();
}

Subprocess* ExternalDaemon::process() const {
  return process_.get();
}

void ExternalDaemon::Shutdown() {
  if (!process_) return;

  // Before we kill the process, store the addresses. If we're told to
  // start again we'll reuse these. Store only the port if the
  // daemons were using wildcard address for binding.
  const string& wildcard_ip = Substitute("$0", kWildcardIpAddr);
  if (get_rpc_bind_address() != wildcard_ip) {
    bound_rpc_ = bound_rpc_hostport();
    bound_http_ = bound_http_hostport();
  } else {
    bound_rpc_.set_host(wildcard_ip);
    bound_rpc_.set_port(bound_rpc_hostport().port());
    bound_http_.set_host(wildcard_ip);
    bound_http_.set_port(bound_http_hostport().port());
  }

  if (IsProcessAlive()) {
    if (!paused_) {
      // In coverage builds, ask the process nicely to flush coverage info
      // before we kill -9 it. Otherwise, we never get any coverage from
      // external clusters.
      FlushCoverage();
      // Similarly, check for leaks in LSAN builds before killing.
      CheckForLeaks();
    }

    LOG(INFO) << "Killing " << exe_ << " with pid " << process_->pid();
    ignore_result(process_->Kill(SIGKILL));
  }
  WARN_NOT_OK(process_->Wait(), "Waiting on " + exe_);
  paused_ = false;
  process_.reset();
}

void ExternalDaemon::FlushCoverage() {
#ifndef COVERAGE_BUILD
  return;
#else
  LOG(INFO) << "Attempting to flush coverage for " << exe_ << " pid " << process_->pid();
  server::GenericServiceProxy proxy(messenger_, bound_rpc_addr());

  server::FlushCoverageRequestPB req;
  server::FlushCoverageResponsePB resp;
  rpc::RpcController rpc;

  rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
  Status s = proxy.FlushCoverage(req, &resp, &rpc);
  if (s.ok() && !resp.success()) {
    s = Status::RemoteError("Server does not appear to be running a coverage build");
  }
  WARN_NOT_OK(s, Substitute("Unable to flush coverage on $0 pid $1", exe_, process_->pid()));
#endif
}

void ExternalDaemon::CheckForLeaks() {
#if defined(__has_feature)
#  if __has_feature(address_sanitizer)
  LOG(INFO) << "Attempting to check leaks for " << exe_ << " pid " << process_->pid();
  server::GenericServiceProxy proxy(messenger_, bound_rpc_addr());
  server::CheckLeaksRequestPB req;
  server::CheckLeaksResponsePB resp;
  rpc::RpcController rpc;

  rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
  Status s = proxy.CheckLeaks(req, &resp, &rpc);
  if (s.ok()) {
    if (!resp.success()) {
      s = Status::RemoteError("Server does not appear to be running an LSAN build");
    } else {
      CHECK(!resp.found_leaks()) << "Found leaks in " << exe_ << " pid " << process_->pid();
    }
  }
  WARN_NOT_OK(s, Substitute("Unable to check leaks on $0 pid $1", exe_, process_->pid()));
#  endif
#endif
}

HostPort ExternalDaemon::bound_rpc_hostport() const {
  CHECK(status_);
  CHECK_GE(status_->bound_rpc_addresses_size(), 1);
  HostPort ret;
  CHECK_OK(HostPortFromPB(status_->bound_rpc_addresses(0), &ret));
  return ret;
}

Sockaddr ExternalDaemon::bound_rpc_addr() const {
  HostPort hp = bound_rpc_hostport();
  vector<Sockaddr> addrs;
  CHECK_OK(hp.ResolveAddresses(&addrs));
  CHECK(!addrs.empty());
  return addrs[0];
}

HostPort ExternalDaemon::bound_http_hostport() const {
  CHECK(status_);
  if (status_->bound_http_addresses_size() == 0) {
    return HostPort();
  }
  HostPort ret;
  CHECK_OK(HostPortFromPB(status_->bound_http_addresses(0), &ret));
  return ret;
}

const NodeInstancePB& ExternalDaemon::instance_id() const {
  CHECK(status_);
  return status_->node_instance();
}

const string& ExternalDaemon::uuid() const {
  CHECK(status_);
  return status_->node_instance().permanent_uuid();
}

Status ExternalDaemon::GetInt64Metric(const MetricEntityPrototype* entity_proto,
                                      const char* entity_id,
                                      const MetricPrototype* metric_proto,
                                      const char* value_field,
                                      int64_t* value) const {
  CHECK(bound_http_hostport().Initialized());
  // Fetch metrics whose name matches the given prototype.
  string url = Substitute(
      "http://$0/jsonmetricz?metrics=$1",
      bound_http_hostport().ToString(),
      metric_proto->name());
  EasyCurl curl;
  faststring dst;
  RETURN_NOT_OK(curl.FetchURL(url, &dst));

  // Parse the results, beginning with the top-level entity array.
  JsonReader r(dst.ToString());
  RETURN_NOT_OK(r.Init());
  vector<const Value*> entities;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), NULL, &entities));
  for (const Value* entity : entities) {
    // Find the desired entity.
    string type;
    RETURN_NOT_OK(r.ExtractString(entity, "type", &type));
    if (type != entity_proto->name()) {
      continue;
    }
    if (entity_id) {
      string id;
      RETURN_NOT_OK(r.ExtractString(entity, "id", &id));
      if (id != entity_id) {
        continue;
      }
    }

    // Find the desired metric within the entity.
    vector<const Value*> metrics;
    RETURN_NOT_OK(r.ExtractObjectArray(entity, "metrics", &metrics));
    for (const Value* metric : metrics) {
      string name;
      RETURN_NOT_OK(r.ExtractString(metric, "name", &name));
      if (name != metric_proto->name()) {
        continue;
      }
      RETURN_NOT_OK(r.ExtractInt64(metric, value_field, value));
      return Status::OK();
    }
  }
  string msg;
  if (entity_id) {
    msg = Substitute("Could not find metric $0.$1 for entity $2",
                     entity_proto->name(), metric_proto->name(),
                     entity_id);
  } else {
    msg = Substitute("Could not find metric $0.$1",
                     entity_proto->name(), metric_proto->name());
  }
  return Status::NotFound(msg);
}

//------------------------------------------------------------
// ScopedResumeExternalDaemon
//------------------------------------------------------------

ScopedResumeExternalDaemon::ScopedResumeExternalDaemon(ExternalDaemon* daemon)
    : daemon_(CHECK_NOTNULL(daemon)) {
}

ScopedResumeExternalDaemon::~ScopedResumeExternalDaemon() {
  daemon_->Resume();
}

//------------------------------------------------------------
// ExternalMaster
//------------------------------------------------------------

ExternalMaster::ExternalMaster(ExternalDaemonOptions opts)
    : ExternalDaemon(std::move(opts)) {
  set_rpc_bind_address(Substitute("$0:0", kLoopbackIpAddr));
}

ExternalMaster::ExternalMaster(ExternalDaemonOptions opts,
                               string rpc_bind_address)
    : ExternalDaemon(std::move(opts)) {
  set_rpc_bind_address(std::move(rpc_bind_address));
}

ExternalMaster::~ExternalMaster() {
}

Status ExternalMaster::Start() {
  vector<string> flags(GetCommonFlags());
  flags.push_back("--webserver_port=0");
  flags.push_back("--rpc_bind_addresses=" + get_rpc_bind_address());
  return StartProcess(flags);
}

Status ExternalMaster::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Master cannot be restarted. Must call Shutdown() first.");
  }

  vector<string> flags(GetCommonFlags());
  if (bound_http_.Initialized()) {
    flags.push_back(Substitute("--webserver_port=$0", bound_http_.port()));
  }
  flags.push_back("--rpc_bind_addresses=" + bound_rpc_.ToString());

  return StartProcess(flags);
}

Status ExternalMaster::WaitForCatalogManager() {
  unique_ptr<MasterServiceProxy> proxy(
      new MasterServiceProxy(messenger_, bound_rpc_addr()));
  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall_seconds() < kMasterCatalogManagerTimeoutSeconds) {
    ListTablesRequestPB req;
    ListTablesResponsePB resp;
    RpcController rpc;
    Status s = proxy->ListTables(req, &resp, &rpc);
    if (s.ok()) {
      if (!resp.has_error()) {
        // This master is the leader and is up and running.
        break;
      } else {
        s = StatusFromPB(resp.error().status());
        if (s.IsIllegalState()) {
          // This master is not the leader but is otherwise up and running.
          break;
        } else if (!s.IsServiceUnavailable()) {
          // Unexpected error from master.
          return s;
        }
      }
    } else if (!s.IsTimedOut() && !s.IsNetworkError()) {
      // Unexpected error from proxy.
      return s;
    }

    // There was some kind of transient network error or the master isn't yet
    // ready. Sleep and retry.
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  if (sw.elapsed().wall_seconds() > kMasterCatalogManagerTimeoutSeconds) {
    return Status::TimedOut(
        Substitute("Timed out after $0s waiting for master ($1) startup",
                   kMasterCatalogManagerTimeoutSeconds,
                   bound_rpc_addr().ToString()));
  }
  return Status::OK();
}

vector<string> ExternalMaster::GetCommonFlags() const {
  return {
    "--fs_wal_dir=" + data_dir_,
    "--fs_data_dirs=" + data_dir_,
    "--webserver_interface=localhost",

    // See the in-line comment for "--ipki_server_key_size" flag in
    // ExternalDaemon::StartProcess() method.
    "--ipki_ca_key_size=1024",

    // As for the TSK keys, 512 bits is the minimum since we are using the SHA256
    // digest for token signing/verification.
    "--tsk_num_rsa_bits=512",
  };
}


//------------------------------------------------------------
// ExternalTabletServer
//------------------------------------------------------------

ExternalTabletServer::ExternalTabletServer(ExternalDaemonOptions opts,
                                           string bind_host,
                                           vector<HostPort> master_addrs)
    : ExternalDaemon(std::move(opts)),
      master_addrs_(HostPort::ToCommaSeparatedString(master_addrs)) {
  set_rpc_bind_address(std::move(bind_host));
}

ExternalTabletServer::~ExternalTabletServer() {
}

Status ExternalTabletServer::Start() {
  vector<string> flags;
  flags.push_back("--fs_wal_dir=" + data_dir_);
  flags.push_back("--fs_data_dirs=" + data_dir_);
  flags.push_back(Substitute("--rpc_bind_addresses=$0:0",
                             get_rpc_bind_address()));
  flags.push_back(Substitute("--local_ip_for_outbound_sockets=$0",
                             get_rpc_bind_address()));
  flags.push_back(Substitute("--webserver_interface=$0",
                             get_rpc_bind_address()));
  flags.push_back("--webserver_port=0");
  flags.push_back("--tserver_master_addrs=" + master_addrs_);
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}

Status ExternalTabletServer::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Tablet server cannot be restarted. Must call Shutdown() first.");
  }
  vector<string> flags;
  flags.push_back("--fs_wal_dir=" + data_dir_);
  flags.push_back("--fs_data_dirs=" + data_dir_);
  flags.push_back("--rpc_bind_addresses=" + bound_rpc_.ToString());
  flags.push_back(Substitute("--local_ip_for_outbound_sockets=$0",
                             get_rpc_bind_address()));
  if (bound_http_.Initialized()) {
    flags.push_back(Substitute("--webserver_port=$0", bound_http_.port()));
    flags.push_back(Substitute("--webserver_interface=$0",
                               bound_http_.host()));
  }
  flags.push_back("--tserver_master_addrs=" + master_addrs_);
  return StartProcess(flags);
}


} // namespace kudu
