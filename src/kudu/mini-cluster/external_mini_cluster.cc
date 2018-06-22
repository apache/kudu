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

#include "kudu/mini-cluster/external_mini_cluster.h"

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/master_rpc.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using kudu::client::internal::ConnectToClusterRpc;
using kudu::master::ListTablesRequestPB;
using kudu::master::ListTablesResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcController;
using kudu::server::ServerStatusPB;
using kudu::tserver::ListTabletsRequestPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::TabletServerServiceProxy;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

typedef ListTabletsResponsePB::StatusAndSchemaPB StatusAndSchemaPB;

DECLARE_string(block_manager);

DEFINE_bool(perf_record, false,
            "Whether to run \"perf record --call-graph fp\" on each daemon in the cluster");

namespace kudu {
namespace cluster {

static const char* const kMasterBinaryName = "kudu-master";
static const char* const kTabletServerBinaryName = "kudu-tserver";
static double kTabletServerRegistrationTimeoutSeconds = 15.0;
static double kMasterCatalogManagerTimeoutSeconds = 60.0;

ExternalMiniClusterOptions::ExternalMiniClusterOptions()
    : num_masters(1),
      num_tablet_servers(1),
      bind_mode(MiniCluster::kDefaultBindMode),
      num_data_dirs(1),
      enable_kerberos(false),
      hms_mode(HmsMode::NONE),
      logtostderr(true),
      start_process_timeout(MonoDelta::FromSeconds(30)) {
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

Env* ExternalMiniCluster::env() const {
  return Env::Default();
}

Status ExternalMiniCluster::DeduceBinRoot(std::string* ret) {
  string exe;
  RETURN_NOT_OK(env()->GetExecutablePath(&exe));
  *ret = DirName(exe);
  return Status::OK();
}

Status ExternalMiniCluster::HandleOptions() {
  if (opts_.daemon_bin_path.empty()) {
    RETURN_NOT_OK(DeduceBinRoot(&opts_.daemon_bin_path));
  }

  if (opts_.cluster_root.empty()) {
    // If they don't specify a cluster root, use the current gtest directory.
    opts_.cluster_root = JoinPathSegments(GetTestDataDirectory(), "minicluster-data");
  }

  if (opts_.block_manager_type.empty()) {
    opts_.block_manager_type = FLAGS_block_manager;
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

  Status s = env()->CreateDir(opts_.cluster_root);
  if (!s.ok() && !s.IsAlreadyPresent()) {
    RETURN_NOT_OK_PREPEND(s, "Could not create root dir " + opts_.cluster_root);
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

  if (opts_.hms_mode == HmsMode::ENABLE_HIVE_METASTORE ||
      opts_.hms_mode == HmsMode::ENABLE_METASTORE_INTEGRATION) {
    hms_.reset(new hms::MiniHms());

    if (opts_.enable_kerberos) {
      string spn = "hive/127.0.0.1";
      string ktpath;
      RETURN_NOT_OK_PREPEND(kdc_->CreateServiceKeytab(spn, &ktpath),
                            "could not create keytab");
      hms_->EnableKerberos(kdc_->GetEnvVars()["KRB5_CONFIG"], spn, ktpath,
                           rpc::SaslProtection::kAuthentication);
    }

    RETURN_NOT_OK_PREPEND(hms_->Start(),
                          "Failed to start the Hive Metastore");
  }

  RETURN_NOT_OK_PREPEND(StartMasters(), "failed to start masters");

  for (int i = 1; i <= opts_.num_tablet_servers; i++) {
    RETURN_NOT_OK_PREPEND(AddTabletServer(), Substitute("failed to start tablet server $0", i));
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
      if (opts_.hms_mode == HmsMode::ENABLE_METASTORE_INTEGRATION) {
        master->SetMetastoreIntegration(hms_->uris(), opts_.enable_kerberos);
      }
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

void ExternalMiniCluster::EnableMetastoreIntegration() {
  opts_.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
}

void ExternalMiniCluster::DisableMetastoreIntegration() {
  for (const auto& master : masters_) {
    CHECK(master->IsShutdown()) << "Call Shutdown() before changing the HMS mode";
    master->mutable_flags()->erase(
        std::remove_if(
          master->mutable_flags()->begin(), master->mutable_flags()->end(),
          [] (const string& flag) {
            return StringPiece(flag).starts_with("--hive_metastore");
          }),
        master->mutable_flags()->end());
  }
  opts_.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
}

void ExternalMiniCluster::SetDaemonBinPath(string daemon_bin_path) {
  opts_.daemon_bin_path = std::move(daemon_bin_path);
  for (auto& master : masters_) {
    master->SetExePath(GetBinaryPath(kMasterBinaryName));
  }
  for (auto& ts : tablet_servers_) {
    ts->SetExePath(GetBinaryPath(kTabletServerBinaryName));
  }
}

string ExternalMiniCluster::GetBinaryPath(const string& binary) const {
  CHECK(!opts_.daemon_bin_path.empty());
  return JoinPathSegments(opts_.daemon_bin_path, binary);
}

string ExternalMiniCluster::GetLogPath(const string& daemon_id) const {
  CHECK(!opts_.cluster_root.empty());
  return JoinPathSegments(JoinPathSegments(opts_.cluster_root, daemon_id), "logs");
}

string ExternalMiniCluster::GetDataPath(const string& daemon_id,
                                        boost::optional<uint32_t> dir_index) const {
  CHECK(!opts_.cluster_root.empty());
  string data_path = "data";
  if (dir_index) {
    CHECK_LT(*dir_index, opts_.num_data_dirs);
    data_path = Substitute("$0-$1", data_path, dir_index.get());
  } else {
    CHECK_EQ(1, opts_.num_data_dirs);
  }
  return JoinPathSegments(JoinPathSegments(opts_.cluster_root, daemon_id), data_path);
}

vector<string> ExternalMiniCluster::GetDataPaths(const string& daemon_id) const {
  if (opts_.num_data_dirs == 1) {
    return { GetDataPath(daemon_id) };
  }
  vector<string> paths;
  for (uint32_t dir_index = 0; dir_index < opts_.num_data_dirs; dir_index++) {
    paths.emplace_back(GetDataPath(daemon_id, dir_index));
  }
  return paths;
}

string ExternalMiniCluster::GetWalPath(const string& daemon_id) const {
  CHECK(!opts_.cluster_root.empty());
  return JoinPathSegments(JoinPathSegments(opts_.cluster_root, daemon_id), "wal");
}

namespace {
vector<string> SubstituteInFlags(const vector<string>& orig_flags, int index) {
  string str_index = strings::Substitute("$0", index);
  vector<string> ret;
  for (const string& orig : orig_flags) {
    ret.push_back(StringReplace(orig, "${index}", str_index, true));
  }
  return ret;
}
} // anonymous namespace

Status ExternalMiniCluster::StartMasters() {
  int num_masters = opts_.num_masters;

  // Collect and keep alive the set of master sockets bound with SO_REUSEPORT
  // until all master proccesses are started. This allows the mini-cluster to
  // reserve a set of ports up front, then later start the set of masters, each
  // configured with the full set of ports.
  //
  // TODO(dan): re-bind the ports between node restarts in order to prevent other
  // processess from binding to them in the interim.
  vector<unique_ptr<Socket>> reserved_sockets;
  vector<HostPort> master_rpc_addrs;

  if (!opts_.master_rpc_addresses.empty()) {
    CHECK_EQ(opts_.master_rpc_addresses.size(), num_masters);
    master_rpc_addrs = opts_.master_rpc_addresses;
  } else {
    for (int i = 0; i < num_masters; i++) {
      unique_ptr<Socket> reserved_socket;
      RETURN_NOT_OK_PREPEND(ReserveDaemonSocket(MiniCluster::MASTER, i, opts_.bind_mode,
                                                &reserved_socket),
                            "failed to reserve master socket address");
      Sockaddr addr;
      RETURN_NOT_OK(reserved_socket->GetSocketAddress(&addr));
      master_rpc_addrs.emplace_back(addr.host(), addr.port());
      reserved_sockets.emplace_back(std::move(reserved_socket));
    }
  }

  vector<string> flags = opts_.extra_master_flags;
  flags.emplace_back("--rpc_reuseport=true");
  if (num_masters > 1) {
    flags.emplace_back(Substitute("--master_addresses=$0",
                                  HostPort::ToCommaSeparatedString(master_rpc_addrs)));
  }
  string exe = GetBinaryPath(kMasterBinaryName);

  // Start the masters.
  for (int i = 0; i < num_masters; i++) {
    string daemon_id = Substitute("master-$0", i);

    ExternalDaemonOptions opts;
    opts.messenger = messenger_;
    opts.block_manager_type = opts_.block_manager_type;
    opts.exe = exe;
    opts.wal_dir = GetWalPath(daemon_id);
    opts.data_dirs = GetDataPaths(daemon_id);
    opts.log_dir = GetLogPath(daemon_id);
    if (FLAGS_perf_record) {
      opts.perf_record_filename =
          Substitute("$0/perf-$1.data", opts.log_dir, daemon_id);
    }
    opts.extra_flags = SubstituteInFlags(flags, i);
    opts.start_process_timeout = opts_.start_process_timeout;
    opts.rpc_bind_address = master_rpc_addrs[i];
    if (opts_.hms_mode == HmsMode::ENABLE_METASTORE_INTEGRATION) {
      opts.extra_flags.emplace_back(Substitute("--hive_metastore_uris=$0", hms_->uris()));
      opts.extra_flags.emplace_back(Substitute("--hive_metastore_sasl_enabled=$0",
                                               opts_.enable_kerberos));
    }
    opts.logtostderr = opts_.logtostderr;

    scoped_refptr<ExternalMaster> peer = new ExternalMaster(opts);
    if (opts_.enable_kerberos) {
      RETURN_NOT_OK_PREPEND(peer->EnableKerberos(kdc_.get(), master_rpc_addrs[i].host()),
                            "could not enable Kerberos");
    }
    RETURN_NOT_OK_PREPEND(peer->Start(),
                          Substitute("Unable to start Master at index $0", i));
    masters_.emplace_back(std::move(peer));
  }

  return Status::OK();
}

string ExternalMiniCluster::GetBindIpForTabletServer(int index) const {
  return MiniCluster::GetBindIpForDaemon(MiniCluster::TSERVER, index, opts_.bind_mode);
}

string ExternalMiniCluster::GetBindIpForMaster(int index) const {
  return MiniCluster::GetBindIpForDaemon(MiniCluster::MASTER, index, opts_.bind_mode);
}

Status ExternalMiniCluster::AddTabletServer() {
  CHECK(leader_master() != nullptr)
      << "Must have started at least 1 master before adding tablet servers";

  int idx = tablet_servers_.size();
  string daemon_id = Substitute("ts-$0", idx);

  vector<HostPort> master_hostports = master_rpc_addrs();
  string bind_host = GetBindIpForTabletServer(idx);

  ExternalDaemonOptions opts;
  opts.messenger = messenger_;
  opts.block_manager_type = opts_.block_manager_type;
  opts.exe = GetBinaryPath(kTabletServerBinaryName);
  opts.wal_dir = GetWalPath(daemon_id);
  opts.data_dirs = GetDataPaths(daemon_id);
  opts.log_dir = GetLogPath(daemon_id);
  if (FLAGS_perf_record) {
    opts.perf_record_filename =
        Substitute("$0/perf-$1.data", opts.log_dir, daemon_id);
  }
  opts.extra_flags = SubstituteInFlags(opts_.extra_tserver_flags, idx);
  opts.start_process_timeout = opts_.start_process_timeout;
  opts.rpc_bind_address = HostPort(bind_host, 0);
  opts.logtostderr = opts_.logtostderr;

  scoped_refptr<ExternalTabletServer> ts = new ExternalTabletServer(opts, master_hostports);
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
  TabletServerServiceProxy proxy(messenger_, ts->bound_rpc_addr(), ts->bound_rpc_addr().host());
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
  vector<pair<Sockaddr, string>> addrs_with_names;
  Sockaddr leader_master_addr;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);

  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    addrs_with_names.emplace_back(master->bound_rpc_addr(), master->bound_rpc_addr().host());
  }
  const auto& cb = [&](const Status& status,
                       const pair<Sockaddr, string>& leader_master,
                       const master::ConnectToMasterResponsePB& resp) {
    if (status.ok()) {
      leader_master_addr = leader_master.first;
    }
    sync.StatusCB(status);
  };
  rpc::UserCredentials user_credentials;
  RETURN_NOT_OK(user_credentials.SetLoggedInRealUser());
  rpc.reset(new ConnectToClusterRpc(cb,
                                    std::move(addrs_with_names),
                                    deadline,
                                    MonoDelta::FromSeconds(5),
                                    messenger_,
                                    user_credentials));
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

vector<HostPort> ExternalMiniCluster::master_rpc_addrs() const {
  vector<HostPort> master_hostports;
  for (const auto& master : masters_) {
    master_hostports.emplace_back(master->bound_rpc_hostport());
  }
  return master_hostports;
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
  const auto& addr = CHECK_NOTNULL(master(idx))->bound_rpc_addr();
  return std::make_shared<MasterServiceProxy>(messenger_, addr, addr.host());
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
  const auto& addr = daemon->bound_rpc_addr();
  server::GenericServiceProxy proxy(messenger_, addr, addr.host());

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

string ExternalMiniCluster::WalRootForTS(int ts_idx) const {
  return tablet_server(ts_idx)->wal_dir();
}

string ExternalMiniCluster::UuidForTS(int ts_idx) const {
  return tablet_server(ts_idx)->uuid();
}

//------------------------------------------------------------
// ExternalDaemon
//------------------------------------------------------------

ExternalDaemon::ExternalDaemon(ExternalDaemonOptions opts)
    : opts_(std::move(opts)) {
  CHECK(rpc_bind_address().Initialized());
}

ExternalDaemon::~ExternalDaemon() {
}

Status ExternalDaemon::EnableKerberos(MiniKdc* kdc, const string& bind_host) {
  string spn = "kudu/" + bind_host;
  string ktpath;
  RETURN_NOT_OK_PREPEND(kdc->CreateServiceKeytab(spn, &ktpath),
                        "could not create keytab");
  extra_env_ = kdc->GetEnvVars();

  // Insert Kerberos flags at the front of extra_flags, so that user specified
  // flags will override them.
  opts_.extra_flags.insert(opts_.extra_flags.begin(), {
    Substitute("--keytab_file=$0", ktpath),
    Substitute("--principal=$0", spn),
    "--rpc_authentication=required",
    "--superuser_acl=test-admin",
    "--user_acl=test-user",
  });

  return Status::OK();
}

Status ExternalDaemon::StartProcess(const vector<string>& user_flags) {
  CHECK(!process_);

  vector<string> argv;

  // First the exe for argv[0].
  argv.push_back(opts_.exe);

  // Then all the flags coming from the minicluster framework.
  argv.insert(argv.end(), user_flags.begin(), user_flags.end());

  // Disable fsync to dramatically speed up runtime. This is safe as no tests
  // rely on forcefully cutting power to a machine or equivalent.
  argv.emplace_back("--never_fsync");

  // Generate smaller RSA keys -- generating a 1024-bit key is faster
  // than generating the default 2048-bit key, and we don't care about
  // strong encryption in tests. Setting it lower (e.g. 512 bits) results
  // in OpenSSL errors RSA_sign:digest too big for rsa key:rsa_sign.c:122
  // since we are using strong/high TLS v1.2 cipher suites, so the minimum
  // size of TLS-related RSA key is 768 bits (due to the usage of
  // the ECDHE-RSA-AES256-GCM-SHA384 suite). However, to work with Java
  // client it's necessary to have at least 1024 bits for certificate RSA key
  // due to Java security policies.
  argv.emplace_back("--ipki_server_key_size=1024");

  // Disable minidumps by default since many tests purposely inject faults.
  argv.emplace_back("--enable_minidumps=false");

  // Disable redaction.
  argv.emplace_back("--redact=none");

  // Enable metrics logging.
  argv.emplace_back("--metrics_log_interval_ms=1000");

  if (opts_.logtostderr) {
    // Ensure that logging goes to the test output and doesn't get buffered.
    argv.emplace_back("--logtostderr");
    argv.emplace_back("--logbuflevel=-1");
  }

  // Even if we are logging to stderr, metrics logs and minidumps end up being
  // written based on -log_dir. So, we have to set that too.
  argv.push_back("--log_dir=" + log_dir());
  RETURN_NOT_OK(env_util::CreateDirsRecursively(Env::Default(), log_dir()));

  // Tell the server to dump its port information so we can pick it up.
  string info_path = JoinPathSegments(data_dirs()[0], "info.pb");
  argv.push_back("--server_dump_info_path=" + info_path);
  argv.emplace_back("--server_dump_info_format=pb");

  // We use ephemeral ports in many tests. They don't work for production, but are OK
  // in unit tests.
  argv.emplace_back("--rpc_server_allow_ephemeral_ports");

  // Allow unsafe and experimental flags from tests, since we often use
  // fault injection, etc.
  argv.emplace_back("--unlock_experimental_flags");
  argv.emplace_back("--unlock_unsafe_flags");

  // Then the "extra flags" passed into the ctor (from the ExternalMiniCluster
  // options struct). These come at the end so they can override things like
  // web port or RPC bind address if necessary.
  argv.insert(argv.end(), opts_.extra_flags.begin(), opts_.extra_flags.end());

  // A previous instance of the daemon may have run in the same directory. So, remove
  // the previous info file if it's there.
  ignore_result(Env::Default()->DeleteFile(info_path));

  // Start the daemon.
  unique_ptr<Subprocess> p(new Subprocess(argv));
  p->SetEnvVars(extra_env_);
  string env_str;
  JoinMapKeysAndValues(extra_env_, "=", ",", &env_str);
  LOG(INFO) << "Running " << opts_.exe << "\n" << JoinStrings(argv, "\n")
            << " with env {" << env_str << "}";
  RETURN_NOT_OK_PREPEND(p->Start(),
                        Substitute("Failed to start subprocess $0", opts_.exe));

  // If requested, start a monitoring subprocess.
  unique_ptr<Subprocess> perf_record;
  if (!opts_.perf_record_filename.empty()) {
    perf_record.reset(new Subprocess({
      "perf",
      "record",
      "--call-graph",
      "fp",
      "-o",
      opts_.perf_record_filename,
      Substitute("--pid=$0", p->pid())
    }, SIGINT));
    RETURN_NOT_OK_PREPEND(perf_record->Start(),
                          "Could not start perf record subprocess");
  }

  // The process is now starting -- wait for the bound port info to show up.
  Stopwatch sw;
  sw.start();
  bool success = false;
  while (sw.elapsed().wall_seconds() < opts_.start_process_timeout.ToSeconds()) {
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
      perf_record_process_.swap(perf_record);
      return Status::OK();
    }

    RETURN_NOT_OK_PREPEND(s, Substitute("Failed waiting on $0", opts_.exe));
    string exit_info;
    RETURN_NOT_OK(p->GetExitStatus(nullptr, &exit_info));
    return Status::RuntimeError(exit_info);
  }

  if (!success) {
    ignore_result(p->Kill(SIGKILL));
    return Status::TimedOut(
        Substitute("Timed out after $0s waiting for process ($1) to write info file ($2)",
                   opts_.start_process_timeout.ToString(), opts_.exe, info_path));
  }

  status_.reset(new ServerStatusPB());
  RETURN_NOT_OK_PREPEND(pb_util::ReadPBFromPath(Env::Default(), info_path, status_.get()),
                        "Failed to read info file from " + info_path);
  LOG(INFO) << "Started " << opts_.exe << " as pid " << p->pid();
  VLOG(1) << opts_.exe << " instance information:\n" << SecureDebugString(*status_);

  process_.swap(p);
  perf_record_process_.swap(perf_record);
  return Status::OK();
}

void ExternalDaemon::SetExePath(string exe) {
  CHECK(IsShutdown()) << "Call Shutdown() before changing the executable path";
  opts_.exe = std::move(exe);
}

void ExternalDaemon::SetMetastoreIntegration(const string& hms_uris,
                                             bool enable_kerberos) {
  opts_.extra_flags.emplace_back(Substitute("--hive_metastore_uris=$0", hms_uris));
  opts_.extra_flags.emplace_back(Substitute("--hive_metastore_sasl_enabled=$0",
                                            enable_kerberos));
}

Status ExternalDaemon::Pause() {
  if (!process_) {
    return Status::IllegalState(Substitute(
        "Request to pause '$0' but the process is not there", opts_.exe));
  }
  VLOG(1) << "Pausing " << opts_.exe << " with pid " << process_->pid();
  const Status s = process_->Kill(SIGSTOP);
  RETURN_NOT_OK(s);
  paused_ = true;
  return s;
}

Status ExternalDaemon::Resume() {
  if (!process_) {
    return Status::IllegalState(Substitute(
        "Request to resume '$0' but the process is not there", opts_.exe));
  }
  VLOG(1) << "Resuming " << opts_.exe << " with pid " << process_->pid();
  const Status s = process_->Kill(SIGCONT);
  RETURN_NOT_OK(s);
  paused_ = false;
  return s;
}

bool ExternalDaemon::IsShutdown() const {
  return !process_;
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
  if (rpc_bind_address().host() != MiniCluster::kWildcardIpAddr) {
    bound_rpc_ = bound_rpc_hostport();
    bound_http_ = bound_http_hostport();
  } else {
    bound_rpc_.set_host(MiniCluster::kWildcardIpAddr);
    bound_rpc_.set_port(bound_rpc_hostport().port());
    bound_http_.set_host(MiniCluster::kWildcardIpAddr);
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

    LOG(INFO) << "Killing " << opts_.exe << " with pid " << process_->pid();
    ignore_result(process_->Kill(SIGKILL));
  }
  WARN_NOT_OK(process_->Wait(), "Waiting on " + opts_.exe);
  paused_ = false;
  process_.reset();
  perf_record_process_.reset();
}

Status ExternalDaemon::DeleteFromDisk() const {
  for (const string& data_dir : data_dirs()) {
    RETURN_NOT_OK(Env::Default()->DeleteRecursively(data_dir));
  }
  RETURN_NOT_OK(Env::Default()->DeleteRecursively(wal_dir()));
  return Status::OK();
}

void ExternalDaemon::FlushCoverage() {
#ifndef COVERAGE_BUILD
  return; // NOLINT(*)
#else
  LOG(INFO) << "Attempting to flush coverage for " << opts_.exe << " pid " << process_->pid();
  server::GenericServiceProxy proxy(
      opts_.messenger, bound_rpc_addr(), bound_rpc_addr().host());

  server::FlushCoverageRequestPB req;
  server::FlushCoverageResponsePB resp;
  rpc::RpcController rpc;

  rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
  Status s = proxy.FlushCoverage(req, &resp, &rpc);
  if (s.ok() && !resp.success()) {
    s = Status::RemoteError("Server does not appear to be running a coverage build");
  }
  WARN_NOT_OK(s, Substitute("Unable to flush coverage on $0 pid $1", opts_.exe, process_->pid()));
#endif
}

void ExternalDaemon::CheckForLeaks() {
#if defined(__has_feature)
#  if __has_feature(address_sanitizer)
  LOG(INFO) << "Attempting to check leaks for " << opts_.exe << " pid " << process_->pid();
  server::GenericServiceProxy proxy(opts_.messenger, bound_rpc_addr(), bound_rpc_addr().host());

  server::CheckLeaksRequestPB req;
  server::CheckLeaksResponsePB resp;
  rpc::RpcController rpc;

  rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
  Status s = proxy.CheckLeaks(req, &resp, &rpc);
  if (s.ok()) {
    if (!resp.success()) {
      s = Status::RemoteError("Server does not appear to be running an LSAN build");
    } else {
      CHECK(!resp.found_leaks()) << "Found leaks in " << opts_.exe << " pid " << process_->pid();
    }
  }
  WARN_NOT_OK(s, Substitute("Unable to check leaks on $0 pid $1", opts_.exe, process_->pid()));
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

//------------------------------------------------------------
// ScopedResumeExternalDaemon
//------------------------------------------------------------

ScopedResumeExternalDaemon::ScopedResumeExternalDaemon(ExternalDaemon* daemon)
    : daemon_(CHECK_NOTNULL(daemon)) {
}

ScopedResumeExternalDaemon::~ScopedResumeExternalDaemon() {
  WARN_NOT_OK(daemon_->Resume(), "Could not resume external daemon");
}

//------------------------------------------------------------
// ExternalMaster
//------------------------------------------------------------

ExternalMaster::ExternalMaster(ExternalDaemonOptions opts)
    : ExternalDaemon(std::move(opts)) {
}

ExternalMaster::~ExternalMaster() {
}

Status ExternalMaster::Start() {
  vector<string> flags(GetCommonFlags());
  flags.push_back(Substitute("--rpc_bind_addresses=$0", rpc_bind_address().ToString()));
  flags.push_back(Substitute("--webserver_interface=$0", rpc_bind_address().host()));
  flags.emplace_back("--webserver_port=0");
  return StartProcess(flags);
}

Status ExternalMaster::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Master cannot be restarted. Must call Shutdown() first.");
  }

  vector<string> flags(GetCommonFlags());
  flags.push_back(Substitute("--rpc_bind_addresses=$0", bound_rpc_.ToString()));

  if (bound_http_.Initialized()) {
    flags.push_back(Substitute("--webserver_interface=$0", bound_http_.host()));
    flags.push_back(Substitute("--webserver_port=$0", bound_http_.port()));
  } else {
    flags.push_back(Substitute("--webserver_interface=$0", bound_rpc_.host()));
    flags.emplace_back("--webserver_port=0");
  }

  return StartProcess(flags);
}

Status ExternalMaster::WaitForCatalogManager() {
  unique_ptr<MasterServiceProxy> proxy(new MasterServiceProxy(
      opts_.messenger, bound_rpc_addr(), bound_rpc_addr().host()));
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
      }
      s = StatusFromPB(resp.error().status());
      if (s.IsIllegalState()) {
        // This master is not the leader but is otherwise up and running.
        break;
      }
      if (!s.IsServiceUnavailable()) {
        // Unexpected error from master.
        return s;
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
    "--fs_wal_dir=" + wal_dir(),
    "--fs_data_dirs=" + JoinStrings(data_dirs(), ","),
    "--block_manager=" + opts_.block_manager_type,
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
                                           vector<HostPort> master_addrs)
    : ExternalDaemon(std::move(opts)),
      master_addrs_(std::move(master_addrs)) {
  DCHECK(!master_addrs_.empty());
}

ExternalTabletServer::~ExternalTabletServer() {
}

Status ExternalTabletServer::Start() {
  vector<string> flags;
  flags.push_back("--fs_wal_dir=" + wal_dir());
  flags.push_back("--fs_data_dirs=" + JoinStrings(data_dirs(), ","));
  flags.push_back("--block_manager=" + opts_.block_manager_type);
  flags.push_back(Substitute("--rpc_bind_addresses=$0",
                             rpc_bind_address().ToString()));
  flags.push_back(Substitute("--local_ip_for_outbound_sockets=$0",
                             rpc_bind_address().host()));
  flags.push_back(Substitute("--webserver_interface=$0",
                             rpc_bind_address().host()));
  flags.emplace_back("--webserver_port=0");
  flags.push_back(Substitute("--tserver_master_addrs=$0",
                             HostPort::ToCommaSeparatedString(master_addrs_)));
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}

Status ExternalTabletServer::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Tablet server cannot be restarted. Must call Shutdown() first.");
  }
  vector<string> flags;
  flags.push_back("--fs_wal_dir=" + wal_dir());
  flags.push_back("--fs_data_dirs=" + JoinStrings(data_dirs(), ","));
  flags.push_back("--block_manager=" + opts_.block_manager_type);
  flags.push_back(Substitute("--rpc_bind_addresses=$0", bound_rpc_.ToString()));
  flags.push_back(Substitute("--local_ip_for_outbound_sockets=$0",
                             rpc_bind_address().host()));
  if (bound_http_.Initialized()) {
    flags.push_back(Substitute("--webserver_port=$0", bound_http_.port()));
    flags.push_back(Substitute("--webserver_interface=$0",
                               bound_http_.host()));
  }
  flags.push_back(Substitute("--tserver_master_addrs=$0",
                             HostPort::ToCommaSeparatedString(master_addrs_)));
  return StartProcess(flags);
}

} // namespace cluster
} // namespace kudu
