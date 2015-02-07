// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/integration-tests/external_mini_cluster.h"

#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_rpc.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::string;
using std::tr1::shared_ptr;
using strings::Substitute;
using kudu::master::GetLeaderMasterRpc;
using kudu::master::MasterServiceProxy;
using kudu::server::ServerStatusPB;

namespace kudu {

static const char* const kMasterBinaryName = "kudu-master";
static const char* const kTabletServerBinaryName = "kudu-tablet_server";
static double kProcessStartTimeoutSeconds = 10.0;
static double kTabletServerRegistrationTimeoutSeconds = 10.0;

ExternalMiniClusterOptions::ExternalMiniClusterOptions()
    : num_masters(1),
      num_tablet_servers(1) {
}

ExternalMiniClusterOptions::~ExternalMiniClusterOptions() {
}


ExternalMiniCluster::ExternalMiniCluster(const ExternalMiniClusterOptions& opts)
  : opts_(opts) {
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
                        .set_negotiation_threads(1)
                        .Build(&messenger_),
                        "Failed to start Messenger for minicluster");

  Status s = Env::Default()->CreateDir(data_root_);
  if (!s.ok() && !s.IsAlreadyPresent()) {
    RETURN_NOT_OK_PREPEND(s, "Could not create root dir " + data_root_);
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

void ExternalMiniCluster::Shutdown(NodeSelectionMode mode) {
  if (mode == ALL) {
    BOOST_FOREACH(const scoped_refptr<ExternalMaster>& master, masters_) {
      if (master) {
        master->Shutdown();
      }
    }
  }

  BOOST_FOREACH(const scoped_refptr<ExternalTabletServer>& ts, tablet_servers_) {
    ts->Shutdown();
  }
}

Status ExternalMiniCluster::Restart() {
  BOOST_FOREACH(const scoped_refptr<ExternalMaster>& master, masters_) {
    if (master && master->IsShutdown()) {
      RETURN_NOT_OK_PREPEND(master->Restart(), "Cannot restart master bound at: " +
                                               master->bound_rpc_hostport().ToString());
    }
  }

  BOOST_FOREACH(const scoped_refptr<ExternalTabletServer>& ts, tablet_servers_) {
    if (ts->IsShutdown()) {
      RETURN_NOT_OK_PREPEND(ts->Restart(), "Cannot restart tablet server bound at: " +
                                           ts->bound_rpc_hostport().ToString());
    }
  }

  return Status::OK();
}

string ExternalMiniCluster::GetBinaryPath(const string& binary) const {
  CHECK(!daemon_bin_path_.empty());
  return JoinPathSegments(daemon_bin_path_, binary);
}

string ExternalMiniCluster::GetDataPath(const string& daemon_id) const {
  CHECK(!data_root_.empty());
  return JoinPathSegments(data_root_, daemon_id);
}

namespace {
vector<string> SubstituteInFlags(const vector<string>& orig_flags,
                                 int index) {
  string str_index = strings::Substitute("$0", index);
  vector<string> ret;
  BOOST_FOREACH(const string& orig, orig_flags) {
    ret.push_back(StringReplace(orig, "${index}", str_index, true));
  }
  return ret;
}

} // anonymous namespace

Status ExternalMiniCluster::StartSingleMaster() {
  string exe = GetBinaryPath(kMasterBinaryName);
  scoped_refptr<ExternalMaster> master =
      new ExternalMaster(exe, GetDataPath("master"),
                         SubstituteInFlags(opts_.extra_master_flags, 0));
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
    string addr = Substitute("127.0.0.1:$0", opts_.master_rpc_ports[i]);
    peer_addrs.push_back(addr);
  }
  string peer_addrs_str = JoinStrings(peer_addrs, ",");
  vector<string> flags = opts_.extra_master_flags;
  flags.push_back("--master_quorum=" + peer_addrs_str);
  flags.push_back("--enable_leader_failure_detection=true");
  string exe = GetBinaryPath(kMasterBinaryName);

  // Start the masters.
  for (int i = 0; i < num_masters; i++) {
    scoped_refptr<ExternalMaster> peer =
        new ExternalMaster(exe,
                           GetDataPath(Substitute("master-$0", i)),
                           peer_addrs[i],
                           SubstituteInFlags(flags, i));
    RETURN_NOT_OK_PREPEND(peer->Start(),
                          Substitute("Unable to start Master at index $0", i));
    masters_.push_back(peer);
  }

  return Status::OK();
}

Status ExternalMiniCluster::AddTabletServer() {
  CHECK(leader_master() != NULL)
      << "Must have started at least 1 master before adding tablet servers";

  int idx = tablet_servers_.size();

  string exe = GetBinaryPath(kTabletServerBinaryName);
  vector<HostPort> master_hostports;
  for (int i = 0; i < num_masters(); i++) {
    master_hostports.push_back(DCHECK_NOTNULL(master(i))->bound_rpc_hostport());
  }
  scoped_refptr<ExternalTabletServer> ts =
    new ExternalTabletServer(exe, GetDataPath(Substitute("ts-$0", idx)),
                             master_hostports,
                             SubstituteInFlags(opts_.extra_tserver_flags, idx));
  RETURN_NOT_OK(ts->Start());
  tablet_servers_.push_back(ts);
  return Status::OK();
}

Status ExternalMiniCluster::WaitForTabletServerCount(int count, const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(timeout);

  while (true) {
    MonoDelta remaining = deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE));
    if (remaining.ToSeconds() < 0) {
      return Status::TimedOut(Substitute("$0 TS(s) never registered with master", count));
    }

    for (int i = 0; i < masters_.size(); i++) {
      master::ListTabletServersRequestPB req;
      master::ListTabletServersResponsePB resp;
      rpc::RpcController rpc;
      rpc.set_timeout(remaining);
      RETURN_NOT_OK_PREPEND(master_proxy(i)->ListTabletServers(req, &resp, &rpc),
                            "ListTabletServers RPC failed");
      // ListTabletServers() may return servers that are no longer online.
      // Do a second step of verification to verify that the descs that we got
      // are aligned (same uuid/seqno) with the TSs that we have in the cluster.
      int match_count = 0;
      BOOST_FOREACH(const master::ListTabletServersResponsePB_Entry& e, resp.servers()) {
        BOOST_FOREACH(const scoped_refptr<ExternalTabletServer>& ets, tablet_servers_) {
          if (ets->instance_id().permanent_uuid() == e.instance_id().permanent_uuid() &&
              ets->instance_id().instance_seqno() == e.instance_id().instance_seqno()) {
            match_count++;
            break;
          }
        }
      }
      if (match_count == count) {
        LOG(INFO) << count << " TS(s) registered with Master";
        return Status::OK();
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
}

Status ExternalMiniCluster::GetLeaderMasterIndex(int* idx) {
  scoped_refptr<GetLeaderMasterRpc> rpc;
  Synchronizer sync;
  vector<Sockaddr> addrs;
  HostPort leader_master_hp;
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromSeconds(5));

  BOOST_FOREACH(const scoped_refptr<ExternalMaster>& master, masters_) {
    addrs.push_back(master->bound_rpc_addr());
  }
  rpc.reset(new GetLeaderMasterRpc(sync.AsStatusCallback(),
                                   addrs,
                                   deadline,
                                   messenger_,
                                   &leader_master_hp));
  rpc->SendRpc();
  RETURN_NOT_OK(sync.Wait());
  bool found = false;
  for (int i = 0; i < masters_.size(); i++) {
    if (masters_[i]->bound_rpc_hostport().port() == leader_master_hp.port()) {
      found = true;
      *idx = i;
      break;
    }
  }
  if (!found) {
    // There is never a situation where shis should happen, so it's
    // better to exit with a FATAL log message right away vs. return a
    // Status::IllegalState().
    LOG(FATAL) << "Leader master is not in masters_";
  }
  return Status::OK();
}

shared_ptr<MasterServiceProxy> ExternalMiniCluster::master_proxy() {
  CHECK_EQ(masters_.size(), 1);
  return master_proxy(0);
}

shared_ptr<MasterServiceProxy> ExternalMiniCluster::master_proxy(int idx) {
  CHECK_LT(idx, masters_.size());
  return shared_ptr<MasterServiceProxy>(
      new MasterServiceProxy(messenger_, CHECK_NOTNULL(master(idx))->bound_rpc_addr()));
}

Status ExternalMiniCluster::CreateClient(client::KuduClientBuilder& builder,
                                         shared_ptr<client::KuduClient>* client) {
  CHECK(!masters_.empty());
  builder.clear_master_server_addrs();
  BOOST_FOREACH(const scoped_refptr<ExternalMaster>& master, masters_) {
    builder.add_master_server_addr(master->bound_rpc_hostport().ToString());
  }
  return builder.Build(client);
}

//------------------------------------------------------------
// ExternalDaemon
//------------------------------------------------------------

ExternalDaemon::ExternalDaemon(const string& exe,
                               const string& data_dir,
                               const vector<string>& extra_flags) :
  exe_(exe),
  data_dir_(data_dir),
  extra_flags_(extra_flags) {
}

ExternalDaemon::~ExternalDaemon() {
}


Status ExternalDaemon::StartProcess(const vector<string>& user_flags) {
  CHECK(!process_);

  vector<string> argv;
  // First the exe for argv[0]
  argv.push_back(BaseName(exe_));

  // Then all the flags coming from the minicluster framework.
  argv.insert(argv.end(), user_flags.begin(), user_flags.end());

  // Then the "extra flags" passed into the ctor (from the ExternalMiniCluster
  // options struct). These come at the end so they can override things like
  // web port or RPC bind address if necessary.
  argv.insert(argv.end(), extra_flags_.begin(), extra_flags_.end());

  // Tell the server to dump its port information so we can pick it up.
  string info_path = JoinPathSegments(data_dir_, "info.pb");
  argv.push_back("--server_dump_info_path=" + info_path);
  argv.push_back("--server_dump_info_format=pb");

  // A previous instance of the daemon may have run in the same directory. So, remove
  // the previous info file if it's there.
  ignore_result(Env::Default()->DeleteFile(info_path));

  // Ensure that logging goes to the test output and doesn't get buffered.
  argv.push_back("--logtostderr");
  argv.push_back("--logbuflevel=-1");

  // Ensure that we only bind to local host in tests.
  argv.push_back("--webserver_interface=localhost");

  gscoped_ptr<Subprocess> p(new Subprocess(exe_, argv));
  p->ShareParentStdout(false);
  LOG(INFO) << "Running " << exe_ << "\n" << JoinStrings(argv, "\n");
  RETURN_NOT_OK_PREPEND(p->Start(),
                        Substitute("Failed to start subprocess $0", exe_));

  // The process is now starting -- wait for the bound port info to show up.
  Stopwatch sw;
  bool success = false;
  while (sw.elapsed().wall < kProcessStartTimeoutSeconds) {
    if (Env::Default()->FileExists(info_path)) {
      success = true;
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
    int rc;
    Status s = p->WaitNoBlock(&rc);
    if (s.IsTimedOut()) {
      // The process is still running.
      continue;
    }
    RETURN_NOT_OK_PREPEND(s, Substitute("Failed waiting on $0", exe_));
    return Status::RuntimeError(
      Substitute("Process exited with rc=$0", rc),
      exe_);
  }

  if (!success) {
    ignore_result(p->Kill(SIGKILL));
    return Status::TimedOut("Timed out waiting for process to start",
                            exe_);
  }

  status_.reset(new ServerStatusPB());
  RETURN_NOT_OK_PREPEND(pb_util::ReadPBFromPath(Env::Default(), info_path, status_.get()),
                        "Failed to read info file from " + info_path);
  LOG(INFO) << "Started " << exe_ << " as pid " << p->pid();
  VLOG(1) << exe_ << " instance information:\n" << status_->DebugString();

  process_.swap(p);
  return Status::OK();
}

Status ExternalDaemon::Pause() {
  if (!process_) return Status::OK();
  VLOG(1) << "Pausing " << exe_ << " with pid " << process_->pid();
  return process_->Kill(SIGSTOP);
}

Status ExternalDaemon::Resume() {
  if (!process_) return Status::OK();
  VLOG(1) << "Resuming " << exe_ << " with pid " << process_->pid();
  return process_->Kill(SIGCONT);
}

bool ExternalDaemon::IsShutdown() const {
  return process_.get() == NULL;
}

void ExternalDaemon::Shutdown() {
  if (!process_) return;
  // Before we kill the process, store the addresses. If we're told to
  // start again we'll reuse these.
  bound_rpc_ = bound_rpc_hostport();
  bound_http_ = bound_http_hostport();

  LOG(INFO) << "Killing " << exe_ << " with pid " << process_->pid();
  ignore_result(process_->Kill(SIGKILL));
  int ret;
  WARN_NOT_OK(process_->Wait(&ret), "Waiting on " + exe_);
  process_.reset();
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
  CHECK_GE(status_->bound_http_addresses_size(), 1);
  HostPort ret;
  CHECK_OK(HostPortFromPB(status_->bound_http_addresses(0), &ret));
  return ret;
}

const NodeInstancePB& ExternalDaemon::instance_id() const {
  CHECK(status_);
  return status_->node_instance();
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

ExternalMaster::ExternalMaster(const string& exe,
                               const string& data_dir,
                               const vector<string>& extra_flags)
    : ExternalDaemon(exe, data_dir, extra_flags),
      rpc_bind_address_("127.0.0.1:0") {
}

ExternalMaster::ExternalMaster(const string& exe,
                               const string& data_dir,
                               const string& rpc_bind_address,
                               const std::vector<string>& extra_flags)
    : ExternalDaemon(exe, data_dir, extra_flags),
      rpc_bind_address_(rpc_bind_address) {
}

ExternalMaster::~ExternalMaster() {
}

Status ExternalMaster::Start() {
  vector<string> flags;
  flags.push_back("--master_wal_dir=" + data_dir_);
  flags.push_back("--master_data_dirs=" + data_dir_);
  flags.push_back("--master_rpc_bind_addresses=" + rpc_bind_address_);
  flags.push_back("--master_web_port=0");
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}

Status ExternalMaster::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Master cannot be restarted. Must call Shutdown() first.");
  }
  vector<string> flags;
  flags.push_back("--master_wal_dir=" + data_dir_);
  flags.push_back("--master_data_dirs=" + data_dir_);
  flags.push_back("--master_rpc_bind_addresses=" + rpc_bind_address_);
  flags.push_back(Substitute("--master_web_port=$0", bound_http_.port()));
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}


//------------------------------------------------------------
// ExternalTabletServer
//------------------------------------------------------------

ExternalTabletServer::ExternalTabletServer(const string& exe,
                                           const string& data_dir,
                                           const vector<HostPort>& master_addrs,
                                           const vector<string>& extra_flags)
  : ExternalDaemon(exe, data_dir, extra_flags),
    master_addrs_(HostPort::ToCommaSeparatedString(master_addrs)) {
}

ExternalTabletServer::~ExternalTabletServer() {
}

Status ExternalTabletServer::Start() {
  vector<string> flags;
  flags.push_back("--tablet_server_wal_dir=" + data_dir_);
  flags.push_back("--tablet_server_data_dirs=" + data_dir_);
  flags.push_back("--tablet_server_rpc_bind_addresses=127.0.0.1:0");
  flags.push_back("--tablet_server_web_port=0");
  flags.push_back("--tablet_server_master_addrs=" + master_addrs_);
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}

Status ExternalTabletServer::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Tablet server cannot be restarted. Must call Shutdown() first.");
  }
  vector<string> flags;
  flags.push_back("--tablet_server_wal_dir=" + data_dir_);
  flags.push_back("--tablet_server_data_dirs=" + data_dir_);
  flags.push_back("--tablet_server_rpc_bind_addresses=" + bound_rpc_.ToString());
  flags.push_back(Substitute("--tablet_server_web_port=$0", bound_http_.port()));
  flags.push_back("--tablet_server_master_addrs=" + master_addrs_);
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}


} // namespace kudu
