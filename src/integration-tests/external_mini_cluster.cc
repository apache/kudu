// Copyright (c) 2014, Cloudera, inc.

#include "integration-tests/external_mini_cluster.h"

#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "client/client.h"
#include "common/wire_protocol.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "master/master.proxy.h"
#include "server/server_base.pb.h"
#include "rpc/messenger.h"
#include "util/env.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/path_util.h"
#include "util/pb_util.h"
#include "util/stopwatch.h"
#include "util/subprocess.h"

using std::string;
using std::tr1::shared_ptr;
using strings::Substitute;
using kudu::master::MasterServiceProxy;
using kudu::server::ServerStatusPB;

namespace kudu {

static const char* const kMasterBinaryName = "kudu-master";
static const char* const kTabletServerBinaryName = "kudu-tablet_server";
static double kProcessStartTimeoutSeconds = 10.0;
static double kTabletServerRegistrationTimeoutSeconds = 10.0;

ExternalMiniClusterOptions::ExternalMiniClusterOptions()
  : num_tablet_servers(1) {
}

ExternalMiniClusterOptions::~ExternalMiniClusterOptions() {
}


ExternalMiniCluster::ExternalMiniCluster(const ExternalMiniClusterOptions& opts)
  : opts_(opts),
    started_(false) {
}

ExternalMiniCluster::~ExternalMiniCluster() {
  if (started_) {
    Shutdown();
  }
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
    // If they don't specify a data root, use the current gtest
    // directory.
    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
    if (!test_info) {
      return Status::InvalidArgument("Must specify 'data_root' unless running in a gtest");
    }
    string dir;
    CHECK_OK(Env::Default()->GetTestDirectory(&dir));

    dir += Substitute(
      "/$0.$1.$2",
      StringReplace(test_info->test_case_name(), "/", "_", true).c_str(),
      StringReplace(test_info->name(), "/", "_", true).c_str(),
      Env::Default()->NowMicros());
    ignore_result(Env::Default()->CreateDir(dir));
    data_root_ = JoinPathSegments(dir, "minicluster-data");
  }

  return Status::OK();
}

Status ExternalMiniCluster::Start() {
  CHECK(!started_);
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

  RETURN_NOT_OK_PREPEND(StartMaster(),
                        "Failed to start Master");

  for (int i = 1; i <= opts_.num_tablet_servers; i++) {
    RETURN_NOT_OK_PREPEND(AddTabletServer(),
                          Substitute("Failed starting tablet server $0", i));
  }
  RETURN_NOT_OK(WaitForTabletServerCount(
                  opts_.num_tablet_servers,
                  MonoDelta::FromSeconds(kTabletServerRegistrationTimeoutSeconds)));

  started_ = true;
  return Status::OK();
}

void ExternalMiniCluster::Shutdown() {
  if (master_) {
    master_->Shutdown();
    master_ = NULL;
  }

  BOOST_FOREACH(const scoped_refptr<ExternalDaemon>& ts, tablet_servers_) {
    ts->Shutdown();
  }

  tablet_servers_.clear();

  if (messenger_) {
    messenger_->Shutdown();
    messenger_.reset();
  }

  started_ = false;
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

Status ExternalMiniCluster::StartMaster() {
  string exe = GetBinaryPath(kMasterBinaryName);
  master_ = new ExternalMaster(exe, GetDataPath("master"),
                               SubstituteInFlags(opts_.extra_master_flags, 0));
  return master_->Start();
}

Status ExternalMiniCluster::AddTabletServer() {
  CHECK(master_) << "Must have started master before adding tablet servers";

  int idx = tablet_servers_.size();

  string exe = GetBinaryPath(kTabletServerBinaryName);
  scoped_refptr<ExternalTabletServer> ts =
    new ExternalTabletServer(exe, GetDataPath(Substitute("ts-$0", idx)),
                             master_->bound_rpc_hostport(),
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

    master::ListTabletServersRequestPB req;
    master::ListTabletServersResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(remaining);
    RETURN_NOT_OK_PREPEND(master_proxy()->ListTabletServers(req, &resp, &rpc),
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
    usleep(1 * 1000); // 1ms
  }
}

shared_ptr<MasterServiceProxy> ExternalMiniCluster::master_proxy() {
  return shared_ptr<MasterServiceProxy>(
    new MasterServiceProxy(messenger_, master_->bound_rpc_addr()));
}

Status ExternalMiniCluster::CreateClient(const client::KuduClientOptions& their_opts,
                                         shared_ptr<client::KuduClient>* client) {
  CHECK(started_);

  client::KuduClientOptions opts(their_opts);
  opts.master_server_addr = master_->bound_rpc_hostport().ToString();

  return client::KuduClient::Create(opts, client);
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
    usleep(10 * 1000);
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
  VLOG(1) << "Started " << exe_ << ":\n" << status_->DebugString();

  process_.swap(p);
  return Status::OK();
}

void ExternalDaemon::Shutdown() {
  if (!process_) return;
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
// ExternalMaster
//------------------------------------------------------------

ExternalMaster::ExternalMaster(const string& exe,
                               const string& data_dir,
                               const vector<string>& extra_flags)
  : ExternalDaemon(exe, data_dir, extra_flags) {
}

ExternalMaster::~ExternalMaster() {
}

Status ExternalMaster::Start() {
  vector<string> flags;
  flags.push_back("--master_base_dir=" + data_dir_);
  flags.push_back("--master_rpc_bind_addresses=127.0.0.1:0");
  flags.push_back("--master_web_port=0");
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}

//------------------------------------------------------------
// ExternalTabletServer
//------------------------------------------------------------

ExternalTabletServer::ExternalTabletServer(const string& exe,
                                           const string& data_dir,
                                           const HostPort& master_addr,
                                           const vector<string>& extra_flags)
  : ExternalDaemon(exe, data_dir, extra_flags),
    master_addr_(master_addr.ToString()) {
}

ExternalTabletServer::~ExternalTabletServer() {
}

Status ExternalTabletServer::Start() {
  vector<string> flags;
  flags.push_back("--tablet_server_base_dir=" + data_dir_);
  flags.push_back("--tablet_server_rpc_bind_addresses=127.0.0.1:0");
  flags.push_back("--tablet_server_web_port=0");
  flags.push_back("--tablet_server_master_addr=" + master_addr_);
  RETURN_NOT_OK(StartProcess(flags));
  return Status::OK();
}

} // namespace kudu
