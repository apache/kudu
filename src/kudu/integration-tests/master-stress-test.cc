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

#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/atomic.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int64(timeout_ms);
DEFINE_int32(num_create_table_threads, 4,
            "Number of threads that should create tables");
DEFINE_int32(num_alter_table_threads, 2,
            "Number of threads that should alter tables");
DEFINE_int32(num_delete_table_threads, 2,
            "Number of threads that should delete tables");
DEFINE_int32(num_replace_tablet_threads, 2,
            "Number of threads that should replace tablets");
DEFINE_int32(num_seconds_to_run, 10,
             "Number of seconds that the test should run");

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::cluster::ExternalMaster;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::itest::ListTablets;
using kudu::itest::SentryMode;
using kudu::master::ListTablesRequestPB;
using kudu::master::ListTablesResponsePB;
using kudu::master::ReplaceTabletRequestPB;
using kudu::master::ReplaceTabletResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tools::LeaderMasterProxy;
using std::pair;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

static const MonoDelta kDefaultAdminTimeout = MonoDelta::FromSeconds(300);
static const MonoDelta kTransientStateBackoff = MonoDelta::FromMilliseconds(50);

// Parameterized based on HmsMode and whether or not to enable Sentry integration.
class MasterStressTest : public ExternalMiniClusterITestBase,
                         public ::testing::WithParamInterface<pair<HmsMode, SentryMode>> {
 public:
  MasterStressTest()
    : done_(1),
      num_tables_created_(0),
      num_tables_altered_(0),
      num_tables_deleted_(0),
      num_tablets_replaced_(0),
      num_masters_restarted_(0),
      table_names_condvar_(&table_names_lock_),
      rand_(SeedRandom()) {
  }

  ~MasterStressTest() {
    STLDeleteValues(&ts_map_);
  }

  void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_masters = 3;
    opts.num_tablet_servers = 3;
    // In case of concurrent activity or an inferior node, it might take longer
    // time to start up a process or negotiate a connection. The default
    // settings for connection negotiation and process startup timeouts are
    // too short in that case.
    opts.start_process_timeout = MonoDelta::FromSeconds(60);
    opts.rpc_negotiation_timeout = MonoDelta::FromSeconds(30);

    opts.hms_mode = std::get<0>(GetParam());
    bool enable_sentry = (std::get<1>(GetParam()) == SentryMode::ENABLED);
    opts.enable_sentry = enable_sentry;
    opts.enable_kerberos = enable_sentry;
    // Tune down the notification log poll period in order to speed up catalog convergence.
    opts.extra_master_flags.emplace_back("--hive_metastore_notification_log_poll_period_seconds=1");

    // Set max missed heartbeats periods to 1.0 (down from 3.0).
    opts.extra_master_flags.emplace_back("--leader_failure_max_missed_heartbeat_periods=1.0");

    // Don't preallocate log segments, since we're creating many tablets here.
    // If each preallocates 64M or so, we use a ton of disk space in this
    // test, and it fails on normal sized /tmp dirs.
    opts.extra_master_flags.emplace_back("--log_preallocate_segments=false");
    opts.extra_tserver_flags.emplace_back("--log_preallocate_segments=false");

    // In case of address/thread sanitizer, the stress tests scenarios
    // induce frequent retries and leader re-elections. To avoid overflowing
    // RPC queues too often and allow the scenario to eventually complete,
    // let's make the Raft heartbeat interval 2x times higher for address/thread
    // sanitizer builds.
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
    opts.extra_tserver_flags.emplace_back("--raft_heartbeat_interval_ms=1000");
#endif

    // Reduce various timeouts below as to make the detection of leader master
    // failures (specifically, failures as result of long pauses) more rapid.

    // Set the TS->master heartbeat timeout to 1 second (down from 15 seconds).
    opts.extra_tserver_flags.emplace_back("--heartbeat_rpc_timeout_ms=1000");

    // Allow one TS heartbeat failure before retrying with back-off (down from 3).
    opts.extra_tserver_flags.emplace_back("--heartbeat_max_failures_before_backoff=1");

    // Set custom TS->master heartbeat interval. This is to allow for faster
    // registration of tablet servers with recently restarted masters.
    opts.extra_tserver_flags.emplace_back(
        Substitute("--heartbeat_interval_ms=$0", kTsToMasterHbIntervalMs));

    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder builder;

    // Create and alter table operation timeouts can be extended via their
    // builders, but there's no such option for DeleteTable, so we extend
    // the global operation timeout.
    builder.default_admin_operation_timeout(kDefaultAdminTimeout);

    // The C++ client currently uses the normal RPC timeout to retry DDL
    // operations to alternate masters, which effectively reduces the admin
    // timeout to the RPC timeout. With HMS integration some DDL operations can
    // exceed the default 10s timeout.
    builder.default_rpc_timeout(MonoDelta::FromSeconds(60));

    ASSERT_OK(cluster_->CreateClient(&builder, &client_));

    // Populate the ts_map_ for the ReplaceTablet thread.
    const auto& addr = cluster_->master(0)->bound_rpc_addr();
    shared_ptr<MasterServiceProxy> m_proxy(
        new MasterServiceProxy(cluster_->messenger(), addr, addr.host()));
    ASSERT_OK(CreateTabletServerMap(m_proxy, cluster_->messenger(), &ts_map_));

    if (enable_sentry) {
      itest::SetupAdministratorPrivileges(cluster_->kdc(),
                                          cluster_->sentry()->address());
    }
  }

  void TearDown() override {
    Shutdown();
    KuduTest::TearDown();
  }

  void Shutdown() {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
  }

  void CreateTableThread() {
    while (done_.count()) {
      // Create a basic table with a random name.
      KuduSchema schema;
      KuduSchemaBuilder b;
      b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
      CHECK_OK(b.Build(&schema));

      string to_create = GenerateTableName();
      unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
      Status s = table_creator->table_name(to_create)
          .schema(&schema)
          .set_range_partition_columns({ "key" })
          .wait(false)
          .timeout(kDefaultAdminTimeout)
          .Create();
      if (s.IsAlreadyPresent()) {
        // The client retried after the RPC timed out, but the master did in
        // fact create the table.
        //
        // TODO: Should be fixed with Exactly Once semantics, see KUDU-1537.
        continue;
      }
      if (s.IsServiceUnavailable()) {
        // The client retried after the RPC timed out, and the retried RPC
        // arrived at the master while the table was still being created.
        //
        // TODO: Should be fixed with Exactly Once semantics, see KUDU-1537.
        continue;
      }
      if (s.IsInvalidArgument() &&
          MatchPattern(s.ToString(), "*not enough live tablet servers*")) {
        // The test placed enough load on the cluster that some tservers haven't
        // heartbeat in a while, or the leader master has not been alive for
        // long enough to receive heartbeats from all tservers.
        //
        // TODO(KUDU-1358): remove this special case once table creation
        // following leader restart is robust.
        continue;
      }
      CHECK_OK(s);
      num_tables_created_.Increment();
      PutTableName(to_create);

      done_.WaitFor(MonoDelta::FromMilliseconds(200));
    }

  }

  void AlterTableThread() {
    while (done_.count()) {
      // Rename a table at random.
      string to_alter;
      if (!BlockingGetTableName(&to_alter)) {
        break;
      }
      string new_table_name = GenerateTableName();
      unique_ptr<KuduTableAlterer> table_alterer(
          client_->NewTableAlterer(to_alter));
      Status s = table_alterer
        ->RenameTo(new_table_name)
        ->wait(false)
        ->timeout(kDefaultAdminTimeout)
        ->Alter();
      if (s.IsNotFound()) {
        // The client retried after the RPC timed out, but the master did in
        // fact rename the table, or is actively renaming it.
        //
        // TODO: Should be fixed with Exactly Once semantics, see KUDU-1537.
        continue;
      }
      CHECK_OK(s);
      num_tables_altered_.Increment();
      PutTableName(new_table_name);

      done_.WaitFor(MonoDelta::FromMilliseconds(200));
    }
  }

  void DeleteTableThread() {
    while (done_.count()) {
      // Delete a table at random.
      string to_delete;
      if (!BlockingGetTableName(&to_delete)) {
        break;
      }
      Status s = client_->DeleteTable(to_delete);
      if (s.IsNotFound()) {
        // The client retried after the RPC timed out, but the master did in
        // fact delete the table.
        //
        // TODO: Should be fixed with Exactly Once semantics, see KUDU-1537.
        continue;
      }
      CHECK_OK(s);
      num_tables_deleted_.Increment();

      done_.WaitFor(MonoDelta::FromMilliseconds(200));
    }
  }

  void ReplaceTabletThread() {
    LeaderMasterProxy lm_proxy(client_);

    // Since there are three tablet servers and 3 replicas per tablet, it doesn't
    // matter which tablet server we list tablets from.
    auto* ts = ts_map_.begin()->second;
    vector<tserver::ListTabletsResponsePB_StatusAndSchemaPB> tablet_ids;
    while (done_.count()) {
      tablet_ids.clear();
      Status s = ListTablets(ts, kDefaultAdminTimeout, &tablet_ids);
      // Network error is a legitimate case to retry -- the server might
      // not be up yet.
      if (s.IsNetworkError()) {
        SleepFor(kTransientStateBackoff);
        continue;
      }
      CHECK_OK(s);
      // An empty list of tablets is a legitimate case to retry -- tablet
      // server might not yet registered or at this point all tables are
      // deleted.
      if (tablet_ids.empty()) {
        SleepFor(kTransientStateBackoff);
        continue;
      }
      int idx = rand_.Uniform(tablet_ids.size());
      const string& tablet_id = tablet_ids[idx].tablet_status().tablet_id();
      ReplaceTabletRequestPB req;
      ReplaceTabletResponsePB resp;
      req.set_tablet_id(tablet_id);
      s = lm_proxy.SyncRpc<ReplaceTabletRequestPB, ReplaceTabletResponsePB>(
          req, &resp, "ReplaceTablet", &MasterServiceProxy::ReplaceTabletAsync);
      // NotFound is OK because it means the tablet was already replaced or deleted.
      if (!s.IsNotFound()) {
        CHECK_OK(s);
        num_tablets_replaced_.Increment();
      }
      done_.WaitFor(MonoDelta::FromMilliseconds(200));
    }
  }

  Status WaitForMasterUpAndRunning(const shared_ptr<Messenger>& messenger,
                                   ExternalMaster* master) {
    const auto& addr = master->bound_rpc_addr();
    unique_ptr<MasterServiceProxy> proxy(new MasterServiceProxy(messenger, addr, addr.host()));
    while (true) {
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
      } else if (!s.IsNetworkError()) {
        // Unexpected error from proxy.
        return s;
      }

      // There was some kind of transient network error or the master isn't yet
      // ready. Sleep and retry.
      SleepFor(kTransientStateBackoff);
    }
    return Status::OK();
  }

  void RestartMasterLoop() {
    shared_ptr<Messenger> messenger;
    MessengerBuilder bld("RestartMasterMessenger");
    CHECK_OK(bld.Build(&messenger));

    MonoTime deadline(MonoTime::Now());
    deadline.AddDelta(MonoDelta::FromSeconds(FLAGS_num_seconds_to_run));

    MonoTime now(MonoTime::Now());
    while (now < deadline) {
      ExternalMaster* master = cluster_->master(
          rand_.Uniform(cluster_->num_masters()));
      master->Shutdown();

      // Give the rest of the test a chance to operate with the master down.
      SleepFor(MonoDelta::FromMilliseconds(
          kTsToMasterHbIntervalMs + rand_.Uniform(kTsToMasterHbIntervalMs)));

      CHECK_OK(master->Restart());

      // Wait for the master to start servicing requests before restarting the
      // next one.
      //
      // This isn't necessary for correctness, but it helps give the masters
      // enough uptime so that they can actually make forward progress on
      // client requests.
      CHECK_OK(WaitForMasterUpAndRunning(messenger, master));
      num_masters_restarted_.Increment();

      // Allow the tablet servers to send in heartbeats and clients to connect.
      // Due to the way how Kudu client and AsyncLeaderMasterRpc perform
      // exponential back-off while retrying RPCs, it's crucial to keep the
      // master up and running for about 2+ seconds to give those a better
      // chance contacting the master (so, 1500 with the time spent by the
      // WaitForMasterUpAndRunning does the trick). It's also important to let
      // masters elect a new leader if the former one was shutdown, so the
      // uptime interval for the restarted master should be a multiple
      // of the master's Raft heartbeat interval (as of now it's 500ms).
      SleepFor(MonoDelta::FromMilliseconds(
          2000 + rand_.Uniform(kTsToMasterHbIntervalMs)));
      now = MonoTime::Now();
    }
  }

 protected:
  static constexpr int kTsToMasterHbIntervalMs = 500;

  CountDownLatch done_;
  AtomicInt<uint64_t> num_tables_created_;
  AtomicInt<uint64_t> num_tables_altered_;
  AtomicInt<uint64_t> num_tables_deleted_;
  AtomicInt<uint64_t> num_tablets_replaced_;
  AtomicInt<uint64_t> num_masters_restarted_;

  Mutex table_names_lock_;
  ConditionVariable table_names_condvar_;
  vector<string> table_names_;

 private:
  string GenerateTableName() {
    return Substitute("default.Table_$0", oid_generator_.Next());
  }

  bool BlockingGetTableName(string* chosen_table) {
    std::lock_guard<Mutex> l(table_names_lock_);
    while (table_names_.empty() && done_.count()) {
      table_names_condvar_.Wait();
    }
    if (done_.count() == 0) {
      return false;
    }

    // Choose a table name at random. Remove it from the vector by replacing
    // it with the last name; this is more efficient than erasing in place.
    int num_tables = table_names_.size();
    int idx = rand_.Uniform(num_tables);
    *chosen_table = table_names_[idx];
    if (num_tables > 1) {
      table_names_[idx] = table_names_[num_tables - 1];
    }
    table_names_.pop_back();
    return true;
  }

  void PutTableName(const string& table_name) {
    std::lock_guard<Mutex> l(table_names_lock_);
    table_names_.push_back(table_name);
    table_names_condvar_.Signal();
  }

  ThreadSafeRandom rand_;
  ObjectIdGenerator oid_generator_;
  unique_ptr<ExternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;

  // Used to ListTablets in the ReplaceTablet thread.
  // This member is not protected by a lock but it is
  // only read by the ReplaceTablet threads.
  std::unordered_map<string, itest::TServerDetails*> ts_map_;
};

// Run the test with the HMS/Sentry integration enabled and disabled. Sentry integration
// should be only enabled when HMS integration is enabled.
INSTANTIATE_TEST_CASE_P(HmsSentryConfigurations, MasterStressTest, ::testing::ValuesIn(
    vector<pair<HmsMode, SentryMode>> {
      { HmsMode::NONE, SentryMode::DISABLED },
      { HmsMode::ENABLE_METASTORE_INTEGRATION, SentryMode::DISABLED },
      { HmsMode::ENABLE_METASTORE_INTEGRATION, SentryMode::ENABLED },
    }
));

TEST_P(MasterStressTest, Test) {
  OverrideFlagForSlowTests("num_create_table_threads", "10");
  OverrideFlagForSlowTests("num_alter_table_threads", "5");
  OverrideFlagForSlowTests("num_delete_table_threads", "5");
  OverrideFlagForSlowTests("num_replace_tablet_threads", "5");
  OverrideFlagForSlowTests("num_seconds_to_run", "30");

  // This is for SyncRpc() calls using LeaderMasterProxy.
  FLAGS_timeout_ms = kDefaultAdminTimeout.ToMilliseconds();

  // Start all of the threads.
  vector<thread> threads;
  for (int i = 0; i < FLAGS_num_create_table_threads; i++) {
    threads.emplace_back(&MasterStressTest::CreateTableThread, this);
  }
  for (int i = 0; i < FLAGS_num_alter_table_threads; i++) {
    threads.emplace_back(&MasterStressTest::AlterTableThread, this);
  }
  for (int i = 0; i < FLAGS_num_delete_table_threads; i++) {
    threads.emplace_back(&MasterStressTest::DeleteTableThread, this);
  }
  for (int i = 0; i < FLAGS_num_replace_tablet_threads; i++) {
    threads.emplace_back(&MasterStressTest::ReplaceTabletThread, this);
  }

  // Let the test run. The main thread will periodically restart masters.
  RestartMasterLoop();

  // Stop all of the threads.
  done_.CountDown();
  table_names_condvar_.Broadcast();
  int i = 0;
  for (auto& t : threads) {
    LOG(INFO) << Substitute("Killing test thread $0/$1", ++i, threads.size());
    t.join();
    LOG(INFO) << Substitute("Killed test thread $0/$1", i, threads.size());
  }

  // Shut down now so that the log messages below are more visible.
  Shutdown();

  LOG(INFO) << "Tables created: " << num_tables_created_.Load();
  LOG(INFO) << "Tables altered: " << num_tables_altered_.Load();
  LOG(INFO) << "Tables deleted: " << num_tables_deleted_.Load();
  LOG(INFO) << "Tablets replaced: " << num_tablets_replaced_.Load();
  LOG(INFO) << "Masters restarted: " << num_masters_restarted_.Load();
}
} // namespace kudu
