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

#include "kudu/tablet/tablet_replica-test-base.h"

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/test_macros.h"

using kudu::consensus::ConsensusBootstrapInfo;
using kudu::consensus::ConsensusMetadata;
using kudu::consensus::ConsensusMetadataManager;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftPeerPB;
using kudu::log::Log;
using kudu::log::LogOptions;
using kudu::pb_util::SecureDebugString;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::ResultTracker;
using kudu::tablet::KuduTabletTest;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::shared_ptr;
using std::unique_ptr;
using std::string;

METRIC_DECLARE_entity(tablet);

namespace kudu {
namespace tablet {

namespace {
const MonoDelta kLeadershipTimeout = MonoDelta::FromSeconds(10);
} // anonymous namespace

Status TabletReplicaTestBase::ExecuteWrite(TabletReplica* replica, const WriteRequestPB& req) {
  WriteResponsePB resp;
  unique_ptr<WriteOpState> op_state(new WriteOpState(replica,
                                                     &req,
                                                     nullptr, // No RequestIdPB
                                                     &resp));

  CountDownLatch rpc_latch(1);
  op_state->set_completion_callback(unique_ptr<OpCompletionCallback>(
      new LatchOpCompletionCallback<WriteResponsePB>(&rpc_latch, &resp)));

  RETURN_NOT_OK(replica->SubmitWrite(std::move(op_state)));
  rpc_latch.Wait();
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.per_row_errors_size() > 0) {
    return StatusFromPB(resp.per_row_errors(0).error());
  }
  return Status::OK();
}

void TabletReplicaTestBase::SetUp() {
  KuduTabletTest::SetUp();

  ASSERT_OK(ThreadPoolBuilder("prepare").Build(&prepare_pool_));
  ASSERT_OK(ThreadPoolBuilder("apply").Build(&apply_pool_));
  ASSERT_OK(ThreadPoolBuilder("raft").Build(&raft_pool_));
  MessengerBuilder builder("TxnStatusManagerTest");
  ASSERT_OK(builder.Build(&messenger_));

  cmeta_manager_.reset(new ConsensusMetadataManager(fs_manager()));
  metric_entity_ = METRIC_ENTITY_tablet.Instantiate(&metric_registry_, "test-tablet");
  ASSERT_OK(SetUpReplica());
}

void TabletReplicaTestBase::TearDown() {
  tablet_replica_->Shutdown();
  prepare_pool_->Shutdown();
  apply_pool_->Shutdown();
  raft_pool_->Shutdown();
  KuduTabletTest::TearDown();
}

Status TabletReplicaTestBase::SetUpReplica(bool new_replica) {
  DCHECK(tablet_replica_ == nullptr);

  RaftConfigPB config;
  config.set_opid_index(consensus::kInvalidOpIdIndex);
  RaftPeerPB* config_peer = config.add_peers();
  config_peer->set_permanent_uuid(tablet()->metadata()->fs_manager()->uuid());
  config_peer->mutable_last_known_addr()->set_host("0.0.0.0");
  config_peer->mutable_last_known_addr()->set_port(0);
  config_peer->set_member_type(RaftPeerPB::VOTER);
  if (new_replica) {
    RETURN_NOT_OK(cmeta_manager_->Create(tablet()->tablet_id(), config, consensus::kMinimumTerm));
  }

  // "Bootstrap" and start the TabletReplica.
  const auto& tablet_id = tablet()->tablet_id();
  tablet_replica_.reset(
    new TabletReplica(tablet()->shared_metadata(),
                      cmeta_manager_,
                      *config_peer,
                      apply_pool_.get(),
                      /*txn_coordinator_factory*/nullptr,
                      [tablet_id] (const string& reason) {
                        LOG(INFO) << Substitute(
                            "state change callback run for $0: $1", tablet_id, reason);
                      }));
  // Make the replica use the same LogAnchorRegistry as the tablet harness.
  // TODO(mpercy): Refactor TabletHarness to allow taking a LogAnchorRegistry,
  // while also providing TabletMetadata for consumption by TabletReplica
  // before Tablet is instantiated.
  RETURN_NOT_OK(tablet_replica_->Init({ /*quiescing*/nullptr,
                                        /*num_leaders*/nullptr,
                                        raft_pool_.get() }));
  tablet_replica_->log_anchor_registry_ = tablet()->log_anchor_registry_;
  return Status::OK();
}

Status TabletReplicaTestBase::StartReplica(const ConsensusBootstrapInfo& info) {
  scoped_refptr<Log> log;
  RETURN_NOT_OK(Log::Open(LogOptions(),
                          fs_manager(),
                          /*file_cache*/nullptr,
                          tablet()->tablet_id(),
                          *tablet()->schema(),
                          tablet()->metadata()->schema_version(),
                          metric_entity_.get(),
                          &log));
  tablet_replica_->SetBootstrapping();
  return tablet_replica_->Start(info,
                                tablet(),
                                clock(),
                                messenger_,
                                scoped_refptr<ResultTracker>(),
                                log,
                                prepare_pool_.get(),
                                dns_resolver_.get());
}

Status TabletReplicaTestBase::StartReplicaAndWaitUntilLeader(const ConsensusBootstrapInfo& info) {
  RETURN_NOT_OK(StartReplica(info));
  return tablet_replica_->consensus()->WaitUntilLeaderForTests(kLeadershipTimeout);
}

Status TabletReplicaTestBase::RestartReplica(bool reset_tablet) {
  tablet_replica_->Shutdown();
  tablet_replica_.reset();
  // Reset the underlying harness's tablet if requested.
  if (reset_tablet) {
    CreateTestTablet();
    // NOTE: the FsManager is owned by the harness, so refreshing the harness
    // means we have to refresh anything that depends on its FsManager.
    cmeta_manager_.reset(new ConsensusMetadataManager(fs_manager()));
  }
  RETURN_NOT_OK(SetUpReplica(/*new_replica=*/ false));
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(cmeta_manager_->Load(tablet_replica_->tablet_id(), &cmeta));
  shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;
  ConsensusBootstrapInfo bootstrap_info;

  tablet_replica_->SetBootstrapping();
  RETURN_NOT_OK(BootstrapTablet(tablet_replica_->tablet_metadata(),
                                cmeta->CommittedConfig(),
                                clock(),
                                /*mem_tracker*/nullptr,
                                /*result_tracker*/nullptr,
                                &metric_registry_,
                                /*file_cache*/nullptr,
                                tablet_replica_,
                                tablet_replica_->log_anchor_registry(),
                                &tablet,
                                &log,
                                &bootstrap_info));
  RETURN_NOT_OK(tablet_replica_->Start(bootstrap_info,
                                       tablet,
                                       clock(),
                                       messenger_,
                                       scoped_refptr<ResultTracker>(),
                                       log,
                                       prepare_pool_.get(),
                                       dns_resolver_.get()));
  // Wait for the replica to be usable.
  return tablet_replica_->consensus()->WaitUntilLeaderForTests(kLeadershipTimeout);
}

} // namespace tablet
} // namespace kudu
