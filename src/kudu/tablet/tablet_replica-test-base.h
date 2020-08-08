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

#include <memory>

#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

class Schema;

namespace consensus {
struct ConsensusBootstrapInfo;
}  // namespace consensus

namespace rpc {
class Messenger;
}  // namespace rpc

namespace tserver {
class WriteRequestPB;
}  // namespace tserver

namespace tablet {

class TabletReplicaTestBase : public KuduTabletTest {
 public:
  explicit TabletReplicaTestBase(const Schema& schema)
      : KuduTabletTest(schema, TabletHarness::Options::ClockType::HYBRID_CLOCK),
        dns_resolver_(new DnsResolver) {}

  // Submits the given request to the given replica, waiting for the request to
  // complete before returning.
  static Status ExecuteWrite(TabletReplica* replica, const tserver::WriteRequestPB& req);

  void SetUp() override;
  void TearDown() override;

  // Initializes the tablet replica.
  Status SetUpReplica(bool new_replica = true);

  // Bootstraps the tablet replica.
  Status StartReplica(const consensus::ConsensusBootstrapInfo& info);
  Status StartReplicaAndWaitUntilLeader(const consensus::ConsensusBootstrapInfo& info);

  // Shuts down and restarts the tablet replica, bootstrapping it from its
  // on-disk stores and WALs.
  //
  // If 'reset_tablet' is set to true, resets the underlying tablet harness'
  // Tablet instance. The 'false' default is for cases in which the tablet may
  // have persisted a schema change; otherwise rebuilding the tablet may crash
  // with a schema mismatch.
  Status RestartReplica(bool reset_tablet = false);

  const scoped_refptr<TabletReplica>& tablet_replica() const { return tablet_replica_; }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  std::shared_ptr<rpc::Messenger> messenger_;
  std::unique_ptr<ThreadPool> prepare_pool_;
  std::unique_ptr<ThreadPool> apply_pool_;
  std::unique_ptr<ThreadPool> raft_pool_;
  std::unique_ptr<DnsResolver> dns_resolver_;

  scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  // NOTE: This must be destroyed before thread pools.
  scoped_refptr<TabletReplica> tablet_replica_;
};

} // namespace tablet
} // namespace kudu
