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
#ifndef KUDU_TABLET_TABLET_BOOTSTRAP_H_
#define KUDU_TABLET_TABLET_BOOTSTRAP_H_

#include <memory>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class MetricRegistry;
class Partition;
class PartitionSchema;

namespace log {
class Log;
class LogAnchorRegistry;
}

namespace consensus {
struct ConsensusBootstrapInfo;
class ConsensusMetadataManager;
} // namespace consensus

namespace rpc {
class ResultTracker;
} // namespace rpc

namespace clock {
class Clock;
}

namespace tablet {
class Tablet;
class TabletMetadata;

extern const char* kLogRecoveryDir;

// Bootstraps a tablet, initializing it with the provided metadata. If the tablet
// has blocks and log segments, this method rebuilds the soft state by replaying
// the Log.
//
// This is a synchronous method, but is typically called within a thread pool by
// TSTabletManager.
Status BootstrapTablet(const scoped_refptr<TabletMetadata>& tablet_meta,
                       const scoped_refptr<consensus::ConsensusMetadataManager>& cmeta_manager,
                       const scoped_refptr<clock::Clock>& clock,
                       const std::shared_ptr<MemTracker>& mem_tracker,
                       const scoped_refptr<rpc::ResultTracker>& result_tracker,
                       MetricRegistry* metric_registry,
                       const scoped_refptr<TabletReplica>& tablet_replica,
                       std::shared_ptr<Tablet>* rebuilt_tablet,
                       scoped_refptr<log::Log>* rebuilt_log,
                       const scoped_refptr<log::LogAnchorRegistry>& log_anchor_registry,
                       consensus::ConsensusBootstrapInfo* consensus_info);

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TABLET_BOOTSTRAP_H_ */
