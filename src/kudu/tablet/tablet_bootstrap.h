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
#pragma once

#include <memory>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class FileCache;
class MemTracker;
class MetricRegistry;

namespace log {
class Log;
class LogAnchorRegistry;
} // namespace log

namespace consensus {
class RaftConfigPB;
struct ConsensusBootstrapInfo;
} // namespace consensus

namespace rpc {
class ResultTracker;
} // namespace rpc

namespace clock {
class Clock;
} // namespace clock

namespace tablet {
class Tablet;
class TabletMetadata;
class TabletReplica;

extern const char* kLogRecoveryDir;

// Bootstraps a tablet, initializing it with the provided metadata. If the tablet
// has blocks and log segments, this method rebuilds the soft state by replaying
// the Log.
//
// This is a synchronous method, but is typically called within a thread pool by
// TSTabletManager.
Status BootstrapTablet(scoped_refptr<TabletMetadata> tablet_meta,
                       consensus::RaftConfigPB committed_raft_config,
                       clock::Clock* clock,
                       std::shared_ptr<MemTracker> mem_tracker,
                       scoped_refptr<rpc::ResultTracker> result_tracker,
                       MetricRegistry* metric_registry,
                       FileCache* file_cache,
                       scoped_refptr<TabletReplica> tablet_replica,
                       scoped_refptr<log::LogAnchorRegistry> log_anchor_registry,
                       std::shared_ptr<Tablet>* rebuilt_tablet,
                       scoped_refptr<log::Log>* rebuilt_log,
                       consensus::ConsensusBootstrapInfo* consensus_info);

}  // namespace tablet
}  // namespace kudu
