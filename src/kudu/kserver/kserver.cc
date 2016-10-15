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

#include "kudu/kserver/kserver.h"

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "kudu/rpc/messenger.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

using std::string;

namespace kudu {

using server::ServerBaseOptions;

namespace kserver {

METRIC_DEFINE_histogram(server, op_apply_queue_length, "Operation Apply Queue Length",
                        MetricUnit::kTasks,
                        "Number of operations waiting to be applied to the tablet. "
                        "High queue lengths indicate that the server is unable to process "
                        "operations as fast as they are being written to the WAL.",
                        10000, 2);

METRIC_DEFINE_histogram(server, op_apply_queue_time, "Operation Apply Queue Time",
                        MetricUnit::kMicroseconds,
                        "Time that operations spent waiting in the apply queue before being "
                        "processed. High queue times indicate that the server is unable to "
                        "process operations as fast as they are being written to the WAL.",
                        10000000, 2);

METRIC_DEFINE_histogram(server, op_apply_run_time, "Operation Apply Run Time",
                        MetricUnit::kMicroseconds,
                        "Time that operations spent being applied to the tablet. "
                        "High values may indicate that the server is under-provisioned or "
                        "that operations consist of very large batches.",
                        10000000, 2);


KuduServer::KuduServer(string name,
                       const ServerBaseOptions& options,
                       const string& metric_namespace)
    : ServerBase(std::move(name), options, metric_namespace) {
}

Status KuduServer::Init() {
  RETURN_NOT_OK(ServerBase::Init());

  ThreadPoolMetrics metrics = {
      METRIC_op_apply_queue_length.Instantiate(metric_entity_),
      METRIC_op_apply_queue_time.Instantiate(metric_entity_),
      METRIC_op_apply_run_time.Instantiate(metric_entity_)
  };
  RETURN_NOT_OK(ThreadPoolBuilder("apply")
                .set_metrics(std::move(metrics))
                .Build(&tablet_apply_pool_));

  // These pools are shared by all replicas hosted by this server.
  //
  // Submitted tasks use blocking IO (raft_pool_) or acquire long-held locks
  // (tablet_prepare_pool_) so we configure no upper bound on the maximum
  // number of threads in each pool (otherwise the default value of "number of
  // CPUs" may cause blocking tasks to starve other "fast" tasks). However, the
  // effective upper bound is the number of replicas as each will submit its
  // own tasks via a dedicated token.
  RETURN_NOT_OK(ThreadPoolBuilder("prepare")
                .set_max_threads(std::numeric_limits<int>::max())
                .Build(&tablet_prepare_pool_));
  RETURN_NOT_OK(ThreadPoolBuilder("raft")
                .set_trace_metric_prefix("raft")
                .set_max_threads(std::numeric_limits<int>::max())
                .Build(&raft_pool_));

  return Status::OK();
}

Status KuduServer::Start() {
  RETURN_NOT_OK(ServerBase::Start());
  return Status::OK();
}

void KuduServer::Shutdown() {
  // Shut down the messenger early, waiting for any reactor threads to finish
  // running. This ensures that any ref-counted objects inside closures run by
  // reactor threads will be destroyed before we shut down server-wide thread
  // pools below, which is important because those objects may own tokens
  // belonging to the pools.
  //
  // Note: prior to this call, it is assumed that any incoming RPCs deferred
  // from reactor threads have already been cleaned up.
  messenger_->Shutdown();

  // The shutdown order here shouldn't matter; shutting down the messenger
  // first ensures that all outstanding RaftConsensus instances are destroyed.
  // Thus, there shouldn't be lingering activity on any of these pools.
  if (raft_pool_) {
    raft_pool_->Shutdown();
  }
  if (tablet_apply_pool_) {
    tablet_apply_pool_->Shutdown();
  }
  if (tablet_prepare_pool_) {
    tablet_prepare_pool_->Shutdown();
  }
  ServerBase::Shutdown();
}

} // namespace kserver
} // namespace kudu
