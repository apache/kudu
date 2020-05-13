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

#include "kudu/tablet/ops/op_tracker.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/op_driver.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"

DEFINE_int64(tablet_transaction_memory_limit_mb, 64,
             "Maximum amount of memory that may be consumed by all in-flight "
             "ops belonging to a particular tablet. When this limit "
             "is reached, new ops will be rejected and clients will "
             "be forced to retry them. If -1, op memory tracking is "
             "disabled.");
TAG_FLAG(tablet_transaction_memory_limit_mb, advanced);

DECLARE_int64(rpc_max_message_size);

METRIC_DEFINE_gauge_uint64(tablet, all_transactions_inflight,
                           "Ops In Flight",
                           kudu::MetricUnit::kTransactions,
                           "Number of ops currently in-flight, including any type.",
                           kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_uint64(tablet, write_transactions_inflight,
                           "Write Ops In Flight",
                           kudu::MetricUnit::kTransactions,
                           "Number of write ops currently in-flight",
                           kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_uint64(tablet, alter_schema_transactions_inflight,
                           "Alter Schema Ops In Flight",
                           kudu::MetricUnit::kTransactions,
                           "Number of alter schema ops currently in-flight",
                           kudu::MetricLevel::kDebug);

METRIC_DEFINE_counter(tablet, transaction_memory_pressure_rejections,
                      "Op Memory Pressure Rejections",
                      kudu::MetricUnit::kTransactions,
                      "Number of ops rejected because the tablet's op"
                      "memory usage exceeds the op memory limit or the limit"
                      "of an ancestral tracker.",
                      kudu::MetricLevel::kWarn);

METRIC_DEFINE_counter(tablet, transaction_memory_limit_rejections,
                      "Tablet Op Memory Limit Rejections",
                      kudu::MetricUnit::kTransactions,
                      "Number of ops rejected because the tablet's "
                      "op memory limit was reached.",
                      kudu::MetricLevel::kWarn);

using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

static bool ValidateOpMemoryLimit(const char* flagname, int64_t value) {
  // -1 is a special value for the  --tablet_transaction_memory_limit_mb flag.
  if (value < -1) {
    LOG(ERROR) << Substitute("$0: invalid value for flag $1", value, flagname);
    return false;
  }
  return true;
}
DEFINE_validator(tablet_transaction_memory_limit_mb, ValidateOpMemoryLimit);

static bool ValidateOpMemoryAndRpcSize() {
  const int64_t op_max_size =
      FLAGS_tablet_transaction_memory_limit_mb * 1024 * 1024;
  const int64_t rpc_max_size = FLAGS_rpc_max_message_size;
  if (op_max_size >= 0 && op_max_size < rpc_max_size) {
    LOG(ERROR) << Substitute(
        "--tablet_transaction_memory_limit_mb is set too low compared with "
        "--rpc_max_message_size; increase --tablet_transaction_memory_limit_mb "
        "at least up to $0", (rpc_max_size + 1024 * 1024 - 1) / (1024 * 1024));
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(transaction_memory_and_rpc_size,
                     ValidateOpMemoryAndRpcSize);

namespace kudu {
namespace tablet {

#define MINIT(x) x(METRIC_##x.Instantiate(entity))
#define GINIT(x) x(METRIC_##x.Instantiate(entity, 0))
OpTracker::Metrics::Metrics(const scoped_refptr<MetricEntity>& entity)
    : GINIT(all_transactions_inflight),
      GINIT(write_transactions_inflight),
      GINIT(alter_schema_transactions_inflight),
      MINIT(transaction_memory_pressure_rejections),
      MINIT(transaction_memory_limit_rejections) {
}
#undef GINIT
#undef MINIT

OpTracker::State::State()
  : memory_footprint(0) {
}

OpTracker::OpTracker() {
}

OpTracker::~OpTracker() {
  std::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(pending_ops_.size(), 0);
}

Status OpTracker::Add(OpDriver* driver) {
  int64_t driver_mem_footprint = driver->state()->request()->SpaceUsed();
  if (mem_tracker_ && !mem_tracker_->TryConsume(driver_mem_footprint)) {
    if (metrics_) {
      metrics_->transaction_memory_pressure_rejections->Increment();
      if (!mem_tracker_->CanConsumeNoAncestors(driver_mem_footprint)) {
        metrics_->transaction_memory_limit_rejections->Increment();
      }
    }

    // May be null in unit tests.
    TabletReplica* replica = driver->state()->tablet_replica();

    string msg = Substitute(
        "op on tablet $0 rejected due to memory pressure: the memory "
        "usage of this op ($1) plus the current consumption ($2) "
        "exceeds the op memory limit ($3) or the limit of an ancestral "
        "memory tracker.",
        replica ? replica->tablet()->tablet_id() : "(unknown)",
        driver_mem_footprint, mem_tracker_->consumption(), mem_tracker_->limit());

    KLOG_EVERY_N_SECS(WARNING, 1) << msg << THROTTLE_MSG;

    return Status::ServiceUnavailable(msg);
  }

  IncrementCounters(*driver);

  // Cache the op memory footprint so we needn't refer to the request
  // again, as it may disappear between now and then.
  State st;
  st.memory_footprint = driver_mem_footprint;
  std::lock_guard<simple_spinlock> l(lock_);
  InsertOrDie(&pending_ops_, driver, st);
  return Status::OK();
}

void OpTracker::IncrementCounters(const OpDriver& driver) const {
  if (!metrics_) {
    return;
  }

  metrics_->all_transactions_inflight->Increment();
  switch (driver.op_type()) {
    case Op::WRITE_OP:
      metrics_->write_transactions_inflight->Increment();
      break;
    case Op::ALTER_SCHEMA_OP:
      metrics_->alter_schema_transactions_inflight->Increment();
      break;
  }
}

void OpTracker::DecrementCounters(const OpDriver& driver) const {
  if (!metrics_) {
    return;
  }

  DCHECK_GT(metrics_->all_transactions_inflight->value(), 0);
  metrics_->all_transactions_inflight->Decrement();
  switch (driver.op_type()) {
    case Op::WRITE_OP:
      DCHECK_GT(metrics_->write_transactions_inflight->value(), 0);
      metrics_->write_transactions_inflight->Decrement();
      break;
    case Op::ALTER_SCHEMA_OP:
      DCHECK_GT(metrics_->alter_schema_transactions_inflight->value(), 0);
      metrics_->alter_schema_transactions_inflight->Decrement();
      break;
  }
}

void OpTracker::Release(OpDriver* driver) {
  DecrementCounters(*driver);

  // Remove the op from the map updating memory consumption if needed.
  std::lock_guard<simple_spinlock> l(lock_);
  State st = FindOrDie(pending_ops_, driver);
  if (mem_tracker_) {
    mem_tracker_->Release(st.memory_footprint);
  }
  if (PREDICT_FALSE(pending_ops_.erase(driver) != 1)) {
    LOG(FATAL) << "Could not remove pending op from map: "
        << driver->ToStringUnlocked();
  }
}

void OpTracker::GetPendingOps(
    vector<scoped_refptr<OpDriver> >* pending_out) const {
  DCHECK(pending_out->empty());
  std::lock_guard<simple_spinlock> l(lock_);
  for (const TxnMap::value_type& e : pending_ops_) {
    // Increments refcount of each op.
    pending_out->push_back(e.first);
  }
}

int OpTracker::GetNumPendingForTests() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return pending_ops_.size();
}

void OpTracker::WaitForAllToFinish() const {
  // Wait indefinitely.
  CHECK_OK(WaitForAllToFinish(MonoDelta::FromNanoseconds(std::numeric_limits<int64_t>::max())));
}

Status OpTracker::WaitForAllToFinish(const MonoDelta& timeout) const {
  static constexpr size_t kMaxTxnsToPrint = 50;
  int wait_time_us = 250;
  int num_complaints = 0;
  MonoTime start_time = MonoTime::Now();
  MonoTime next_log_time = start_time + MonoDelta::FromSeconds(1);

  while (1) {
    vector<scoped_refptr<OpDriver> > ops;
    GetPendingOps(&ops);

    if (ops.empty()) {
      break;
    }

    MonoTime now = MonoTime::Now();
    MonoDelta diff = now - start_time;
    if (diff > timeout) {
      return Status::TimedOut(Substitute("Timed out waiting for all ops to finish. "
                                         "$0 ops pending. Waited for $1",
                                         ops.size(), diff.ToString()));
    }
    if (now > next_log_time) {
      LOG(WARNING) << Substitute("OpTracker waiting for $0 outstanding ops to"
                                 " complete now for $1", ops.size(), diff.ToString());
      LOG(INFO) << Substitute("Dumping up to $0 currently running ops: ",
                              kMaxTxnsToPrint);
      const auto num_op_limit = std::min(ops.size(), kMaxTxnsToPrint);
      for (auto i = 0; i < num_op_limit; i++) {
        LOG(INFO) << ops[i]->ToString();
      }

      num_complaints++;
      // Exponential back-off on how often the ops are dumped.
      next_log_time = now + MonoDelta::FromSeconds(1 << std::min(8, num_complaints));
    }
    wait_time_us = std::min(wait_time_us * 5 / 4, 1000000);
    SleepFor(MonoDelta::FromMicroseconds(wait_time_us));
  }
  return Status::OK();
}

void OpTracker::StartInstrumentation(
    const scoped_refptr<MetricEntity>& metric_entity) {
  metrics_.reset(new Metrics(metric_entity));
}

void OpTracker::StartMemoryTracking(
    const shared_ptr<MemTracker>& parent_mem_tracker) {
  if (FLAGS_tablet_transaction_memory_limit_mb != -1) {
    mem_tracker_ = MemTracker::CreateTracker(
        FLAGS_tablet_transaction_memory_limit_mb * 1024 * 1024,
        "op_tracker",
        parent_mem_tracker);
  }
}

}  // namespace tablet
}  // namespace kudu
