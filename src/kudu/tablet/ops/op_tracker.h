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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/ops/op_driver.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {

class MemTracker;
class MonoDelta;

namespace tablet {

// Each TabletReplica has a OpTracker which keeps track of pending ops.
// Each "LeaderOp" will register itself by calling Add().
// It will remove itself by calling Release().
class OpTracker {
 public:
  OpTracker();
  ~OpTracker();

  // Adds an op to the set of tracked ops.
  //
  // In the event that the tracker's memory limit is exceeded, returns a
  // ServiceUnavailable status.
  Status Add(OpDriver* driver);

  // Removes the op from the pending list.
  // Also triggers the deletion of the Op object, if its refcount == 0.
  void Release(OpDriver* driver);

  // Populates list of currently-running ops into 'pending_out' vector.
  void GetPendingOps(std::vector<scoped_refptr<OpDriver> >* pending_out) const;

  // Returns number of pending ops.
  int GetNumPendingForTests() const;

  void WaitForAllToFinish() const;
  Status WaitForAllToFinish(const MonoDelta& timeout) const;

  void StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity);
  void StartMemoryTracking(const std::shared_ptr<MemTracker>& parent_mem_tracker);

 private:
  struct Metrics {
    explicit Metrics(const scoped_refptr<MetricEntity>& entity);

    scoped_refptr<AtomicGauge<uint64_t> > all_transactions_inflight;
    scoped_refptr<AtomicGauge<uint64_t> > write_transactions_inflight;
    scoped_refptr<AtomicGauge<uint64_t> > alter_schema_transactions_inflight;

    scoped_refptr<Counter> transaction_memory_pressure_rejections;
    scoped_refptr<Counter> transaction_memory_limit_rejections;
  };

  // Increments relevant metric counters.
  void IncrementCounters(const OpDriver& driver) const;

  // Decrements relevant metric counters.
  void DecrementCounters(const OpDriver& driver) const;

  mutable simple_spinlock lock_;

  // Per-op state that is tracked along with the op itself.
  struct State {
    State();

    // Approximate memory footprint of the op.
    int64_t memory_footprint;
  };

  // Protected by 'lock_'.
  typedef std::unordered_map<scoped_refptr<OpDriver>,
      State,
      ScopedRefPtrHashFunctor<OpDriver>,
      ScopedRefPtrEqualToFunctor<OpDriver> > TxnMap;
  TxnMap pending_ops_;

  std::unique_ptr<Metrics> metrics_;

  std::shared_ptr<MemTracker> mem_tracker_;

  DISALLOW_COPY_AND_ASSIGN(OpTracker);
};

}  // namespace tablet
}  // namespace kudu
