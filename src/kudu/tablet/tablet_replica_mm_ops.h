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

#ifndef KUDU_TABLET_TABLET_REPLICA_MM_OPS_H_
#define KUDU_TABLET_TABLET_REPLICA_MM_OPS_H_

#include <cstdint>
#include <string>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/locks.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/stopwatch.h"

namespace kudu {
namespace tablet {

class FlushOpPerfImprovementPolicy {
 public:
  ~FlushOpPerfImprovementPolicy() {}

  // Sets the performance improvement based on the anchored ram if it's over the threshold,
  // else it will set it based on how long it has been since the last flush.
  static void SetPerfImprovementForFlush(MaintenanceOpStats* stats, double elapsed_ms);

 private:
  FlushOpPerfImprovementPolicy() {}
};

class TabletReplicaOpBase : public MaintenanceOp {
 public:
  explicit TabletReplicaOpBase(std::string name, IOUsage io_usage, TabletReplica* tablet_replica);

 protected:
  const std::string& table_id() const override;

  TabletReplica *const tablet_replica_;
};

// Maintenance op for MRS flush. Only one can happen at a time.
class FlushMRSOp : public TabletReplicaOpBase {
 public:
  explicit FlushMRSOp(TabletReplica* tablet_replica)
    : TabletReplicaOpBase(StringPrintf("FlushMRSOp(%s)",
                                       tablet_replica->tablet()->tablet_id().c_str()),
                          MaintenanceOp::HIGH_IO_USAGE,
                          tablet_replica) {
    time_since_flush_.start();
  }

  void UpdateStats(MaintenanceOpStats* stats) override;

  bool Prepare() override;

  void Perform() override;

  scoped_refptr<Histogram> DurationHistogram() const override;

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;

 private:
  // Lock protecting time_since_flush_.
  mutable simple_spinlock lock_;
  Stopwatch time_since_flush_;
};

// Maintenance op for DMS flush.
// Reports stats for all the DMS this tablet contains but only flushes one in Perform().
class FlushDeltaMemStoresOp : public TabletReplicaOpBase {
 public:
  explicit FlushDeltaMemStoresOp(TabletReplica* tablet_replica)
    : TabletReplicaOpBase(StringPrintf("FlushDeltaMemStoresOp(%s)",
                                       tablet_replica->tablet()->tablet_id().c_str()),
                          MaintenanceOp::HIGH_IO_USAGE,
                          tablet_replica) {
    time_since_flush_.start();
  }

  void UpdateStats(MaintenanceOpStats* stats) override;

  bool Prepare() override {
    return true;
  }

  void Perform() override;

  scoped_refptr<Histogram> DurationHistogram() const override;

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;

 private:
  // Lock protecting time_since_flush_
  mutable simple_spinlock lock_;
  Stopwatch time_since_flush_;
};

// Maintenance task that runs log GC. Reports log retention that represents the amount of data
// that can be GC'd.
//
// Only one LogGC op can run at a time.
class LogGCOp : public TabletReplicaOpBase {
 public:
  explicit LogGCOp(TabletReplica* tablet_replica);

  void UpdateStats(MaintenanceOpStats* stats) override;

  bool Prepare() override;

  void Perform() override;

  scoped_refptr<Histogram> DurationHistogram() const override;

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;

 private:
  scoped_refptr<Histogram> log_gc_duration_;
  scoped_refptr<AtomicGauge<uint32_t> > log_gc_running_;
  mutable Semaphore sem_;
};

} // namespace tablet
} // namespace kudu

#endif /* KUDU_TABLET_TABLET_REPLICA_MM_OPS_H_ */
