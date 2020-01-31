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

#include <atomic>
#include <cstdint>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/maintenance_manager.h"

namespace kudu {

class Histogram;
template <class T>
class AtomicGauge;

namespace tablet {

class Tablet;

class TabletOpBase : public MaintenanceOp {
 public:
  TabletOpBase(std::string name, IOUsage io_usage, Tablet* tablet);
  std::string LogPrefix() const;

 protected:
  int32_t priority() const override;

  Tablet* const tablet_;
};

// MaintenanceOp for rowset compaction.
//
// This periodically invokes the tablet's CompactionPolicy to select a compaction.  The
// compaction policy's "quality" is used as a proxy for the performance improvement which
// is exposed back to the maintenance manager. As compactions become more fruitful (i.e.
// more overlapping rowsets), the perf_improvement score goes up, increasing priority
// with which a compaction on this tablet will be selected by the maintenance manager.
class CompactRowSetsOp : public TabletOpBase {
 public:
  explicit CompactRowSetsOp(Tablet* tablet);

  void UpdateStats(MaintenanceOpStats* stats) override;

  bool Prepare() override;

  void Perform() override;

  scoped_refptr<Histogram> DurationHistogram() const override;

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;

 private:
  mutable simple_spinlock lock_;
  MaintenanceOpStats prev_stats_;
  uint64_t last_num_mrs_flushed_;
  uint64_t last_num_rs_compacted_;
};

// MaintenanceOp to run minor compaction on delta stores.
//
// There is only one MinorDeltaCompactionOp per tablet, so it picks the RowSet that needs the most
// work. The RS we end up compacting in Perform() can be different than the one reported in
// UpdateStats, we just pick the worst each time.
class MinorDeltaCompactionOp : public TabletOpBase {
 public:
  explicit MinorDeltaCompactionOp(Tablet* tablet);

  void UpdateStats(MaintenanceOpStats* stats) override;

  bool Prepare() override;

  void Perform() override;

  scoped_refptr<Histogram> DurationHistogram() const override;

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;

 private:
  mutable simple_spinlock lock_;
  MaintenanceOpStats prev_stats_;
  uint64_t last_num_mrs_flushed_;
  uint64_t last_num_dms_flushed_;
  uint64_t last_num_rs_compacted_;
  uint64_t last_num_rs_minor_delta_compacted_;
};

// MaintenanceOp to run major compaction on delta stores.
//
// It functions just like MinorDeltaCompactionOp does, except it runs major compactions.
class MajorDeltaCompactionOp : public TabletOpBase {
 public:
  explicit MajorDeltaCompactionOp(Tablet* tablet);

  void UpdateStats(MaintenanceOpStats* stats) override;

  bool Prepare() override;

  void Perform() override;

  scoped_refptr<Histogram> DurationHistogram() const override;

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;

 private:
  mutable simple_spinlock lock_;
  MaintenanceOpStats prev_stats_;
  uint64_t last_num_mrs_flushed_;
  uint64_t last_num_dms_flushed_;
  uint64_t last_num_rs_compacted_;
  uint64_t last_num_rs_minor_delta_compacted_;
  uint64_t last_num_rs_major_delta_compacted_;
};

// MaintenanceOp to garbage-collect undo delta blocks that are older than the
// ancient history mark.
class UndoDeltaBlockGCOp : public TabletOpBase {
 public:
  explicit UndoDeltaBlockGCOp(Tablet* tablet);

  // Estimates the number of bytes that may potentially be in ancient delta
  // undo blocks. Over time, as Perform() is invoked, this estimate gets more
  // accurate.
  void UpdateStats(MaintenanceOpStats* stats) override;

  bool Prepare() override;

  // Deletes ancient history data from disk. This also initializes undo delta
  // blocks greedily (in a budgeted manner controlled by the
  // --undo_delta_block_gc_init_budget_millis gflag) that makes the estimate
  // performed in UpdateStats() more accurate.
  void Perform() override;

  scoped_refptr<Histogram> DurationHistogram() const override;

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;

 private:
  std::string LogPrefix() const;

  DISALLOW_COPY_AND_ASSIGN(UndoDeltaBlockGCOp);
};

// MaintenanceOp to garbage-collect entire rowsets that are fully deleted and
// older than the ancient history mark.
class DeletedRowsetGCOp : public TabletOpBase {
 public:
  explicit DeletedRowsetGCOp(Tablet* tablet);

  // Estimate the number of bytes from rowsets that have been fully deleted and
  // exist entirely before the AHM (i.e. their most recent update happened
  // before the AHM).
  void UpdateStats(MaintenanceOpStats* stats) override;

  // If this op is already running, we shouldn't run it again.
  bool Prepare() override {
    bool false_ref = false;
    return running_.compare_exchange_strong(false_ref, true);
  }

  // Deletes ancient deleted rowsets from disk.
  void Perform() override;

  // Metrics for this op.
  scoped_refptr<Histogram> DurationHistogram() const override;
  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override;
 private:
  std::string LogPrefix() const;

  // Used to ensure only a single instance of this op is scheduled per tablet
  // at a time.
  std::atomic<bool> running_;

  DISALLOW_COPY_AND_ASSIGN(DeletedRowsetGCOp);
};

} // namespace tablet
} // namespace kudu

