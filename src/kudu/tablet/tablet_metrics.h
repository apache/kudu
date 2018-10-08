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
#ifndef KUDU_TABLET_TABLET_METRICS_H
#define KUDU_TABLET_TABLET_METRICS_H

#include <cstdint>
#include <stddef.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"

namespace kudu {

class Arena;

namespace tablet {

struct ProbeStats;

// Container for all metrics specific to a single tablet.
struct TabletMetrics {
  explicit TabletMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  // Add a batch of probe stats to the metrics.
  //
  // We use C-style array passing here since the call site allocates the
  // ProbeStats from an arena.
  //
  // This allocates temporary scratch space from work_arena.
  void AddProbeStats(const ProbeStats* stats_array, int len, Arena* work_arena);

  // Operation rates.
  scoped_refptr<Counter> rows_inserted;
  scoped_refptr<Counter> rows_upserted;
  scoped_refptr<Counter> rows_updated;
  scoped_refptr<Counter> rows_deleted;
  scoped_refptr<Counter> insertions_failed_dup_key;
  scoped_refptr<Counter> upserts_as_updates;

  // Scanner metrics.
  scoped_refptr<Counter> scanner_rows_returned;
  scoped_refptr<Counter> scanner_cells_returned;
  scoped_refptr<Counter> scanner_bytes_returned;
  scoped_refptr<Counter> scanner_rows_scanned;
  scoped_refptr<Counter> scanner_cells_scanned_from_disk;
  scoped_refptr<Counter> scanner_bytes_scanned_from_disk;
  scoped_refptr<Counter> scans_started;
  scoped_refptr<AtomicGauge<size_t>> tablet_active_scanners;

  // Probe stats.
  scoped_refptr<Counter> bloom_lookups;
  scoped_refptr<Counter> key_file_lookups;
  scoped_refptr<Counter> delta_file_lookups;
  scoped_refptr<Counter> mrs_lookups;

  // Operation stats.
  scoped_refptr<Counter> bytes_flushed;
  scoped_refptr<Counter> undo_delta_block_gc_bytes_deleted;

  scoped_refptr<Histogram> bloom_lookups_per_op;
  scoped_refptr<Histogram> key_file_lookups_per_op;
  scoped_refptr<Histogram> delta_file_lookups_per_op;

  scoped_refptr<Histogram> commit_wait_duration;
  scoped_refptr<Histogram> snapshot_read_inflight_wait_duration;
  scoped_refptr<Histogram> write_op_duration_client_propagated_consistency;
  scoped_refptr<Histogram> write_op_duration_commit_wait_consistency;

  scoped_refptr<AtomicGauge<uint32_t> > flush_dms_running;
  scoped_refptr<AtomicGauge<uint32_t> > flush_mrs_running;
  scoped_refptr<AtomicGauge<uint32_t> > compact_rs_running;
  scoped_refptr<AtomicGauge<uint32_t> > delta_minor_compact_rs_running;
  scoped_refptr<AtomicGauge<uint32_t> > delta_major_compact_rs_running;
  scoped_refptr<AtomicGauge<uint32_t> > undo_delta_block_gc_running;
  scoped_refptr<AtomicGauge<int64_t> > undo_delta_block_estimated_retained_bytes;

  scoped_refptr<Histogram> flush_dms_duration;
  scoped_refptr<Histogram> flush_mrs_duration;
  scoped_refptr<Histogram> compact_rs_duration;
  scoped_refptr<Histogram> delta_minor_compact_rs_duration;
  scoped_refptr<Histogram> delta_major_compact_rs_duration;
  scoped_refptr<Histogram> undo_delta_block_gc_init_duration;
  scoped_refptr<Histogram> undo_delta_block_gc_delete_duration;
  scoped_refptr<Histogram> undo_delta_block_gc_perform_duration;

  scoped_refptr<Counter> leader_memory_pressure_rejections;

  // Compaction metrics.
  scoped_refptr<AtomicGauge<double>> average_diskrowset_height;
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_TABLET_METRICS_H */
