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
#ifndef KUDU_MASTER_TABLE_METRICS_H
#define KUDU_MASTER_TABLE_METRICS_H

#include <cstdint>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"

namespace kudu {
namespace master {

// The table metrics consist of the LEADER tablet metrics.
//
// The tservers periodically update tablet metrics based on the gflag
// FLAGS_update_tablet_stats_interval_ms. Then each tserver sends its
// own LEADER tablets' metrics to all of the masters through heartbeat
// messages. But only the LEADER master aggregates and exposes these
// metrics. These metrics are pre-replication.
//
// Note: the process is asynchronous, so the data are lagging.
//
// At the same time, there will be fluctuation of metrics possibly if
// the tablet's leadership changes. And if the new LEADER master is
// elected, the metrics may not be accurate until all tservers report,
// and the time window should be FLAGS_heartbeat_interval_ms.
struct TableMetrics {
  explicit TableMetrics(const scoped_refptr<MetricEntity>& entity);

  scoped_refptr<AtomicGauge<int64_t>> on_disk_size;
  scoped_refptr<AtomicGauge<int64_t>> live_row_count;
};

} // namespace master
} // namespace kudu

#endif
