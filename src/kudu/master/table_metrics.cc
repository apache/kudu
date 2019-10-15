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
#include "kudu/master/table_metrics.h"


namespace kudu {
namespace master {

// Table-specific stats.
METRIC_DEFINE_gauge_int64(table, on_disk_size, "Table Size On Disk",
    kudu::MetricUnit::kBytes,
    "Pre-replication aggregated disk space used by all tablets in this table, "
    "including metadata.");
METRIC_DEFINE_gauge_int64(table, live_row_count, "Table Live Row count",
    kudu::MetricUnit::kRows,
    "Pre-replication aggregated number of live rows in this table. "
    "When the table doesn't support live row counting, -1 will be returned.");

#define GINIT(x) x(METRIC_##x.Instantiate(entity, 0))

TableMetrics::TableMetrics(const scoped_refptr<MetricEntity>& entity)
  : GINIT(on_disk_size),
    GINIT(live_row_count) {
}
#undef GINIT

} // namespace master
} // namespace kudu
