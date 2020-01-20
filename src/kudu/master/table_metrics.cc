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

#include <mutex>

#include "kudu/gutil/map-util.h"

METRIC_DECLARE_gauge_size(merged_entities_count_of_table);

namespace kudu {
namespace master {

// Table-specific stats.
METRIC_DEFINE_gauge_uint64(table, on_disk_size, "Table Size On Disk",
    kudu::MetricUnit::kBytes,
    "Pre-replication aggregated disk space used by all tablets in this table, "
    "including metadata.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_uint64(table, live_row_count, "Table Live Row count",
    kudu::MetricUnit::kRows,
    "Pre-replication aggregated number of live rows in this table. "
    "Only accurate if all tablets in the table support live row counting.",
    kudu::MetricLevel::kInfo);

#define GINIT(x) x(METRIC_##x.Instantiate(entity, 0))
#define HIDEINIT(x, v) x(METRIC_##x.InstantiateHidden(entity, v))
TableMetrics::TableMetrics(const scoped_refptr<MetricEntity>& entity)
  : GINIT(on_disk_size),
    GINIT(live_row_count),
    HIDEINIT(merged_entities_count_of_table, 1) {
}
#undef GINIT
#undef HIDEINIT

void TableMetrics::AddTabletNoOnDiskSize(const std::string& tablet_id) {
  std::lock_guard<simple_spinlock> l(lock_);
  InsertIfNotPresent(&tablet_ids_no_on_disk_size_, tablet_id);
}

void TableMetrics::DeleteTabletNoOnDiskSize(const std::string& tablet_id) {
  std::lock_guard<simple_spinlock> l(lock_);
  tablet_ids_no_on_disk_size_.erase(tablet_id);
}

bool TableMetrics::ContainsTabletNoOnDiskSize(const std::string& tablet_id) const {
  std::lock_guard<simple_spinlock> l(lock_);
  return ContainsKey(tablet_ids_no_on_disk_size_, tablet_id);
}

bool TableMetrics::TableSupportsOnDiskSize() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return tablet_ids_no_on_disk_size_.empty();
}

void TableMetrics::AddTabletNoLiveRowCount(const std::string& tablet_id) {
  std::lock_guard<simple_spinlock> l(lock_);
  InsertIfNotPresent(&tablet_ids_no_live_row_count_, tablet_id);
}

void TableMetrics::DeleteTabletNoLiveRowCount(const std::string& tablet_id) {
  std::lock_guard<simple_spinlock> l(lock_);
  tablet_ids_no_live_row_count_.erase(tablet_id);
}

bool TableMetrics::ContainsTabletNoLiveRowCount(const std::string& tablet_id) const {
  std::lock_guard<simple_spinlock> l(lock_);
  return ContainsKey(tablet_ids_no_live_row_count_, tablet_id);
}

bool TableMetrics::TableSupportsLiveRowCount() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return tablet_ids_no_live_row_count_.empty();
}

} // namespace master
} // namespace kudu
