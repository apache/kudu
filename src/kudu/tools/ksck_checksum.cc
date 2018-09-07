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

#include "kudu/tools/ksck_checksum.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>

#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck.h"

using std::endl;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using strings::Substitute;

DEFINE_int32(checksum_timeout_sec, 3600,
             "Maximum total seconds to wait for a checksum scan to complete "
             "before timing out.");
DEFINE_int32(checksum_scan_concurrency, 4,
             "Number of concurrent checksum scans to execute per tablet server.");
DEFINE_bool(checksum_snapshot, true, "Should the checksum scanner use a snapshot scan?");
DEFINE_uint64(checksum_snapshot_timestamp,
              kudu::tools::KsckChecksumOptions::kCurrentTimestamp,
              "Timestamp to use for snapshot checksum scans. Defaults to 0, which "
              "uses the current timestamp of a tablet server involved in the scan.");

namespace kudu {
namespace tools {

KsckChecksumOptions::KsckChecksumOptions()
    : timeout(MonoDelta::FromSeconds(FLAGS_checksum_timeout_sec)),
      scan_concurrency(FLAGS_checksum_scan_concurrency),
      use_snapshot(FLAGS_checksum_snapshot),
      snapshot_timestamp(FLAGS_checksum_snapshot_timestamp) {}

KsckChecksumOptions::KsckChecksumOptions(MonoDelta timeout, int scan_concurrency,
                                         bool use_snapshot, uint64_t snapshot_timestamp)
    : timeout(timeout),
      scan_concurrency(scan_concurrency),
      use_snapshot(use_snapshot),
      snapshot_timestamp(snapshot_timestamp) {}

ChecksumResultReporter::ChecksumResultReporter(int num_tablet_replicas)
    : expected_count_(num_tablet_replicas),
      responses_(num_tablet_replicas),
      rows_summed_(0),
      disk_bytes_summed_(0) {}

void ChecksumResultReporter::ReportProgress(int64_t delta_rows, int64_t delta_bytes) {
  rows_summed_.IncrementBy(delta_rows);
  disk_bytes_summed_.IncrementBy(delta_bytes);
}

// Write an entry to the result map indicating a response from the remote.
void ChecksumResultReporter::ReportResult(const string& tablet_id,
                                          const string& replica_uuid,
                                          const Status& status,
                                          uint64_t checksum) {
  std::lock_guard<simple_spinlock> guard(lock_);
  unordered_map<string, ResultPair>& replica_results =
      LookupOrInsert(&checksums_, tablet_id, unordered_map<string, ResultPair>());
  InsertOrDie(&replica_results, replica_uuid, ResultPair(status, checksum));
  responses_.CountDown();
}

// Blocks until either the number of results plus errors reported equals
// num_tablet_replicas (from the constructor), or until the timeout expires,
// whichever comes first. Progress messages are printed to 'out'.
// Returns false if the timeout expired before all responses came in.
// Otherwise, returns true.
// Print progress updates to 'out' if it is non-null.
bool ChecksumResultReporter::WaitFor(const MonoDelta& timeout, std::ostream* out) const {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;

  bool done = false;
  while (!done) {
    MonoTime now = MonoTime::Now();
    auto rem_ms = (deadline - now).ToMilliseconds();
    if (rem_ms <= 0) return false;

    constexpr int64_t max_wait_ms = 5000;
    done = responses_.WaitFor(
        MonoDelta::FromMilliseconds(std::min(rem_ms, max_wait_ms)));
    if (out) {
      string status = done ? "finished in" : "running for";
      int run_time_sec = (MonoTime::Now() - start).ToSeconds();
      (*out) << Substitute("Checksum $0 $1s: $2/$3 replicas remaining "
                           "($4 from disk, $5 rows summed)",
                           status,
                           run_time_sec,
                           responses_.count(),
                           expected_count_,
                           HumanReadableNumBytes::ToString(disk_bytes_summed_.Load()),
                           HumanReadableInt::ToString(rows_summed_.Load()))
             << endl;
    }
  }
  return true;
}

TabletServerChecksumCallbacks::TabletServerChecksumCallbacks(
    scoped_refptr<ChecksumResultReporter> reporter,
    shared_ptr<KsckTabletServer> tablet_server,
    SharedTabletQueue queue,
    string tablet_id,
    KsckChecksumOptions options)
    : reporter_(std::move(reporter)),
      tablet_server_(std::move(tablet_server)),
      queue_(std::move(queue)),
      options_(options),
      tablet_id_(std::move(tablet_id)) {}

void TabletServerChecksumCallbacks::Progress(int64_t rows_summed, int64_t disk_bytes_summed) {
  reporter_->ReportProgress(rows_summed, disk_bytes_summed);
}

void TabletServerChecksumCallbacks::Finished(const Status& status, uint64_t checksum) {
  reporter_->ReportResult(tablet_id_, tablet_server_->uuid(), status, checksum);

  std::pair<Schema, string> table_tablet;
  if (queue_->BlockingGet(&table_tablet)) {
    const Schema& table_schema = table_tablet.first;
    tablet_id_ = table_tablet.second;
    tablet_server_->RunTabletChecksumScanAsync(tablet_id_, table_schema, options_, this);
  } else {
    delete this;
  }
}

} // namespace tools
} // namespace kudu
