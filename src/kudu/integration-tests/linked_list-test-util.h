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

#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"

namespace kudu {

static const char* const kKeyColumnName = "rand_key";
static const char* const kLinkColumnName = "link_to";
static const char* const kInsertTsColumnName = "insert_ts";
static const char* const kUpdatedColumnName = "updated";
static const int64_t kNoSnapshot = -2;
static const int64_t kSnapshotAtNow = -1;
static const int64_t kNoParticularCountExpected = -1;

// Vector of snapshot timestamp, count pairs.
typedef std::vector<std::pair<uint64_t, int64_t> > SnapsAndCounts;

// Provides methods for writing data and reading it back in such a way that
// facilitates checking for data integrity.
class LinkedListTester {
 public:
  LinkedListTester(client::sp::shared_ptr<client::KuduClient> client,
                   std::string table_name, int num_chains, int num_tablets,
                   int num_replicas, bool enable_mutation)
      : verify_projection_(
            {kKeyColumnName, kLinkColumnName, kUpdatedColumnName}),
        table_name_(std::move(table_name)),
        num_chains_(num_chains),
        num_tablets_(num_tablets),
        num_replicas_(num_replicas),
        enable_mutation_(enable_mutation),
        latency_histogram_(1000000, 3),
        client_(std::move(client)) {
    client::KuduSchemaBuilder b;

    b.AddColumn(kKeyColumnName)->Type(client::KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn(kLinkColumnName)->Type(client::KuduColumnSchema::INT64)->NotNull();
    b.AddColumn(kInsertTsColumnName)->Type(client::KuduColumnSchema::INT64)->NotNull();
    b.AddColumn(kUpdatedColumnName)->Type(client::KuduColumnSchema::BOOL)->NotNull()
      ->Default(client::KuduValue::FromBool(false));
    CHECK_OK(b.Build(&schema_));
  }

  // The modes for WaitAndVerify
  enum WaitAndVerifyMode {
    // Perform snapshots scans in the past but finish with a "READ_LATEST" scan.
    // This should be used when the a majority of the cluster is down.
    FINISH_WITH_SCAN_LATEST,
    // Perform the snapshot scans in the past and finish with a snapshot scan in the present.
    FINISH_WITH_SNAPSHOT_SCAN
  };

  // Create the table.
  Status CreateLinkedListTable();

  // Load the table with the linked list test pattern.
  //
  // Runs for the amount of time designated by 'run_for'.
  // Sets *written_count to the number of rows inserted.
  Status LoadLinkedList(
      const MonoDelta& run_for,
      int num_samples,
      int64_t *written_count);

  // Variant of VerifyLinkedListRemote that verifies at the specified snapshot timestamp.
  Status VerifyLinkedListAtSnapshotRemote(uint64_t snapshot_timestamp,
                                          int64_t expected,
                                          bool log_errors,
                                          const std::function<Status(const std::string&)>& cb,
                                          int64_t* verified_count) {
    return VerifyLinkedListRemote(snapshot_timestamp,
                                  expected,
                                  log_errors,
                                  cb,
                                  verified_count);
  }

  Status VerifyLinkedListAtLatestRemote(int64_t expected,
                                        bool log_errors,
                                        const std::function<Status(const std::string&)>& cb,
                                        int64_t* verified_count) {
    return VerifyLinkedListRemote(kNoSnapshot,
                                  expected,
                                  log_errors,
                                  cb,
                                  verified_count);
  }

  // Variant of VerifyLinkedListremote that verifies at 'now'.
  Status VerifyLinkedListAtNowSnapshotRemote(int64_t expected,
                                             bool log_errors,
                                             const std::function<Status(const std::string&)>& cb,
                                             int64_t* verified_count) {
    return VerifyLinkedListRemote(kSnapshotAtNow,
                                  expected,
                                  log_errors,
                                  cb,
                                  verified_count);
  }

  // Run the verify step on a table with RPCs. Calls the provided callback 'cb' once during
  // verification to test scanner fault tolerance.
  Status VerifyLinkedListRemote(uint64_t snapshot_timestamp,
                                int64_t expected,
                                bool log_errors,
                                const std::function<Status(const std::string&)>& cb,
                                int64_t* verified_count);

  // Run the verify step on a specific tablet.
  Status VerifyLinkedListLocal(const tablet::Tablet* tablet,
                               int64_t expected,
                               int64_t* verified_count);

  // A variant of VerifyLinkedListRemote that is more robust towards ongoing
  // bootstrapping and replication.
  Status WaitAndVerify(int seconds_to_run,
                       int64_t expected,
                       WaitAndVerifyMode mode = FINISH_WITH_SNAPSHOT_SCAN) {
    return WaitAndVerify(seconds_to_run,
                         expected,
                         [](const std::string& /*uuid*/) { return Status::OK(); },
                         mode);
  }

  // A variant of WaitAndVerify that also takes a callback to be run once during verification.
  Status WaitAndVerify(int seconds_to_run,
                       int64_t expected,
                       const std::function<Status(const std::string&)>& cb,
                       WaitAndVerifyMode mode = FINISH_WITH_SNAPSHOT_SCAN);

  // Generates a vector of keys for the table such that each tablet is
  // responsible for an equal fraction of the int64 key space.
  std::vector<const KuduPartialRow*> GenerateSplitRows(const client::KuduSchema& schema);

  // Generate a vector of ints which form the split keys.
  std::vector<int64_t> GenerateSplitInts();

  void DumpInsertHistogram(bool print_flags);

 protected:
  client::KuduSchema schema_;
  const std::vector<std::string> verify_projection_;
  const std::string table_name_;
  const int num_chains_;
  const int num_tablets_;
  const int num_replicas_;
  const bool enable_mutation_;
  HdrHistogram latency_histogram_;
  client::sp::shared_ptr<client::KuduClient> client_;
  SnapsAndCounts sampled_timestamps_and_counts_;
};

// Generates the linked list pattern.
// Since we can insert multiple chain in parallel, this encapsulates the
// state for each chain.
class LinkedListChainGenerator {
 public:
  // 'chain_idx' is a unique ID for this chain. Chains with different indexes
  // will always generate distinct sets of keys (thus avoiding the possibility of
  // a collision even in a longer run).
  ATTRIBUTE_NO_SANITIZE_INTEGER
  explicit LinkedListChainGenerator(int chain_idx)
    : chain_idx_(chain_idx),
      rand_(chain_idx * 0xDEADBEEF),
      prev_key_(0) {
    CHECK_GE(chain_idx, 0);
    CHECK_LT(chain_idx, 65536);
  }

  ~LinkedListChainGenerator() {
  }

  // Generate a random 64-bit unsigned int.
  uint64_t Rand64() {
    return (implicit_cast<uint64_t>(rand_.Next()) << 32) | rand_.Next();
  }

  Status GenerateNextInsert(client::KuduTable* table, client::KuduSession* session) {
    // Encode the chain index in the lowest 16 bits so that different chains never
    // intersect.
    int64_t this_key = (Rand64() << 16) | chain_idx_;
    int64_t ts = GetCurrentTimeMicros();

    std::unique_ptr<client::KuduInsert> insert(table->NewInsert());
    CHECK_OK(insert->mutable_row()->SetInt64(kKeyColumnName, this_key));
    CHECK_OK(insert->mutable_row()->SetInt64(kInsertTsColumnName, ts));
    CHECK_OK(insert->mutable_row()->SetInt64(kLinkColumnName, prev_key_));
    RETURN_NOT_OK_PREPEND(session->Apply(insert.release()),
                          strings::Substitute("Unable to apply insert with key $0 at ts $1",
                                              this_key, ts));
    prev_key_ = this_key;
    return Status::OK();
  }

  int64_t prev_key() const {
    return prev_key_;
  }

 private:
  const int chain_idx_;

  // This is a linear congruential random number generator, so it won't repeat until
  // it has exhausted its period (which is quite large)
  Random rand_;

  // The previously output key.
  int64_t prev_key_;

  DISALLOW_COPY_AND_ASSIGN(LinkedListChainGenerator);
};

// A thread that updates the timestamps of rows whose keys are put in its BlockingQueue.
class ScopedRowUpdater {
 public:

  // Create and start a new ScopedUpdater. 'table' must remain valid for
  // the lifetime of this object.
  explicit ScopedRowUpdater(client::KuduTable* table)
    : table_(table),
      to_update_(kint64max), // no limit
      updater_([this]() { this->RowUpdaterThread(); }) {
  }

  ~ScopedRowUpdater() {
    to_update_.Shutdown();
    updater_.join();
  }

  BlockingQueue<int64_t>* to_update() { return &to_update_; }

 private:
  void RowUpdaterThread() {
    client::sp::shared_ptr<client::KuduSession> session(table_->client()->NewSession());
    session->SetTimeoutMillis(60000 /* 60 seconds */);
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_BACKGROUND));

    int64_t next_key = 0;
    while (to_update_.BlockingGet(&next_key).ok()) {
      std::unique_ptr<client::KuduUpdate> update(table_->NewUpdate());
      CHECK_OK(update->mutable_row()->SetInt64(kKeyColumnName, next_key));
      CHECK_OK(update->mutable_row()->SetBool(kUpdatedColumnName, true));
      CHECK_OK(session->Apply(update.release()));
    }

    FlushSessionOrDie(session);
  }

  client::KuduTable* table_;
  BlockingQueue<int64_t> to_update_;
  std::thread updater_;
};

// Helper class to hold results from a linked list scan and perform the
// verification step on the data.
class LinkedListVerifier {
 public:
  LinkedListVerifier(int num_chains, bool enable_mutation, int64_t expected,
                     std::vector<int64_t> split_key_ints);

  // Start the scan timer. The duration between starting the scan and verifying
  // the data is logged in the VerifyData() step, so this should be called
  // immediately before starting the table(t) scan.
  void StartScanTimer();

  // Register a new row result during the verify step.
  void RegisterResult(int64_t key, int64_t link, bool updated);

  // Run the common verify step once the scanned data is stored.
  Status VerifyData(int64_t* verified_count, bool log_errors);

 private:
  // Print a summary of the broken links to the log.
  void SummarizeBrokenLinks(const std::vector<int64_t>& broken_links);

  const int num_chains_;
  const int64_t expected_;
  const bool enable_mutation_;
  const std::vector<int64_t> split_key_ints_;
  std::vector<int64_t> seen_key_;
  std::vector<int64_t> seen_link_to_;
  int errors_;
  Stopwatch scan_timer_;
};

/////////////////////////////////////////////////////////////
// LinkedListTester
/////////////////////////////////////////////////////////////

inline std::vector<const KuduPartialRow*> LinkedListTester::GenerateSplitRows(
    const client::KuduSchema& schema) {
  std::vector<const KuduPartialRow*> split_keys;
  for (int64_t val : GenerateSplitInts()) {
    KuduPartialRow* row = schema.NewRow();
    CHECK_OK(row->SetInt64(kKeyColumnName, val));
    split_keys.push_back(row);
  }
  return split_keys;
}

inline std::vector<int64_t> LinkedListTester::GenerateSplitInts() {
  std::vector<int64_t> ret;
  ret.reserve(num_tablets_ - 1);
  const int64_t increment = kint64max / num_tablets_;
  for (int64_t i = 1; i < num_tablets_; i++) {
    ret.push_back(i * increment);
  }
  return ret;
}

inline Status LinkedListTester::CreateLinkedListTable() {
  std::unique_ptr<client::KuduTableCreator> table_creator(
      client_->NewTableCreator());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  RETURN_NOT_OK_PREPEND(table_creator->table_name(table_name_)
                        .schema(&schema_)
                        .set_range_partition_columns({ kKeyColumnName })
                        .split_rows(GenerateSplitRows(schema_))
                        .num_replicas(num_replicas_)
                        .Create(),
                        "Failed to create table");
#pragma GCC diagnostic pop
  return Status::OK();
}

inline Status LinkedListTester::LoadLinkedList(
    const MonoDelta& run_for,
    int num_samples,
    int64_t *written_count) {

  sampled_timestamps_and_counts_.clear();
  client::sp::shared_ptr<client::KuduTable> table;
  RETURN_NOT_OK_PREPEND(client_->OpenTable(table_name_, &table),
                        "Could not open table " + table_name_);

  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + run_for;

  client::sp::shared_ptr<client::KuduSession> session(client_->NewSession());
  session->SetTimeoutMillis(60000 /* 60 seconds */);
  RETURN_NOT_OK_PREPEND(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH),
                        "Couldn't set flush mode");

  ScopedRowUpdater updater(table.get());
  std::vector<std::unique_ptr<LinkedListChainGenerator>> chains;
  chains.reserve(num_chains_);
  for (int i = 0; i < num_chains_; i++) {
    chains.emplace_back(new LinkedListChainGenerator(i));
  }

  MonoDelta sample_interval = MonoDelta::FromMicroseconds(run_for.ToMicroseconds() / num_samples);
  MonoTime next_sample = start + sample_interval;
  LOG(INFO) << "Running for: " << run_for.ToString();
  LOG(INFO) << "Sampling every " << sample_interval.ToMicroseconds() << " us";

  *written_count = 0;
  int iter = 0;
  while (true) {
    if (iter++ % 10000 == 0) {
      LOG(INFO) << "Written " << (*written_count) << " rows in chain";
      DumpInsertHistogram(false);
    }

    MonoTime now = MonoTime::Now();
    if (next_sample < now) {
      Timestamp now(client_->GetLatestObservedTimestamp());
      sampled_timestamps_and_counts_.emplace_back(now.ToUint64() + 1, *written_count);
      next_sample += sample_interval;
      LOG(INFO) << "Sample at HT timestamp: " << now.ToString()
                << " Inserted count: " << *written_count;
    }
    if (deadline < now) {
      LOG(INFO) << "Finished inserting list. Added " << (*written_count) << " in chain";
      LOG(INFO) << "Last entries inserted had keys:";
      for (int i = 0; i < num_chains_; i++) {
        LOG(INFO) << i << ": " << chains[i]->prev_key();
      }
      return Status::OK();
    }
    for (const auto& chain : chains) {
      RETURN_NOT_OK_PREPEND(chain->GenerateNextInsert(table.get(), session.get()),
                            "Unable to generate next insert into linked list chain");
    }

    MonoTime flush_start(MonoTime::Now());
    FlushSessionOrDie(session);
    MonoDelta elapsed = MonoTime::Now() - flush_start;
    latency_histogram_.Increment(elapsed.ToMicroseconds());

    (*written_count) += chains.size();

    if (enable_mutation_) {
      // Rows have been inserted; they're now safe to update.
      for (const auto& chain : chains) {
        updater.to_update()->Put(chain->prev_key());
      }
    }
  }
}

inline void LinkedListTester::DumpInsertHistogram(bool print_flags) {
  // We dump to cout instead of using glog so the output isn't prefixed with
  // line numbers. This makes it less ugly to copy-paste into JIRA, etc.
  using std::cout;
  using std::endl;

  const HdrHistogram* h = &latency_histogram_;

  cout << "------------------------------------------------------------" << endl;
  cout << "Histogram for latency of insert operations (microseconds)" << endl;
  if (print_flags) {
    cout << "Flags: " << google::CommandlineFlagsIntoString() << endl;
  }
  cout << "Note: each insert is a batch of " << num_chains_ << " rows." << endl;
  cout << "------------------------------------------------------------" << endl;
  h->DumpHumanReadable(&std::cout);
}

// Verify that the given sorted vector does not contain any duplicate entries.
// If it does, *errors will be incremented once per duplicate and the given message
// will be logged.
static void VerifyNoDuplicateEntries(const std::vector<int64_t>& ints, int* errors,
                                     const std::string& message) {
  for (int i = 1; i < ints.size(); i++) {
    if (ints[i] == ints[i - 1]) {
      LOG(ERROR) << message << ": " << ints[i];
      (*errors)++;
    }
  }
}

inline Status LinkedListTester::VerifyLinkedListRemote(
    uint64_t snapshot_timestamp, int64_t expected, bool log_errors,
    const std::function<Status(const std::string&)>& cb, int64_t* verified_count) {

  client::sp::shared_ptr<client::KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(table_name_, &table));

  std::string snapshot_str;
  if (snapshot_timestamp == kSnapshotAtNow) {
    snapshot_str = "NOW";
  } else {
    snapshot_str = clock::HybridClock::StringifyTimestamp(Timestamp(snapshot_timestamp));
  }

  client::KuduScanner scanner(table.get());
  RETURN_NOT_OK_PREPEND(scanner.SetProjectedColumnNames(verify_projection_), "Bad projection");
  RETURN_NOT_OK(scanner.SetBatchSizeBytes(0)); // Force at least one NextBatch RPC.
  RETURN_NOT_OK(scanner.SetTimeoutMillis(60 * 1000 /* 60 seconds */));

  if (snapshot_timestamp != kNoSnapshot) {
    RETURN_NOT_OK(scanner.SetReadMode(client::KuduScanner::READ_AT_SNAPSHOT));
    RETURN_NOT_OK(scanner.SetFaultTolerant());
    if (snapshot_timestamp != kSnapshotAtNow) {
      RETURN_NOT_OK(scanner.SetSnapshotRaw(snapshot_timestamp));
    }
  } else {
    RETURN_NOT_OK(scanner.SetReadMode(client::KuduScanner::READ_LATEST));
  }

  LOG(INFO) << "Verifying Snapshot: " << snapshot_str << " Expected Rows: " << expected;

  RETURN_NOT_OK_PREPEND(scanner.Open(), "Couldn't open scanner");

  RETURN_NOT_OK(scanner.SetBatchSizeBytes(1024)); // More normal batch size.

  LinkedListVerifier verifier(num_chains_, enable_mutation_, expected,
                              GenerateSplitInts());
  verifier.StartScanTimer();

  bool cb_called = false;
  while (scanner.HasMoreRows()) {
    // If we're doing a snapshot scan with a big enough cluster, call the callback on the scanner's
    // tserver. Do this only once.
    if (snapshot_timestamp != kSnapshotAtNow && !cb_called) {
      client::KuduTabletServer* kts_ptr;
      RETURN_NOT_OK(scanner.GetCurrentServer(&kts_ptr));
      std::unique_ptr<client::KuduTabletServer> kts(kts_ptr);
      const auto& down_ts = kts->uuid();
      LOG(INFO) << "Calling callback on tserver " << down_ts;
      RETURN_NOT_OK(cb(down_ts));
      cb_called = true;
    }
    client::KuduScanBatch batch;
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&batch), "Couldn't fetch next row batch");
    for (const client::KuduScanBatch::RowPtr row : batch) {
      int64_t key;
      int64_t link;
      bool updated;
      RETURN_NOT_OK(row.GetInt64(0, &key));
      RETURN_NOT_OK(row.GetInt64(1, &link));

      // For non-snapshot reads we also verify that all rows were updated. We don't
      // for snapshot reads as updates are performed by their own thread. This means
      // that there is no guarantee that, for any snapshot timestamp that comes before
      // all writes are completed, all rows will be updated.
      if (snapshot_timestamp == kSnapshotAtNow) {
        RETURN_NOT_OK(row.GetBool(2, &updated));
      } else {
        updated = enable_mutation_;
      }

      verifier.RegisterResult(key, link, updated);
    }
  }

  Status s = verifier.VerifyData(verified_count, log_errors);
  LOG(INFO) << "Snapshot: " << snapshot_str << " verified. Result: " << s.ToString();
  return s;
}

inline Status LinkedListTester::VerifyLinkedListLocal(
    const tablet::Tablet* tablet, int64_t expected, int64_t* verified_count) {
  DCHECK(tablet);
  LinkedListVerifier verifier(num_chains_, enable_mutation_, expected,
                              GenerateSplitInts());
  verifier.StartScanTimer();

  const Schema* tablet_schema = tablet->schema().get();
  // Cannot use schemas with col indexes in a scan (assertions fire).
  SchemaPtr projection_ptr(
      new Schema(tablet_schema->columns(), tablet_schema->num_key_columns()));
  Schema& projection = *projection_ptr.get();
  std::unique_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK_PREPEND(tablet->NewRowIterator(projection_ptr, &iter),
                        "Cannot create new row iterator");
  RETURN_NOT_OK_PREPEND(iter->Init(nullptr), "Cannot initialize row iterator");

  RowBlockMemory mem(1024);
  RowBlock block(&projection, 100, &mem);
  while (iter->HasNext()) {
    mem.Reset();
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (int i = 0; i < block.nrows(); i++) {
      int64_t key;
      int64_t link;
      bool updated;

      const RowBlockRow& row = block.row(i);
      key = *tablet_schema->ExtractColumnFromRow<INT64>(row, 0);
      link = *tablet_schema->ExtractColumnFromRow<INT64>(row, 1);
      updated = *tablet_schema->ExtractColumnFromRow<BOOL>(row, 3);

      verifier.RegisterResult(key, link, updated);
    }
  }

  return verifier.VerifyData(verified_count, true);
}

inline Status LinkedListTester::WaitAndVerify(
    int seconds_to_run, int64_t expected,
    const std::function<Status(const std::string&)>& cb, WaitAndVerifyMode mode) {

  std::list<std::pair<int64_t, int64_t>> samples_as_list(
      sampled_timestamps_and_counts_.begin(),
      sampled_timestamps_and_counts_.end());

  int64_t seen = 0;
  bool called = false;
  Stopwatch sw;
  sw.start();

  Status s;
  do {
    // We'll give the tablets 5 seconds to start up regardless of how long we
    // inserted for. There's some fixed cost startup time, especially when
    // replication is enabled.
    const int kBaseTimeToWaitSecs = 5;
    bool last_attempt = sw.elapsed().wall_seconds() > kBaseTimeToWaitSecs + seconds_to_run;
    s = Status::OK();
    auto iter = samples_as_list.begin();

    while (iter != samples_as_list.end()) {
      // Only call the callback once, on the first verify pass, since it may be destructive.
      if (iter == samples_as_list.begin() && !called) {
        s = VerifyLinkedListAtSnapshotRemote((*iter).first, (*iter).second, last_attempt, cb,
                                             &seen);
        called = true;
      } else {
        s = VerifyLinkedListAtSnapshotRemote(
            (*iter).first, (*iter).second, last_attempt,
            [](const std::string& /*uuid*/) { return Status::OK(); },
            &seen);
      }

      if (s.ok() && (*iter).second != seen) {
        // If we've seen less rows than we were expecting we should fail and not retry.
        //
        // The reasoning is the following:
        //
        // - We know that when we read this snapshot's timestamp the writes had completed, thus
        //   at timestamp '(*iter).first' any replica should have precisely '(*iter).second' rows.
        // - We also chose to perform a snapshot scan, which, when passed a timestamp, waits for
        //   that timestamp to become "clean", i.e. it makes sure that all transactions with lower
        //   timestamps have completed before it actually performs the scan.
        //
        // Together these conditions mean that if we don't get the expected rows back something
        // is wrong with the read path or with the write path and we should fail immediately.
        return Status::Corruption(strings::Substitute("Got wrong row count on snapshot. "
            "Expected: $0, Got:$1", (*iter).second, seen));
      }

      if (!s.ok()) break;
      // If the snapshot verification returned OK erase it so that we don't recheck
      // even if a later snapshot or the final verification failed.
      iter = samples_as_list.erase(iter);
    }

    // Perform the last scan with the required mode.
    switch (mode) {
      case FINISH_WITH_SNAPSHOT_SCAN:
        if (s.ok()) {
          RETURN_NOT_OK(VerifyLinkedListAtNowSnapshotRemote(
              expected, last_attempt,
              [](const std::string& /*uuid*/) { return Status::OK(); }, &seen));
        }
        break;
      case FINISH_WITH_SCAN_LATEST:
        // Scans in READ_LATEST mode will, by design, likely return a stale view of the tablet
        // so, in this case, retry.
        if (s.ok()) {
          s = VerifyLinkedListAtLatestRemote(
              expected, last_attempt,
              [](const std::string& /*uuid*/) { return Status::OK(); }, &seen);
        }
        break;
    }

    if (!s.ok()) {
      KLOG_EVERY_N(INFO, 10) << "Table not yet ready: " << seen << "/"
                             << expected << " rows (status: "
                             << s.ToString() << ")";
      if (last_attempt) {
        // We'll give it an equal amount of time to re-load the data as it took
        // to write it in. Typically it completes much faster than that.
        return Status::TimedOut("Timed out waiting for table to be accessible again",
                                s.ToString());
      }

      // Sleep and retry until timeout.
      SleepFor(MonoDelta::FromMilliseconds(20));
    }
  } while (!s.ok());

  LOG(INFO) << "Successfully verified " << expected << " rows";

  return Status::OK();
}

/////////////////////////////////////////////////////////////
// LinkedListVerifier
/////////////////////////////////////////////////////////////

LinkedListVerifier::LinkedListVerifier(int num_chains, bool enable_mutation,
                                       int64_t expected,
                                       std::vector<int64_t> split_key_ints)
    : num_chains_(num_chains),
      expected_(expected),
      enable_mutation_(enable_mutation),
      split_key_ints_(std::move(split_key_ints)),
      errors_(0) {
  if (expected != kNoParticularCountExpected) {
    DCHECK_GE(expected, 0);
    seen_key_.reserve(expected);
    seen_link_to_.reserve(expected);
  }
}

void LinkedListVerifier::StartScanTimer() {
  scan_timer_.start();
}

void LinkedListVerifier::RegisterResult(int64_t key, int64_t link, bool updated) {
  seen_key_.push_back(key);
  if (link != 0) {
    // Links to entry 0 don't count - the first inserts use this link
    seen_link_to_.push_back(link);
  }

  if (updated != enable_mutation_) {
    LOG(ERROR) << "Entry " << key << " was incorrectly "
      << (enable_mutation_ ? "not " : "") << "updated";
    errors_++;
  }
}

void LinkedListVerifier::SummarizeBrokenLinks(const std::vector<int64_t>& broken_links) {
  std::vector<int64_t> errors_by_tablet(split_key_ints_.size() + 1);

  int n_logged = 0;
  const int kMaxToLog = 100;

  for (int64_t broken : broken_links) {
    int tablet = std::upper_bound(split_key_ints_.begin(),
                                  split_key_ints_.end(),
                                  broken) - split_key_ints_.begin();
    DCHECK_GE(tablet, 0);
    DCHECK_LT(tablet, errors_by_tablet.size());
    errors_by_tablet[tablet]++;

    if (n_logged < kMaxToLog) {
      LOG(ERROR) << "Entry " << broken << " was linked to but not present";
      n_logged++;
      if (n_logged == kMaxToLog) {
        LOG(ERROR) << "... no more broken links will be logged";
      }
    }
  }

  // Summarize the broken links by which tablet they fell into.
  if (!broken_links.empty()) {
    for (int tablet = 0; tablet < errors_by_tablet.size(); tablet++) {
      LOG(ERROR) << "Error count for tablet #" << tablet << ": " << errors_by_tablet[tablet];
    }
  }
}

Status LinkedListVerifier::VerifyData(int64_t* verified_count, bool log_errors) {
  *verified_count = seen_key_.size();
  LOG(INFO) << "Done collecting results (" << (*verified_count) << " rows in "
            << scan_timer_.elapsed().wall_millis() << "ms)";

  VLOG(1) << "Sorting results before verification of linked list structure...";
  std::sort(seen_key_.begin(), seen_key_.end());
  std::sort(seen_link_to_.begin(), seen_link_to_.end());
  VLOG(1) << "Done sorting";

  // Verify that no key was seen multiple times or linked to multiple times
  VerifyNoDuplicateEntries(seen_key_, &errors_, "Seen row key multiple times");
  VerifyNoDuplicateEntries(seen_link_to_, &errors_, "Seen link to row multiple times");
  // Verify that every key that was linked to was present
  std::vector<int64_t> broken_links = STLSetDifference(seen_link_to_, seen_key_);
  errors_ += broken_links.size();
  if (log_errors) {
    SummarizeBrokenLinks(broken_links);
  }

  // Verify that only the expected number of keys were seen but not linked to.
  // Only the last "batch" should have this characteristic.
  std::vector<int64_t> not_linked_to = STLSetDifference(seen_key_, seen_link_to_);
  if (not_linked_to.size() != num_chains_) {
    LOG_IF(ERROR, log_errors)
      << "Had " << not_linked_to.size() << " entries which were seen but not"
      << " linked to. Expected only " << num_chains_;
    errors_++;
  }

  if (errors_ > 0) {
    return Status::Corruption("Had one or more errors during verification (see log)");
  }

  if (expected_ != *verified_count) {
    return Status::IllegalState(strings::Substitute(
        "Missing rows, but with no broken link in the chain. This means that "
        "a suffix of the inserted rows went missing. Expected=$0, seen=$1.",
        expected_, *verified_count));
  }

  return Status::OK();
}

} // namespace kudu
