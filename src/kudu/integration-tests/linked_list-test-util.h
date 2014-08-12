// Copyright (c) 2014, Cloudera, inc.

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <iostream>
#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/encoded_key.h"
#include "kudu/client/row_result.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/walltime.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"

namespace kudu {

static const char* const kKeyColumnName = "rand_key";
static const char* const kLinkColumnName = "link_to";
static const char* const kInsertTsColumnName = "insert_ts";
static const char* const kUpdatedColumnName = "updated";
static const bool kUpdatedColumnDefault = false;

// Provides methods for writing data and reading it back in such a way that
// facilitates checking for data integrity.
// NOTE: Users of this class must enable the hybrid clock by setting
// FLAGS_use_hybrid_clock = true.
class LinkedListTester {
 public:
  LinkedListTester(const std::tr1::shared_ptr<client::KuduClient>& client,
                   const std::string& table_name,
                   int num_chains,
                   int num_tablets,
                   int num_replicas,
                   bool enable_mutation)
    : schema_(boost::assign::list_of
                (client::KuduColumnSchema(kKeyColumnName, client::KuduColumnSchema::UINT64))
                (client::KuduColumnSchema(kLinkColumnName, client::KuduColumnSchema::UINT64))
                (client::KuduColumnSchema(kInsertTsColumnName, client::KuduColumnSchema::UINT64))
                (client::KuduColumnSchema(kUpdatedColumnName, client::KuduColumnSchema::BOOL,
                                          false, &kUpdatedColumnDefault)),
              1),
      verify_projection_(boost::assign::list_of
                           (client::KuduColumnSchema(kKeyColumnName,
                                                     client::KuduColumnSchema::UINT64))
                           (client::KuduColumnSchema(kLinkColumnName,
                                                     client::KuduColumnSchema::UINT64))
                           (client::KuduColumnSchema(kUpdatedColumnName,
                                                     client::KuduColumnSchema::BOOL,
                                                     false, &kUpdatedColumnDefault)),
                         1),
      table_name_(table_name),
      num_chains_(num_chains),
      num_tablets_(num_tablets),
      num_replicas_(num_replicas),
      enable_mutation_(enable_mutation),
      latency_histogram_(1000000, 3),
      client_(client) {
  }

  // Create the table.
  Status CreateLinkedListTable();

  // Load the table with the linked list test pattern.
  //
  // Runs for the amount of time designated by 'run_for'.
  // Sets *written_count to the number of rows inserted.
  Status LoadLinkedList(const MonoDelta& run_for,
                        int64_t* written_count);

  // Run the verify step on a table with RPCs.
  Status VerifyLinkedListRemote(int64_t expected, int64_t* verified_count);

  // Run the verify step on a specific tablet.
  Status VerifyLinkedListLocal(const tablet::Tablet* tablet,
                               int64_t expected,
                               int64_t* verified_count);

  // A variant of VerifyLinkedListRemote that is more robust towards ongoing
  // bootstrapping and replication.
  Status WaitAndVerify(int seconds_to_run, int64_t expected);

  // Generates a vector of keys for the table such that each tablet is
  // responsible for an equal fraction of the uint64 key space.
  std::vector<std::string> GenerateSplitKeys(const client::KuduSchema& schema);

  void DumpInsertHistogram(bool print_flags);

 protected:
  const client::KuduSchema schema_;
  const client::KuduSchema verify_projection_;
  const std::string table_name_;
  const int num_chains_;
  const int num_tablets_;
  const int num_replicas_;
  const bool enable_mutation_;
  HdrHistogram latency_histogram_;
  std::tr1::shared_ptr<client::KuduClient> client_;
};

// Generates the linked list pattern.
// Since we can insert multiple chain in parallel, this encapsulates the
// state for each chain.
class LinkedListChainGenerator {
 public:
  // 'chain_idx' is a unique ID for this chain. Chains with different indexes
  // will always generate distinct sets of keys (thus avoiding the possibility of
  // a collision even in a longer run).
  explicit LinkedListChainGenerator(uint32_t chain_idx)
    : chain_idx_(chain_idx),
      rand_(chain_idx * 0xDEADBEEF),
      prev_key_(0) {
    CHECK_LT(chain_idx, 256);
  }

  ~LinkedListChainGenerator() {
  }

  // Generate a random 64-bit int.
  uint64_t Rand64() {
    return (implicit_cast<int64_t>(rand_.Next()) << 32) | rand_.Next();
  }

  Status GenerateNextInsert(client::KuduTable* table, client::KuduSession* session) {
    // Encode the chain index in the lowest 8 bits so that different chains never
    // intersect.
    uint64_t this_key = (Rand64() << 8) | chain_idx_;
    uint64_t ts = GetCurrentTimeMicros();

    gscoped_ptr<client::KuduInsert> insert = table->NewInsert();
    CHECK_OK(insert->mutable_row()->SetUInt64(kKeyColumnName, this_key));
    CHECK_OK(insert->mutable_row()->SetUInt64(kInsertTsColumnName, ts));
    CHECK_OK(insert->mutable_row()->SetUInt64(kLinkColumnName, prev_key_));
    RETURN_NOT_OK_PREPEND(session->Apply(insert.Pass()),
                          strings::Substitute("Unable to apply insert with key $0 at ts $1",
                                              this_key, ts));
    prev_key_ = this_key;
    return Status::OK();
  }

  uint64_t prev_key() const {
    return prev_key_;
  }

 private:
  const uint8_t chain_idx_;

  // This is a linear congruential random number generator, so it won't repeat until
  // it has exhausted its period (which is quite large)
  Random rand_;

  // The previously output key.
  uint64_t prev_key_;

  DISALLOW_COPY_AND_ASSIGN(LinkedListChainGenerator);
};

// A thread that updates the timestamps of rows whose keys are put in its BlockingQueue.
class ScopedRowUpdater {
 public:

  // Create and start a new ScopedUpdater. 'table' must remain valid for
  // the lifetime of this object.
  explicit ScopedRowUpdater(client::KuduTable* table)
    : table_(table),
      to_update_(kuint64max) { // no limit
    CHECK_OK(Thread::Create("linked_list-test", "updater",
                            &ScopedRowUpdater::RowUpdaterThread, this, &updater_));
  }

  ~ScopedRowUpdater() {
    to_update_.Shutdown();
    if (updater_) {
      updater_->Join();
    }
  }

  BlockingQueue<uint64_t>* to_update() { return &to_update_; }

 private:
  void RowUpdaterThread() {
    std::tr1::shared_ptr<client::KuduSession> session(table_->client()->NewSession());
    session->SetTimeoutMillis(15000);
    CHECK_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));

    uint64_t next_key;
    int64_t updated_count = 0;
    while (to_update_.BlockingGet(&next_key)) {
      gscoped_ptr<client::KuduUpdate> update(table_->NewUpdate());
      CHECK_OK(update->mutable_row()->SetUInt64(kKeyColumnName, next_key));
      CHECK_OK(update->mutable_row()->SetBool(kUpdatedColumnName, true));
      CHECK_OK(session->Apply(update.Pass()));
      if (++updated_count % 50 == 0) {
        FlushSessionOrDie(session);
      }
    }

    FlushSessionOrDie(session);
  }

  client::KuduTable* table_;
  BlockingQueue<uint64_t> to_update_;
  scoped_refptr<Thread> updater_;
};

// Helper class to hold results from a linked list scan and perform the
// verification step on the data.
class LinkedListVerifier {
 public:
  LinkedListVerifier(int num_chains, bool enable_mutation, int64_t expected);

  // Start the scan timer. The duration between starting the scan and verifying
  // the data is logged in the VerifyData() step, so this should be called
  // immediately before starting the table(t) scan.
  void StartScanTimer();

  // Register a new row result during the verify step.
  void RegisterResult(uint64_t key, uint64_t link, bool updated);

  // Run the common verify step once the scanned data is stored.
  Status VerifyData(int64_t* verified_count);

 private:
  int num_chains_;
  int64_t expected_;
  bool enable_mutation_;
  std::vector<uint64_t> seen_key_;
  std::vector<uint64_t> seen_link_to_;
  int errors_;
  Stopwatch scan_timer_;
};

/////////////////////////////////////////////////////////////
// LinkedListTester
/////////////////////////////////////////////////////////////

std::vector<string> LinkedListTester::GenerateSplitKeys(const client::KuduSchema& schema) {
  client::KuduEncodedKeyBuilder key_builder(schema);
  gscoped_ptr<client::KuduEncodedKey> key;
  std::vector<string> split_keys;
  uint64_t increment = kuint64max / num_tablets_;
  for (uint64_t i = 1; i < num_tablets_; i++) {
    uint64_t val = i * increment;
    key_builder.Reset();
    key_builder.AddColumnKey(&val);
    key.reset(key_builder.BuildEncodedKey());
    split_keys.push_back(key->ToString());
  }
  return split_keys;
}

Status LinkedListTester::CreateLinkedListTable() {
  RETURN_NOT_OK_PREPEND(client_->NewTableCreator()
                        ->table_name(table_name_)
                        .schema(&schema_)
                        .split_keys(GenerateSplitKeys(schema_))
                        .num_replicas(num_replicas_)
                        .Create(),
                        "Failed to create table");
  return Status::OK();
}

Status LinkedListTester::LoadLinkedList(const MonoDelta& run_for,
                                        int64_t *written_count) {
  scoped_refptr<client::KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(table_name_, &table));

  MonoTime deadline = MonoTime::Now(MonoTime::COARSE);
  deadline.AddDelta(run_for);

  std::tr1::shared_ptr<client::KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(15000);
  RETURN_NOT_OK_PREPEND(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH),
                        "Couldn't set flush mode");

  ScopedRowUpdater updater(table.get());
  std::vector<LinkedListChainGenerator*> chains;
  ElementDeleter d(&chains);
  for (int i = 0; i < num_chains_; i++) {
    chains.push_back(new LinkedListChainGenerator(i));
  }

  *written_count = 0;
  int iter = 0;
  while (true) {
    if (iter++ % 10000 == 0) {
      LOG(INFO) << "Written " << (*written_count) << " rows in chain";
      DumpInsertHistogram(false);
    }

    if (deadline.ComesBefore(MonoTime::Now(MonoTime::COARSE))) {
      LOG(INFO) << "Finished inserting list. Added " << (*written_count) << " in chain";
      LOG(INFO) << "Last entries inserted had keys:";
      for (int i = 0; i < num_chains_; i++) {
        LOG(INFO) << i << ": " << chains[i]->prev_key();
      }
      return Status::OK();
    }
    BOOST_FOREACH(LinkedListChainGenerator* chain, chains) {
      RETURN_NOT_OK(chain->GenerateNextInsert(table.get(), session.get()));
    }

    MonoTime start(MonoTime::Now(MonoTime::FINE));
    FlushSessionOrDie(session);
    MonoDelta elapsed = MonoTime::Now(MonoTime::FINE).GetDeltaSince(start);
    latency_histogram_.Increment(elapsed.ToMicroseconds());

    (*written_count) += chains.size();

    if (enable_mutation_) {
      // Rows have been inserted; they're now safe to update.
      BOOST_FOREACH(LinkedListChainGenerator* chain, chains) {
        updater.to_update()->Put(chain->prev_key());
      }
    }
  }
}

void LinkedListTester::DumpInsertHistogram(bool print_flags) {
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
  cout << "Count: " << h->TotalCount() << endl;
  cout << "Mean: " << h->MeanValue() << endl;
  cout << "Percentiles:" << endl;
  cout << "   0%  (min) = " << h->MinValue() << endl;
  cout << "  25%        = " << h->ValueAtPercentile(25) << endl;
  cout << "  50%  (med) = " << h->ValueAtPercentile(50) << endl;
  cout << "  75%        = " << h->ValueAtPercentile(75) << endl;
  cout << "  95%        = " << h->ValueAtPercentile(95) << endl;
  cout << "  99%        = " << h->ValueAtPercentile(99) << endl;
  cout << "  99.9%      = " << h->ValueAtPercentile(99.9) << endl;
  cout << "  99.99%     = " << h->ValueAtPercentile(99.99) << endl;
  cout << "  100% (max) = " << h->MaxValue() << endl;
  if (h->MaxValue() >= h->highest_trackable_value()) {
    cout << "*NOTE: some values were greater than highest trackable value" << endl;
  }
}

// Verify that the given sorted vector does not contain any duplicate entries.
// If it does, *errors will be incremented once per duplicate and the given message
// will be logged.
static void VerifyNoDuplicateEntries(const std::vector<uint64_t>& ints, int* errors,
                                     const string& message) {
  for (int i = 1; i < ints.size(); i++) {
    if (ints[i] == ints[i - 1]) {
      LOG(ERROR) << message << ": " << ints[i];
      (*errors)++;
    }
  }
}

Status LinkedListTester::VerifyLinkedListRemote(int64_t expected, int64_t* verified_count) {
  scoped_refptr<client::KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(table_name_, &table));

  client::KuduScanner scanner(table.get());
  RETURN_NOT_OK_PREPEND(scanner.SetProjection(&verify_projection_), "Bad projection");
  RETURN_NOT_OK_PREPEND(scanner.Open(), "Couldn't open scanner");

  LinkedListVerifier verifier(num_chains_, enable_mutation_, expected);
  verifier.StartScanTimer();

  std::vector<client::KuduRowResult> rows;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&rows));
    BOOST_FOREACH(const client::KuduRowResult& row, rows) {
      uint64_t key;
      uint64_t link;
      bool updated;
      RETURN_NOT_OK(row.GetUInt64(0, &key));
      RETURN_NOT_OK(row.GetUInt64(1, &link));
      RETURN_NOT_OK(row.GetBool(2, &updated));

      verifier.RegisterResult(key, link, updated);
    }
    rows.clear();
  }

  return verifier.VerifyData(verified_count);
}

Status LinkedListTester::VerifyLinkedListLocal(const tablet::Tablet* tablet,
                                               int64_t expected,
                                               int64_t* verified_count) {
  DCHECK(tablet != NULL);
  LinkedListVerifier verifier(num_chains_, enable_mutation_, expected);
  verifier.StartScanTimer();

  const shared_ptr<Schema>& tablet_schema = tablet->schema();
  // Cannot use schemas with col indexes in a scan (assertions fire).
  Schema projection(tablet_schema->columns(), tablet_schema->num_key_columns());
  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK_PREPEND(tablet->NewRowIterator(projection, &iter),
                        "Cannot create new row iterator");
  RETURN_NOT_OK_PREPEND(iter->Init(NULL), "Cannot initialize row iterator");

  Arena arena(1024, 1024);
  RowBlock block(projection, 100, &arena);
  while (iter->HasNext()) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (int i = 0; i < block.nrows(); i++) {
      uint64_t key;
      uint64_t link;
      bool updated;

      const RowBlockRow& row = block.row(i);
      key = *tablet_schema->ExtractColumnFromRow<UINT64>(row, 0);
      link = *tablet_schema->ExtractColumnFromRow<UINT64>(row, 1);
      updated = *tablet_schema->ExtractColumnFromRow<BOOL>(row, 3);

      verifier.RegisterResult(key, link, updated);
    }
  }

  return verifier.VerifyData(verified_count);
}

Status LinkedListTester::WaitAndVerify(int seconds_to_run, int64_t expected) {

  int64_t seen;
  Stopwatch sw;
  sw.start();

  Status s;
  do {
    s = VerifyLinkedListRemote(expected, &seen);

    // TODO: when we enable hybridtime consistency for the scans,
    // then we should not allow !s.ok() here. But, with READ_LATEST
    // scans, we could have a lagging replica of one tablet, with an
    // up-to-date replica of another tablet, and end up with broken links
    // in the chain.

    if (!s.ok()) {
      // We'll give the tablets 3 seconds to start up regardless of how long we
      // inserted for. There's some fixed cost startup time, especially when
      // replication is enabled.
      const int kBaseTimeToWaitSecs = 3;

      LOG(INFO) << "Table not yet ready: " << expected << "/" << seen << " rows";
      if (sw.elapsed().wall_seconds() > kBaseTimeToWaitSecs + seconds_to_run) {
        // We'll give it an equal amount of time to re-load the data as it took
        // to write it in. Typically it completes much faster than that.
        return Status::TimedOut("Timed out waiting for table to be accessible again",
                                s.ToString());
      }

      // Sleep and retry until timeout.
      usleep(20*1000);
    }
  } while (!s.ok());

  return Status::OK();
}

/////////////////////////////////////////////////////////////
// LinkedListVerifier
/////////////////////////////////////////////////////////////

LinkedListVerifier::LinkedListVerifier(int num_chains, bool enable_mutation, int64_t expected)
  : num_chains_(num_chains),
    expected_(expected),
    enable_mutation_(enable_mutation),
    errors_(0) {
  seen_key_.reserve(expected);
  seen_link_to_.reserve(expected);
}

void LinkedListVerifier::StartScanTimer() {
  scan_timer_.start();
}

void LinkedListVerifier::RegisterResult(uint64_t key, uint64_t link, bool updated) {
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

Status LinkedListVerifier::VerifyData(int64_t* verified_count) {
  *verified_count = seen_key_.size();
  LOG(INFO) << "Done collecting results (" << (*verified_count) << " rows in "
            << scan_timer_.elapsed().wall_millis() << "ms)";

  LOG(INFO) << "Sorting results before verification of linked list structure...";
  std::sort(seen_key_.begin(), seen_key_.end());
  std::sort(seen_link_to_.begin(), seen_link_to_.end());
  LOG(INFO) << "Done sorting";

  // Verify that no key was seen multiple times or linked to multiple times
  VerifyNoDuplicateEntries(seen_key_, &errors_, "Seen row key multiple times");
  VerifyNoDuplicateEntries(seen_link_to_, &errors_, "Seen link to row multiple times");
  // Verify that every key that was linked to was present
  std::vector<uint64_t> broken_links = STLSetDifference(seen_link_to_, seen_key_);
  BOOST_FOREACH(uint64_t broken, broken_links) {
    LOG(ERROR) << "Entry " << broken << " was linked to but not present";
    errors_++;
  }

  // Verify that only the expected number of keys were seen but not linked to.
  // Only the last "batch" should have this characteristic.
  std::vector<uint64_t> not_linked_to = STLSetDifference(seen_key_, seen_link_to_);
  if (not_linked_to.size() != num_chains_) {
    LOG(ERROR) << "Had " << not_linked_to.size() << " entries which were seen but not"
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
