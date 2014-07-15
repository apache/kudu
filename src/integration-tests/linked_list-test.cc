// Copyright (c) 2014, Cloudera, inc.
//
// This is an integration test similar to TestLoadAndVerify in HBase.
// It creates a table and writes linked lists into it, where each row
// points to the previously written row. For example, a sequence of inserts
// may be:
//
//  rand_key   | link_to   |  insert_ts
//   12345          0           1
//   823          12345         2
//   9999          823          3
// (each insert links to the key of the previous insert)
//
// During insertion, a configurable number of parallel chains may be inserted.
// To verify, the table is scanned, and we ensure that every key is linked to
// either zero or one times, and no link_to refers to a missing key.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <utility>
#include <vector>

#include "client/client.h"
#include "client/encoded_key.h"
#include "client/row_result.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/split.h"
#include "gutil/walltime.h"
#include "integration-tests/external_mini_cluster.h"
#include "util/random.h"
#include "util/stopwatch.h"
#include "util/test_util.h"
#include "util/hdr_histogram.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduEncodedKey;
using kudu::client::KuduEncodedKeyBuilder;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduInsert;

using strings::Substitute;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using std::vector;

DEFINE_int32(seconds_to_run, 0, "Number of seconds for which to run the test "
             "(default 0 autoselects based on test mode)");
enum {
  kDefaultRunTimeSlow = 30,
  kDefaultRunTimeFast = 1
};

DEFINE_int32(num_chains, 50, "Number of parallel chains to generate");
DEFINE_int32(num_tablets, 3, "Number of tablets over which to split the data");
DEFINE_int32(num_tablet_servers, 3, "Number of tablet servers to start");
DEFINE_int32(num_replicas, 3, "Number of replicas per tablet server");

DEFINE_string(ts_flags, "", "Flags to pass through to tablet servers");

static const char* const kKeyColumnName = "rand_key";
static const char* const kLinkColumnName = "link_to";
static const char* const kInsertTsColumnName = "insert_ts";

namespace kudu {

class LinkedListTest : public KuduTest {
 public:
  LinkedListTest()
    : schema_(boost::assign::list_of
              (KuduColumnSchema(kKeyColumnName, UINT64))
              (KuduColumnSchema(kLinkColumnName, UINT64))
              (KuduColumnSchema(kInsertTsColumnName, UINT64)),
              1),
      verify_projection_(boost::assign::list_of
                         (KuduColumnSchema(kKeyColumnName, UINT64))
                         (KuduColumnSchema(kLinkColumnName, UINT64)),
                         1),
      latency_histogram_(1000000, 3) {
  }

  void SetUp() OVERRIDE {
    KuduTest::SetUp();
    SeedRandom();

    RestartCluster();
  }

  void RestartCluster() {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = FLAGS_num_tablet_servers;
    opts.data_root = GetTestPath("linked-list-cluster");
    opts.extra_tserver_flags.push_back("--skip_remove_old_recovery_dir");
    opts.extra_tserver_flags.push_back("--tablet_server_rpc_bind_addresses=127.0.0.1:705${index}");
    if (!FLAGS_ts_flags.empty()) {
      vector<string> flags = strings::Split(FLAGS_ts_flags, " ");
      BOOST_FOREACH(const string& flag, flags) {
        opts.extra_tserver_flags.push_back(flag);
      }
    }
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_STATUS_OK(cluster_->Start());
    KuduClientBuilder builder;
    ASSERT_STATUS_OK(cluster_->CreateClient(builder, &client_));
  }

  // Load the table with the linked list test pattern.
  //
  // Runs for the amount of time designated by 'run_for'.
  // Sets *written_count to the number of rows inserted.
  Status LoadLinkedList(const MonoDelta& run_for,
                        int64_t* written_count);
  Status VerifyLinkedList(int64_t expected, int64_t* verified_count);

  // A variant of VerifyLinkedList that is more robust towards ongoing
  // bootstrapping and replication.
  void WaitAndVerify(int64_t expected);

  // Generates a vector of keys for the table such that each tablet is
  // responsible for an equal fraction of the uint64 key space.
  vector<string> GenerateSplitKeys() const;

  void DumpInsertHistogram(bool print_flags);

 protected:
  static const char* kTableName;
  const KuduSchema schema_;
  const KuduSchema verify_projection_;
  gscoped_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  HdrHistogram latency_histogram_;
};

const char *LinkedListTest::kTableName = "linked_list";

namespace {

// Generates the linked list pattern.
// Since we can insert multiple chain in parallel, this encapsulates the
// state for each chain.
class ChainGenerator {
 public:
  // 'chain_idx' is a unique ID for this chain. Chains with different indexes
  // will always generate distinct sets of keys (thus avoiding the possibility of
  // a collision even in a longer run).
  explicit ChainGenerator(uint32_t chain_idx)
    : chain_idx_(chain_idx),
      rand_(chain_idx * 0xDEADBEEF),
      prev_key_(0) {
    CHECK_LT(chain_idx, 256);
  }

  ~ChainGenerator() {
  }

  // Generate a random 64-bit int.
  uint64_t Rand64() {
    return (implicit_cast<int64_t>(rand_.Next()) << 32) | rand_.Next();
  }

  Status GenerateNextInsert(KuduTable* table, KuduSession* session) {
    // Encode the chain index in the lowest 8 bits so that different chains never
    // intersect.
    uint64_t this_key = (Rand64() << 8) | chain_idx_;
    uint64_t ts = GetCurrentTimeMicros();
    gscoped_ptr<KuduInsert> insert = table->NewInsert();
    CHECK_OK(insert->mutable_row()->SetUInt64(kKeyColumnName, this_key));
    CHECK_OK(insert->mutable_row()->SetUInt64(kInsertTsColumnName, ts));
    CHECK_OK(insert->mutable_row()->SetUInt64(kLinkColumnName, prev_key_));
    RETURN_NOT_OK_PREPEND(session->Apply(insert.Pass()),
                          Substitute("Unable to apply insert with key $0 at ts $1",
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

  DISALLOW_COPY_AND_ASSIGN(ChainGenerator);
};
} // anonymous namespace

vector<string> LinkedListTest::GenerateSplitKeys() const {
  KuduEncodedKeyBuilder key_builder(schema_);
  gscoped_ptr<KuduEncodedKey> key;
  vector<string> split_keys;
  uint64_t increment = kuint64max / FLAGS_num_tablets;
  for (uint64_t i = 1; i < FLAGS_num_tablets; i++) {
    uint64_t val = i * increment;
    key_builder.Reset();
    key_builder.AddColumnKey(&val);
    key.reset(key_builder.BuildEncodedKey());
    split_keys.push_back(key->ToString());
  }
  return split_keys;
}

Status LinkedListTest::LoadLinkedList(const MonoDelta& run_for,
                                      int64_t *written_count) {

  RETURN_NOT_OK_PREPEND(client_->NewTableCreator()
                        ->table_name(kTableName)
                        .schema(&schema_)
                        .split_keys(GenerateSplitKeys())
                        .num_replicas(FLAGS_num_replicas)
                        .Create(),
                        "Failed to create table");
  scoped_refptr<KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(kTableName, &table));

  MonoTime deadline = MonoTime::Now(MonoTime::COARSE);
  deadline.AddDelta(run_for);

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(15000);
  RETURN_NOT_OK_PREPEND(session->SetFlushMode(KuduSession::MANUAL_FLUSH),
                        "Couldn't set flush mode");

  vector<ChainGenerator*> chains;
  ElementDeleter d(&chains);
  for (int i = 0; i < FLAGS_num_chains; i++) {
    chains.push_back(new ChainGenerator(i));
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
      for (int i = 0; i < FLAGS_num_chains; i++) {
        LOG(INFO) << i << ": " << chains[i]->prev_key();
      }
      return Status::OK();
    }
    BOOST_FOREACH(ChainGenerator* chain, chains) {
      RETURN_NOT_OK(chain->GenerateNextInsert(table.get(), session.get()));
    }

    MicrosecondsInt64 st = GetCurrentTimeMicros();
    Status s = session->Flush();
    int64_t elapsed = GetCurrentTimeMicros() - st;
    latency_histogram_.Increment(elapsed);

    if (!s.ok()) {
      vector<client::KuduError*> errors;
      bool overflow;
      session->GetPendingErrors(&errors, &overflow);
      BOOST_FOREACH(client::KuduError* err, errors) {
        LOG(WARNING) << "Flush error for row " << err->failed_op().ToString()
                     << ": " << err->status().ToString();
        s = err->status();
        delete err;
      }

      return s;
    }
    (*written_count) += chains.size();
  }
}

void LinkedListTest::DumpInsertHistogram(bool print_flags) {
  // We dump to cout instead of using glog so the output isn't prefixed with
  // line numbers. This makes it less ugly to copy-paste into JIRA, etc.
  using std::cout;
  using std::endl;

  const HdrHistogram* h = &latency_histogram_; // shorter alias

  cout << "------------------------------------------------------------" << endl;
  cout << "Histogram for latency of insert operations (microseconds)" << endl;
  if (print_flags) {
    cout << "Flags: " << google::CommandlineFlagsIntoString() << endl;
  }
  cout << "Note: each insert is a batch of " << FLAGS_num_chains << " rows." << endl;
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
static void VerifyNoDuplicateEntries(const vector<uint64_t>& ints, int* errors,
                                     const string& message) {
  for (int i = 1; i < ints.size(); i++) {
    if (ints[i] == ints[i - 1]) {
      LOG(ERROR) << message << ": " << ints[i];
      (*errors)++;
    }
  }
}

Status LinkedListTest::VerifyLinkedList(int64_t expected, int64_t* verified_count) {
  scoped_refptr<KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(kTableName, &table));
  KuduScanner scanner(table.get());
  RETURN_NOT_OK_PREPEND(scanner.SetProjection(&verify_projection_), "Bad projection");
  RETURN_NOT_OK_PREPEND(scanner.Open(), "Couldn't open scanner");

  vector<KuduRowResult> rows;
  vector<uint64_t> seen_key;
  vector<uint64_t> seen_link_to;
  seen_key.reserve(expected);
  seen_link_to.reserve(expected);

  Stopwatch sw;
  sw.start();
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&rows));
    BOOST_FOREACH(const KuduRowResult& row, rows) {
      uint64_t key;
      uint64_t link;
      RETURN_NOT_OK(row.GetUInt64(0, &key));
      RETURN_NOT_OK(row.GetUInt64(1, &link));
      seen_key.push_back(key);
      if (link != 0) {
        // Links to entry 0 don't count - the first inserts use this link
        seen_link_to.push_back(link);
      }
    }
    rows.clear();
  }
  *verified_count = seen_key.size();
  LOG(INFO) << "Done collecting results (" << (*verified_count) << " rows in "
            << sw.elapsed().wall_millis() << "ms)";

  LOG(INFO) << "Sorting results before verification of linked list structure...";
  std::sort(seen_key.begin(), seen_key.end());
  std::sort(seen_link_to.begin(), seen_link_to.end());
  LOG(INFO) << "Done sorting";


  int errors = 0;

  // Verify that no key was seen multiple times or linked to multiple times
  VerifyNoDuplicateEntries(seen_key, &errors, "Seen row key multiple times");
  VerifyNoDuplicateEntries(seen_link_to, &errors, "Seen link to row multiple times");
  // Verify that every key that was linked to was present
  vector<uint64_t> broken_links = STLSetDifference(seen_link_to, seen_key);
  BOOST_FOREACH(uint64_t broken, broken_links) {
    LOG(ERROR) << "Entry " << broken << " was linked to but not present";
    errors++;
  }

  // Verify that only the expected number of keys were seen but not linked to.
  // Only the last "batch" should have this characteristic.
  vector<uint64_t> not_linked_to = STLSetDifference(seen_key, seen_link_to);
  if (not_linked_to.size() != FLAGS_num_chains) {
    LOG(ERROR) << "Had " << not_linked_to.size() << " entries which were seen but not"
               << " linked to. Expected only " << FLAGS_num_chains;
    errors++;
  }

  if (errors > 0) {
    return Status::Corruption("Had one or more errors during verification (see log)");
  }
  return Status::OK();
}

void LinkedListTest::WaitAndVerify(int64_t expected) {
  int64_t seen;
  Stopwatch sw;
  sw.start();
  while (true) {
    Status s = VerifyLinkedList(expected, &seen);

    // TODO: when we enable hybridtime consistency for the scans,
    // then we should not allow !s.ok() here. But, with READ_LATEST
    // scans, we could have a lagging replica of one tablet, with an
    // up-to-date replica of another tablet, and end up with broken links
    // in the chain.

    if (!s.ok() || expected != seen) {
      // We'll give the tablets 3 seconds to start up regardless of how long we
      // inserted for. There's some fixed cost startup time, especially when
      // replication is enabled.
      const int kBaseTimeToWaitSecs = 3;

      if (!s.ok()) {
        LOG(INFO) << "Table not yet ready: " << s.ToString();
      } else {
        LOG(INFO) << "Table not yet ready: " << expected << "/" << seen << " rows";
      }
      if (sw.elapsed().wall_seconds() > kBaseTimeToWaitSecs + FLAGS_seconds_to_run) {
        // We'll give it an equal amount of time to re-load the data as it took
        // to write it in. Typically it completes much faster than that.
        FAIL() << "Timed out waiting for table to be accessible again.";
      }
      usleep(20*1000);
      continue;
    }
    ASSERT_STATUS_OK(s);
    ASSERT_EQ(expected, seen)
      << "Missing rows, but with no broken link in the chain. This means that "
      << "a suffix of the inserted rows went missing.";
    break;
  }
}

TEST_F(LinkedListTest, TestLoadAndVerify) {
  if (FLAGS_seconds_to_run == 0) {
    FLAGS_seconds_to_run = AllowSlowTests() ? kDefaultRunTimeSlow : kDefaultRunTimeFast;
  }

  int64_t written = 0;
  ASSERT_STATUS_OK(LoadLinkedList(MonoDelta::FromSeconds(FLAGS_seconds_to_run), &written));

  // TODO: currently we don't use hybridtime on the C++ client, so it's possible when we
  // scan after writing we may not see all of our writes (we may scan a replica). So,
  // we use WaitAndVerify here instead of a plain Verify.
  ASSERT_NO_FATAL_FAILURE(WaitAndVerify(written));

  // Check in-memory state with a downed TS. Scans may try other replicas.
  if (FLAGS_num_tablet_servers > 1) {
    cluster_->tablet_server(0)->Shutdown();
    ASSERT_NO_FATAL_FAILURE(WaitAndVerify(written));
  }

  // Kill and restart the cluster, verify data remains.
  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  // We need to loop here because the tablet may spend some time in BOOTSTRAPPING state
  // initially after a restart. TODO: Scanner should support its own retries in this circumstance.
  // Remove this loop once client is more fleshed out.
  ASSERT_NO_FATAL_FAILURE(WaitAndVerify(written));

  // Check post-replication state with a downed TS.
  if (FLAGS_num_tablet_servers > 1) {
    cluster_->tablet_server(0)->Shutdown();
    ASSERT_NO_FATAL_FAILURE(WaitAndVerify(written));
  }

  ASSERT_NO_FATAL_FAILURE(RestartCluster());
  // Sleep a little bit, so that the tablet is proably in bootstrapping state.
  usleep(100 * 1000);
  // Restart while bootstrapping
  ASSERT_NO_FATAL_FAILURE(RestartCluster());
  ASSERT_NO_FATAL_FAILURE(WaitAndVerify(written));

  // Dump the performance info at the very end, so it's easy to read. On a failed
  // test, we don't care about this stuff anwyay.
  DumpInsertHistogram(true);
}

} // namespace kudu
