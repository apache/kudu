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
#include <tr1/memory>
#include <tr1/unordered_map>
#include <utility>
#include <vector>

#include "client/client.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "gutil/walltime.h"
#include "integration-tests/external_mini_cluster.h"
#include "util/random.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientOptions;
using kudu::client::KuduScanner;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::Insert;

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

static const char* const kKeyColumnName = "rand_key";
static const char* const kLinkColumnName = "link_to";
static const char* const kInsertTsColumnName = "insert_ts";

namespace kudu {

class LinkedListTest : public KuduTest {
 public:
  LinkedListTest()
    : schema_(boost::assign::list_of
              (ColumnSchema(kKeyColumnName, UINT64))
              (ColumnSchema(kLinkColumnName, UINT64))
              (ColumnSchema(kInsertTsColumnName, UINT64)),
              1),
      verify_projection_(boost::assign::list_of
                         (ColumnSchema(kKeyColumnName, UINT64))
                         (ColumnSchema(kLinkColumnName, UINT64)),
                         1) {
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
    opts.num_tablet_servers = 1;
    opts.data_root = GetTestPath("linked-list-cluster");

    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_STATUS_OK(cluster_->Start());
    ASSERT_STATUS_OK(cluster_->CreateClient(KuduClientOptions(), &client_));
  }

  // Load the table with the linked list test pattern.
  //
  // Runs for the amount of time designated by 'run_for'.
  // Sets *written_count to the number of rows inserted.
  Status LoadLinkedList(const MonoDelta& run_for,
                        int64_t* written_count);
  Status VerifyLinkedList(int64_t* verified_count);

  void WaitForBootstrapToFinishAndVerify(int64_t* seen);

 protected:
  static const char* kTableName;
  const Schema schema_;
  const Schema verify_projection_;
  gscoped_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
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
    gscoped_ptr<Insert> insert = table->NewInsert();
    CHECK_OK(insert->mutable_row()->SetUInt64(kKeyColumnName, this_key));
    CHECK_OK(insert->mutable_row()->SetUInt64(kInsertTsColumnName, ts));
    CHECK_OK(insert->mutable_row()->SetUInt64(kLinkColumnName, prev_key_));
    RETURN_NOT_OK_PREPEND(session->Apply(&insert),
                          Substitute("Unable to apply insert with key $0 at ts $1",
                                     this_key, ts));
    return Status::OK();
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

Status LinkedListTest::LoadLinkedList(const MonoDelta& run_for,
                                      int64_t* written_count) {
  RETURN_NOT_OK_PREPEND(client_->CreateTable(kTableName, schema_),
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
  while (true) {
    LOG_EVERY_N(INFO, 10000) << "Written " << (*written_count) << " rows in chain";

    if (deadline.ComesBefore(MonoTime::Now(MonoTime::COARSE))) {
      LOG(INFO) << "Finished inserting list. Added " << (*written_count) << " in chain";
      return Status::OK();
    }
    BOOST_FOREACH(ChainGenerator* chain, chains) {
      RETURN_NOT_OK(chain->GenerateNextInsert(table.get(), session.get()));
    }

    Status s = session->Flush();
    if (!s.ok()) {
      vector<client::Error*> errors;
      bool overflow;
      session->GetPendingErrors(&errors, &overflow);
      BOOST_FOREACH(client::Error* err, errors) {
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

struct VerificationState {
  VerificationState() : seen_key(false), seen_link(false) {}

  uint8_t seen_key;
  uint8_t seen_link;
};

Status LinkedListTest::VerifyLinkedList(int64_t* verified_count) {
  scoped_refptr<KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(kTableName, &table));
  KuduScanner scanner(table.get());
  RETURN_NOT_OK_PREPEND(scanner.SetProjection(verify_projection_), "Bad projection");
  RETURN_NOT_OK_PREPEND(scanner.Open(), "Couldn't open scanner");

  typedef unordered_map<uint64_t, VerificationState> StateMap;
  StateMap state;

  vector<const uint8_t*> rows;

  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&rows));
    BOOST_FOREACH(const uint8_t* row_ptr, rows) {
      ConstContiguousRow row(verify_projection_, row_ptr);

      uint64_t key = *verify_projection_.ExtractColumnFromRow<UINT64>(row, 0);
      uint64_t link = *verify_projection_.ExtractColumnFromRow<UINT64>(row, 1);

      {
        VerificationState& s = LookupOrInsert(&state, key, VerificationState());
        s.seen_key++;
      }

      if (link != 0) {
        VerificationState& s = LookupOrInsert(&state, link, VerificationState());
        s.seen_link++;
      }
    }
  }

  int errors = 0;

  BOOST_FOREACH(const StateMap::value_type& e, state) {
    if (e.second.seen_link && !e.second.seen_key) {
      LOG(ERROR) << "Entry " << e.first << " was linked to but not present";
      errors++;
    }
    if (e.second.seen_link > 1) {
      LOG(ERROR) << "Entry " << e.first << " linked multiple times";
      errors++;
    }
    if (e.second.seen_key > 1) {
      LOG(ERROR) << "Entry " << e.first << " seen multiple times";
      errors++;
    }
  }

  LOG(INFO) << "Verified " << state.size() << " entries";
  *verified_count = state.size();
  if (errors > 0) {
    return Status::Corruption("Had one or more errors during verification (see log)");
  }
  return Status::OK();
}

void LinkedListTest::WaitForBootstrapToFinishAndVerify(int64_t* seen) {
  Stopwatch sw;
  sw.start();
  while (true) {
    Status s = VerifyLinkedList(seen);
    if (s.IsServiceUnavailable()) {
      if (sw.elapsed().wall_seconds() > FLAGS_seconds_to_run) {
        // We'll give it an equal amount of time to re-load the data as it took
        // to write it in. Typically it completes much faster than that.
        FAIL() << "Timed out waiting for table to be accessible again.";
      }
      usleep(20*1000);
      continue;
    }
    ASSERT_STATUS_OK(s);
    break;
  }
}

TEST_F(LinkedListTest, TestLoadAndVerify) {
  if (FLAGS_seconds_to_run == 0) {
    FLAGS_seconds_to_run = AllowSlowTests() ? kDefaultRunTimeSlow : kDefaultRunTimeFast;
  }

  int64_t written = 0;
  ASSERT_STATUS_OK(LoadLinkedList(MonoDelta::FromSeconds(FLAGS_seconds_to_run), &written));

  int64_t seen = 0;
  ASSERT_STATUS_OK(VerifyLinkedList(&seen));
  ASSERT_EQ(written, seen);

  // Kill and restart the cluster, verify data remains.
  ASSERT_NO_FATAL_FAILURE(RestartCluster());

  // We need to loop here because the tablet may spend some time in BOOTSTRAPPING state
  // initially after a restart. TODO: Scanner should support its own retries in this circumstance.
  // Remove this loop once client is more fleshed out.
  ASSERT_NO_FATAL_FAILURE(WaitForBootstrapToFinishAndVerify(&seen));
  ASSERT_EQ(written, seen);

  ASSERT_NO_FATAL_FAILURE(RestartCluster());
  // Sleep a little bit, so that the tablet is proably in bootstrapping state.
  usleep(100 * 1000);
  // Restart while bootstrapping
  ASSERT_NO_FATAL_FAILURE(RestartCluster());
  ASSERT_NO_FATAL_FAILURE(WaitForBootstrapToFinishAndVerify(&seen));
  ASSERT_EQ(written, seen);
}

} // namespace kudu
