// Copyright (c) 2014, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cmath>
#include <cstdlib>
#include <string>
#include <tr1/memory>
#include <vector>

#include "client/client.h"
#include "client/row_result.h"
#include "client/write_op.h"
#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "gutil/strings/strcat.h"
#include "gutil/strings/substitute.h"
#include "integration-tests/mini_cluster.h"
#include "master/mini_master.h"
#include "tablet/maintenance_manager.h"
#include "util/async_util.h"
#include "util/countdown_latch.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"
#include "util/test_util.h"
#include "util/status.h"
#include "util/thread.h"
#include "util/random.h"

// Test size parameters
DEFINE_int32(concurrent_inserts, 8, "Number of inserting clients to launch");
DEFINE_int32(inserts_per_client, 1000,
             "Number of rows inserted by each inserter client");
DEFINE_int32(rows_per_batch, 250, "Number of rows per client batch");

using boost::assign::list_of;
using std::string;
using std::tr1::shared_ptr;
using std::vector;

namespace kudu {
namespace tablet {

using client::Insert;
using client::KuduClient;
using client::KuduClientOptions;
using client::KuduRowResult;
using client::KuduScanner;
using client::KuduSession;
using client::KuduTable;

class FullStackInsertScanTest : public KuduTest {
 protected:
  FullStackInsertScanTest()
    : kNumInsertClients(FLAGS_concurrent_inserts),
    kNumInsertsPerClient(FLAGS_inserts_per_client),
    kNumRows(kNumInsertClients * kNumInsertsPerClient),
    kFlushEveryN(FLAGS_rows_per_batch),
    // schema has kNumIntCols contiguous columns of Int32 and Int64, in order.
    schema_(list_of
            (ColumnSchema("key", UINT64))
            (ColumnSchema("string_val", STRING))
            (ColumnSchema("int32_val1", INT32))
            (ColumnSchema("int32_val2", INT32))
            (ColumnSchema("int32_val3", INT32))
            (ColumnSchema("int32_val4", INT32))
            (ColumnSchema("int64_val1", INT64))
            (ColumnSchema("int64_val2", INT64))
            (ColumnSchema("int64_val3", INT64))
            (ColumnSchema("int64_val4", INT64)), 1),
    sessions_(kNumInsertClients),
    tables_(kNumInsertClients) {
  }

  const int kNumInsertClients;
  const int kNumInsertsPerClient;
  const int kNumRows;

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    MaintenanceManager::Disable();
    ASSERT_GE(kNumInsertClients, 0);
    ASSERT_GE(kNumInsertsPerClient, 0);
    InitCluster();
    shared_ptr<KuduClient> reader;
    ASSERT_OK(KuduClient::Create(client_opts_, &reader));
    ASSERT_OK(reader->CreateTable(kTableName, schema_));
    ASSERT_OK(reader->OpenTable(kTableName, &reader_table_));
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  void DoConcurrentClientInserts();
  void DoTestScans();

 private:
  // Generate random row according to schema_.
  static void RandomRow(Random* rng, PartialRow* row,
                        char* buf, uint64_t key, int id);

  void InitCluster() {
    // Start mini-cluster with 1 tserver, config client options
    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 1));
    ASSERT_OK(cluster_->Start());
    client_opts_.master_server_addr =
      cluster_->mini_master()->bound_rpc_addr().ToString();
  }

  // Adds newly generated client's session and table pointers to arrays at id
  void CreateNewClient(int id) {
    shared_ptr<KuduClient> client;
    CHECK_OK(KuduClient::Create(client_opts_, &client));
    CHECK_OK(client->OpenTable(kTableName, &tables_[id]));
    shared_ptr<KuduSession> session = client->NewSession();
    session->SetTimeoutMillis(kSessionTimeoutMs);
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    sessions_[id] = session;
  }

  // Insert the rows that are associated with that ID.
  void InsertRows(CountDownLatch* cdl, int id);

  // Run a scan from the reader_client_ with the projection schema schema
  // and LOG_TIMING message msg.
  void ScanProjection(const Schema& schema, const string& msg);

  Schema StringSchema() const;
  Schema Int32Schema() const;
  Schema Int64Schema() const;

  static const char* const kTableName;
  static const int kSessionTimeoutMs = 5000;
  static const int kRandomStrMinLength = 16;
  static const int kRandomStrMaxLength = 31;
  static const int kNumIntCols = 4;
  enum {
    kKeyCol,
    kStrCol,
    kInt32ColBase,
    kInt64ColBase = kInt32ColBase + kNumIntCols
  };
  const int kFlushEveryN;

  Schema schema_;
  shared_ptr<MiniCluster> cluster_;
  KuduClientOptions client_opts_;
  scoped_refptr<KuduTable> reader_table_;
  // Concurrent client insertion test variables
  vector<shared_ptr<KuduSession> > sessions_;
  vector<scoped_refptr<KuduTable> > tables_;
};

namespace {

// Fills buf randomly with a string between min and max length
// then appends a null character. Buffer should have room for
// max + 1 chars.
void RandomString(Random* rng, char* buf, int min, int max) {
  CHECK_GE(min, 0);
  CHECK_LE(min, max);
  int size = min + rng->Uniform(max - min + 1);
  buf[size] = '\0';
  int i;
  uint32_t random;
  for (i = 0; i <= size - sizeof(random); i += sizeof(random)) {
    random = rng->Next();
    memcpy(&buf[i], &random, sizeof(random));
  }
  random = rng->Next();
  memcpy(&buf[i], &random, size - i);
}

int64_t rand64(Random* rng) {
  int64_t ret = rng->Next();
  ret <<= sizeof(int);
  ret |= rng->Next();
  return ret;
}

// If key is approximately at an even multiple of 1/10 of the way between
// start and end, then a % completion update is printed to LOG(INFO)
// Assumes that end - start + 1 fits into an int
void ReportTenthDone(uint64_t key, uint64_t start, uint64_t end,
                     int id, int numids) {
  int done = key - start + 1;
  int total = end - start + 1;
  if (total < 10) return;
  if (done % (total / 10) == 0) {
    int percent = done * 100 / total;
    LOG(INFO) << "Insertion thread " << id << " of "
              << numids << " is "<< percent << "% done.";
  }
}

void ReportAllDone(int id, int numids) {
  LOG(INFO) << "Insertion thread " << id << " of  "
            << numids << " is 100% done.";
}

} // anonymous namespace

const char* const FullStackInsertScanTest::kTableName = "full-stack-mrs-test-tbl";

TEST_F(FullStackInsertScanTest, ClientInsertScanStressTest) {
  DoConcurrentClientInserts();
  DoTestScans();
}

void FullStackInsertScanTest::DoConcurrentClientInserts() {
  vector<scoped_refptr<Thread> > threads(kNumInsertClients);
  CountDownLatch start_latch(kNumInsertClients + 1);
  SeedRandom();
  for (int i = 0; i < kNumInsertClients; ++i) {
    CreateNewClient(i);
    ASSERT_OK(Thread::Create(CURRENT_TEST_NAME(),
                             StrCat(CURRENT_TEST_CASE_NAME(), "-id", i),
                             &FullStackInsertScanTest::InsertRows, this,
                             &start_latch, i, &threads[i]));
    start_latch.CountDown();
  }
  LOG_TIMING(INFO,
             strings::Substitute("concurrent inserts ($0 rows, $1 threads)",
                                 kNumRows, kNumInsertClients)) {
    start_latch.CountDown();
    BOOST_FOREACH(const scoped_refptr<Thread>& thread, threads) {
      ASSERT_OK(ThreadJoiner(thread.get())
                .warn_every_ms(15000)
                .Join());
    }
  }
}

void FullStackInsertScanTest::DoTestScans() {
  LOG(INFO) << "Doing test scans on table of " << kNumRows << " rows.";
  ScanProjection(Schema(vector<ColumnSchema>(), 0), "empty projection, 0 col");
  ScanProjection(schema_.CreateKeyProjection(), "key scan, 1 col");
  ScanProjection(schema_, "full schema scan, 10 col");
  ScanProjection(StringSchema(), "String projection, 1 col");
  ScanProjection(Int32Schema(), "Int32 projection, 4 col");
  ScanProjection(Int64Schema(), "Int64 projection, 4 col");
}

void FullStackInsertScanTest::InsertRows(CountDownLatch* start_latch, int id) {
  Random rng(id);

  start_latch->Wait();
  // Retrieve id's session and table
  KuduSession* session = CHECK_NOTNULL(sessions_[id].get());
  KuduTable* table = CHECK_NOTNULL(tables_[id].get());
  // Identify start and end of keyrange id is responsible for
  uint64_t start = kNumInsertsPerClient * id;
  uint64_t end = start + kNumInsertsPerClient;
  // Printed id value is in the range 1..kNumInsertClients inclusive
  ++id;
  // Use synchronizer to keep 1 asynchronous batch flush maximum
  Synchronizer sync;
  sync.AsStatusCallback().Run(Status::OK());
  // Maintain buffer for random string generation
  char randstr[kRandomStrMaxLength + 1];
  // Insert in the id's key range
  for (uint64_t key = start; key < end; ++key) {
    gscoped_ptr<Insert> insert = table->NewInsert();
    RandomRow(&rng, insert->mutable_row(), randstr, key, id);
    CHECK_OK(session->Apply(&insert));

    // Report updates or flush every so often, using the synchronizer to always
    // start filling up the next batch while previous one is sent out.
    if (key % kFlushEveryN == 0) {
      sync.Wait();
      sync.Reset();
      session->FlushAsync(sync.AsStatusCallback());
    }
    ReportTenthDone(key, start, end, id, kNumInsertClients);
  }
  ReportAllDone(id, kNumInsertClients);
  sync.Wait();
  CHECK_OK(session->Flush());
}

void FullStackInsertScanTest::ScanProjection(const Schema& schema,
                                             const string& msg) {
  KuduScanner scanner(reader_table_.get());
  CHECK_OK(scanner.SetProjection(&schema));
  uint64_t nrows = 0;
  LOG_TIMING(INFO, msg) {
    ASSERT_OK(scanner.Open());
    vector<KuduRowResult> rows;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&rows));
      nrows += rows.size();
      rows.clear();
    }
  }
  ASSERT_EQ(nrows, kNumRows);
}

// Fills in the fields for a row as defined by the Schema below
// name: (key,      string_val, int32_val$, int64_val$)
// type: (uint64_t, string,     int32_t x4, int64_t x4)
// The first int32 gets the id and the first int64 gets the thread
// id. The key is assigned to "key," and the other fields are random.
void FullStackInsertScanTest::RandomRow(Random* rng, PartialRow* row, char* buf,
                                        uint64_t key, int id) {
  CHECK_OK(row->SetUInt64(kKeyCol, key));
  RandomString(rng, buf, kRandomStrMinLength, kRandomStrMaxLength);
  CHECK_OK(row->SetStringCopy(kStrCol, buf));
  CHECK_OK(row->SetInt32(kInt32ColBase, id));
  CHECK_OK(row->SetInt64(kInt64ColBase, Thread::current_thread()->tid()));
  for (int i = 1; i < kNumIntCols; ++i) {
    CHECK_OK(row->SetInt32(kInt32ColBase + i, rng->Next()));
    CHECK_OK(row->SetInt64(kInt64ColBase + i, rand64(rng)));
  }
}

Schema FullStackInsertScanTest::StringSchema() const {
  return Schema(list_of(schema_.column(kKeyCol)), 0);
}

Schema FullStackInsertScanTest::Int32Schema() const {
  vector<ColumnSchema> cols;
  for (int i = 0; i < kNumIntCols; ++i) {
    cols.push_back(schema_.column(kInt32ColBase + i));
  }
  return Schema(cols, 0);
}

Schema FullStackInsertScanTest::Int64Schema() const {
  vector<ColumnSchema> cols;
  for (int i = 0; i < kNumIntCols; ++i) {
    cols.push_back(schema_.column(kInt64ColBase + i));
  }
  return Schema(cols, 0);
}

} // namespace tablet
} // namespace kudu
