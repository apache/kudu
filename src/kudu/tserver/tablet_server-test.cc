// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/tserver/tablet_server-test-base.h"

#include "kudu/consensus/log-test-base.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/util/crc.h"
#include "kudu/util/curl_util.h"

using std::string;
using std::tr1::shared_ptr;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::server::Clock;
using kudu::server::HybridClock;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using strings::Substitute;
using kudu::consensus::ChangeConfigRequestPB;
using kudu::consensus::ChangeConfigResponsePB;

DEFINE_int32(single_threaded_insert_latency_bench_warmup_rows, 100,
             "Number of rows to insert in the warmup phase of the single threaded"
             " tablet server insert latency micro-benchmark");

DEFINE_int32(single_threaded_insert_latency_bench_insert_rows, 1000,
             "Number of rows to insert in the testing phase of the single threaded"
             " tablet server insert latency micro-benchmark");

DECLARE_int32(tablet_server_scan_batch_size_rows);
DECLARE_int32(metrics_retirement_age_ms);

// Declare these metrics prototypes for simpler unit testing of their behavior.
METRIC_DECLARE_counter(rows_inserted);
METRIC_DECLARE_counter(rows_updated);
METRIC_DECLARE_counter(rows_deleted);

namespace kudu {
namespace tserver {

class TabletServerTest : public TabletServerTestBase {
 public:
  // Starts the tablet server, override to start it later.
  virtual void SetUp() OVERRIDE {
    TabletServerTestBase::SetUp();
    StartTabletServer();
  }
};

TEST_F(TabletServerTest, TestPingServer) {
  // Ping the server.
  PingRequestPB req;
  PingResponsePB resp;
  RpcController controller;
  ASSERT_STATUS_OK(proxy_->Ping(req, &resp, &controller));
}

TEST_F(TabletServerTest, TestWebPages) {
  EasyCurl c;
  faststring buf;
  string addr = mini_server_->bound_http_addr().ToString();

  // Tablets page should list tablet.
  ASSERT_STATUS_OK(c.FetchURL(strings::Substitute("http://$0/tablets", addr),
                              &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), kTabletId);
  ASSERT_STR_CONTAINS(buf.ToString(), "<td>&lt;start of table&gt;</td>");
  ASSERT_STR_CONTAINS(buf.ToString(), "<td>&lt;end of table&gt;</td>");

  // Tablet page should include the schema.
  ASSERT_STATUS_OK(c.FetchURL(strings::Substitute("http://$0/tablet?id=$1", addr, kTabletId),
                              &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), "<th>key</th>");
  ASSERT_STR_CONTAINS(buf.ToString(), "<td>string NULLABLE</td>");

  // Test fetching metrics.
  // Fetching metrics has the side effect of retiring metrics, but not in a single pass.
  // So, we check a couple of times in a loop -- thus, if we had a bug where one of these
  // metrics was accidentally un-referenced too early, we'd cause it to get retired.
  // If the metrics survive several passes of fetching, then we are pretty sure they will
  // stick around properly for the whole lifetime of the server.
  FLAGS_metrics_retirement_age_ms = 0;
  for (int i = 0; i < 3; i++) {
    SCOPED_TRACE(i);
    ASSERT_STATUS_OK(c.FetchURL(strings::Substitute("http://$0/jsonmetricz", addr, kTabletId),
                                &buf));

    // Check for the existence of some particular metrics for which we've had early-retirement
    // bugs in the past.
    ASSERT_STR_CONTAINS(buf.ToString(), "clock.clock_timestamp");
    ASSERT_STR_CONTAINS(buf.ToString(), "kudu.tabletserver.tserver.scanners.active_scanners");
    ASSERT_STR_CONTAINS(buf.ToString(), "threading.total_threads");
#ifdef TCMALLOC_ENABLED
    ASSERT_STR_CONTAINS(buf.ToString(), "tcmalloc.tcmalloc_max_total_thread_cache_bytes");
#endif
  }
}

TEST_F(TabletServerTest, TestInsert) {
  WriteRequestPB req;

  req.set_tablet_id(kTabletId);

  WriteResponsePB resp;
  RpcController controller;

  scoped_refptr<TabletPeer> tablet;
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));
  scoped_refptr<Counter> rows_inserted =
      METRIC_rows_inserted.Instantiate(*tablet->tablet()->GetMetricContext());
  ASSERT_EQ(0, rows_inserted->value());
  tablet.reset();

  // Send a bad insert which has an empty schema. This should result
  // in an error.
  {
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678, "hello world via RPC",
                   req.mutable_row_operations());

    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::MISMATCHED_SCHEMA, resp.error().code());
    Status s = StatusFromPB(resp.error().status());
    EXPECT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Client missing required column: key[uint32 NOT NULL]");
    req.clear_row_operations();
  }

  // Send an empty request with the correct schema.
  // This should succeed and do nothing.
  {
    controller.Reset();
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    req.clear_row_operations();
  }

  // Send an actual row insert.
  {
    controller.Reset();
    RowOperationsPB* data = req.mutable_row_operations();
    data->Clear();

    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678,
                   "hello world via RPC", data);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    req.clear_row_operations();
    ASSERT_EQ(1, rows_inserted->value());
  }

  // Send a batch with multiple rows, one of which is a duplicate of
  // the above insert. This should generate one error into per_row_errors.
  {
    controller.Reset();
    RowOperationsPB* data = req.mutable_row_operations();
    data->Clear();

    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1, 1, "ceci n'est pas une dupe", data);
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 2, 1, "also not a dupe key", data);
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 1, "I am a duplicate key", data);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();
    ASSERT_EQ(1, resp.per_row_errors().size());
    ASSERT_EQ(2, resp.per_row_errors().Get(0).row_index());
    Status s = StatusFromPB(resp.per_row_errors().Get(0).error());
    ASSERT_STR_CONTAINS(s.ToString(), "Already present");
    ASSERT_EQ(3, rows_inserted->value());  // This counter only counts successful inserts.
  }

  // get the clock's current timestamp
  Timestamp now_before = mini_server_->server()->clock()->Now();

  rows_inserted = NULL;
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 1))
                                            (KeyValue(2, 1))
                                            (KeyValue(1234, 5678)));

  // get the clock's timestamp after replay
  Timestamp now_after = mini_server_->server()->clock()->Now();

  // make sure 'now_after' is greater than or equal to 'now_before'
  ASSERT_GE(now_after.value(), now_before.value());
}

TEST_F(TabletServerTest, TestExternalConsistencyModes_ClientPropagated) {
  WriteRequestPB req;
  req.set_tablet_id(kTabletId);
  WriteResponsePB resp;
  RpcController controller;

  scoped_refptr<TabletPeer> tablet;
  ASSERT_TRUE(
      mini_server_->server()->tablet_manager()->LookupTablet(kTabletId,
                                                             &tablet));
  scoped_refptr<Counter> rows_inserted =
      METRIC_rows_inserted.Instantiate(
          *tablet->tablet()->GetMetricContext());
  ASSERT_EQ(0, rows_inserted->value());

  // get the current time
  Timestamp current = mini_server_->server()->clock()->Now();
  // advance current to some time in the future. we do 5 secs to make
  // sure this timestamp will still be in the future when it reaches the
  // server.
  current = HybridClock::TimestampFromMicroseconds(
      HybridClock::GetPhysicalValueMicros(current) + 5000000);

  // Send an actual row insert.
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678, "hello world via RPC",
                 req.mutable_row_operations());

  // set the external consistency mode and the timestamp
  req.set_external_consistency_mode(CLIENT_PROPAGATED);

  req.set_propagated_timestamp(current.ToUint64());
  SCOPED_TRACE(req.DebugString());
  ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
  SCOPED_TRACE(resp.DebugString());
  ASSERT_FALSE(resp.has_error());
  req.clear_row_operations();
  ASSERT_EQ(1, rows_inserted->value());

  // make sure the server returned a write timestamp where only
  // the logical value was increased since he should have updated
  // its clock with the client's value.
  Timestamp write_timestamp(resp.timestamp());

  ASSERT_EQ(HybridClock::GetPhysicalValueMicros(current),
            HybridClock::GetPhysicalValueMicros(write_timestamp));

  ASSERT_EQ(HybridClock::GetLogicalValue(current) + 1,
            HybridClock::GetLogicalValue(write_timestamp));
}

TEST_F(TabletServerTest, TestExternalConsistencyModes_CommitWait) {
  WriteRequestPB req;
  req.set_tablet_id(kTabletId);
  WriteResponsePB resp;
  RpcController controller;
  HybridClock* hclock = down_cast<HybridClock*, Clock>(mini_server_->server()->clock());

  scoped_refptr<TabletPeer> tablet;
  ASSERT_TRUE(
      mini_server_->server()->tablet_manager()->LookupTablet(kTabletId,
                                                             &tablet));
  scoped_refptr<Counter> rows_inserted =
      METRIC_rows_inserted.Instantiate(
          *tablet->tablet()->GetMetricContext());
  ASSERT_EQ(0, rows_inserted->value());

  // get current time, with and without error
  Timestamp now_before;
  uint64_t error_before;
  hclock->NowWithError(&now_before, &error_before);

  uint64_t now_before_usec = HybridClock::GetPhysicalValueMicros(now_before);
  LOG(INFO) << "Submitting write with commit wait at: " << now_before_usec << " us +- "
      << error_before << " us";

  // Send an actual row insert.
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678, "hello world via RPC",
                 req.mutable_row_operations());

  // set the external consistency mode to COMMIT_WAIT
  req.set_external_consistency_mode(COMMIT_WAIT);

  SCOPED_TRACE(req.DebugString());
  ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
  SCOPED_TRACE(resp.DebugString());
  ASSERT_FALSE(resp.has_error());
  req.clear_row_operations();
  ASSERT_EQ(1, rows_inserted->value());

  // Two things must have happened.
  // 1 - The write timestamp must be greater than 'now_before'
  // 2 - The write must have taken at least 'error_before' to complete (two
  //     times more in average).

  Timestamp now_after;
  uint64_t error_after;
  hclock->NowWithError(&now_after, &error_after);

  Timestamp write_timestamp(resp.timestamp());

  uint64_t write_took = HybridClock::GetPhysicalValueMicros(now_after) -
      HybridClock::GetPhysicalValueMicros(now_before);

  LOG(INFO) << "Write applied at: " << HybridClock::GetPhysicalValueMicros(write_timestamp)
      << " us, current time: " << HybridClock::GetPhysicalValueMicros(now_after)
      << " us, write took: " << write_took << " us";

  ASSERT_GT(write_timestamp.value(), now_before.value());

  // see HybridClockTest.TestWaitUntilAfter_TestCase2
  if (error_after >= error_before) {
    ASSERT_GE(write_took, 2 * error_before);
  } else {
    ASSERT_GE(write_took, error_before);
  }
}


TEST_F(TabletServerTest, TestInsertAndMutate) {

  scoped_refptr<TabletPeer> tablet;
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));
  scoped_refptr<Counter> rows_inserted =
      METRIC_rows_inserted.Instantiate(*tablet->tablet()->GetMetricContext());
  scoped_refptr<Counter> rows_updated =
      METRIC_rows_updated.Instantiate(*tablet->tablet()->GetMetricContext());
  scoped_refptr<Counter> rows_deleted =
      METRIC_rows_deleted.Instantiate(*tablet->tablet()->GetMetricContext());
  ASSERT_EQ(0, rows_inserted->value());
  ASSERT_EQ(0, rows_updated->value());
  ASSERT_EQ(0, rows_deleted->value());
  tablet.reset();

  RpcController controller;

  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    RowOperationsPB* data = req.mutable_row_operations();
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1, 1, "original1", data);
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 2, 2, "original2", data);
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 3, 3, "original3", data);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();
    ASSERT_EQ(0, resp.per_row_errors().size());
    ASSERT_EQ(3, rows_inserted->value());
    ASSERT_EQ(0, rows_updated->value());
    controller.Reset();
  }

  // Try and mutate the rows inserted above
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 1, 2, "mutation1",
                   req.mutable_row_operations());
    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 2, 3, "mutation2",
                   req.mutable_row_operations());
    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 3, 4, "mutation3",
                   req.mutable_row_operations());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();
    ASSERT_EQ(0, resp.per_row_errors().size());
    ASSERT_EQ(3, rows_inserted->value());
    ASSERT_EQ(3, rows_updated->value());
    controller.Reset();
  }

  // Try and mutate a non existent row key (should get an error)
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 1234, 2, "mutated",
                   req.mutable_row_operations());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();
    ASSERT_EQ(1, resp.per_row_errors().size());
    ASSERT_EQ(3, rows_updated->value());
    controller.Reset();
  }

  // Try and delete 1 row
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    AddTestKeyToPB(RowOperationsPB::DELETE, schema_, 1, req.mutable_row_operations());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(0, resp.per_row_errors().size());
    ASSERT_EQ(3, rows_updated->value());
    ASSERT_EQ(1, rows_deleted->value());
    controller.Reset();
  }

  // Now try and mutate a row we just deleted, we should get an error
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 1, 2, "mutated1",
                   req.mutable_row_operations());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(1, resp.per_row_errors().size());
    controller.Reset();
  }

  ASSERT_EQ(3, rows_inserted->value());
  ASSERT_EQ(3, rows_updated->value());

  // At this point, we have two rows left (row key 2 and 3).
  VerifyRows(schema_, boost::assign::list_of(KeyValue(2, 3))
                                            (KeyValue(3, 4)));

  // Do a mixed operation (some insert, update, and delete, some of which fail)
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    RowOperationsPB* ops = req.mutable_row_operations();
    // op 0: Mutate row 1, which doesn't exist. This should fail.
    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 1, 3, "mutate_should_fail", ops);
    // op 1: Insert a new row 4 (succeeds)
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 4, 4, "new row 4", ops);
    // op 2: Delete a non-existent row 5 (should fail)
    AddTestKeyToPB(RowOperationsPB::DELETE, schema_, 5, ops);
    // op 3: Insert a new row 6 (succeeds)
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 6, 6, "new row 6", ops);

    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(2, resp.per_row_errors().size());
    EXPECT_EQ("row_index: 0 error { code: NOT_FOUND message: \"key not found\" }",
              resp.per_row_errors(0).ShortDebugString());
    EXPECT_EQ("row_index: 2 error { code: NOT_FOUND message: \"key not found\" }",
              resp.per_row_errors(1).ShortDebugString());
    controller.Reset();
  }

  // get the clock's current timestamp
  Timestamp now_before = mini_server_->server()->clock()->Now();

  rows_inserted = NULL;
  rows_updated = NULL;
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(2, 3))
                                            (KeyValue(3, 4))
                                            (KeyValue(4, 4))
                                            (KeyValue(6, 6)));

  // get the clock's timestamp after replay
  Timestamp now_after = mini_server_->server()->clock()->Now();

  // make sure 'now_after' is greater that or equal to 'now_before'
  ASSERT_GE(now_after.value(), now_before.value());
}

// Test that passing a schema with fields not present in the tablet schema
// throws an exception.
TEST_F(TabletServerTest, TestInvalidWriteRequest_BadSchema) {
  SchemaBuilder schema_builder(schema_);
  ASSERT_STATUS_OK(schema_builder.AddColumn("col_doesnt_exist", UINT32));
  Schema bad_schema_with_ids = schema_builder.Build();
  Schema bad_schema = schema_builder.BuildWithoutIds();

  // Send a row insert with an extra column
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    RpcController controller;

    req.set_tablet_id(kTabletId);
    RowOperationsPB* data = req.mutable_row_operations();
    ASSERT_STATUS_OK(SchemaToPB(bad_schema, req.mutable_schema()));

    KuduPartialRow row(&bad_schema);
    CHECK_OK(row.SetUInt32("key", 1234));
    CHECK_OK(row.SetUInt32("int_val", 5678));
    CHECK_OK(row.SetStringCopy("string_val", "hello world via RPC"));
    CHECK_OK(row.SetUInt32("col_doesnt_exist", 91011));
    RowOperationsPBEncoder enc(data);
    enc.Add(RowOperationsPB::INSERT, row);

    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::MISMATCHED_SCHEMA, resp.error().code());
    ASSERT_STR_CONTAINS(resp.error().status().message(),
                        "Client provided column col_doesnt_exist[uint32 NOT NULL]"
                        " not present in tablet");
  }

  // Send a row mutation with an extra column and IDs
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    RpcController controller;

    req.set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToPB(bad_schema_with_ids, req.mutable_schema()));

    AddTestKeyToPB(RowOperationsPB::UPDATE, bad_schema_with_ids, 1,
                   req.mutable_row_operations());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::INVALID_SCHEMA, resp.error().code());
    ASSERT_STR_CONTAINS(resp.error().status().message(),
                        "User requests should not have Column IDs");
  }
}

// Executes mutations each time a Tablet goes through a compaction/flush
// lifecycle hook. This allows to create mutations of all possible types
// deterministically. The purpose is to make sure such mutations are replayed
// correctly on tablet bootstrap.
class MyCommonHooks : public Tablet::FlushCompactCommonHooks,
                      public Tablet::FlushFaultHooks,
                      public Tablet::CompactionFaultHooks {
 public:
  explicit MyCommonHooks(TabletServerTest* test)
  : test_(test),
    iteration_(0) {}

  Status DoHook(uint32_t key, uint32_t new_int_val) {
    test_->UpdateTestRowRemote(0, key, new_int_val);
    return Status::OK();
  }

  // This should go in pre-flush and get flushed
  virtual Status PostSwapNewMemRowSet() OVERRIDE {
    return DoHook(1, 10 + iteration_);
  }
  // This should go in after the flush, but before
  // the duplicating row set, i.e., this should appear as
  // a missed delta.
  virtual Status PostTakeMvccSnapshot() OVERRIDE {
    return DoHook(2, 20 + iteration_);
  }
  // This too should appear as a missed delta.
  virtual Status PostWriteSnapshot() OVERRIDE {
    return DoHook(3, 30 + iteration_);
  }
  // This should appear as a duplicated mutation
  virtual Status PostSwapInDuplicatingRowSet() OVERRIDE {
    return DoHook(4, 40 + iteration_);
  }
  // This too should appear as a duplicated mutation
  virtual Status PostReupdateMissedDeltas() OVERRIDE {
    return DoHook(5, 50 + iteration_);
  }
  // This should go into the new delta.
  virtual Status PostSwapNewRowSet() OVERRIDE {
    return DoHook(6, 60 + iteration_);
  }
  // This should go in pre-flush (only on compactions)
  virtual Status PostSelectIterators() OVERRIDE {
    return DoHook(7, 70 + iteration_);
  }
  void increment_iteration() {
    iteration_++;
  }
 protected:
  TabletServerTest* test_;
  int iteration_;
};

// Tests performing mutations that are going to the initial MRS
// or to a DMS, when the MRS is flushed. This also tests that the
// log produced on recovery allows to re-recover the original state.
TEST_F(TabletServerTest, TestRecoveryWithMutationsWhileFlushing) {

  InsertTestRowsRemote(0, 1, 7);

  shared_ptr<MyCommonHooks> hooks(new MyCommonHooks(this));

  tablet_peer_->tablet()->SetFlushHooksForTests(hooks);
  tablet_peer_->tablet()->SetCompactionHooksForTests(hooks);
  tablet_peer_->tablet()->SetFlushCompactCommonHooksForTests(hooks);

  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());

  // Shutdown the tserver and try and rebuild the tablet from the log
  // produced on recovery (recovery flushed no state, but produced a new
  // log).
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 10))
                                            (KeyValue(2, 20))
                                            (KeyValue(3, 30))
                                            (KeyValue(4, 40))
                                            (KeyValue(5, 50))
                                            (KeyValue(6, 60))
                                            // the last hook only fires on compaction
                                            // so this isn't mutated
                                            (KeyValue(7, 7)));

  // Shutdown and rebuild again to test that the log generated during
  // the previous recovery allows to perform recovery again.
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 10))
                                            (KeyValue(2, 20))
                                            (KeyValue(3, 30))
                                            (KeyValue(4, 40))
                                            (KeyValue(5, 50))
                                            (KeyValue(6, 60))
                                            (KeyValue(7, 7)));
}

// Tests performing mutations that are going to a DMS or to the following
// DMS, when the initial one is flushed.
TEST_F(TabletServerTest, TestRecoveryWithMutationsWhileFlushingAndCompacting) {

  InsertTestRowsRemote(0, 1, 7);

  shared_ptr<MyCommonHooks> hooks(new MyCommonHooks(this));

  tablet_peer_->tablet()->SetFlushHooksForTests(hooks);
  tablet_peer_->tablet()->SetCompactionHooksForTests(hooks);
  tablet_peer_->tablet()->SetFlushCompactCommonHooksForTests(hooks);

  // flush the first time
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());

  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 10))
                                            (KeyValue(2, 20))
                                            (KeyValue(3, 30))
                                            (KeyValue(4, 40))
                                            (KeyValue(5, 50))
                                            (KeyValue(6, 60))
                                            (KeyValue(7, 7)));
  hooks->increment_iteration();

  // set the hooks on the new tablet
  tablet_peer_->tablet()->SetFlushHooksForTests(hooks);
  tablet_peer_->tablet()->SetCompactionHooksForTests(hooks);
  tablet_peer_->tablet()->SetFlushCompactCommonHooksForTests(hooks);

  // insert an additional row so that we can flush
  InsertTestRowsRemote(0, 8, 1);

  // flush an additional MRS so that we have two DiskRowSets and then compact
  // them making sure that mutations executed mid compaction are replayed as
  // expected
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 11))
                                            (KeyValue(2, 21))
                                            (KeyValue(3, 31))
                                            (KeyValue(4, 41))
                                            (KeyValue(5, 51))
                                            (KeyValue(6, 61))
                                            (KeyValue(7, 7))
                                            (KeyValue(8, 8)));

  hooks->increment_iteration();
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // get the clock's current timestamp
  Timestamp now_before = mini_server_->server()->clock()->Now();

  // Shutdown the tserver and try and rebuild the tablet from the log
  // produced on recovery (recovery flushed no state, but produced a new
  // log).
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 11))
                                            (KeyValue(2, 22))
                                            (KeyValue(3, 32))
                                            (KeyValue(4, 42))
                                            (KeyValue(5, 52))
                                            (KeyValue(6, 62))
                                            (KeyValue(7, 72))
                                            (KeyValue(8, 8)));

  // get the clock's timestamp after replay
  Timestamp now_after = mini_server_->server()->clock()->Now();

  // make sure 'now_after' is greater than or equal to 'now_before'
  ASSERT_GE(now_after.value(), now_before.value());
}

#define ANFF ASSERT_NO_FATAL_FAILURE

// Regression test for KUDU-176. Ensures that after a major delta compaction,
// restarting properly recovers the tablet.
TEST_F(TabletServerTest, TestKUDU_176_RecoveryAfterMajorDeltaCompaction) {

  // Flush a DRS with 1 rows.
  ASSERT_NO_FATAL_FAILURE(InsertTestRowsRemote(0, 1, 1));
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 1))));

  // Update it, flush deltas.
  ANFF(UpdateTestRowRemote(0, 1, 2));
  ASSERT_STATUS_OK(tablet_peer_->tablet()->FlushBiggestDMS());
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 2))));

  // Major compact deltas.
  {
    vector<shared_ptr<tablet::RowSet> > rsets;
    tablet_peer_->tablet()->GetRowSetsForTests(&rsets);
    ASSERT_STATUS_OK(tablet_peer_->tablet()->DoMajorDeltaCompaction(
                       boost::assign::list_of(1)(2),
                       rsets[0]));
  }

  // Verify that data is still the same.
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 2))));

  // Verify that data remains after a restart.
  ASSERT_STATUS_OK(ShutdownAndRebuildTablet());
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 2))));
}

// Regression test for KUDU-177. Ensures that after a major delta compaction,
// rows that were in the old DRS's DMS are properly replayed.
TEST_F(TabletServerTest, TestKUDU_177_RecoveryOfDMSEditsAfterMajorDeltaCompaction) {
  // Flush a DRS with 1 rows.
  ANFF(InsertTestRowsRemote(0, 1, 1));
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 1))));

  // Update it, flush deltas.
  ANFF(UpdateTestRowRemote(0, 1, 2));
  ASSERT_STATUS_OK(tablet_peer_->tablet()->FlushBiggestDMS());

  // Update it again, so this last update is in the DMS.
  ANFF(UpdateTestRowRemote(0, 1, 3));
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 3))));

  // Major compact deltas. This doesn't include the DMS, but the old
  // DMS should "move over" to the output of the delta compaction.
  {
    vector<shared_ptr<tablet::RowSet> > rsets;
    tablet_peer_->tablet()->GetRowSetsForTests(&rsets);
    ASSERT_STATUS_OK(tablet_peer_->tablet()->DoMajorDeltaCompaction(
                       boost::assign::list_of(1)(2),
                       rsets[0]));
  }
  // Verify that data is still the same.
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 3))));

  // Verify that the update remains after a restart.
  ASSERT_STATUS_OK(ShutdownAndRebuildTablet());
  ANFF(VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 3))));
}

TEST_F(TabletServerTest, TestClientGetsErrorBackWhenRecoveryFailed) {
  ANFF(InsertTestRowsRemote(0, 1, 7));

  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());

  // Save the log path before shutting down the tablet (and destroying
  // the tablet peer).
  string log_path = tablet_peer_->log()->ActiveSegmentPathForTests();
  ShutdownTablet();

  ASSERT_STATUS_OK(log::CorruptLogFile(env_.get(), log_path,
                                       log::FLIP_BYTE, 300));

  ASSERT_FALSE(ShutdownAndRebuildTablet().ok());

  // Connect to it.
  ASSERT_STATUS_OK(CreateClientProxies(mini_server_->bound_rpc_addr(),
                                       &proxy_, &admin_proxy_, &consensus_proxy_));

  WriteRequestPB req;
  req.set_tablet_id(kTabletId);

  WriteResponsePB resp;
  rpc::RpcController controller;

  // We're expecting the write to fail.
  ASSERT_STATUS_OK(DCHECK_NOTNULL(proxy_.get())->Write(req, &resp, &controller));
  ASSERT_EQ(TabletServerErrorPB::TABLET_NOT_RUNNING, resp.error().code());
  ASSERT_STR_CONTAINS(resp.error().status().message(), "Tablet not RUNNING: FAILED");
}

TEST_F(TabletServerTest, TestScan) {
  int num_rows = AllowSlowTests() ? 10000 : 1000;
  InsertTestRowsDirect(0, num_rows);

  ScanResponsePB resp;
  ASSERT_NO_FATAL_FAILURE(OpenScannerWithAllColumns(&resp));

  // Ensure that the scanner ID came back and got inserted into the
  // ScannerManager map.
  string scanner_id = resp.scanner_id();
  ASSERT_TRUE(!scanner_id.empty());
  {
    SharedScanner junk;
    ASSERT_TRUE(mini_server_->server()->scanner_manager()->LookupScanner(scanner_id, &junk));
  }

  // Drain all the rows from the scanner.
  vector<string> results;
  ASSERT_NO_FATAL_FAILURE(
    DrainScannerToStrings(resp.scanner_id(), schema_, &results));
  ASSERT_EQ(num_rows, results.size());

  KuduPartialRow row(&schema_);
  for (int i = 0; i < num_rows; i++) {
    BuildTestRow(i, &row);
    string expected = "(" + row.ToString() + ")";
    ASSERT_EQ(expected, results[i]);
  }

  // Since the rows are drained, the scanner should be automatically removed
  // from the scanner manager.
  {
    SharedScanner junk;
    ASSERT_FALSE(mini_server_->server()->scanner_manager()->LookupScanner(scanner_id, &junk));
  }
}

TEST_F(TabletServerTest, TestScannerOpenWhenServerShutsDown) {
  InsertTestRowsDirect(0, 1);

  ScanResponsePB resp;
  ASSERT_NO_FATAL_FAILURE(OpenScannerWithAllColumns(&resp));

  // Scanner is now open. The test will now shut down the TS with the scanner still
  // out there. Due to KUDU-161 this used to fail, since the scanner (and thus the MRS)
  // stayed open longer than the anchor registry
}

TEST_F(TabletServerTest, TestSnapshotScan) {
  int num_rows = AllowSlowTests() ? 1000 : 100;
  int num_batches = AllowSlowTests() ? 100 : 10;
  vector<uint64_t> write_timestamps_collector;

  // perform a series of writes and collect the timestamps
  InsertTestRowsRemote(0, 0, num_rows, num_batches, NULL, kTabletId, &write_timestamps_collector);

  // now perform snapshot scans.
  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  int batch_idx = 1;
  BOOST_FOREACH(uint64_t write_timestamp, write_timestamps_collector) {
    req.Clear();
    resp.Clear();
    rpc.Reset();
    // Set up a new request with no predicates, all columns.
    const Schema& projection = schema_;
    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(kTabletId);
    scan->set_read_mode(READ_AT_SNAPSHOT);

    // Decode and re-encode the timestamp. Note that a snapshot at 'write_timestamp'
    // does not include the written rows, so we increment that timestamp by one
    // to make sure we get those rows back
    Timestamp read_timestamp(write_timestamp);
    read_timestamp = Timestamp(read_timestamp.value() + 1);
    scan->set_snap_timestamp(read_timestamp.ToUint64());

    ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
    req.set_call_seq_id(0);

    // Send the call
    {
      SCOPED_TRACE(req.DebugString());
      req.set_batch_size_bytes(0); // so it won't return data right away
      ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());
    }

    ASSERT_TRUE(resp.has_more_results());
    // Drain all the rows from the scanner.
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(DrainScannerToStrings(resp.scanner_id(), schema_, &results));
    // on each scan we should get (num_rows / num_batches) * batch_idx rows back
    int expected_num_rows = (num_rows / num_batches) * batch_idx;
    ASSERT_EQ(expected_num_rows, results.size());

    if (VLOG_IS_ON(2)) {
      VLOG(2) << "Scanner: " << resp.scanner_id() << " performing a snapshot read at: "
              << read_timestamp.ToString() << " got back: ";
      BOOST_FOREACH(const string& result, results) {
        VLOG(2) << result;
      }
    }

    // assert that the first and last rows were the expected ones
    ASSERT_EQ("(uint32 key=0, uint32 int_val=0, string string_val=original0)", results[0]);
    ASSERT_EQ(Substitute("(uint32 key=$0, uint32 int_val=$0, string string_val=original$0)",
                         (batch_idx * (num_rows / num_batches) - 1)), results[results.size() - 1]);
    batch_idx++;
  }
}

TEST_F(TabletServerTest, TestSnapshotScan_WithoutSnapshotTimestamp) {
  vector<uint64_t> write_timestamps_collector;
  // perform a write
  InsertTestRowsRemote(0, 0, 1, 1, NULL, kTabletId, &write_timestamps_collector);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);
  req.set_batch_size_bytes(0); // so it won't return data right away
  scan->set_read_mode(READ_AT_SNAPSHOT);

  Timestamp now = mini_server_->server()->clock()->Now();

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    req.set_batch_size_bytes(0); // so it won't return data right away
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // make sure that the snapshot timestamp that was selected is >= now
  ASSERT_GE(resp.snap_timestamp(), now.ToUint64());
}

// Tests that a snapshot in the future (beyond what clock->Now() returns) fails as an
// invalid snapshot.
TEST_F(TabletServerTest, TestSnapshotScan_SnapshotInTheFutureFails) {
  vector<uint64_t> write_timestamps_collector;
  // perform a write
  InsertTestRowsRemote(0, 0, 1, 1, NULL, kTabletId, &write_timestamps_collector);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);
  req.set_batch_size_bytes(0); // so it won't return data right away
  scan->set_read_mode(READ_AT_SNAPSHOT);

  Timestamp read_timestamp(write_timestamps_collector[0]);
  // increment the write timestamp by 5 secs, the server will definitely consider
  // this in the future.
  read_timestamp = HybridClock::TimestampFromMicroseconds(
      HybridClock::GetPhysicalValueMicros(read_timestamp) + 5000000);
  scan->set_snap_timestamp(read_timestamp.ToUint64());

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::INVALID_SNAPSHOT, resp.error().code());
  }
}


// Test tserver shutdown with an active scanner open.
TEST_F(TabletServerTest, TestSnapshotScan_OpenScanner) {
  vector<uint64_t> write_timestamps_collector;
  // Write and flush and write, so we have some rows in MRS and DRS
  InsertTestRowsRemote(0, 0, 100, 2, NULL, kTabletId, &write_timestamps_collector);
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  InsertTestRowsRemote(0, 100, 100, 2, NULL, kTabletId, &write_timestamps_collector);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);
  req.set_batch_size_bytes(0);
  scan->set_read_mode(READ_AT_SNAPSHOT);

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }
  // Intentionally do not drain the scanner at the end, to leave it open.
  // This tests tablet server shutdown with an active scanner.
}


// Test retrying a snapshot scan using last_row.
TEST_F(TabletServerTest, TestSnapshotScan_LastRow) {
  const int num_rows = AllowSlowTests() ? 1000 : 100;
  const int num_batches = AllowSlowTests() ? 10 : 5;
  const int batch_size = num_rows / num_batches;

  // Generate some interleaved rows
  for (int i = 0; i < batch_size; i++) {
    ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
    for (int j = 0; j < num_rows; j++) {
      if (j % batch_size == i) {
        InsertTestRowsDirect(j, 1);
      }
    }
  }

  // Remove all the key columns from the projection.
  // This makes sure the scanner adds them in for sorting but removes them before returning
  // to the client.
  SchemaBuilder sb(schema_);
  for (int i = 0; i < schema_.num_key_columns(); i++) {
    sb.RemoveColumn(schema_.column(i).name());
  }
  const Schema& projection = sb.BuildWithoutIds();

  // Scan the whole tablet with a few different batch sizes.
  for (int i = 1; i < 10000; i *= 2) {
    ScanResponsePB resp;
    ScanRequestPB req;
    RpcController rpc;

    // Set up a new snapshot scan without a specified timestamp.
    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
    req.set_call_seq_id(0);
    scan->set_read_mode(READ_AT_SNAPSHOT);
    scan->set_order_by_primary_key(true);

    // Send the call
    {
      SCOPED_TRACE(req.DebugString());
      req.set_batch_size_bytes(0); // so it won't return data right away
      ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());
    }

    vector<string> results;
    do {
      rpc.Reset();
      // Send the call.
      {
        SCOPED_TRACE(req.DebugString());
        req.set_batch_size_bytes(i);
        ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
        SCOPED_TRACE(resp.DebugString());
        ASSERT_FALSE(resp.has_error());
      }
      // Save the rows into 'results' vector.
      StringifyRowsFromResponse(projection, rpc, resp, &results);
      // Retry the scan, setting the last_row_key and snapshot based on the response.
      scan->set_encoded_last_row_key(resp.encoded_last_row_key());
      scan->set_snap_timestamp(resp.snap_timestamp());
    } while (resp.has_more_results());

    ASSERT_EQ(num_rows, results.size());

    // Verify that we get the rows back in order.
    KuduPartialRow row(&projection);
    for (int j = 0; j < num_rows; j++) {
      ASSERT_OK(row.SetUInt32(0, j * 2));
      ASSERT_OK(row.SetStringCopy(1, StringPrintf("hello %d", j)));
      string expected = "(" + row.ToString() + ")";
      ASSERT_EQ(expected, results[j]);
    }
  }
}


// Tests that a read in the future succeeds if a propagated_timestamp (that is even
// further in the future) follows along. Also tests that the clock was updated so
// that no writes will ever have a timestamp post this snapshot.
TEST_F(TabletServerTest, TestSnapshotScan_SnapshotInTheFutureWithPropagatedTimestamp) {
  vector<uint64_t> write_timestamps_collector;
  // perform a write
  InsertTestRowsRemote(0, 0, 1, 1, NULL, kTabletId, &write_timestamps_collector);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);
  req.set_batch_size_bytes(0); // so it won't return data right away
  scan->set_read_mode(READ_AT_SNAPSHOT);

  Timestamp read_timestamp(write_timestamps_collector[0]);
  // increment the write timestamp by 5 secs, the server will definitely consider
  // this in the future.
  read_timestamp = HybridClock::TimestampFromMicroseconds(
      HybridClock::GetPhysicalValueMicros(read_timestamp) + 5000000);
  scan->set_snap_timestamp(read_timestamp.ToUint64());

  // send a propagated timestamp that is an additional 100 msecs into the future.
  Timestamp propagated_timestamp = HybridClock::TimestampFromMicroseconds(
      HybridClock::GetPhysicalValueMicros(read_timestamp) + 100000);
  scan->set_propagated_timestamp(propagated_timestamp.ToUint64());

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // make sure the server's current clock returns a value that is larger than the
  // propagated timestamp. It should have the same physical time, but higher
  // logical time (due to various calls to clock.Now() when processing the request).
  Timestamp now = mini_server_->server()->clock()->Now();

  ASSERT_EQ(HybridClock::GetPhysicalValueMicros(propagated_timestamp),
            HybridClock::GetPhysicalValueMicros(now));

  ASSERT_GT(HybridClock::GetLogicalValue(now),
            HybridClock::GetLogicalValue(propagated_timestamp));

  vector<string> results;
  ASSERT_NO_FATAL_FAILURE(DrainScannerToStrings(resp.scanner_id(), schema_, &results));
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("(uint32 key=0, uint32 int_val=0, string string_val=original0)", results[0]);
}


// Test that a read in the future fails, even if a propagated_timestamp is sent along,
// if the read_timestamp is beyond the propagated_timestamp.
TEST_F(TabletServerTest, TestSnapshotScan__SnapshotInTheFutureBeyondPropagatedTimestampFails) {
  vector<uint64_t> write_timestamps_collector;
  // perform a write
  InsertTestRowsRemote(0, 0, 1, 1, NULL, kTabletId, &write_timestamps_collector);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);
  req.set_batch_size_bytes(0); // so it won't return data right away
  scan->set_read_mode(READ_AT_SNAPSHOT);

  Timestamp read_timestamp(write_timestamps_collector[0]);
  // increment the write timestamp by 5 secs, the server will definitely consider
  // this in the future.
  read_timestamp = HybridClock::TimestampFromMicroseconds(
      HybridClock::GetPhysicalValueMicros(read_timestamp) + 5000000);
  scan->set_snap_timestamp(read_timestamp.ToUint64());

  // send a propagated timestamp that is an less than the read timestamp (but still
  // in the future as far the server is concerned).
  Timestamp propagated_timestamp = HybridClock::TimestampFromMicroseconds(
      HybridClock::GetPhysicalValueMicros(read_timestamp) - 100000);
  scan->set_propagated_timestamp(propagated_timestamp.ToUint64());

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::INVALID_SNAPSHOT, resp.error().code());
  }
}

TEST_F(TabletServerTest, TestScanWithStringPredicates) {
  InsertTestRowsDirect(0, 100);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  req.set_batch_size_bytes(0); // so it won't return data right away
  ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

  // Set up a range predicate: "hello 50" < string_val <= "hello 59"
  ColumnRangePredicatePB* pred = scan->add_range_predicates();
  pred->mutable_column()->CopyFrom(scan->projected_columns(2));

  pred->set_lower_bound("hello 50");
  pred->set_upper_bound("hello 59");

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // Drain all the rows from the scanner.
  vector<string> results;
  ASSERT_NO_FATAL_FAILURE(
    DrainScannerToStrings(resp.scanner_id(), schema_, &results));
  ASSERT_EQ(10, results.size());
  ASSERT_EQ("(uint32 key=50, uint32 int_val=100, string string_val=hello 50)", results[0]);
  ASSERT_EQ("(uint32 key=59, uint32 int_val=118, string string_val=hello 59)", results[9]);
}

TEST_F(TabletServerTest, TestScanWithPredicates) {
  // TODO: need to test adding a predicate on a column which isn't part of the
  // projection! I don't think we implemented this at the tablet layer yet,
  // but should do so.

  int num_rows = AllowSlowTests() ? 10000 : 1000;
  InsertTestRowsDirect(0, num_rows);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  req.set_batch_size_bytes(0); // so it won't return data right away
  ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

  // Set up a range predicate: 51 <= key <= 100
  ColumnRangePredicatePB* pred = scan->add_range_predicates();
  pred->mutable_column()->CopyFrom(scan->projected_columns(0));

  uint32_t lower_bound_int = 51;
  uint32_t upper_bound_int = 100;
  pred->mutable_lower_bound()->append(reinterpret_cast<char*>(&lower_bound_int),
                                      sizeof(lower_bound_int));
  pred->mutable_upper_bound()->append(reinterpret_cast<char*>(&upper_bound_int),
                                      sizeof(upper_bound_int));

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // Drain all the rows from the scanner.
  vector<string> results;
  ASSERT_NO_FATAL_FAILURE(
    DrainScannerToStrings(resp.scanner_id(), schema_, &results));
  ASSERT_EQ(50, results.size());
}

TEST_F(TabletServerTest, TestScanWithEncodedPredicates) {
  InsertTestRowsDirect(0, 100);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  req.set_batch_size_bytes(0); // so it won't return data right away
  ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

  // Set up a range predicate: 51 <= key <= 60
  // using encoded keys
  uint32_t start_key_int = 51;
  uint32_t stop_key_int = 60;
  EncodedKeyBuilder ekb(&schema_);
  ekb.AddColumnKey(&start_key_int);
  gscoped_ptr<EncodedKey> start_encoded(ekb.BuildEncodedKey());

  ekb.Reset();
  ekb.AddColumnKey(&stop_key_int);
  gscoped_ptr<EncodedKey> stop_encoded(ekb.BuildEncodedKey());

  scan->mutable_encoded_start_key()->assign(
    reinterpret_cast<const char*>(start_encoded->encoded_key().data()),
    start_encoded->encoded_key().size());
  scan->mutable_encoded_stop_key()->assign(
    reinterpret_cast<const char*>(stop_encoded->encoded_key().data()),
    stop_encoded->encoded_key().size());

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // Drain all the rows from the scanner.
  vector<string> results;
  ASSERT_NO_FATAL_FAILURE(
    DrainScannerToStrings(resp.scanner_id(), schema_, &results));
  ASSERT_EQ(10, results.size());
  EXPECT_EQ("(uint32 key=51, uint32 int_val=102, string string_val=hello 51)",
            results.front());
  EXPECT_EQ("(uint32 key=60, uint32 int_val=120, string string_val=hello 60)",
            results.back());
}


// Test requesting more rows from a scanner which doesn't exist
TEST_F(TabletServerTest, TestBadScannerID) {
  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  req.set_scanner_id("does-not-exist");

  SCOPED_TRACE(req.DebugString());
  ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
  SCOPED_TRACE(resp.DebugString());
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(TabletServerErrorPB::SCANNER_EXPIRED, resp.error().code());
}

// Test passing a scanner ID, but also filling in some of the NewScanRequest
// field.
TEST_F(TabletServerTest, TestInvalidScanRequest_NewScanAndScannerID) {
  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  req.set_batch_size_bytes(0); // so it won't return data right away
  req.set_scanner_id("x");
  SCOPED_TRACE(req.DebugString());
  Status s = proxy_->Scan(req, &resp, &rpc);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Must not pass both a scanner_id and new_scan_request");
}


// Test that passing a projection with fields not present in the tablet schema
// throws an exception.
TEST_F(TabletServerTest, TestInvalidScanRequest_BadProjection) {
  const Schema projection(boost::assign::list_of
                          (ColumnSchema("col_doesnt_exist", UINT32)),
                          0);
  VerifyScanRequestFailure(projection,
                           TabletServerErrorPB::MISMATCHED_SCHEMA,
                           "Some columns are not present in the current schema: col_doesnt_exist");
}

// Test that passing a projection with mismatched type/nullability throws an exception.
TEST_F(TabletServerTest, TestInvalidScanRequest_BadProjectionTypes) {
  Schema projection;

  // Verify mismatched nullability for the not-null int field
  ASSERT_STATUS_OK(
    projection.Reset(boost::assign::list_of
      (ColumnSchema("int_val", UINT32, true)),     // should be NOT NULL
      0));
  VerifyScanRequestFailure(projection,
                           TabletServerErrorPB::MISMATCHED_SCHEMA,
                           "The column 'int_val' must have type uint32 NOT "
                           "NULL found uint32 NULLABLE");

  // Verify mismatched nullability for the nullable string field
  ASSERT_STATUS_OK(
    projection.Reset(boost::assign::list_of
      (ColumnSchema("string_val", STRING, false)), // should be NULLABLE
      0));
  VerifyScanRequestFailure(projection,
                           TabletServerErrorPB::MISMATCHED_SCHEMA,
                           "The column 'string_val' must have type string "
                           "NULLABLE found string NOT NULL");

  // Verify mismatched type for the not-null int field
  ASSERT_STATUS_OK(
    projection.Reset(boost::assign::list_of
      (ColumnSchema("int_val", UINT16, false)),     // should be UINT32 NOT NULL
      0));
  VerifyScanRequestFailure(projection,
                           TabletServerErrorPB::MISMATCHED_SCHEMA,
                           "The column 'int_val' must have type uint32 NOT "
                           "NULL found uint16 NOT NULL");

  // Verify mismatched type for the nullable string field
  ASSERT_STATUS_OK(
    projection.Reset(boost::assign::list_of
      (ColumnSchema("string_val", UINT32, true)), // should be STRING NULLABLE
      0));
  VerifyScanRequestFailure(projection,
                           TabletServerErrorPB::MISMATCHED_SCHEMA,
                           "The column 'string_val' must have type string "
                           "NULLABLE found uint32 NULLABLE");
}

// Test that passing a projection with Column IDs throws an exception.
// Column IDs are assigned to the user request schema on the tablet server
// based on the latest schema.
TEST_F(TabletServerTest, TestInvalidScanRequest_WithIds) {
  shared_ptr<Schema> projection(tablet_peer_->tablet()->schema());
  ASSERT_TRUE(projection->has_column_ids());
  VerifyScanRequestFailure(*projection.get(),
                           TabletServerErrorPB::INVALID_SCHEMA,
                           "User requests should not have Column IDs");
}

// Test scanning a tablet that has no entries.
TEST_F(TabletServerTest, TestScan_NoResults) {
  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  req.set_batch_size_bytes(0); // so it won't return data right away
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());

    // Because there are no entries, we should immediately return "no results".
    ASSERT_FALSE(resp.has_more_results());
  }
}

TEST_F(TabletServerTest, TestAlterSchema) {
  AlterSchemaRequestPB req;
  AlterSchemaResponsePB resp;
  RpcController rpc;

  InsertTestRowsRemote(0, 0, 2);

  // Add one column with a default value
  const uint32_t c2_write_default = 5;
  const uint32_t c2_read_default = 7;
  SchemaBuilder builder(schema_);
  ASSERT_STATUS_OK(builder.AddColumn("c2", UINT32, false, &c2_read_default, &c2_write_default));
  Schema s2 = builder.Build();

  req.set_tablet_id(kTabletId);
  req.set_schema_version(1);
  ASSERT_STATUS_OK(SchemaToPB(s2, req.mutable_schema()));

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(admin_proxy_->AlterSchema(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  {
    InsertTestRowsRemote(0, 2, 2);
    scoped_refptr<TabletPeer> tablet;
    ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));
    ASSERT_STATUS_OK(tablet->tablet()->Flush());
  }

  const Schema projection(boost::assign::list_of
                          (ColumnSchema("key", UINT32))
                          (ColumnSchema("c2", UINT32)),
                          1);

  // Try recovering from the original log
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(projection, boost::assign::list_of(KeyValue(0, 7))
                                               (KeyValue(1, 7))
                                               (KeyValue(2, 5))
                                               (KeyValue(3, 5)));

  // Try recovering from the log generated on recovery
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(projection, boost::assign::list_of(KeyValue(0, 7))
                                               (KeyValue(1, 7))
                                               (KeyValue(2, 5))
                                               (KeyValue(3, 5)));
}

TEST_F(TabletServerTest, TestCreateTablet_TabletExists) {
  CreateTabletRequestPB req;
  CreateTabletResponsePB resp;
  RpcController rpc;

  req.set_table_id("testtb");
  req.set_tablet_id(kTabletId);
  req.set_start_key(" ");
  req.set_end_key(" ");
  req.set_table_name("testtb");
  req.mutable_quorum()->CopyFrom(mini_server_->CreateLocalQuorum());
  ASSERT_STATUS_OK(SchemaToPB(SchemaBuilder(schema_).Build(),
                              req.mutable_schema()));

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(admin_proxy_->CreateTablet(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::TABLET_ALREADY_EXISTS, resp.error().code());
  }
}

TEST_F(TabletServerTest, TestDeleteTablet) {
  scoped_refptr<TabletPeer> tablet;

  // Verify that the tablet exists
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));

  // Put some data in the tablet. We flush and insert more rows to ensure that
  // there is data both in the MRS and on disk.
  ASSERT_NO_FATAL_FAILURE(InsertTestRowsRemote(0, 1, 1));
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  ASSERT_NO_FATAL_FAILURE(InsertTestRowsRemote(0, 2, 1));

  // Drop any local references to the tablet from within this test,
  // so that when we delete it on the server, it's not held alive
  // by the test code.
  tablet_peer_.reset();
  tablet.reset();

  DeleteTabletRequestPB req;
  DeleteTabletResponsePB resp;
  RpcController rpc;

  req.set_tablet_id(kTabletId);

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(admin_proxy_->DeleteTablet(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // Verify that the tablet is removed from the tablet map
  ASSERT_FALSE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));

  // Verify that fetching metrics doesn't crash. Regression test for KUDU-638.
  EasyCurl c;
  faststring buf;
  ASSERT_STATUS_OK(c.FetchURL(strings::Substitute(
                                "http://$0/jsonmetricz",
                                mini_server_->bound_http_addr().ToString()),
                              &buf));

  // TODO: Verify that the data was trashed
}

TEST_F(TabletServerTest, TestDeleteTablet_TabletNotCreated) {
  DeleteTabletRequestPB req;
  DeleteTabletResponsePB resp;
  RpcController rpc;

  req.set_tablet_id("NotPresentTabletId");

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(admin_proxy_->DeleteTablet(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::TABLET_NOT_FOUND, resp.error().code());
  }
}

// Test that with concurrent requests to delete the same tablet, one wins and
// the other fails, with no assertion failures. Regression test for KUDU-345.
TEST_F(TabletServerTest, TestConcurrentDeleteTablet) {
  // Verify that the tablet exists
  scoped_refptr<TabletPeer> tablet;
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));

  static const int kNumDeletes = 2;
  RpcController rpcs[kNumDeletes];
  DeleteTabletResponsePB responses[kNumDeletes];
  CountDownLatch latch(kNumDeletes);

  DeleteTabletRequestPB req;
  req.set_tablet_id(kTabletId);

  for (int i = 0; i < kNumDeletes; i++) {
    SCOPED_TRACE(req.DebugString());
    admin_proxy_->DeleteTabletAsync(req, &responses[i], &rpcs[i],
                                    boost::bind(&CountDownLatch::CountDown, &latch));
  }
  latch.Wait();

  int num_success = 0;
  for (int i = 0; i < kNumDeletes; i++) {
    ASSERT_TRUE(rpcs[i].finished());
    LOG(INFO) << "STATUS " << i << ": " << rpcs[i].status().ToString();
    LOG(INFO) << "RESPONSE " << i << ": " << responses[i].DebugString();
    if (!responses[i].has_error()) {
      num_success++;
    }
  }

  // Verify that the tablet is removed from the tablet map
  ASSERT_FALSE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));
  ASSERT_EQ(1, num_success);
}

TEST_F(TabletServerTest, TestChangeConfiguration) {
  ChangeConfigRequestPB req;
  ChangeConfigResponsePB resp;
  RpcController rpc;

  req.set_tablet_id(kTabletId);

  QuorumPB* new_quorum = req.mutable_new_config();
  new_quorum->set_local(true);
  new_quorum->clear_opid_index();
  QuorumPeerPB* peer = new_quorum->add_peers();
  peer->set_permanent_uuid(mini_server_->server()->instance_pb().permanent_uuid());

  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(consensus_proxy_->ChangeConfig(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    rpc.Reset();
  }

  // Now try and insert some rows, and shutdown and rebuild
  // the TS so that we know that the tablet survives.
  InsertTestRowsRemote(0, 1, 7);

  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(schema_, boost::assign::list_of(KeyValue(1, 1))
                                            (KeyValue(2, 2))
                                            (KeyValue(3, 3))
                                            (KeyValue(4, 4))
                                            (KeyValue(5, 5))
                                            (KeyValue(6, 6))
                                            (KeyValue(7, 7)));
}

namespace {

// Return a random valid value for QuorumPeerPB::Role.
QuorumPeerPB::Role RandomRole() {
  const google::protobuf::EnumDescriptor* desc = metadata::QuorumPeerPB_Role_descriptor();
  int idx = rand() % desc->value_count();
  return static_cast<QuorumPeerPB::Role>(desc->value(idx)->number());
}

} // anonymous namespace

// Test that when a change config. transaction changes the tablet state, such as the
// quorum role, the tablet manager gets notified and includes that information in the next
// tablet report.
TEST_F(TabletServerTest, TestChangeConfiguration_TsTabletManagerReportsNewRoles) {
  ChangeConfigRequestPB req;
  ChangeConfigResponsePB resp;
  RpcController rpc;

  req.set_tablet_id(kTabletId);

  QuorumPB* new_quorum = req.mutable_new_config();
  new_quorum->set_local(true);
  new_quorum->clear_opid_index();
  QuorumPeerPB* peer = new_quorum->add_peers();
  peer->set_permanent_uuid(mini_server_->server()->instance_pb().permanent_uuid());
  SeedRandom();

  // loop and send multiple config. change requests where we change the
  // role of the peer. TSTabletManager should acknowledge the role changes.
  for (int i = 0; i < 10; i++) {
    SCOPED_TRACE(Substitute("Iter: $0", i));
    QuorumPeerPB::Role random_role = RandomRole();
    peer->set_role(random_role);
    SCOPED_TRACE("Request: " + req.ShortDebugString());
    ASSERT_STATUS_OK(consensus_proxy_->ChangeConfig(req, &resp, &rpc));
    SCOPED_TRACE("Response: " + resp.ShortDebugString());
    ASSERT_FALSE(resp.has_error());
    rpc.Reset();
    // Now check that the tablet report reports the correct role
    kudu::master::TabletReportPB report;
    mini_server_->server()->tablet_manager()->GenerateIncrementalTabletReport(&report);
    ASSERT_EQ(report.updated_tablets_size(), 1) << report.ShortDebugString();
    kudu::master::ReportedTabletPB tablet_report = report.updated_tablets(0);
    ASSERT_EQ(tablet_report.role(), random_role)
      << "Tablet report: " << report.ShortDebugString() << "; "
      << "Random role: " << QuorumPeerPB::Role_Name(random_role);
  }
}

TEST_F(TabletServerTest, TestInsertLatencyMicroBenchmark) {
  HistogramPrototype hist_proto("insert-latency",
                                MetricUnit::kMicroseconds,
                                "TabletServer single threaded insert latency.",
                                10000000,
                                2);

  scoped_refptr<Histogram> histogram = hist_proto.Instantiate(ts_test_metric_context_);

  uint64_t warmup = AllowSlowTests() ?
      FLAGS_single_threaded_insert_latency_bench_warmup_rows : 10;

  for (int i = 0; i < warmup; i++) {
    InsertTestRowsRemote(0, i, 1);
  }

  uint64_t max_rows = AllowSlowTests() ?
      FLAGS_single_threaded_insert_latency_bench_insert_rows : 100;

  MonoTime start = MonoTime::Now(MonoTime::FINE);

  for (int i = warmup; i < warmup + max_rows; i++) {
    MonoTime before = MonoTime::Now(MonoTime::FINE);
    InsertTestRowsRemote(0, i, 1);
    MonoTime after = MonoTime::Now(MonoTime::FINE);
    MonoDelta delta = after.GetDeltaSince(before);
    histogram->Increment(delta.ToMicroseconds());
  }

  MonoTime end = MonoTime::Now(MonoTime::FINE);
  double throughput = ((max_rows - warmup) * 1.0) / end.GetDeltaSince(start).ToSeconds();

  // Generate the JSON.
  std::stringstream out;
  JsonWriter writer(&out);
  ASSERT_STATUS_OK(histogram->WriteAsJson("ts-insert-latency", &writer, NORMAL));

  LOG(INFO) << "Throughput: " << throughput << " rows/sec.";
  LOG(INFO) << out.str();
}

// Simple test to ensure we can destroy an RpcServer in different states of
// initialization before Start()ing it.
TEST_F(TabletServerTest, TestRpcServerCreateDestroy) {
  RpcServerOptions opts;
  {
    RpcServer server1(opts);
  }
  {
    RpcServer server2(opts);
    MessengerBuilder mb("foo");
    shared_ptr<Messenger> messenger;
    ASSERT_STATUS_OK(mb.Build(&messenger));
    ASSERT_STATUS_OK(server2.Init(messenger));
  }
}

TEST_F(TabletServerTest, TestWriteOutOfBounds) {
  const char *tabletId = "TestWriteOutOfBoundsTablet";
  EncodedKeyBuilder key_builder(&schema_);
  int val = 10;
  key_builder.AddColumnKey(&val);
  gscoped_ptr<EncodedKey> start(key_builder.BuildEncodedKey());
  val = 20;
  key_builder.Reset();
  key_builder.AddColumnKey(&val);
  gscoped_ptr<EncodedKey> end(key_builder.BuildEncodedKey());
  ASSERT_STATUS_OK(mini_server_->server()->tablet_manager()->CreateNewTablet(
      "TestWriteOutOfBoundsTable", tabletId,
      start->encoded_key().ToString(),
      end->encoded_key().ToString(),
      tabletId, SchemaBuilder(schema_).Build(),
      mini_server_->CreateLocalQuorum(), NULL));

  ASSERT_STATUS_OK(WaitForTabletRunning(tabletId));

  WriteRequestPB req;
  WriteResponsePB resp;
  RpcController controller;
  req.set_tablet_id(tabletId);
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));
  const google::protobuf::EnumDescriptor* op_type = RowOperationsPB_Type_descriptor();
  for (int i = 0; i < op_type->value_count() - 1; i++) { // INSERT and UPDATE
    RowOperationsPB* data = req.mutable_row_operations();
    AddTestRowToPB(static_cast<RowOperationsPB_Type>(op_type->value(i)->number()),
                   schema_, val, 1, "1", data);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());

    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, resp.error().code());
    Status s = StatusFromPB(resp.error().status());
    EXPECT_TRUE(s.IsNotFound());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Not found: Row not within tablet range");
    data->Clear();
    controller.Reset();
  }
}

static uint32_t CalcTestRowChecksum(int32_t key, uint8_t string_field_defined = true) {
  crc::Crc* crc = crc::GetCrc32cInstance();
  uint64_t row_crc = 0;

  string strval = strings::Substitute("original$0", key);
  uint32_t index = 0;
  crc->Compute(&index, sizeof(index), &row_crc, NULL);
  crc->Compute(&key, sizeof(int32_t), &row_crc, NULL);

  index = 1;
  crc->Compute(&index, sizeof(index), &row_crc, NULL);
  crc->Compute(&key, sizeof(int32_t), &row_crc, NULL);

  index = 2;
  crc->Compute(&index, sizeof(index), &row_crc, NULL);
  crc->Compute(&string_field_defined, sizeof(string_field_defined), &row_crc, NULL);
  if (string_field_defined) {
    crc->Compute(strval.c_str(), strval.size(), &row_crc, NULL);
  }
  return static_cast<uint32_t>(row_crc);
}

// Simple test to check that our checksum scans work as expected.
TEST_F(TabletServerTest, TestChecksumScan) {
  uint64_t total_crc = 0;

  ChecksumRequestPB req;
  req.mutable_new_request()->set_tablet_id(kTabletId);
  req.mutable_new_request()->set_read_mode(READ_LATEST);
  ASSERT_OK(SchemaToColumnPBsWithoutIds(schema_,
                                        req.mutable_new_request()->mutable_projected_columns()));
  ChecksumRequestPB new_req = req;  // Cache "new" request.

  ChecksumResponsePB resp;
  RpcController controller;
  ASSERT_STATUS_OK(proxy_->Checksum(req, &resp, &controller));

  // No rows.
  ASSERT_EQ(total_crc, resp.checksum());
  ASSERT_FALSE(resp.has_more_results());

  // First row.
  int32_t key = 1;
  InsertTestRowsRemote(0, key, 1);
  controller.Reset();
  ASSERT_STATUS_OK(proxy_->Checksum(req, &resp, &controller));
  total_crc += CalcTestRowChecksum(key);
  uint64_t first_crc = total_crc; // Cache first record checksum.

  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(total_crc, resp.checksum());
  ASSERT_FALSE(resp.has_more_results());

  // Second row (null string field).
  key = 2;
  InsertTestRowsRemote(0, key, 1, 1, NULL, kTabletId, NULL, NULL, false);
  controller.Reset();
  ASSERT_STATUS_OK(proxy_->Checksum(req, &resp, &controller));
  total_crc += CalcTestRowChecksum(key, false);

  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(total_crc, resp.checksum());
  ASSERT_FALSE(resp.has_more_results());

  // Now test the same thing, but with a scan requiring 2 passes (one per row).
  FLAGS_tablet_server_scan_batch_size_rows = 1;
  req.set_batch_size_bytes(1);
  controller.Reset();
  ASSERT_STATUS_OK(proxy_->Checksum(req, &resp, &controller));
  string scanner_id = resp.scanner_id();
  ASSERT_TRUE(resp.has_more_results());
  uint64_t agg_checksum = resp.checksum();

  // Second row.
  req.clear_new_request();
  req.mutable_continue_request()->set_scanner_id(scanner_id);
  req.mutable_continue_request()->set_previous_checksum(agg_checksum);
  controller.Reset();
  ASSERT_STATUS_OK(proxy_->Checksum(req, &resp, &controller));
  ASSERT_EQ(total_crc, resp.checksum());
  ASSERT_FALSE(resp.has_more_results());

  // Finally, delete row 2, so we're back to the row 1 checksum.
  ASSERT_NO_FATAL_FAILURE(DeleteTestRowsRemote(key, 1));
  FLAGS_tablet_server_scan_batch_size_rows = 100;
  req = new_req;
  controller.Reset();
  ASSERT_STATUS_OK(proxy_->Checksum(req, &resp, &controller));
  ASSERT_NE(total_crc, resp.checksum());
  ASSERT_EQ(first_crc, resp.checksum());
  ASSERT_FALSE(resp.has_more_results());
}

class DelayFsyncLogHook : public log::Log::LogFaultHooks {
 public:
  DelayFsyncLogHook() : log_latch1_(1), test_latch1_(1) {}

  virtual Status PostAppend() {
    test_latch1_.CountDown();
    log_latch1_.Wait();
    log_latch1_.Reset(1);
    return Status::OK();
  }

  void Continue() {
    test_latch1_.Wait();
    log_latch1_.CountDown();
  }

 private:
  CountDownLatch log_latch1_;
  CountDownLatch test_latch1_;
};

namespace {

void DeleteOneRowAsync(TabletServerTest* test) {
  test->DeleteTestRowsRemote(10, 1);
}

void CompactAsync(Tablet* tablet, CountDownLatch* flush_done_latch) {
  CHECK_OK(tablet->Compact(Tablet::FORCE_COMPACT_ALL));
  flush_done_latch->CountDown();
}

} // namespace

// Tests that in flight transactions are committed and that commit messages
// are durable before a compaction is allowed to flush the tablet metadata.
//
// This test is in preparation for KUDU-120 and should pass before and after
// it, but was also confirmed to fail if the pre-conditions it tests for
// fail. That is if KUDU-120 is implemented without these pre-requisites
// this test is confirmed to fail.
TEST_F(TabletServerTest, TestKudu120PreRequisites) {

  // Insert a few rows...
  InsertTestRowsRemote(0, 0, 10);
  // ... now flush ...
  ASSERT_OK(tablet_peer_->tablet()->Flush());
  // ... insert a few rows...
  InsertTestRowsRemote(0, 10, 10);
  // ... and flush again so that we have two disk row sets.
  ASSERT_OK(tablet_peer_->tablet()->Flush());

  // Add a hook so that we can make the log wait right after an append
  // (before the callback is triggered).
  log::Log* log = tablet_peer_->log();
  shared_ptr<DelayFsyncLogHook> log_hook(new DelayFsyncLogHook);
  log->SetLogFaultHooksForTests(log_hook);

  // Now start a transaction (delete) and stop just before commit.
  scoped_refptr<kudu::Thread> thread1;
  CHECK_OK(kudu::Thread::Create("DeleteThread", "DeleteThread",
                                DeleteOneRowAsync, this, &thread1));

  // Wait for the replicate message to arrive and continue.
  log_hook->Continue();
  // Wait a few msecs to make sure that the transaction is
  // trying to commit.
  usleep(100* 1000); // 100 msecs

  // Now start a compaction before letting the commit message go through.
  scoped_refptr<kudu::Thread> flush_thread;
  CountDownLatch flush_done_latch(1);
  CHECK_OK(kudu::Thread::Create("CompactThread", "CompactThread",
                                CompactAsync,
                                tablet_peer_->tablet(),
                                &flush_done_latch,
                                &flush_thread));

  // At this point we have both a compaction and a transaction going on.
  // If we allow the transaction to return before the commit message is
  // durable (KUDU-120) that means that the mvcc transaction will no longer
  // be in flight at this moment, nonetheless since we're blocking the WAL
  // and not allowing the commit message to go through, the compaction should
  // be forced to wait.
  //
  // We are thus testing two conditions:
  // - That in-flight transactions are committed.
  // - That commit messages for transactions that were in flight are durable.
  //
  // If these pre-conditions are not met, i.e. if the compaction is not forced
  // to wait here for the conditions to be true, then the below assertion
  // will fail, since the transaction's commit write callback will only
  // return when we allow it (in log_hook->Continue());
  CHECK(!flush_done_latch.WaitFor(MonoDelta::FromMilliseconds(300)));

  // Now let the rest go through.
  log_hook->Continue();
  log_hook->Continue();
  flush_done_latch.Wait();
}

} // namespace tserver
} // namespace kudu
