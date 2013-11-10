// Copyright (c) 2013, Cloudera, inc.
#include "tserver/tablet_server-test-base.h"

#include "util/curl_util.h"

using std::string;
using std::tr1::shared_ptr;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;

// Declare these metrics prototypes for simpler unit testing of their behavior.
METRIC_DECLARE_counter(rows_inserted);
METRIC_DECLARE_counter(rows_updated);

namespace kudu {
namespace tserver {

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

  // Tablet page should include the schema.
  ASSERT_STATUS_OK(c.FetchURL(strings::Substitute("http://$0/tablet?id=$1", addr, kTabletId),
                              &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), "<th>key</th>");
  ASSERT_STR_CONTAINS(buf.ToString(), "<td>string NOT NULL</td>");
}

TEST_F(TabletServerTest, TestInsert) {
  WriteRequestPB req;

  req.set_tablet_id(kTabletId);

  WriteResponsePB resp;
  RpcController controller;

  shared_ptr<TabletPeer> tablet;
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));
  Counter* rows_inserted = FindOrCreateCounter(*tablet->tablet()->GetMetricContextForTests(),
      METRIC_rows_inserted);
  ASSERT_EQ(0, rows_inserted->value());

  // Send a bad insert which has an empty schema. This should result
  // in an error.
  {
    RowwiseRowBlockPB* data = req.mutable_to_insert_rows();
    // Fill in an empty "rows" structure.
    data->mutable_rows();
    data->set_num_key_columns(0);
    data->set_num_rows(0);

    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::MISMATCHED_SCHEMA, resp.error().code());
    Status s = StatusFromPB(resp.error().status());
    EXPECT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Mismatched key projection schema, expected: Schema "
                        "[key[uint32 NOT NULL]]");
    req.clear_to_insert_rows();
  }

  // Send an empty insert with the correct schema.
  // This should succeed and do nothing.
  {
    controller.Reset();
    RowwiseRowBlockPB* data = req.mutable_to_insert_rows();
    data->Clear();
    data->mutable_rows(); // Set empty rows data.
    data->set_num_rows(0);

    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    req.clear_to_insert_rows();
  }

  // Send an actual row insert.
  {
    controller.Reset();
    RowwiseRowBlockPB* data = req.mutable_to_insert_rows();
    data->Clear();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    AddTestRowToBlockPB(schema_, 1234, 5678, "hello world via RPC", data);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    req.clear_to_insert_rows();
    ASSERT_EQ(1, rows_inserted->value());
  }

  // Send a batch with multiple rows, one of which is a duplicate of
  // the above insert. This should generate one error into per_row_errors.
  {
    controller.Reset();
    RowwiseRowBlockPB* data = req.mutable_to_insert_rows();
    data->Clear();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    AddTestRowToBlockPB(schema_, 1, 1, "ceci n'est pas une dupe", data);
    AddTestRowToBlockPB(schema_, 2, 1, "also not a dupe key", data);
    AddTestRowToBlockPB(schema_, 1234, 1, "I am a duplicate key", data);
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

  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(boost::assign::list_of(KeyValue(1, 1))
                                   (KeyValue(2, 1))
                                   (KeyValue(1234, 5678)));
}

TEST_F(TabletServerTest, TestInsertAndMutate) {

  shared_ptr<TabletPeer> tablet;
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));
  Counter* rows_inserted = FindOrCreateCounter(*tablet->tablet()->GetMetricContextForTests(),
      METRIC_rows_inserted);
  Counter* rows_updated = FindOrCreateCounter(*tablet->tablet()->GetMetricContextForTests(),
      METRIC_rows_updated);
  ASSERT_EQ(0, rows_inserted->value());
  ASSERT_EQ(0, rows_updated->value());

  RpcController controller;

  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    RowwiseRowBlockPB* data = req.mutable_to_insert_rows();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    AddTestRowToBlockPB(schema_, 1, 1, "original1", data);
    AddTestRowToBlockPB(schema_, 2, 2, "original2", data);
    AddTestRowToBlockPB(schema_, 3, 3, "original3", data);
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
    RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    Slice mutation1("mutated1");
    Slice mutation2("mutated22");
    Slice mutation3("mutated333");

    faststring mutations;
    AddTestMutationToRowBlockAndBuffer(schema_, 1, 2, mutation1, data, &mutations);
    AddTestMutationToRowBlockAndBuffer(schema_, 2, 3, mutation2, data, &mutations);
    AddTestMutationToRowBlockAndBuffer(schema_, 3, 4, mutation3, data, &mutations);
    req.set_encoded_mutations(mutations.data(), mutations.size());
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
    RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());
    Slice mutation("mutated");
    faststring mutations;
    AddTestMutationToRowBlockAndBuffer(schema_, 1234, 2, mutation, data, &mutations);
    req.set_encoded_mutations(mutations.data(), mutations.size());
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
    RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());
    faststring mutations;
    AddTestDeletionToRowBlockAndBuffer(schema_, 1, data, &mutations);
    req.set_encoded_mutations(mutations.data(), mutations.size());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(0, resp.per_row_errors().size());
    ASSERT_EQ(4, rows_updated->value());
    controller.Reset();
  }

  // Now try and mutate a row we just deleted, we should get an error
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());
    Slice mutation1("mutated1");
    faststring mutations;
    AddTestMutationToRowBlockAndBuffer(schema_, 1, 2, mutation1, data, &mutations);
    req.set_encoded_mutations(mutations.data(), mutations.size());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(1, resp.per_row_errors().size());
    controller.Reset();
  }

  ASSERT_EQ(3, rows_inserted->value());
  ASSERT_EQ(4, rows_updated->value());

  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(boost::assign::list_of(KeyValue(2, 3))
                                   (KeyValue(3, 4)));
}

// Test various invalid calls for mutations
TEST_F(TabletServerTest, TestInvalidMutations) {
  RpcController controller;

  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kTabletId);

  // Set up the key block. All of the cases in this test will use
  // this same key.
  RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
  ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
  data->set_num_key_columns(schema_.num_key_columns());

  AddTestKeyToBlock(key_schema_, 0, data);

  // Send a mutations buffer where the length prefix is too short
  {
    req.set_encoded_mutations("\x01");
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    EXPECT_TRUE(resp.has_error());
    EXPECT_EQ(TabletServerErrorPB::INVALID_MUTATION, resp.error().code());
    controller.Reset();
  }

  // Send a mutations buffer where the length prefix points past the
  // end of the buffer
  {
    req.set_encoded_mutations("\xff\x00\x00\x00", 4);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    EXPECT_TRUE(resp.has_error());
    EXPECT_EQ(TabletServerErrorPB::INVALID_MUTATION, resp.error().code());
    controller.Reset();
  }

  // Try to send an invalid mutation type to the server.
  {
    req.set_encoded_mutations("\x01\x00\x00\x00""x", 5);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(1, resp.per_row_errors().size());
    ASSERT_STR_CONTAINS(resp.per_row_errors(0).error().message(),
                        "bad type enum value");
    controller.Reset();
  }

  // Try to send a REINSERT mutation to the server -- this should fail
  // since REINSERTs only happen within the server, not from a client.
  {
    // Set up a REINSERT mutation
    char scratch[schema_.byte_size()];
    memset(scratch, 0, schema_.byte_size());
    faststring tmp;
    RowChangeListEncoder encoder(schema_, &tmp);
    encoder.SetToReinsert(Slice(scratch, schema_.byte_size()));

    faststring buf;
    PutFixed32LengthPrefixedSlice(&buf, Slice(tmp));
    req.set_encoded_mutations(buf.data(), buf.size());

    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    EXPECT_FALSE(resp.has_error());
    ASSERT_EQ(1, resp.per_row_errors().size());
    ASSERT_STR_CONTAINS(resp.per_row_errors(0).error().message(),
                        "User may not specify REINSERT");
    controller.Reset();
  }

  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  // expect no rows since all mutations failed
  VerifyRows(vector<KeyValue>());

  // TODO: add test for UPDATE with a column which doesn't exist,
  // or otherwise malformed.
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
  virtual Status PostSwapNewMemRowSet() {
    return DoHook(1, 10 + iteration_);
  }
  // This should go in after the flush, but before
  // the duplicating row set, i.e., this should appear as
  // a missed delta.
  virtual Status PostTakeMvccSnapshot() {
    return DoHook(2, 20 + iteration_);
  }
  // This too should appear as a missed delta.
  virtual Status PostWriteSnapshot() {
    return DoHook(3, 30 + iteration_);
  }
  // This should appear as a duplicated mutation
  virtual Status PostSwapInDuplicatingRowSet() {
    return DoHook(4, 40 + iteration_);
  }
  // This too should appear as a duplicated mutation
  virtual Status PostReupdateMissedDeltas() {
    return DoHook(5, 50 + iteration_);
  }
  // This should go into the new delta.
  virtual Status PostSwapNewRowSet() {
    return DoHook(6, 60 + iteration_);
  }
  // This should go in pre-flush (only on compactions)
  virtual Status PostSelectIterators() {
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
  VerifyRows(boost::assign::list_of(KeyValue(1, 10))
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
  VerifyRows(boost::assign::list_of(KeyValue(1, 10))
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
  VerifyRows(boost::assign::list_of(KeyValue(1, 10))
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
  VerifyRows(boost::assign::list_of(KeyValue(1, 11))
                                   (KeyValue(2, 21))
                                   (KeyValue(3, 31))
                                   (KeyValue(4, 41))
                                   (KeyValue(5, 51))
                                   (KeyValue(6, 61))
                                   (KeyValue(7, 7))
                                   (KeyValue(8, 8)));

  hooks->increment_iteration();
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Shutdown the tserver and try and rebuild the tablet from the log
  // produced on recovery (recovery flushed no state, but produced a new
  // log).
  ASSERT_NO_FATAL_FAILURE(ShutdownAndRebuildTablet());
  VerifyRows(boost::assign::list_of(KeyValue(1, 11))
                                   (KeyValue(2, 22))
                                   (KeyValue(3, 32))
                                   (KeyValue(4, 42))
                                   (KeyValue(5, 52))
                                   (KeyValue(6, 62))
                                   (KeyValue(7, 72))
                                   (KeyValue(8, 8)));

}

TEST_F(TabletServerTest, TestScan) {
  int num_rows = AllowSlowTests() ? 10000 : 1000;
  InsertTestRowsDirect(num_rows);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.has_more_results());
  }

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
    DrainScannerToStrings(resp.scanner_id(), projection, &results));
  ASSERT_EQ(num_rows, results.size());

  for (int i = 0; i < num_rows; i++) {
    string expected = schema_.DebugRow(BuildTestRow(i));
    ASSERT_EQ(expected, results[i]);
  }

  // Since the rows are drained, the scanner should be automatically removed
  // from the scanner manager.
  {
    SharedScanner junk;
    ASSERT_FALSE(mini_server_->server()->scanner_manager()->LookupScanner(scanner_id, &junk));
  }
}

TEST_F(TabletServerTest, TestScanWithStringPredicates) {
  InsertTestRowsDirect(100);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
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
  InsertTestRowsDirect(num_rows);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
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
  req.set_scanner_id("x");
  SCOPED_TRACE(req.DebugString());
  Status s = proxy_->Scan(req, &resp, &rpc);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Must not pass both a scanner_id and new_scan_request");
}

TEST_F(TabletServerTest, TestInvalidScanRequest_BadProjection) {
  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  const Schema projection(boost::assign::list_of
                          (ColumnSchema("col_doesnt_exist", UINT32)),
                          1);
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::MISMATCHED_SCHEMA, resp.error().code());
    ASSERT_STR_CONTAINS(resp.error().status().message(), "must have a default value or be nullable");
  }
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
  ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);

  // Send the call
  {
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());

    // Because there are no entries, we should immediately return "no results"
    // and not bother handing back a scanner ID.
    ASSERT_FALSE(resp.has_more_results());
    ASSERT_FALSE(resp.has_scanner_id());
  }
}

} // namespace tserver
} // namespace kudu
