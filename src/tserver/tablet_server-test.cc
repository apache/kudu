// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <tr1/memory>
#include <vector>

#include "common/wire_protocol.h"
#include "common/wire_protocol-test-util.h"
#include "common/schema.h"
#include "gutil/strings/join.h"
#include "server/metadata.h"
#include "server/metadata_util.h"
#include "server/rpc_server.h"
#include "tablet/tablet.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/scanners.h"
#include "tserver/tablet_server.h"
#include "tserver/tserver.proxy.h"
#include "util/net/sockaddr.h"
#include "util/status.h"
#include "util/coding.h"
#include "util/test_util.h"
#include "rpc/messenger.h"

using std::string;
using std::tr1::shared_ptr;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tablet::Tablet;

namespace kudu {
namespace tserver {

class TabletServerTest : public KuduTest {
 public:
  TabletServerTest() {
    CreateTestSchema(&schema_);
    key_schema_ = schema_.CreateKeyProjection();
    rb_.reset(new RowBuilder(schema_));
  }

  virtual void SetUp() {
    KuduTest::SetUp();

    // Start server.
    mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TabletServerTest-fsroot")));
    ASSERT_STATUS_OK(mini_server_->Start());

    // Set up a tablet inside the server.
    ASSERT_STATUS_OK(mini_server_->AddTestTablet(kTabletId, schema_));
    ASSERT_TRUE(mini_server_->server()->LookupTablet(kTabletId, &tablet_));

    // Connect to it.
    ASSERT_NO_FATAL_FAILURE(CreateClientProxy(mini_server_->bound_rpc_addr(), &proxy_));
  }

  virtual void TearDown() {
    // TODO: once the server has a Stop() method we should probably call it!
    // Otherwise we almost certainly leak threads and sockets between test cases.
    KuduTest::TearDown();
  }

 private:

  void CreateClientProxy(const Sockaddr &addr, gscoped_ptr<TabletServerServiceProxy>* proxy) {
    if (!client_messenger_) {
      MessengerBuilder bld("Client");
      ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    }
    proxy->reset(new TabletServerServiceProxy(client_messenger_, addr));
  }

 protected:
  static const char* kTabletId;

  // Inserts 'num_rows' test rows directly into the tablet (i.e not via RPC)
  void InsertTestRows(int num_rows) {
    tablet::TransactionContext tx_ctx;
    for (int i = 0; i < num_rows; i++) {
      CHECK_OK(tablet_->Insert(&tx_ctx, BuildTestRow(i)));
      tx_ctx.Reset();
    }
  }

  ConstContiguousRow BuildTestRow(int index) {
    rb_->Reset();
    rb_->AddUint32(index);
    rb_->AddUint32(index * 2);
    rb_->AddString(StringPrintf("hello %d", index));
    return rb_->row();
  }

  void DrainScannerToStrings(const string& scanner_id, const Schema& projection,
                             vector<string>* results) {
    RpcController rpc;
    ScanRequestPB req;
    ScanResponsePB resp;
    req.set_scanner_id(scanner_id);

    do {
      rpc.Reset();
      req.set_batch_size_bytes(10000);
      SCOPED_TRACE(req.DebugString());
      ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());

      vector<const uint8_t*> rows;
      ASSERT_STATUS_OK(ExtractRowsFromRowBlockPB(projection,resp.mutable_data(), &rows));
      LOG(INFO) << "Round trip got " << rows.size() << " rows";
      BOOST_FOREACH(const uint8_t* row_ptr, rows) {
        ConstContiguousRow row(projection, row_ptr);
        results->push_back(projection.DebugRow(row));
      }
    } while (resp.has_more_results());
  }

  Schema schema_;
  Schema key_schema_;
  gscoped_ptr<RowBuilder> rb_;

  shared_ptr<Messenger> client_messenger_;

  gscoped_ptr<MiniTabletServer> mini_server_;
  shared_ptr<Tablet> tablet_;
  gscoped_ptr<TabletServerServiceProxy> proxy_;
};

const char* TabletServerTest::kTabletId = "TestTablet";


TEST_F(TabletServerTest, TestPingServer) {
  // Ping the server.
  PingRequestPB req;
  PingResponsePB resp;
  RpcController controller;
  ASSERT_STATUS_OK(proxy_->Ping(req, &resp, &controller));
}

TEST_F(TabletServerTest, TestInsert) {
  WriteRequestPB req;

  req.set_tablet_id(kTabletId);

  WriteResponsePB resp;
  RpcController controller;

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
                        "Mismatched schema, expected: Schema "
                        "[key[type='uint32'], int_val[type='uint32'],"
                        " string_val[type='string']]");
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
  }
}

TEST_F(TabletServerTest, TestInsertAndMutate) {

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
    controller.Reset();
  }
  // Try and delete the rows
  {
    WriteRequestPB req;
    WriteResponsePB resp;
    req.set_tablet_id(kTabletId);
    RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());
    faststring mutations;
    AddTestDeletionToRowBlockAndBuffer(schema_, 1, data, &mutations);
    AddTestDeletionToRowBlockAndBuffer(schema_, 2, data, &mutations);
    AddTestDeletionToRowBlockAndBuffer(schema_, 3, data, &mutations);
    req.set_encoded_mutations(mutations.data(), mutations.size());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(0, resp.per_row_errors().size());
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

  // TODO: add test for UPDATE with a column which doesn't exist,
  // or otherwise malformed.
}

TEST_F(TabletServerTest, TestScan) {
  int num_rows = AllowSlowTests() ? 10000 : 1000;
  InsertTestRows(num_rows);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  const Schema& projection = schema_;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id("");
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
  InsertTestRows(100);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id("");
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
  InsertTestRows(num_rows);

  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id("");
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
  scan->set_tablet_id("x");
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
  scan->set_tablet_id("x");
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
  scan->set_tablet_id("");
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
