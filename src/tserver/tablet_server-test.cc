// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <tr1/memory>
#include <vector>

#include "common/wire_protocol.h"
#include "common/schema.h"
#include "gutil/strings/join.h"
#include "server/metadata.h"
#include "server/metadata_util.h"
#include "server/rpc_server.h"
#include "tablet/tablet.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "tserver/tserver.proxy.h"
#include "util/net/sockaddr.h"
#include "util/status.h"
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
  TabletServerTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING)),
              1) {
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
    ASSERT_NO_FATAL_FAILURE(CreateClientProxy(mini_server_->bound_addr(), &proxy_));
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

  void AddTestRowToBlockPB(uint32_t key, uint32_t int_val, const string& string_val,
                           RowwiseRowBlockPB* block) {
    RowBuilder rb(schema_);
    rb.AddUint32(key);
    rb.AddUint32(int_val);
    rb.AddString(string_val);
    AddRowToRowBlockPB(rb.row(), block);
  }

  Schema schema_;

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
  InsertRequestPB req;

  req.set_tablet_id(kTabletId);

  InsertResponsePB resp;
  RpcController controller;

  // Send a bad insert which has an empty schema. This should result
  // in an error.
  {
    RowwiseRowBlockPB* data = req.mutable_data();
    // Fill in an empty "rows" structure.
    data->mutable_rows();
    data->set_num_key_columns(0);

    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Insert(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(TabletServerErrorPB::MISMATCHED_SCHEMA, resp.error().code());
    Status s = StatusFromPB(resp.error().status());
    EXPECT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Mismatched schema, expected: Schema "
                        "[key[type='uint32'], int_val[type='uint32'],"
                        " string_val[type='string']]");
  }

  // Send an empty insert with the correct schema.
  // This should succeed and do nothing.
  {
    controller.Reset();
    RowwiseRowBlockPB* data = req.mutable_data();
    data->Clear();
    data->mutable_rows(); // Set empty rows data.

    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Insert(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // Send an actual row insert.
  {
    controller.Reset();
    RowwiseRowBlockPB* data = req.mutable_data();
    data->Clear();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    AddTestRowToBlockPB(1234, 5678, "hello world via RPC", data);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Insert(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // Send a batch with multiple rows, one of which is a duplicate of
  // the above insert. This should generate one error into per_row_errors.
  {
    controller.Reset();
    RowwiseRowBlockPB* data = req.mutable_data();
    data->Clear();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    AddTestRowToBlockPB(1, 1, "ceci n'est pas une dupe", data);
    AddTestRowToBlockPB(2, 1, "also not a dupe key", data);
    AddTestRowToBlockPB(1234, 1, "I am a duplicate key", data);
    SCOPED_TRACE(req.DebugString());
    ASSERT_STATUS_OK(proxy_->Insert(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();
    ASSERT_EQ(1, resp.per_row_errors().size());
    ASSERT_EQ(2, resp.per_row_errors().Get(0).row_index());
    Status s = StatusFromPB(resp.per_row_errors().Get(0).error());
    ASSERT_STR_CONTAINS(s.ToString(), "Already present");
  }

}

} // namespace tserver
} // namespace kudu
