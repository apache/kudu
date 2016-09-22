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

#include "kudu/tools/tool_action.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

DECLARE_int64(timeout_ms); // defined in ksck

namespace kudu {
namespace tools {

using client::KuduRowResult;
using client::KuduScanBatch;
using client::KuduSchema;
using rpc::RpcController;
using server::ServerStatusPB;
using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;
using tablet::TabletStatusPB;
using tserver::DeleteTabletRequestPB;
using tserver::DeleteTabletResponsePB;
using tserver::ListTabletsRequestPB;
using tserver::ListTabletsResponsePB;
using tserver::NewScanRequestPB;
using tserver::ScanRequestPB;
using tserver::ScanResponsePB;
using tserver::TabletServerAdminServiceProxy;
using tserver::TabletServerServiceProxy;

// This class only exists so that Dump() can easily be friended with
// KuduSchema and KuduScanBatch.
class ReplicaDumper {
 public:
  static Status Dump(const Schema& schema,
                     const string& tablet_id,
                     TabletServerServiceProxy* proxy) {
    KuduSchema client_schema(schema);

    ScanRequestPB req;
    ScanResponsePB resp;

    NewScanRequestPB* new_req = req.mutable_new_scan_request();
    RETURN_NOT_OK(SchemaToColumnPBs(
        schema, new_req->mutable_projected_columns(),
        SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES));
    new_req->set_tablet_id(tablet_id);
    new_req->set_cache_blocks(false);
    new_req->set_order_mode(ORDERED);
    new_req->set_read_mode(READ_AT_SNAPSHOT);

    do {
      RpcController rpc;
      rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
      RETURN_NOT_OK_PREPEND(proxy->Scan(req, &resp, &rpc), "Scan() failed");

      if (resp.has_error()) {
        return Status::IOError("Failed to read: ",
                               resp.error().ShortDebugString());
      }

      // The first response has a scanner ID. We use this for all subsequent
      // responses.
      if (resp.has_scanner_id()) {
        req.set_scanner_id(resp.scanner_id());
        req.clear_new_scan_request();
      }
      req.set_call_seq_id(req.call_seq_id() + 1);

      // Nothing to process from this scan result.
      if (!resp.has_data()) {
        continue;
      }

      KuduScanBatch::Data results;
      RETURN_NOT_OK(results.Reset(&rpc,
                                  &schema,
                                  &client_schema,
                                  make_gscoped_ptr(resp.release_data())));
      vector<KuduRowResult> rows;
      results.ExtractRows(&rows);
      for (const auto& r : rows) {
        cout << r.ToString() << endl;
      }
    } while (resp.has_more_results());
    return Status::OK();
  }
};

namespace {

const char* const kReasonArg = "reason";
const char* const kTServerAddressArg = "tserver_address";
const char* const kTServerAddressDesc = "Address of a Kudu Tablet Server of "
    "form 'hostname:port'. Port may be omitted if the Tablet Server is bound "
    "to the default port.";

Status GetReplicas(TabletServerServiceProxy* proxy,
                   vector<ListTabletsResponsePB::StatusAndSchemaPB>* replicas) {
  ListTabletsRequestPB req;
  ListTabletsResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));

  RETURN_NOT_OK(proxy->ListTablets(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  replicas->assign(resp.status_and_schema().begin(),
                   resp.status_and_schema().end());
  return Status::OK();
}

Status CheckReplicas(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);

  unique_ptr<TabletServerServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(address, tserver::TabletServer::kDefaultPort,
                           &proxy));

  vector<ListTabletsResponsePB::StatusAndSchemaPB> replicas;
  RETURN_NOT_OK(GetReplicas(proxy.get(), &replicas));

  bool all_running = true;
  for (const auto& r : replicas) {
    const TabletStatusPB& rs = r.tablet_status();
    if (rs.state() != tablet::RUNNING) {
      cerr << "Tablet id: " << rs.tablet_id() << " is "
           << tablet::TabletStatePB_Name(rs.state()) << endl;
      all_running = false;
    }
  }

  if (all_running) {
    cout << "All tablets are running" << endl;
    return Status::OK();
  }
  return Status::IllegalState("Not all tablets are running");
}

Status DeleteReplica(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& reason = FindOrDie(context.required_args, kReasonArg);

  ServerStatusPB status;
  RETURN_NOT_OK(GetServerStatus(address, tserver::TabletServer::kDefaultPort,
                                &status));

  unique_ptr<TabletServerAdminServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(address, tserver::TabletServer::kDefaultPort,
                           &proxy));

  DeleteTabletRequestPB req;
  DeleteTabletResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));

  req.set_tablet_id(tablet_id);
  req.set_dest_uuid(status.node_instance().permanent_uuid());
  req.set_reason(reason);
  req.set_delete_type(tablet::TABLET_DATA_TOMBSTONED);
  RETURN_NOT_OK_PREPEND(proxy->DeleteTablet(req, &resp, &rpc),
                        "DeleteTablet() failed");
  if (resp.has_error()) {
    return Status::IOError("Failed to delete tablet: ",
                           resp.error().ShortDebugString());
  }
  return Status::OK();
}

Status DumpReplica(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  unique_ptr<TabletServerServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(address, tserver::TabletServer::kDefaultPort,
                           &proxy));

  vector<ListTabletsResponsePB::StatusAndSchemaPB> replicas;
  RETURN_NOT_OK(GetReplicas(proxy.get(), &replicas));

  Schema schema;
  for (const auto& r : replicas) {
    if (r.tablet_status().tablet_id() == tablet_id) {
      RETURN_NOT_OK(SchemaFromPB(r.schema(), &schema));
      break;
    }
  }
  if (!schema.initialized()) {
    return Status::NotFound("cannot find replica", tablet_id);
  }
  return ReplicaDumper::Dump(schema, tablet_id, proxy.get());
}

Status ListReplicas(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  unique_ptr<TabletServerServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(address, tserver::TabletServer::kDefaultPort,
                           &proxy));

  vector<ListTabletsResponsePB::StatusAndSchemaPB> replicas;
  RETURN_NOT_OK(GetReplicas(proxy.get(), &replicas));

  for (const auto& r : replicas) {
    Schema schema;
    RETURN_NOT_OK_PREPEND(
        SchemaFromPB(r.schema(), &schema),
        "Unable to deserialize schema from " + address);
    PartitionSchema partition_schema;
    RETURN_NOT_OK_PREPEND(
        PartitionSchema::FromPB(r.partition_schema(), schema, &partition_schema),
        "Unable to deserialize partition schema from " + address);

    const TabletStatusPB& rs = r.tablet_status();

    Partition partition;
    Partition::FromPB(rs.partition(), &partition);

    const string& state = tablet::TabletStatePB_Name(rs.state());
    cout << "Tablet id: " << rs.tablet_id() << endl;
    cout << "State: " << state << endl;
    cout << "Table name: " << rs.table_name() << endl;
    cout << "Partition: "
         << partition_schema.PartitionDebugString(partition, schema) << endl;
    if (rs.has_estimated_on_disk_size()) {
      cout << "Estimated on disk size: "
           << HumanReadableNumBytes::ToString(rs.estimated_on_disk_size()) << endl;
    }
    cout << "Schema: " << schema.ToString() << endl;
  }

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildRemoteReplicaMode() {
  unique_ptr<Action> check_replicas =
      ActionBuilder("check", &CheckReplicas)
      .Description("Check if all replicas on a Kudu Tablet Server are running")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .Build();

  unique_ptr<Action> delete_replica =
      ActionBuilder("delete", &DeleteReplica)
      .Description("Delete a replica from a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kReasonArg, "Reason for deleting the replica" })
      .Build();

  unique_ptr<Action> dump_replica =
      ActionBuilder("dump", &DumpReplica)
      .Description("Dump the data of a replica on a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .Build();

  unique_ptr<Action> list =
      ActionBuilder("list", &ListReplicas)
      .Description("List all replicas on a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .Build();

  return ModeBuilder("remote_replica")
      .Description("Operate on replicas on a Kudu Tablet Server")
      .AddAction(std::move(check_replicas))
      .AddAction(std::move(delete_replica))
      .AddAction(std::move(dump_replica))
      .AddAction(std::move(list))
      .Build();
}

} // namespace tools
} // namespace kudu

