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

#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/move.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DEFINE_bool(force_copy, false,
            "Force the copy when the destination tablet server has this replica");
DECLARE_int64(timeout_ms); // defined in ksck

namespace kudu {
namespace tools {

using client::KuduRowResult;
using client::KuduScanBatch;
using client::KuduSchema;
using consensus::ConsensusServiceProxy;
using consensus::RaftConfigPB;
using consensus::RaftPeerPB;
using consensus::StartTabletCopyRequestPB;
using consensus::StartTabletCopyResponsePB;
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
using tserver::TabletServer;
using tserver::TabletServerErrorPB;
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

    // Scan and dump the tablet.
    // Note that we do a READ_LATEST scan as we might be scanning a tablet who lost majority
    // and thus cannot do snapshot scans.
    // TODO(dalves) When KUDU-1704 is in change this to perform stale snapshot reads, which
    // can be ordered.
    NewScanRequestPB* new_req = req.mutable_new_scan_request();
    RETURN_NOT_OK(SchemaToColumnPBs(
        schema, new_req->mutable_projected_columns(),
        SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES));
    new_req->set_tablet_id(tablet_id);
    new_req->set_cache_blocks(false);

    do {
      RpcController rpc;
      rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
      RETURN_NOT_OK_PREPEND(proxy->Scan(req, &resp, &rpc), "Scan() failed");

      if (resp.has_error()) {
        return Status::IOError("Failed to read: ",
                               pb_util::SecureShortDebugString(resp.error()));
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
                                  client::KuduScanner::NO_FLAGS,
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
const char* const kSrcAddressArg = "src_address";
const char* const kDstAddressArg = "dst_address";
const char* const kPeerUUIDsArg = "peer uuids";
const char* const kPeerUUIDsArgDesc = "List of peer uuids to be part of new config";

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
                           pb_util::SecureShortDebugString(resp.error()));
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

Status CopyReplica(const RunnerContext& context) {
  const string& src_address = FindOrDie(context.required_args, kSrcAddressArg);
  const string& dst_address = FindOrDie(context.required_args, kDstAddressArg);
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  ServerStatusPB dst_status;
  RETURN_NOT_OK(GetServerStatus(dst_address, TabletServer::kDefaultPort,
                                &dst_status));
  ServerStatusPB src_status;
  RETURN_NOT_OK(GetServerStatus(src_address, TabletServer::kDefaultPort,
                                &src_status));

  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(dst_address, TabletServer::kDefaultPort, &proxy));

  StartTabletCopyRequestPB req;
  StartTabletCopyResponsePB resp;
  RpcController rpc;
  req.set_dest_uuid(dst_status.node_instance().permanent_uuid());
  req.set_tablet_id(tablet_id);
  req.set_copy_peer_uuid(src_status.node_instance().permanent_uuid());
  *req.mutable_copy_peer_addr() = src_status.bound_rpc_addresses(0);
  // Provide a force option if the destination tablet server has the
  // replica otherwise tablet-copy will fail.
  if (FLAGS_force_copy) {
    req.set_caller_term(std::numeric_limits<int64_t>::max());
  }

  LOG(INFO) << "Sending copy replica request:\n" << pb_util::SecureDebugString(req);
  LOG(WARNING) << "NOTE: this copy may happen asynchronously "
               << "and may timeout if the tablet size is large. Watch the logs on "
               << "the target tablet server for indication of progress.";

  RETURN_NOT_OK(proxy->StartTabletCopy(req, &resp, &rpc));
  if (resp.has_error()) {
    RETURN_NOT_OK_PREPEND(
        StatusFromPB(resp.error().status()),
        strings::Substitute("Remote server returned error code $0",
                            TabletServerErrorPB::Code_Name(resp.error().code())));
  }
  return Status::OK();
}

Status UnsafeChangeConfig(const RunnerContext& context) {
  // Parse and validate arguments.
  const string& dst_address = FindOrDie(context.required_args, kTServerAddressArg);
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  ServerStatusPB dst_status;
  RETURN_NOT_OK(GetServerStatus(dst_address, TabletServer::kDefaultPort,
                                &dst_status));

  if (context.variadic_args.empty()) {
    return Status::InvalidArgument("No peer UUIDs specified for the new config");
  }

  RaftConfigPB new_config;
  for (const auto& arg : context.variadic_args) {
    RaftPeerPB new_peer;
    new_peer.set_permanent_uuid(arg);
    new_config.add_peers()->CopyFrom(new_peer);
  }

  // Send a request to replace the config to node dst_address.
  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(dst_address, TabletServer::kDefaultPort, &proxy));
  consensus::UnsafeChangeConfigRequestPB req;
  consensus::UnsafeChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
  req.set_dest_uuid(dst_status.node_instance().permanent_uuid());
  req.set_tablet_id(tablet_id);
  req.set_caller_id("kudu-tools");
  *req.mutable_new_config() = new_config;
  RETURN_NOT_OK(proxy->UnsafeChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildRemoteReplicaMode() {
  unique_ptr<Action> check_replicas =
      ActionBuilder("check", &CheckReplicas)
      .Description("Check if all tablet replicas on a Kudu Tablet Server are running")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .Build();

  unique_ptr<Action> copy_replica =
      ActionBuilder("copy", &CopyReplica)
      .Description("Copy a tablet replica from one Kudu Tablet Server to another")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kSrcAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kDstAddressArg, kTServerAddressDesc })
      .AddOptionalParameter("force_copy")
      .Build();

  unique_ptr<Action> delete_replica =
      ActionBuilder("delete", &DeleteReplica)
      .Description("Delete a tablet replica from a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kReasonArg, "Reason for deleting the replica" })
      .Build();

  unique_ptr<Action> dump_replica =
      ActionBuilder("dump", &DumpReplica)
      .Description("Dump the data of a tablet replica on a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .Build();

  unique_ptr<Action> list =
      ActionBuilder("list", &ListReplicas)
      .Description("List all tablet replicas on a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .Build();

  unique_ptr<Action> unsafe_change_config =
      ActionBuilder("unsafe_change_config", &UnsafeChangeConfig)
      .Description("Force the specified replica to adopt a new Raft config")
      .ExtraDescription("The members of the new Raft config must be a subset "
                        "of (or the same as) the members of the existing "
                        "committed Raft config on that replica.")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredVariadicParameter({ kPeerUUIDsArg, kPeerUUIDsArgDesc })
      .Build();

  return ModeBuilder("remote_replica")
      .Description("Operate on remote tablet replicas on a Kudu Tablet Server")
      .AddAction(std::move(check_replicas))
      .AddAction(std::move(copy_replica))
      .AddAction(std::move(delete_replica))
      .AddAction(std::move(dump_replica))
      .AddAction(std::move(list))
      .AddAction(std::move(unsafe_change_config))
      .Build();
}

} // namespace tools
} // namespace kudu

