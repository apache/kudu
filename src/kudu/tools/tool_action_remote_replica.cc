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

#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
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
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DEFINE_bool(force_copy, false,
            "Force the copy when the destination tablet server has this replica");
DEFINE_bool(include_schema, true,
            "Whether to include the schema of each replica");
DEFINE_string(src_tablet_id, "",
              "The source tablet id. If not specified, use <tablet_id>.");
DEFINE_bool(ignore_address_from_rpc, false,
            "Whether to ignore the source address from the response when consulting the "
            "source peer. If true, use the <src_address> directly from the command line.");
DEFINE_uint64(schema_version, 0, "The new schema version to set.");
DECLARE_bool(force);
DECLARE_string(table_name);
DECLARE_string(tablets);
DECLARE_int64(timeout_ms); // defined in ksck

using kudu::client::KuduRowResult;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::StartTabletCopyRequestPB;
using kudu::consensus::StartTabletCopyResponsePB;
using kudu::rpc::RpcController;
using kudu::server::ServerStatusPB;
using kudu::tablet::TabletStatusPB;
using kudu::tserver::DeleteTabletRequestPB;
using kudu::tserver::DeleteTabletResponsePB;
using kudu::tserver::ListTabletsRequestPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::NewScanRequestPB;
using kudu::tserver::ScanRequestPB;
using kudu::tserver::ScanResponsePB;
using kudu::tserver::TabletServer;
using kudu::tserver::TabletServerAdminServiceProxy;
using kudu::tserver::TabletServerErrorPB;
using kudu::tserver::TabletServerServiceProxy;
using std::cerr;
using std::cout;
using std::endl;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace tools {

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
                                  &resp));
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

constexpr const char* const kReasonArg = "reason";
constexpr const char* const kSrcAddressArg = "src_address";
constexpr const char* const kDstAddressArg = "dst_address";
constexpr const char* const kPeerUUIDsArg = "peer uuids";
constexpr const char* const kPeerUUIDsArgDesc =
    "List of peer uuids to be part of new config, separated by whitespace";


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
    // It's ok if the tablet replica isn't running if it's tombstoned.
    if (rs.state() != tablet::RUNNING &&
        rs.tablet_data_state() != tablet::TABLET_DATA_TOMBSTONED) {
      cerr << "Tablet id: " << rs.tablet_id() << " is "
           << tablet::TabletStatePB_Name(rs.state()) << endl;
      all_running = false;
    }
  }

  if (all_running) {
    cout << "All tablet replicas are running" << endl;
    return Status::OK();
  }
  return Status::IllegalState("Not all tablet replicas are running");
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

  unordered_set<string> tablet_ids = strings::Split(FLAGS_tablets, ",");
  for (const auto& r : replicas) {
    if (!FLAGS_table_name.empty() &&
        r.tablet_status().table_name() != FLAGS_table_name) {
      continue;
    }
    if (!FLAGS_tablets.empty() &&
        !ContainsKey(tablet_ids, r.tablet_status().tablet_id())) {
      continue;
    }
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
    cout << "Last status: " << rs.last_status() << endl;
    if (r.has_role()) {
      cout << "Role: " << RaftPeerPB::Role_Name(r.role()) << endl;
    }
    cout << "Table name: " << rs.table_name() << endl;
    cout << "Partition: "
         << partition_schema.PartitionDebugString(partition, schema) << endl;
    if (rs.has_estimated_on_disk_size()) {
      cout << "Estimated on disk size: "
           << HumanReadableNumBytes::ToString(rs.estimated_on_disk_size()) << endl;
    }
    const string& data_state = tablet::TabletDataState_Name(rs.tablet_data_state());
    cout << "Data state: " << data_state << endl;
    if (rs.data_dirs_size() != 0) {
      cout << "Data dirs: " << JoinStrings(rs.data_dirs(), ", ") << endl;
    } else {
      cout << "Data dirs: <not available>" << endl;
    }
    if (FLAGS_include_schema) {
      cout << "Schema: " << schema.ToString() << endl;
    }
  }

  return Status::OK();
}

Status CopyReplica(const RunnerContext& context) {
  const string& src_address = FindOrDie(context.required_args, kSrcAddressArg);
  const string& dst_address = FindOrDie(context.required_args, kDstAddressArg);
  const string& dst_tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  string src_tablet_id;
  if (FLAGS_src_tablet_id.empty()) {
    src_tablet_id = dst_tablet_id;
  } else {
    src_tablet_id = FLAGS_src_tablet_id;
  }

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
  req.set_tablet_id(dst_tablet_id);
  req.set_src_tablet_id(src_tablet_id);
  req.set_copy_peer_uuid(src_status.node_instance().permanent_uuid());
  if (FLAGS_ignore_address_from_rpc) {
    HostPort hp;
    RETURN_NOT_OK(hp.ParseString(src_address, TabletServer::kDefaultPort));
    *req.mutable_copy_peer_addr() = HostPortToPB(hp);
  } else {
    *req.mutable_copy_peer_addr() = src_status.bound_rpc_addresses(0);
  }
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

Status UnsafeSetSchemaVersion(const RunnerContext& context) {
  // Parse and validate arguments.
  const string& dst_address = FindOrDie(context.required_args, kTServerAddressArg);
  const string& tablet_ids_str = FindOrDie(context.required_args, kTabletIdsCsvArg);
  const vector<string> tablet_ids = strings::Split(tablet_ids_str, ",", strings::SkipEmpty());
  if (tablet_ids.empty()) {
    return Status::InvalidArgument("no tablet identifiers provided");
  }
  const set<string> unique_tablet_ids(tablet_ids.begin(), tablet_ids.end());
  if (unique_tablet_ids.size() != tablet_ids.size()) {
    LOG(WARNING) << "Please notice that there are some duplicate tablet ids.";
  }

  ServerStatusPB dst_status;
  RETURN_NOT_OK(GetServerStatus(dst_address, TabletServer::kDefaultPort,
                                &dst_status));

  unique_ptr<TabletServerServiceProxy> tss_proxy;
  RETURN_NOT_OK(BuildProxy(dst_address, tserver::TabletServer::kDefaultPort,
                           &tss_proxy));

  vector<ListTabletsResponsePB::StatusAndSchemaPB> replicas;
  RETURN_NOT_OK(GetReplicas(tss_proxy.get(), &replicas));

  map<std::string, ListTabletsResponsePB::StatusAndSchemaPB> replicas_by_id;
  for (const auto& replica : replicas) {
    InsertOrDie(&replicas_by_id, replica.tablet_status().tablet_id(), replica);
  }

  unique_ptr<TabletServerAdminServiceProxy> tsas_proxy;
  RETURN_NOT_OK(BuildProxy(dst_address, TabletServer::kDefaultPort, &tsas_proxy));
  vector<string> failed_tablet_ids;
  for (const auto& tablet_id : unique_tablet_ids) {
    const auto* replica = FindOrNull(replicas_by_id, tablet_id);
    if (replica == nullptr) {
      failed_tablet_ids.push_back(tablet_id);
      LOG(WARNING) << strings::Substitute("Tablet $0 is not found", tablet_id);
      continue;
    }

    // Send a request to set the schema_version to node dst_address
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
    tserver::AlterSchemaRequestPB req;
    req.set_dest_uuid(dst_status.node_instance().permanent_uuid());
    req.set_schema_version(FLAGS_schema_version);
    req.set_force(FLAGS_force);
    req.set_tablet_id(tablet_id);
    req.mutable_schema()->CopyFrom(replica->schema());
    tserver::AlterSchemaResponsePB resp;
    RETURN_NOT_OK(tsas_proxy->AlterSchema(req, &resp, &rpc));
    if (resp.has_error()) {
      failed_tablet_ids.push_back(tablet_id);
      LOG(WARNING) << strings::Substitute("Tablet $0 failed: $1",
                                          tablet_id,
                                          StatusFromPB(resp.error().status()).ToString());
      continue;
    }
    LOG(INFO) << strings::Substitute("Tablet $0 success", tablet_id);
  }

  if (!failed_tablet_ids.empty()) {
    return Status::IOError(strings::Substitute("Some tablets failed to set schema version. Failed "
                                               "tablets: $0",
                                               JoinStrings(failed_tablet_ids, ", ")));
  }

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildRemoteReplicaMode() {
  unique_ptr<Action> check_replicas =
      TServerActionBuilder("check", &CheckReplicas)
      .Description("Check if all tablet replicas on a Kudu tablet server are "
                   "running. Tombstoned replica do not count as not running, "
                   "because they are just records of the previous existence of "
                   "a replica.")
      .Build();

  unique_ptr<Action> copy_replica =
      RpcActionBuilder("copy", &CopyReplica)
      .Description("Copy a tablet replica from one Kudu Tablet Server to another")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kSrcAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kDstAddressArg, kTServerAddressDesc })
      .AddOptionalParameter("force_copy")
      .AddOptionalParameter("ignore_address_from_rpc")
      .AddOptionalParameter("src_tablet_id")
      .Build();

  unique_ptr<Action> delete_replica =
      TServerActionBuilder("delete", &DeleteReplica)
      .Description("Delete a tablet replica from a Kudu Tablet Server")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kReasonArg, "Reason for deleting the replica" })
      .Build();

  unique_ptr<Action> dump_replica =
      TServerActionBuilder("dump", &DumpReplica)
      .Description("Dump the data of a tablet replica on a Kudu Tablet Server")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .Build();

  unique_ptr<Action> list =
      TServerActionBuilder("list", &ListReplicas)
      .Description("List all tablet replicas on a Kudu Tablet Server")
      .AddOptionalParameter("include_schema")
      .AddOptionalParameter("table_name")
      .AddOptionalParameter("tablets",
                            string(""),
                            string("Comma-separated list of tablet IDs used to "
                                   "filter the list of replicas"))
      .Build();

  unique_ptr<Action> unsafe_change_config =
      TServerActionBuilder("unsafe_change_config", &UnsafeChangeConfig)
      .Description("Force the specified replica to adopt a new Raft config")
      .ExtraDescription("This tool is useful when a config change is "
                        "necessary because a tablet cannot make progress with "
                        "its current Raft configuration (e.g. to evict "
                        "followers when a majority is unavailable).\n\nNote: "
                        "The members of the new Raft config must be a subset "
                        "of (or the same as) the members of the existing "
                        "committed Raft config.")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredVariadicParameter({ kPeerUUIDsArg, kPeerUUIDsArgDesc })
      .Build();

  unique_ptr<Action> unsafe_set_schema_version =
      TServerActionBuilder("unsafe_set_schema_version", &UnsafeSetSchemaVersion)
          .Description("Set a new schema version on tablet replicas on a Kudu tablet server")
          .AddRequiredParameter({ kTabletIdsCsvArg, kTabletIdsCsvArgDesc })
          .AddOptionalParameter("force")
          .AddOptionalParameter("schema_version")
          .Build();

  return ModeBuilder("remote_replica")
      .Description("Operate on remote tablet replicas on a Kudu Tablet Server")
      .AddAction(std::move(check_replicas))
      .AddAction(std::move(copy_replica))
      .AddAction(std::move(delete_replica))
      .AddAction(std::move(dump_replica))
      .AddAction(std::move(list))
      .AddAction(std::move(unsafe_change_config))
      .AddAction(std::move(unsafe_set_schema_version))
      .Build();
}

} // namespace tools
} // namespace kudu

