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

#include "kudu/tools/ksck_remote.h"

#include <cstdint>
#include <map>
#include <ostream>
#include <unordered_map>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/version_info.pb.h"

DECLARE_int64(timeout_ms); // defined in tool_action_common
DEFINE_bool(checksum_cache_blocks, false, "Should the checksum scanners cache the read blocks");

namespace kudu {
namespace tools {

static const std::string kMessengerName = "ksck";

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduReplica;
using client::KuduScanToken;
using client::KuduScanTokenBuilder;
using client::KuduTable;
using client::KuduTabletServer;
using client::internal::ReplicaController;
using rpc::Messenger;
using rpc::MessengerBuilder;
using rpc::RpcController;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace {
MonoDelta GetDefaultTimeout() {
  return MonoDelta::FromMilliseconds(FLAGS_timeout_ms);
}

// Common flag-fetching routine for masters and tablet servers.
Status FetchUnusualFlagsCommon(const shared_ptr<server::GenericServiceProxy>& proxy,
                               server::GetFlagsResponsePB* resp) {
  server::GetFlagsRequestPB req;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  for (const string& tag : { "experimental", "hidden", "unsafe" }) {
    req.add_tags(tag);
  }
  return proxy->GetFlags(req, resp, &rpc);
}
} // anonymous namespace

Status RemoteKsckMaster::Init() {
  vector<Sockaddr> addresses;
  RETURN_NOT_OK(ParseAddressList(address_,
      master::Master::kDefaultPort,
      &addresses));
  const auto& addr = addresses[0];
  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(address_, master::Master::kDefaultPort));
  const auto& host = hp.host();
  generic_proxy_.reset(new server::GenericServiceProxy(messenger_, addr, host));
  consensus_proxy_.reset(new consensus::ConsensusServiceProxy(messenger_, addr, host));
  return Status::OK();
}

Status RemoteKsckMaster::FetchInfo() {
  state_ = KsckFetchState::FETCH_FAILED;
  server::GetStatusRequestPB req;
  server::GetStatusResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(generic_proxy_->GetStatus(req, &resp, &rpc));
  uuid_ = resp.status().node_instance().permanent_uuid();
  version_ = resp.status().version_info().version_string();
  state_ = KsckFetchState::FETCHED;
  return Status::OK();
}

Status RemoteKsckMaster::FetchConsensusState() {
  CHECK_EQ(state_, KsckFetchState::FETCHED);
  consensus::GetConsensusStateRequestPB req;
  consensus::GetConsensusStateResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  req.set_dest_uuid(uuid_);
  RETURN_NOT_OK_PREPEND(consensus_proxy_->GetConsensusState(req, &resp, &rpc),
                        "could not fetch consensus info from master");
  if (resp.tablets_size() != 1) {
    return Status::IllegalState(Substitute("expected 1 master tablet, but found $0",
                                           resp.tablets_size()));
  }
  const auto& tablet = resp.tablets(0);
  if (tablet.tablet_id() != master::SysCatalogTable::kSysCatalogTabletId) {
    return Status::IllegalState(Substitute("expected master tablet with id $0, but found $1",
                                           master::SysCatalogTable::kSysCatalogTabletId,
                                           tablet.tablet_id()));
  }
  cstate_ = tablet.cstate();
  return Status::OK();
}

Status RemoteKsckMaster::FetchUnusualFlags() {
  server::GetFlagsResponsePB resp;
  Status s = FetchUnusualFlagsCommon(generic_proxy_, &resp);
  if (!s.ok()) {
    flags_state_ = KsckFetchState::FETCH_FAILED;
  } else {
    flags_state_ = KsckFetchState::FETCHED;
    flags_ = resp;
  }
  return s;
}

Status RemoteKsckTabletServer::Init() {
  vector<Sockaddr> addresses;
  RETURN_NOT_OK(ParseAddressList(
      host_port_.ToString(),
      tserver::TabletServer::kDefaultPort, &addresses));
  const auto& addr = addresses[0];
  const auto& host = host_port_.host();
  generic_proxy_.reset(new server::GenericServiceProxy(messenger_, addr, host));
  ts_proxy_.reset(new tserver::TabletServerServiceProxy(messenger_, addr, host));
  consensus_proxy_.reset(new consensus::ConsensusServiceProxy(messenger_, addr, host));
  return Status::OK();
}

Status RemoteKsckTabletServer::FetchInfo(KsckServerHealth* health) {
  DCHECK(health);
  state_ = KsckFetchState::FETCH_FAILED;
  *health = KsckServerHealth::UNAVAILABLE;
  {
    server::GetStatusRequestPB req;
    server::GetStatusResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(GetDefaultTimeout());
    RETURN_NOT_OK_PREPEND(generic_proxy_->GetStatus(req, &resp, &rpc),
                          "could not get status from server");
    version_ = resp.status().version_info().version_string();
    string response_uuid = resp.status().node_instance().permanent_uuid();
    if (response_uuid != uuid()) {
      *health = KsckServerHealth::WRONG_SERVER_UUID;
      return Status::RemoteError(Substitute("ID reported by tablet server ($0) doesn't "
                                 "match the expected ID: $1",
                                 response_uuid, uuid()));
    }
  }

  {
    tserver::ListTabletsRequestPB req;
    tserver::ListTabletsResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(GetDefaultTimeout());
    req.set_need_schema_info(false);
    RETURN_NOT_OK_PREPEND(ts_proxy_->ListTablets(req, &resp, &rpc),
                          "could not list tablets");
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    tablet_status_map_.clear();
    for (auto& status : *resp.mutable_status_and_schema()) {
      tablet_status_map_[status.tablet_status().tablet_id()].Swap(status.mutable_tablet_status());
    }
  }

  {
    server::ServerClockRequestPB req;
    server::ServerClockResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(GetDefaultTimeout());
    RETURN_NOT_OK_PREPEND(generic_proxy_->ServerClock(req, &resp, &rpc),
                          "could not fetch timestamp");
    CHECK(resp.has_timestamp());
    timestamp_ = resp.timestamp();
  }

  state_ = KsckFetchState::FETCHED;
  *health = KsckServerHealth::HEALTHY;
  return Status::OK();
}

Status RemoteKsckTabletServer::FetchConsensusState(KsckServerHealth* health) {
  DCHECK(health);
  *health = KsckServerHealth::UNAVAILABLE;
  tablet_consensus_state_map_.clear();
  consensus::GetConsensusStateRequestPB req;
  consensus::GetConsensusStateResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  req.set_dest_uuid(uuid_);
  RETURN_NOT_OK_PREPEND(consensus_proxy_->GetConsensusState(req, &resp, &rpc),
                        "could not fetch all consensus info");
  for (auto& tablet_info : resp.tablets()) {
    // Don't crash in rare and bad case where multiple remotes have the same UUID and tablet id.
    if (!InsertOrUpdate(&tablet_consensus_state_map_,
                        std::make_pair(uuid_, tablet_info.tablet_id()),
                        tablet_info.cstate())) {
      LOG(ERROR) << "Found duplicated tablet information: tablet " << tablet_info.tablet_id()
                 << " is reported on ts " << uuid_ << " twice";
    }
  }

  *health = KsckServerHealth::HEALTHY;
  return Status::OK();
}

Status RemoteKsckTabletServer::FetchUnusualFlags() {
  server::GetFlagsResponsePB resp;
  Status s = FetchUnusualFlagsCommon(generic_proxy_, &resp);
  if (!s.ok()) {
    flags_state_ = KsckFetchState::FETCH_FAILED;
  } else {
    flags_state_ = KsckFetchState::FETCHED;
    flags_ = resp;
  }
  return s;
}

class ChecksumStepper;

// Simple class to act as a callback in order to collate results from parallel
// checksum scans.
class ChecksumCallbackHandler {
 public:
  explicit ChecksumCallbackHandler(ChecksumStepper* const stepper)
      : stepper_(DCHECK_NOTNULL(stepper)) {
  }

  // Invoked by an RPC completion callback. Simply calls back into the stepper.
  // Then the call to the stepper returns, deletes 'this'.
  void Run();

 private:
  ChecksumStepper* const stepper_;
};

// Simple class to have a "conversation" over multiple requests to a server
// to carry out a multi-part checksum scan.
// If any errors or timeouts are encountered, the checksum operation fails.
// After the ChecksumStepper reports its results to the reporter, it deletes itself.
class ChecksumStepper {
 public:
  ChecksumStepper(string tablet_id, const Schema& schema, string server_uuid,
                  ChecksumOptions options, ChecksumProgressCallbacks* callbacks,
                  shared_ptr<tserver::TabletServerServiceProxy> proxy)
      : schema_(schema),
        tablet_id_(std::move(tablet_id)),
        server_uuid_(std::move(server_uuid)),
        options_(options),
        callbacks_(callbacks),
        proxy_(std::move(proxy)),
        call_seq_id_(0),
        checksum_(0) {
    DCHECK(proxy_);
  }

  void Start() {
    Status s = SchemaToColumnPBs(schema_, &cols_,
                                 SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES);
    if (!s.ok()) {
      callbacks_->Finished(s, 0);
    } else {
      SendRequest(kNewRequest);
    }
  }

  void HandleResponse() {
    gscoped_ptr<ChecksumStepper> deleter(this);
    Status s = rpc_.status();
    if (s.ok() && resp_.has_error()) {
      s = StatusFromPB(resp_.error().status());
    }
    if (!s.ok()) {
      callbacks_->Finished(s, 0);
      return; // Deletes 'this'.
    }
    if (resp_.has_resource_metrics() || resp_.has_rows_checksummed()) {
      int64_t bytes = resp_.resource_metrics().cfile_cache_miss_bytes() +
          resp_.resource_metrics().cfile_cache_hit_bytes();
      callbacks_->Progress(resp_.rows_checksummed(), bytes);
    }
    DCHECK(resp_.has_checksum());
    checksum_ = resp_.checksum();

    // Report back with results.
    if (!resp_.has_more_results()) {
      callbacks_->Finished(s, checksum_);
      return; // Deletes 'this'.
    }

    // We're not done scanning yet. Fetch the next chunk.
    if (resp_.has_scanner_id()) {
      scanner_id_ = resp_.scanner_id();
    }
    SendRequest(kContinueRequest);
    ignore_result(deleter.release()); // We have more work to do.
  }

 private:
  enum RequestType {
    kNewRequest,
    kContinueRequest
  };

  void SendRequest(RequestType type) {
    switch (type) {
      case kNewRequest: {
        req_.set_call_seq_id(call_seq_id_);
        req_.mutable_new_request()->mutable_projected_columns()->CopyFrom(cols_);
        req_.mutable_new_request()->set_tablet_id(tablet_id_);
        req_.mutable_new_request()->set_cache_blocks(FLAGS_checksum_cache_blocks);
        if (options_.use_snapshot) {
          req_.mutable_new_request()->set_read_mode(READ_AT_SNAPSHOT);
          req_.mutable_new_request()->set_snap_timestamp(options_.snapshot_timestamp);
        }
        rpc_.set_timeout(GetDefaultTimeout());
        break;
      }
      case kContinueRequest: {
        req_.Clear();
        resp_.Clear();
        rpc_.Reset();

        req_.set_call_seq_id(++call_seq_id_);
        DCHECK(!scanner_id_.empty());
        req_.mutable_continue_request()->set_scanner_id(scanner_id_);
        req_.mutable_continue_request()->set_previous_checksum(checksum_);
        break;
      }
      default:
        LOG(FATAL) << "Unknown type";
        break;
    }
    gscoped_ptr<ChecksumCallbackHandler> handler(new ChecksumCallbackHandler(this));
    rpc::ResponseCallback cb = boost::bind(&ChecksumCallbackHandler::Run, handler.get());
    proxy_->ChecksumAsync(req_, &resp_, &rpc_, cb);
    ignore_result(handler.release());
  }

  const Schema schema_;
  google::protobuf::RepeatedPtrField<ColumnSchemaPB> cols_;

  const string tablet_id_;
  const string server_uuid_;
  const ChecksumOptions options_;
  ChecksumProgressCallbacks* const callbacks_;
  const shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  uint32_t call_seq_id_;
  string scanner_id_;
  uint64_t checksum_;
  tserver::ChecksumRequestPB req_;
  tserver::ChecksumResponsePB resp_;
  RpcController rpc_;
};

void ChecksumCallbackHandler::Run() {
  stepper_->HandleResponse();
  delete this;
}

void RemoteKsckTabletServer::RunTabletChecksumScanAsync(
        const string& tablet_id,
        const Schema& schema,
        const ChecksumOptions& options,
        ChecksumProgressCallbacks* callbacks) {
  gscoped_ptr<ChecksumStepper> stepper(
      new ChecksumStepper(tablet_id, schema, uuid(), options, callbacks, ts_proxy_));
  stepper->Start();
  ignore_result(stepper.release()); // Deletes self on callback.
}

Status RemoteKsckCluster::Connect() {
  KuduClientBuilder builder;
  builder.default_rpc_timeout(GetDefaultTimeout());
  builder.master_server_addrs(master_addresses_);
  ReplicaController::SetVisibility(&builder, ReplicaController::Visibility::ALL);
  return builder.Build(&client_);
}

Status RemoteKsckCluster::Build(const vector<string>& master_addresses,
                               shared_ptr<KsckCluster>* cluster) {
  CHECK(!master_addresses.empty());
  shared_ptr<Messenger> messenger;
  MessengerBuilder builder(kMessengerName);
  RETURN_NOT_OK(builder.Build(&messenger));
  auto* cl = new RemoteKsckCluster(master_addresses, messenger);
  for (const auto& master : cl->masters()) {
    RETURN_NOT_OK_PREPEND(master->Init(),
                          Substitute("unable to initialize master at $0", master->address()));
  }
  cluster->reset(cl);
  return Status::OK();
}

Status RemoteKsckCluster::RetrieveTabletServers() {
  vector<KuduTabletServer*> servers;
  ElementDeleter deleter(&servers);
  RETURN_NOT_OK(client_->ListTabletServers(&servers));

  TSMap tablet_servers;
  for (const auto* s : servers) {
    shared_ptr<RemoteKsckTabletServer> ts(
        new RemoteKsckTabletServer(s->uuid(),
                                   HostPort(s->hostname(), s->port()),
                                   messenger_));
    RETURN_NOT_OK(ts->Init());
    InsertOrDie(&tablet_servers, ts->uuid(), ts);
  }
  tablet_servers_.swap(tablet_servers);
  return Status::OK();
}

Status RemoteKsckCluster::RetrieveTablesList() {
  vector<string> table_names;
  RETURN_NOT_OK(client_->ListTables(&table_names));

  vector<shared_ptr<KsckTable>> tables_temp;
  for (const auto& n : table_names) {
    client::sp::shared_ptr<KuduTable> t;
    RETURN_NOT_OK(client_->OpenTable(n, &t));

    shared_ptr<KsckTable> table(new KsckTable(t->id(),
                                              n,
                                              *t->schema().schema_,
                                              t->num_replicas()));
    tables_temp.push_back(table);
  }
  tables_.swap(tables_temp);
  return Status::OK();
}

Status RemoteKsckCluster::RetrieveTabletsList(const shared_ptr<KsckTable>& table) {
  vector<shared_ptr<KsckTablet>> tablets;

  client::sp::shared_ptr<KuduTable> client_table;
  RETURN_NOT_OK(client_->OpenTable(table->name(), &client_table));

  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);

  KuduScanTokenBuilder builder(client_table.get());
  RETURN_NOT_OK(builder.Build(&tokens));
  for (const auto* t : tokens) {
    shared_ptr<KsckTablet> tablet(
        new KsckTablet(table.get(), t->tablet().id()));
    vector<shared_ptr<KsckTabletReplica>> replicas;
    for (const auto* r : t->tablet().replicas()) {
      replicas.push_back(std::make_shared<KsckTabletReplica>(
          r->ts().uuid(), r->is_leader(), ReplicaController::is_voter(*r)));
    }
    tablet->set_replicas(std::move(replicas));
    tablets.push_back(tablet);
  }

  table->set_tablets(tablets);
  return Status::OK();
}

} // namespace tools
} // namespace kudu
