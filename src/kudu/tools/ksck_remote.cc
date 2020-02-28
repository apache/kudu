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

#include <atomic>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <mutex>
#include <ostream>
#include <unordered_map>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/client/client-internal.h"
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
#include "kudu/master/master.pb.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_checksum.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/version_info.pb.h"

DECLARE_int64(timeout_ms); // defined in tool_action_common
DECLARE_int32(fetch_info_concurrency);

DEFINE_bool(checksum_cache_blocks, false, "Should the checksum scanners cache the read blocks.");
DEFINE_bool(quiescing_info, true,
            "Whether to display the quiescing-related information of each tablet server, "
            "e.g. number of tablet leaders per server, the number of active scanners "
            "per server.");

using kudu::client::KuduClientBuilder;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::internal::ReplicaController;
using kudu::cluster_summary::ServerHealth;
using kudu::master::ListTabletServersRequestPB;
using kudu::master::ListTabletServersResponsePB;
using kudu::master::TServerStatePB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

static const std::string kMessengerName = "ksck";

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
    unusual_flags_state_ = KsckFetchState::FETCH_FAILED;
  } else {
    unusual_flags_state_ = KsckFetchState::FETCHED;
    unusual_flags_ = std::move(resp);
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
  ts_admin_proxy_.reset(new tserver::TabletServerAdminServiceProxy(messenger_, addr, host));
  consensus_proxy_.reset(new consensus::ConsensusServiceProxy(messenger_, addr, host));
  return Status::OK();
}

Status RemoteKsckTabletServer::FetchInfo(ServerHealth* health) {
  DCHECK(health);
  state_ = KsckFetchState::FETCH_FAILED;
  *health = ServerHealth::UNAVAILABLE;
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
      *health = ServerHealth::WRONG_SERVER_UUID;
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

  RETURN_NOT_OK(FetchCurrentTimestamp());
  if (FLAGS_quiescing_info) {
    FetchQuiescingInfo();
  }

  state_ = KsckFetchState::FETCHED;
  *health = ServerHealth::HEALTHY;
  return Status::OK();
}

void RemoteKsckTabletServer::ServerClockResponseCallback::Run() {
  if (rpc.status().ok()) {
    ts->timestamp_ = resp.timestamp();
  } else {
    LOG(WARNING) << "Failed to retrieve timestamp from " << ts->uuid()
                 << ": " << rpc.status().ToString();
  }
  delete this;
}

void RemoteKsckTabletServer::FetchCurrentTimestampAsync() {
  // 'cb' deletes itself when complete.
  auto* cb = new ServerClockResponseCallback(shared_from_this());
  cb->rpc.set_timeout(GetDefaultTimeout());
  generic_proxy_->ServerClockAsync(cb->req,
                                   &cb->resp,
                                   &cb->rpc,
                                   boost::bind(&ServerClockResponseCallback::Run, cb));
}

Status RemoteKsckTabletServer::FetchCurrentTimestamp() {
  server::ServerClockRequestPB req;
  server::ServerClockResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(generic_proxy_->ServerClock(req, &resp, &rpc));
  timestamp_ = resp.timestamp();
  return Status::OK();
}

void RemoteKsckTabletServer::FetchQuiescingInfo() {
  tserver::QuiesceTabletServerRequestPB req;
  tserver::QuiesceTabletServerResponsePB resp;
  req.set_return_stats(true);
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  rpc.RequireServerFeature(tserver::TabletServerFeatures::QUIESCING);
  Status s = ts_admin_proxy_->Quiesce(req, &resp, &rpc);
  if (!s.ok()) {
    LOG(WARNING) << Substitute("Couldn't fetch quiescing info from tablet server $0 ($1): $2",
                               uuid_, address(), s.ToString());
    return;
  }
  cluster_summary::QuiescingInfo qinfo;
  qinfo.is_quiescing = resp.is_quiescing();
  qinfo.num_leaders = resp.num_leaders();
  qinfo.num_active_scanners = resp.num_active_scanners();
  quiescing_info_ = qinfo;
}

Status RemoteKsckTabletServer::FetchConsensusState(ServerHealth* health) {
  DCHECK(health);
  *health = ServerHealth::UNAVAILABLE;
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

  *health = ServerHealth::HEALTHY;
  return Status::OK();
}

Status RemoteKsckTabletServer::FetchUnusualFlags() {
  server::GetFlagsResponsePB resp;
  Status s = FetchUnusualFlagsCommon(generic_proxy_, &resp);
  if (!s.ok()) {
    unusual_flags_state_ = KsckFetchState::FETCH_FAILED;
  } else {
    unusual_flags_state_ = KsckFetchState::FETCHED;
    unusual_flags_ = std::move(resp);
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
  ChecksumStepper(string tablet_id,
                  Schema schema,
                  string server_uuid,
                  KsckChecksumOptions options,
                  shared_ptr<KsckChecksumManager> manager,
                  shared_ptr<tserver::TabletServerServiceProxy> proxy)
      : schema_(std::move(schema)),
        tablet_id_(std::move(tablet_id)),
        server_uuid_(std::move(server_uuid)),
        options_(std::move(options)),
        manager_(std::move(manager)),
        proxy_(std::move(proxy)),
        call_seq_id_(0),
        checksum_(0) {
    DCHECK(proxy_);
  }

  void Start() {
    Status s = SchemaToColumnPBs(schema_, &cols_,
                                 SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES);
    if (!s.ok()) {
      manager_->ReportResult(tablet_id_, server_uuid_, s, 0);
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
      manager_->ReportResult(tablet_id_, server_uuid_, s, 0);
      return; // Deletes 'this'.
    }
    if (resp_.has_resource_metrics() || resp_.has_rows_checksummed()) {
      int64_t bytes = resp_.resource_metrics().cfile_cache_miss_bytes() +
          resp_.resource_metrics().cfile_cache_hit_bytes();
      manager_->ReportProgress(resp_.rows_checksummed(), bytes);
    }
    DCHECK(resp_.has_checksum());
    checksum_ = resp_.checksum();

    // Report back with results.
    if (!resp_.has_more_results()) {
      manager_->ReportResult(tablet_id_, server_uuid_, s, checksum_);
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
  const KsckChecksumOptions options_;
  shared_ptr<KsckChecksumManager> const manager_;
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
        const KsckChecksumOptions& options,
        shared_ptr<KsckChecksumManager> manager) {
  gscoped_ptr<ChecksumStepper> stepper(
      new ChecksumStepper(tablet_id, schema, uuid(), options, manager, ts_proxy_));
  stepper->Start();
  ignore_result(stepper.release()); // Deletes self on callback.
}

RemoteKsckCluster::RemoteKsckCluster(std::vector<std::string> master_addresses,
                                     std::shared_ptr<rpc::Messenger> messenger)
    : master_addresses_(std::move(master_addresses)),
      messenger_(std::move(messenger)) {
  for (const std::string& master_addr : master_addresses_) {
    masters_.emplace_back(new RemoteKsckMaster(master_addr, messenger_));
  }
  CHECK_OK(ThreadPoolBuilder("RemoteKsckCluster-fetch")
               .set_max_threads(FLAGS_fetch_info_concurrency)
               .set_idle_timeout(MonoDelta::FromMilliseconds(10))
               .Build(&pool_));
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
  // Request that the tablet server states also be reported. This may include
  // information about tablet servers that have not yet registered with the
  // Masters.
  ListTabletServersRequestPB req;
  req.set_include_states(true);
  ListTabletServersResponsePB resp;
  RETURN_NOT_OK(client_->data_->ListTabletServers(
      client_.get(), MonoTime::Now() + client_->default_admin_operation_timeout(), req, &resp));

  TSMap tablet_servers;
  KsckTServerStateMap ts_states;
  for (int i = 0; i < resp.servers_size(); i++) {
    const auto& ts_pb = resp.servers(i);
    const auto& uuid = ts_pb.instance_id().permanent_uuid();
    if (ts_pb.has_state() && ts_pb.state() != TServerStatePB::NONE) {
      // We don't expect updates, but it's straightforward to handle, so let's
      // update instead of die just in case.
      EmplaceOrUpdate(&ts_states, uuid, ts_pb.state());
    }
    // If there's no registration for the tablet server, all we can really get
    // is the state, so move on.
    if (!ts_pb.has_registration()) {
      continue;
    }
    HostPort hp;
    RETURN_NOT_OK(HostPortFromPB(ts_pb.registration().rpc_addresses(0), &hp));
    auto ts = std::make_shared<RemoteKsckTabletServer>(uuid, hp, messenger_, ts_pb.location());
    RETURN_NOT_OK(ts->Init());
    EmplaceOrUpdate(&tablet_servers, uuid, std::move(ts));
  }
  tablet_servers_ = std::move(tablet_servers);
  ts_states_ = std::move(ts_states);
  return Status::OK();
}

Status RemoteKsckCluster::RetrieveTablesList() {
  vector<string> table_names;
  RETURN_NOT_OK(client_->ListTables(&table_names));

  if (table_names.empty()) {
    return Status::OK();
  }

  vector<shared_ptr<KsckTable>> tables;
  tables.reserve(table_names.size());
  simple_spinlock tables_lock;
  int tables_count = 0;
  filtered_tables_count_ = 0;
  filtered_tablets_count_ = 0;

  for (const auto& table_name : table_names) {
    if (!MatchesAnyPattern(table_filters_, table_name)) {
      filtered_tables_count_++;
      VLOG(1) << "Skipping table " << table_name;
      continue;
    }
    tables_count++;
    RETURN_NOT_OK(pool_->SubmitFunc([&]() {
      client::sp::shared_ptr<KuduTable> t;
      Status s = client_->OpenTable(table_name, &t);
      if (!s.ok()) {
        LOG(ERROR) << Substitute("unable to open table $0: $1",
                                 table_name, s.ToString());
        return;
      }
      shared_ptr<KsckTable> table(new KsckTable(t->id(),
                                                table_name,
                                                KuduSchema::ToSchema(t->schema()),
                                                t->num_replicas()));
      {
        std::lock_guard<simple_spinlock> l(tables_lock);
        tables.emplace_back(std::move(table));
      }
    }));
  }
  pool_->Wait();

  tables_.swap(tables);

  if (tables_.size() < tables_count) {
    return Status::NetworkError(
        Substitute("failed to gather info from all filtered tables: $0 of $1 had errors",
                   tables_count - tables_.size(), tables_count));
  }

  return Status::OK();
}

Status RemoteKsckCluster::RetrieveAllTablets() {
  if (tables_.empty()) {
    return Status::OK();
  }

  for (const auto& table : tables_) {
    RETURN_NOT_OK(pool_->SubmitFunc(
        std::bind(&KsckCluster::RetrieveTabletsList, this, table)));
  }
  pool_->Wait();

  return Status::OK();
}

Status RemoteKsckCluster::RetrieveTabletsList(const shared_ptr<KsckTable>& table) {
  client::sp::shared_ptr<KuduTable> client_table;
  RETURN_NOT_OK(client_->OpenTable(table->name(), &client_table));

  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);

  KuduScanTokenBuilder builder(client_table.get());
  RETURN_NOT_OK(builder.Build(&tokens));

  vector<shared_ptr<KsckTablet>> tablets;
  for (const auto* t : tokens) {
    if (!MatchesAnyPattern(tablet_id_filters_, t->tablet().id())) {
      filtered_tablets_count_++;
      VLOG(1) << "Skipping tablet " << t->tablet().id();
      continue;
    }
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
