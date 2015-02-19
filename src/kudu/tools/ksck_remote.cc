// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tools/ksck_remote.h"

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"

DEFINE_int64(timeout_ms, 1000 * 60, "RPC timeout in milliseconds");
DEFINE_int64(tablets_batch_size_max, 100, "How many tablets to get from the Master per RPC");

namespace kudu {
namespace tools {

static const std::string kMessengerName = "ksck";

using rpc::Messenger;
using rpc::MessengerBuilder;
using rpc::RpcController;
using std::tr1::shared_ptr;
using std::vector;
using std::string;
using strings::Substitute;

MonoDelta GetDefaultTimeout() {
  return MonoDelta::FromMilliseconds(FLAGS_timeout_ms);
}

Status RemoteKsckTabletServer::Connect() {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(address_, tserver::TabletServer::kDefaultPort, &addrs));
  proxy_.reset(new tserver::TabletServerServiceProxy(messenger_, addrs[0]));

  tserver::PingRequestPB req;
  tserver::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  last_connect_status_ = proxy_->Ping(req, &resp, &rpc);
  return last_connect_status_;
}

bool RemoteKsckTabletServer::IsConnected() const {
  return last_connect_status_.ok();
}

class ChecksumStepper;

// Simple class to act as a callback in order to collate results from parallel
// checksum scans.
class ChecksumCallbackHandler {
 public:
  explicit ChecksumCallbackHandler(ChecksumStepper* const stepper)
      : stepper(DCHECK_NOTNULL(stepper)) {
  }

  // Invoked by an RPC completion callback. Simply calls back into the stepper.
  // Then the call to the stepper returns, deletes 'this'.
  void Run();

 private:
  ChecksumStepper* const stepper;
};

// Simple class to have a "conversation" over multiple requests to a server
// to carry out a multi-part checksum scan.
// If any errors or timeouts are encountered, the checksum operation fails.
// After the ChecksumStepper reports its results to the reporter, it deletes itself.
class ChecksumStepper {
 public:
  ChecksumStepper(const string& tablet_id,
                  const Schema& schema,
                  const string& server_uuid,
                  const shared_ptr<ChecksumResultReporter>& reporter,
                  const shared_ptr<tserver::TabletServerServiceProxy>& proxy)
      : schema_(schema),
        tablet_id_(tablet_id),
        server_uuid_(server_uuid),
        reporter_(reporter),
        proxy_(proxy),
        call_seq_id_(0),
        checksum_(0) {
    DCHECK(reporter_);
    DCHECK(proxy_);
  }

  Status Start() {
    RETURN_NOT_OK(SchemaToColumnPBsWithoutIds(schema_, &cols_));
    SendRequest(kNewRequest);
    return Status::OK();
  }

  void HandleResponse() {
    gscoped_ptr<ChecksumStepper> deleter(this);
    Status s = rpc_.status();
    if (s.ok() && resp_.has_error()) {
      s = StatusFromPB(resp_.error().status());
    }
    if (!s.ok()) {
      reporter_->ReportError(tablet_id_, server_uuid_, s);
      return; // Deletes 'this'.
    }

    DCHECK(resp_.has_checksum());
    checksum_ = resp_.checksum();

    // Report back with results.
    if (!resp_.has_more_results()) {
      reporter_->ReportResult(tablet_id_, server_uuid_, checksum_);
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
  const shared_ptr<ChecksumResultReporter> reporter_;
  const shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  uint32_t call_seq_id_;
  string scanner_id_;
  uint64_t checksum_;
  tserver::ChecksumRequestPB req_;
  tserver::ChecksumResponsePB resp_;
  RpcController rpc_;
};


void ChecksumCallbackHandler::Run() {
  stepper->HandleResponse();
  delete this;
}

Status RemoteKsckTabletServer::RunTabletChecksumScanAsync(
        const string& tablet_id,
        const Schema& schema,
        const shared_ptr<ChecksumResultReporter>& reporter) {
  RETURN_NOT_OK(EnsureConnected());
  gscoped_ptr<ChecksumStepper> stepper(
      new ChecksumStepper(tablet_id, schema, uuid(), reporter, proxy_));
  RETURN_NOT_OK(stepper->Start());
  ignore_result(stepper.release()); // Deletes self on callback.
  return Status::OK();
}

Status RemoteKsckMaster::Connect() {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(address_, master::Master::kDefaultPort, &addrs));

  MessengerBuilder builder(kMessengerName);
  RETURN_NOT_OK(builder.Build(&messenger_));
  proxy_.reset(new master::MasterServiceProxy(messenger_, addrs[0]));

  master::PingRequestPB req;
  master::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  return proxy_->Ping(req, &resp, &rpc);
}

bool RemoteKsckMaster::IsConnected() const {
  return last_connect_status_.ok();
}

Status RemoteKsckMaster::RetrieveTabletServers(TSMap* tablet_servers) {
  master::ListTabletServersRequestPB req;
  master::ListTabletServersResponsePB resp;
  RpcController rpc;

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->ListTabletServers(req, &resp, &rpc));
  tablet_servers->clear();
  BOOST_FOREACH(const master::ListTabletServersResponsePB_Entry& e, resp.servers()) {
    HostPortPB addr = e.registration().rpc_addresses(0);
    HostPort hp(addr.host(), addr.port());
    shared_ptr<KsckTabletServer> ts(
        new RemoteKsckTabletServer(e.instance_id().permanent_uuid(), hp.ToString(), messenger_));
    InsertOrDie(tablet_servers, ts->uuid(), ts);
  }
  return Status::OK();
}

Status RemoteKsckMaster::RetrieveTablesList(vector<shared_ptr<KsckTable> >* tables) {
  master::ListTablesRequestPB req;
  master::ListTablesResponsePB resp;
  RpcController rpc;

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->ListTables(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  vector<shared_ptr<KsckTable> > tables_temp;
  BOOST_FOREACH(const master::ListTablesResponsePB_TableInfo& info, resp.tables()) {
    Schema schema;
    int num_replicas;
    RETURN_NOT_OK(GetTableInfo(info.name(), &schema, &num_replicas));
    shared_ptr<KsckTable> table(new KsckTable(info.name(), schema, num_replicas));
    tables_temp.push_back(table);
  }
  tables->assign(tables_temp.begin(), tables_temp.end());
  return Status::OK();
}

Status RemoteKsckMaster::RetrieveTabletsList(const shared_ptr<KsckTable>& table) {
  vector<shared_ptr<KsckTablet> > tablets;
  bool more_tablets = true;
  string last_key;
  while (more_tablets) {
    GetTabletsBatch(table->name(), &last_key, tablets, &more_tablets);
  }

  table->set_tablets(tablets);
  return Status::OK();
}

Status RemoteKsckMaster::GetTabletsBatch(const string& table_name,
                                         string* last_key,
                                         vector<shared_ptr<KsckTablet> >& tablets,
                                         bool* more_tablets) {
  master::GetTableLocationsRequestPB req;
  master::GetTableLocationsResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  req.set_max_returned_locations(FLAGS_tablets_batch_size_max);
  req.set_start_key(*last_key);

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->GetTableLocations(req, &resp, &rpc));
  BOOST_FOREACH(const master::TabletLocationsPB& locations, resp.tablet_locations()) {
    shared_ptr<KsckTablet> tablet(new KsckTablet(locations.tablet_id()));
    vector<shared_ptr<KsckTabletReplica> > replicas;
    BOOST_FOREACH(const master::TabletLocationsPB_ReplicaPB& replica, locations.replicas()) {
      bool is_leader = replica.role() == metadata::QuorumPeerPB::LEADER;
      bool is_follower = replica.role() == metadata::QuorumPeerPB::FOLLOWER;
      replicas.push_back(shared_ptr<KsckTabletReplica>(
          new KsckTabletReplica(replica.ts_info().permanent_uuid(), is_leader, is_follower)));
    }
    tablet->set_replicas(replicas);
    tablets.push_back(tablet);
  }
  if (resp.tablet_locations_size() != 0) {
    *last_key = (resp.tablet_locations().end() - 1)->end_key();
  } else {
    return Status::NotFound(Substitute(
      "The Master returned 0 tablets for GetTableLocations of table $0 at start key $1",
      table_name, *(last_key)));
  }
  if (last_key->empty()) {
    *more_tablets = false;
  }
  return Status::OK();
}

Status RemoteKsckMaster::GetTableInfo(const string& table_name, Schema* schema, int* num_replicas) {
  master::GetTableSchemaRequestPB req;
  master::GetTableSchemaResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->GetTableSchema(req, &resp, &rpc));

  RETURN_NOT_OK(SchemaFromPB(resp.schema(), schema));
  *num_replicas = resp.num_replicas();
  return Status::OK();
}

} // namespace tools
} // namespace kudu
