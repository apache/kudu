// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/client-internal.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <limits>
#include <string>
#include <vector>

#include "kudu/client/meta_cache.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master_rpc.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"

namespace kudu {

using master::CreateTableRequestPB;
using master::CreateTableResponsePB;
using master::GetLeaderMasterRpc;
using master::GetTableSchemaRequestPB;
using master::GetTableSchemaResponsePB;
using master::IsAlterTableDoneRequestPB;
using master::IsAlterTableDoneResponsePB;
using master::IsCreateTableDoneRequestPB;
using master::IsCreateTableDoneResponsePB;
using master::MasterServiceProxy;
using master::MasterErrorPB;
using metadata::QuorumPeerPB;

using rpc::Rpc;
using rpc::RpcController;
using strings::Substitute;

namespace client {

using internal::GetTableSchemaRpc;
using internal::RemoteTablet;
using internal::RemoteTabletServer;

// Retry helper, takes a function like: Status funcName(const MonoTime& deadline, bool *retry, ...)
// The function should set the retry flag (default true) if the function should
// be retried again. On retry == false the return status of the function will be
// returned to the caller, otherwise a Status::Timeout() will be returned.
// If the deadline is already expired, no attempt will be made.
static Status RetryFunc(const MonoTime& deadline,
                        const string& retry_msg,
                        const string& timeout_msg,
                        const boost::function<Status(const MonoTime&, bool*)>& func) {
  MonoTime now = MonoTime::Now(MonoTime::FINE);
  if (deadline.Initialized() && !now.ComesBefore(deadline)) {
    return Status::TimedOut(timeout_msg);
  }

  int64_t wait_time = 1000;
  while (1) {
    MonoTime stime = now;
    bool retry = true;
    Status s = func(deadline, &retry);
    if (!retry) {
      return s;
    }

    if (deadline.Initialized()) {
      now = MonoTime::Now(MonoTime::FINE);
      if (!now.ComesBefore(deadline)) {
        break;
      }
    }

    VLOG(1) << retry_msg << " status=" << s.ToString();
    int64_t timeout_usec = std::numeric_limits<uint64_t>::max();
    if (deadline.Initialized()) {
      timeout_usec = deadline.GetDeltaSince(now).ToNanoseconds() -
                     now.GetDeltaSince(stime).ToNanoseconds();
    }
    if (timeout_usec > 0) {
      wait_time = std::min(wait_time * 5 / 4, timeout_usec);
      usleep(wait_time);
      now = MonoTime::Now(MonoTime::FINE);
    }
  }

  return Status::TimedOut(timeout_msg);
}

template<class ReqClass, class RespClass>
Status KuduClient::Data::SyncLeaderMasterRpc(
    const MonoDelta& rpc_timeout,
    const MonoTime& deadline,
    KuduClient* client,
    const ReqClass& req,
    RespClass* resp,
    int* num_attempts,
    const boost::function<Status(MasterServiceProxy*,
                                 const ReqClass&,
                                 RespClass*,
                                 RpcController*)>& func) {
  while (true) {
    RpcController rpc;

    // Set the per-rpc deadline
    MonoTime rpc_deadline = MonoTime::Now(MonoTime::FINE);
    rpc_deadline.AddDelta(rpc_timeout);

    // If a global (per entire operation) deadline is set:
    // 1) Check whether or not we've exceed the deadline.
    // 2) Set per-RPC deadline to min(now + rpc_timeout, deadline).
    if (deadline.Initialized()) {
      MonoTime now = MonoTime::Now(MonoTime::FINE);
      if (deadline.ComesBefore(now)) {
        return Status::TimedOut("timed out waiting for a reply from a leader master");
      }
      rpc_deadline = MonoTime::Earliest(rpc_deadline, deadline);
    }

    rpc.set_deadline(rpc_deadline);

    if (num_attempts != NULL) {
      ++*num_attempts;
    }

    Status s = func(master_proxy_.get(), req, resp, &rpc);
    if (!s.ok()) {
      if (s.IsNetworkError() || s.IsTimedOut()) {
        LOG(WARNING) << "Unable to send the request (" << req.ShortDebugString()
                     << ") to leader Master (" << leader_master_hostport().ToString()
                     << "): " << s.ToString();
        if (master_server_addrs_.size() > 1) {
          LOG(INFO) << "Determining the new leader Master and retrying...";
          s = SetMasterServerProxy(client);
          if (s.ok()) {
            continue;
          }
          LOG(WARNING) << "Unable to determine the new leader Master: " << s.ToString();
        }
      }
    }
    if (s.ok() && resp->has_error()) {
      if (resp->error().code() == MasterErrorPB::NOT_THE_LEADER ||
          resp->error().code() == MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED) {
        if (master_server_addrs_.size() > 1) {
          LOG(INFO) << "Determining the new leader Master and retrying...";
          s = SetMasterServerProxy(client);
          if (s.ok()) {
            continue;
          }
          LOG(WARNING) << "Unable to determine the new leader Master: " << s.ToString();
        }
      }
    }
    return s;
  }
}

KuduClient::Data::Data() {
}

KuduClient::Data::~Data() {
}

Status KuduClient::Data::GetTabletServer(KuduClient* client,
                                         const string& tablet_id,
                                         ReplicaSelection selection,
                                         RemoteTabletServer** ts) {
  // TODO: write a proper async version of this for async client.
  scoped_refptr<RemoteTablet> remote_tablet;
  meta_cache_->LookupTabletByID(tablet_id, &remote_tablet);

  RemoteTabletServer* ret = NULL;
  switch (selection) {
    case LEADER_ONLY:
      ret = remote_tablet->LeaderTServer();
      break;
    case CLOSEST_REPLICA:
      ret = PickClosestReplica(remote_tablet);
      break;
    case FIRST_REPLICA:
      ret = remote_tablet->FirstTServer();
      break;
    default:
      LOG(FATAL) << "Unknown ProxySelection value " << selection;
  }
  if (PREDICT_FALSE(ret == NULL)) {
    return Status::ServiceUnavailable(
        Substitute("No $0 for tablet $1",
                   selection == LEADER_ONLY ? "LEADER" : "replicas", tablet_id));
  }
  Synchronizer s;
  ret->RefreshProxy(client, s.AsStatusCallback(), false);
  RETURN_NOT_OK(s.Wait());

  *ts = ret;
  return Status::OK();
}

Status KuduClient::Data::CreateTable(KuduClient* client,
                                     const CreateTableRequestPB& req,
                                     const KuduSchema& schema,
                                     const MonoTime& deadline) {
  CreateTableResponsePB resp;

  int attempts = 0;
  while (true) {
    Status s =
        SyncLeaderMasterRpc<CreateTableRequestPB,
                            CreateTableResponsePB>(
                                default_admin_operation_timeout_,
                                deadline,
                                client,
                                req,
                                &resp,
                                &attempts,
                                &MasterServiceProxy::CreateTable);
    if (s.ok() && resp.has_error()) {
      if (resp.error().code() == MasterErrorPB::TABLE_ALREADY_PRESENT && attempts > 0) {
        // If the table already exists and the number of attempts is >
        // 0, then it means we may have succeeded in creating the
        // table quorum, but client didn't receive the succesful
        // response (e.g., due to failure before the succesful
        // response could be sent back, or due to a I/O pause or a
        // network blip leading to a timeout, etc...)
        KuduSchema actual_schema;
        RETURN_NOT_OK_PREPEND(
            GetTableSchema(client, req.name(), deadline, &actual_schema),
            Substitute("Unable to check the schema of table $0", req.name()));
        if (schema.Equals(actual_schema)) {
          break;
        } else {
          LOG(ERROR) << "Table " << req.name() << " already exists with a different "
              "schema. Requested schema was: " << schema.schema_->ToString()
                     << ", actual schema is: " << actual_schema.schema_->ToString();
          return s;
        }
      }
      s = StatusFromPB(resp.error().status());
    }
    if (!s.ok()) {
      // If the error is not a leadership issue or a timeout, warn and
      // re-try.  SyncLeaderMasterRpc will check if whether or not
      // 'deadline' has passed.
      LOG(WARNING) << "Unexpected error creating table " << req.name() << ": "
                   << s.ToString();
      continue;
    }
    break;
  }
  return Status::OK();
}

Status KuduClient::Data::IsCreateTableInProgress(KuduClient* client,
                                                 const string& table_name,
                                                 const MonoTime& deadline,
                                                 bool *create_in_progress) {
  IsCreateTableDoneRequestPB req;
  IsCreateTableDoneResponsePB resp;
  req.mutable_table()->set_table_name(table_name);

  Status s =
      SyncLeaderMasterRpc<IsCreateTableDoneRequestPB, IsCreateTableDoneResponsePB>(
          default_admin_operation_timeout_,
          deadline,
          client,
          req,
          &resp,
          NULL,
          &MasterServiceProxy::IsCreateTableDone);
  // RETURN_NOT_OK macro can't take templated function call as param,
  // and SyncLeaderMasterRpc must be explicitly instantiated, else the
  // compiler complains.
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *create_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::Data::WaitForCreateTableToFinish(KuduClient* client,
                                                    const string& table_name,
                                                    const MonoTime& deadline) {
  return RetryFunc(deadline,
                   "Waiting on Create Table to be completed",
                   "Timeout out waiting for Table Creation",
                   boost::bind(&KuduClient::Data::IsCreateTableInProgress,
                               this, client, table_name, _1, _2));
}


Status KuduClient::Data::IsAlterTableInProgress(const string& table_name,
                                                const MonoTime& deadline,
                                                bool *alter_in_progress) {
  IsAlterTableDoneRequestPB req;
  IsAlterTableDoneResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE)));
  RETURN_NOT_OK(master_proxy_->IsAlterTableDone(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *alter_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::Data::WaitForAlterTableToFinish(const string& alter_name,
                                                   const MonoTime& deadline) {
  return RetryFunc(deadline,
                   "Waiting on Alter Table to be completed",
                   "Timeout out waiting for AlterTable",
                   boost::bind(&KuduClient::Data::IsAlterTableInProgress,
                               this,
                               alter_name, _1, _2));
}

Status KuduClient::Data::InitLocalHostNames() {
  // Currently, we just use our configured hostname, and resolve it to come up with
  // a list of potentially local hosts. It would be better to iterate over all of
  // the local network adapters. See KUDU-327.
  string hostname;
  RETURN_NOT_OK(GetHostname(&hostname));

  // We don't want to consider 'localhost' to be local - otherwise if a misconfigured
  // server reports its own name as localhost, all clients will hammer it.
  if (hostname != "localhost" && hostname != "localhost.localdomain") {
    local_host_names_.insert(hostname);
    VLOG(1) << "Considering host " << hostname << " local";
  }

  vector<Sockaddr> addresses;
  RETURN_NOT_OK_PREPEND(HostPort(hostname, 0).ResolveAddresses(&addresses),
                        Substitute("Could not resolve local host name '$0'", hostname));

  BOOST_FOREACH(const Sockaddr& addr, addresses) {
    // Similar to above, ignore local or wildcard addresses.
    if (addr.IsWildcard()) continue;
    if (addr.IsAnyLocalAddress()) continue;

    VLOG(1) << "Considering host " << addr.host() << " local";
    local_host_names_.insert(addr.host());
  }

  return Status::OK();
}

bool KuduClient::Data::IsLocalHostPort(const HostPort& hp) const {
  return ContainsKey(local_host_names_, hp.host());
}

bool KuduClient::Data::IsTabletServerLocal(const RemoteTabletServer& rts) const {
  vector<HostPort> host_ports;
  rts.GetHostPorts(&host_ports);
  BOOST_FOREACH(const HostPort& hp, host_ports) {
    if (IsLocalHostPort(hp)) return true;
  }
  return false;
}

RemoteTabletServer* KuduClient::Data::PickClosestReplica(
  const scoped_refptr<RemoteTablet>& rt) const {

  vector<RemoteTabletServer*> candidates;
  rt->GetRemoteTabletServers(&candidates);

  BOOST_FOREACH(RemoteTabletServer* rts, candidates) {
    if (IsTabletServerLocal(*rts)) {
      return rts;
    }
  }

  // No local one found. Pick a random one
  return !candidates.empty() ? candidates[rand() % candidates.size()] : NULL;
}

namespace internal {

// Gets a table's schema from the leader master. If the leader master
// is down, waits for a new master to become the leader, and then gets
// the table schema from the new leader master.
//
// TODO: When we implement the next fault tolerant client-master RPC
// call (e.g., CreateTable/AlterTable), we should generalize this
// method as to enable code sharing.
class GetTableSchemaRpc : public Rpc {
 public:
  GetTableSchemaRpc(KuduClient* client,
                    const StatusCallback& user_cb,
                    const string& table_name,
                    KuduSchema *out_schema,
                    const MonoTime& deadline,
                    const shared_ptr<rpc::Messenger>& messenger);

  virtual void SendRpc() OVERRIDE;

  virtual string ToString() const OVERRIDE;

  virtual ~GetTableSchemaRpc();

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  void ResetLeaderMasterAndRetry();

  void NewLeaderMasterDeterminedCb(const Status& status);

  KuduClient* client_;
  StatusCallback user_cb_;
  const string table_name_;
  KuduSchema* out_schema_;
  const MonoTime create_time_;
  GetTableSchemaResponsePB resp_;
};

GetTableSchemaRpc::GetTableSchemaRpc(KuduClient* client,
                                     const StatusCallback& user_cb,
                                     const string& table_name,
                                     KuduSchema* out_schema,
                                     const MonoTime& deadline,
                                     const shared_ptr<rpc::Messenger>& messenger)
    : Rpc(deadline, messenger),
      client_(client),
      user_cb_(user_cb),
      table_name_(table_name),
      out_schema_(out_schema),
      create_time_(MonoTime::Now(MonoTime::FINE)) {
  DCHECK(client);
  DCHECK(out_schema);
}

GetTableSchemaRpc::~GetTableSchemaRpc() {
}

void GetTableSchemaRpc::SendRpc() {
  GetTableSchemaRequestPB req;
  req.mutable_table()->set_table_name(table_name_);
  client_->data_->master_proxy()->GetTableSchemaAsync(
      req, &resp_, &retrier().controller(),
      boost::bind(&GetTableSchemaRpc::SendRpcCb, this, Status::OK()));
}

string GetTableSchemaRpc::ToString() const {
  return Substitute("GetTableSchemaRpc(table_name=$0)",
                    table_name_);
}

void GetTableSchemaRpc::ResetLeaderMasterAndRetry() {
  client_->data_->SetMasterServerProxyAsync(
      client_,
      Bind(&GetTableSchemaRpc::NewLeaderMasterDeterminedCb,
           Unretained(this)));
}

void GetTableSchemaRpc::NewLeaderMasterDeterminedCb(const Status& status) {
  if (status.ok()) {
    retrier().controller().Reset();
    SendRpc();
  } else {
    LOG(WARNING) << "Failed to determine new Master: " << status.ToString();
    retrier().DelayedRetry(this);
  }
}

void GetTableSchemaRpc::SendRpcCb(const Status& status) {
  Status new_status = status;
  if (new_status.ok() && retrier().HandleResponse(this, &new_status)) {
    return;
  }

  if (new_status.ok() && resp_.has_error()) {
    if (resp_.error().code() == MasterErrorPB::NOT_THE_LEADER ||
        resp_.error().code() == MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED) {
      LOG(WARNING) << "Leader Master has changed ("
                   << client_->data_->leader_master_hostport().ToString()
                   << " is no longer the leader), re-trying...";
      ResetLeaderMasterAndRetry();
      return;
    }
    new_status = StatusFromPB(resp_.error().status());
  }

  if (new_status.IsTimedOut()) {
    if (MonoTime::Now(MonoTime::FINE).GetDeltaSince(create_time_).LessThan(
            client_->data_->default_select_master_timeout_)) {
      LOG(WARNING) << "Leader Master (" << client_->data_->leader_master_hostport().ToString()
                   << ") timed out, re-trying...";
      ResetLeaderMasterAndRetry();
      return;
    }
  }
  if (new_status.IsNetworkError()) {
    LOG(WARNING) << "Encountered a network error from the Master("
                 << client_->data_->leader_master_hostport().ToString() << "): "
                 << new_status.ToString() << ", retrying...";
    ResetLeaderMasterAndRetry();
    return;
  }

  if (new_status.ok()) {
    Schema server_schema;
    new_status = SchemaFromPB(resp_.schema(), &server_schema);
    if (new_status.ok()) {
      gscoped_ptr<Schema> client_schema(new Schema());
      client_schema->Reset(server_schema.columns(), server_schema.num_key_columns());
      out_schema_->schema_.swap(client_schema);
    }
  }
  if (!new_status.ok()) {
    LOG(WARNING) << ToString() << " failed: " << new_status.ToString();
  }
  user_cb_.Run(new_status);
}

} // namespace internal

Status KuduClient::Data::GetTableSchema(KuduClient* client,
                                        const string& table_name,
                                        const MonoTime& deadline,
                                        KuduSchema* schema) {
  Synchronizer sync;
  GetTableSchemaRpc rpc(client,
                        sync.AsStatusCallback(),
                        table_name,
                        schema,
                        deadline,
                        messenger_);
  rpc.SendRpc();
  return sync.Wait();
}

void KuduClient::Data::LeaderMasterDetermined(const StatusCallback& user_cb,
                                              const Status& status) {
  Status new_status = status;
  // Make a defensive copy of 'user_cb', as it may be deallocated
  // after 'leader_master_rpc_' is reset.
  StatusCallback cb_copy(user_cb);
  if (new_status.ok()) {
    Sockaddr leader_sock_addr;
    new_status = SockaddrFromHostPort(leader_master_hostport_, &leader_sock_addr);
    if (new_status.ok()) {
      master_proxy_.reset(new MasterServiceProxy(messenger_, leader_sock_addr));
    }
  }
  // See the comment in SetMasterServerProxyAsync below.
  leader_master_rpc_.reset();
  leader_master_sem_.unlock();
  cb_copy.Run(new_status);
}

Status KuduClient::Data::SetMasterServerProxy(KuduClient* client) {
  Synchronizer sync;
  SetMasterServerProxyAsync(client, sync.AsStatusCallback());
  return sync.Wait();
}

void KuduClient::Data::SetMasterServerProxyAsync(KuduClient* client, const StatusCallback& cb) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_select_master_timeout_);
  vector<Sockaddr> master_sockaddrs;
  BOOST_FOREACH(const string& master_server_addr, master_server_addrs_) {
    vector<Sockaddr> addrs;
    Status s;
    // TODO: Do address resolution asynchronously as well.
    s = ParseAddressList(master_server_addr, master::Master::kDefaultPort, &addrs);
    if (!s.ok()) {
      cb.Run(s);
      return;
    }
    if (addrs.empty()) {
      cb.Run(Status::InvalidArgument(Substitute("No master address specified by '$0'",
                                                master_server_addr)));
      return;
    }
    if (addrs.size() > 1) {
      LOG(WARNING) << "Specified master server address '" << master_server_addr << "' "
                   << "resolved to multiple IPs. Using " << addrs[0].ToString();
    }
    master_sockaddrs.push_back(addrs[0]);
  }

  // This ensures that no more than one GetLeaderMasterRpc is in
  // flight.  The reason for this is that we need to keep hold of a
  // reference to an in-flight RPC ('leader_master_rpc_') in order not
  // to free it before the RPC completes.
  //
  // The other approach would be to keep a container of references to
  // GetLeaderMasterRpc and the HostPort, but this would have an issue
  // with maintaining an index (or another form of a key) into that
  // container in order to find the right GetLeaderMasterRpc reference
  // and HostPort instance in order to destroy them.
  //
  // (We can't pass 'leader_master_rpc_' to the callback as a
  // scoped_refptr can't be passed into a kudu::StatusCallback using a
  // Bind).
  leader_master_sem_.lock();
  leader_master_rpc_.reset(new GetLeaderMasterRpc(
      Bind(&KuduClient::Data::LeaderMasterDetermined,
           Unretained(this), cb),
      master_sockaddrs,
      deadline,
      messenger_,
      &leader_master_hostport_));
  leader_master_rpc_->SendRpc();
}

HostPort KuduClient::Data::leader_master_hostport() const {
  shared_lock<rw_semaphore> l(&leader_master_sem_);
  return leader_master_hostport_;
}

} // namespace client
} // namespace kudu
