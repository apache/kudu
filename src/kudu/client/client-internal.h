// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_CLIENT_INTERNAL_H
#define KUDU_CLIENT_CLIENT_INTERNAL_H

#include <boost/function.hpp>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/util/rw_semaphore.h"
#include "kudu/util/net/net_util.h"

namespace kudu {

class DnsResolver;
class HostPort;

namespace master {
class AlterTableRequestPB;
class CreateTableRequestPB;
class GetLeaderMasterRpc;
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
class RpcController;
} // namespace rpc

namespace client {

class KuduClient::Data {
 public:
  Data();
  ~Data();

  // Returns the ts that hosts a tablet with the given tablet ID, subject
  // to the given selection criteria.
  //
  // Note: failed replicas are ignored. If no appropriate replica could be
  // found, a non-OK status is returned and 'ts' is untouched.
  Status GetTabletServer(KuduClient* client,
                         const std::string& tablet_id,
                         ReplicaSelection selection,
                         internal::RemoteTabletServer** ts);

  Status CreateTable(KuduClient* client,
                     const master::CreateTableRequestPB& req,
                     const KuduSchema& schema,
                     const MonoTime& deadline);

  Status IsCreateTableInProgress(KuduClient* client,
                                 const std::string& table_name,
                                 const MonoTime& deadline,
                                 bool *create_in_progress);

  Status WaitForCreateTableToFinish(KuduClient* client,
                                    const std::string& table_name,
                                    const MonoTime& deadline);

  Status DeleteTable(KuduClient* client,
                     const std::string& table_name,
                     const MonoTime& deadline);

  Status AlterTable(KuduClient* client,
                    const master::AlterTableRequestPB& alter_steps,
                    const MonoTime& deadline);

  Status IsAlterTableInProgress(KuduClient* client,
                                const std::string& table_name,
                                const MonoTime& deadline,
                                bool *alter_in_progress);

  Status WaitForAlterTableToFinish(KuduClient* client,
                                   const std::string& alter_name,
                                   const MonoTime& deadline);

  Status GetTableSchema(KuduClient* client,
                        const std::string& table_name,
                        const MonoTime& deadline,
                        KuduSchema* schema);

  Status InitLocalHostNames();

  bool IsLocalHostPort(const HostPort& hp) const;

  bool IsTabletServerLocal(const internal::RemoteTabletServer& rts) const;

  // Returns the closest, non-failed replica to the client.
  //
  // Returns NULL if there are no tablet servers, or if they've all failed.
  // Given that the replica list may change at any time, callers should
  // always check the result against NULL.
  internal::RemoteTabletServer* PickClosestReplica(
      const scoped_refptr<internal::RemoteTablet>& rt) const;

  // Sets 'master_proxy_' from the address specified by
  // 'leader_master_hostport_'.  Called by
  // GetLeaderMasterRpc::SendRpcCb() upon successful completion.
  //
  // See also: SetMasterServerProxyAsync.
  void LeaderMasterDetermined(const StatusCallback& user_cb,
                              const Status& status);

  // Asynchronously sets 'master_proxy_' to the leader master by
  // cycling through servers listed in 'master_server_addrs_' until
  // one responds with a quorum configuration that contains the leader
  // master or 'deadline' expires.
  //
  // Invokes 'cb' with the appropriate status when finished.
  //
  // Works with both a distributed and non-distributed configuration.
  void SetMasterServerProxyAsync(KuduClient* client,
                                 const MonoTime& deadline,
                                 const StatusCallback& cb);

  // Synchronous version of SetMasterServerProxyAsync method above.
  //
  // NOTE: since this uses a Synchronizer, this may not be invoked by
  // a method that's on a reactor thread.
  //
  // TODO (KUDU-492): Get rid of this method and re-factor the client
  // to lazily initialize 'master_proxy_'.
  Status SetMasterServerProxy(KuduClient* client,
                              const MonoTime& deadline);

  master::MasterServiceProxy* master_proxy() const {
    return master_proxy_.get();
  }

  HostPort leader_master_hostport() const;

  // Retry 'func' until either:
  //
  // 1) Methods succeeds on a leader master.
  // 2) Method fails for a reason that is not related to network
  //    errors, timeouts, or leadership issues.
  // 3) 'deadline' (if initialized) elapses.
  //
  // If 'num_attempts' is not NULL, it will be incremented on every
  // attempt (successful or not) to call 'func'.
  //
  // NOTE: 'rpc_timeout' is a per-call timeout, while 'deadline' is a
  // per operation deadline. If 'deadline' is not initialized, 'func' is
  // retried forever.
  template<class ReqClass, class RespClass>
  Status SyncLeaderMasterRpc(
      const MonoTime& deadline,
      KuduClient* client,
      const ReqClass& req,
      RespClass* resp,
      int* num_attempts,
      const boost::function<Status(master::MasterServiceProxy*,
                                   const ReqClass&, RespClass*,
                                   rpc::RpcController*)>& func);

  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  gscoped_ptr<DnsResolver> dns_resolver_;
  scoped_refptr<internal::MetaCache> meta_cache_;

  // Set of hostnames and IPs on the local host.
  // This is initialized at client startup.
  std::tr1::unordered_set<std::string> local_host_names_;

  // Options the client was built with.
  std::vector<std::string> master_server_addrs_;
  MonoDelta default_admin_operation_timeout_;
  MonoDelta default_rpc_timeout_;

  // The host port of the leader master. This is set in
  // LeaderMasterDetermined, which is invoked as a callback by
  // SetMasterServerProxyAsync.
  HostPort leader_master_hostport_;

  // Proxy to the leader master.
  std::tr1::shared_ptr<master::MasterServiceProxy> master_proxy_;

  // Ref-counted RPC instance: since 'SetMasterServerProxyAsync' call
  // is asynchronous, we need to hold a reference in this class
  // itself, as to avoid a "use-after-free" scenario.
  scoped_refptr<master::GetLeaderMasterRpc> leader_master_rpc_;

  // Protects 'leader_master_rpc_', 'leader_master_hostport_'.
  //
  // See: KuduClient::Data::SetMasterServerProxyAsync for a more
  // in-depth explanation of why this is needed and how it works.
  mutable rw_semaphore leader_master_sem_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
