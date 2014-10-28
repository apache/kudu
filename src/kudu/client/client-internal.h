// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_CLIENT_INTERNAL_H
#define KUDU_CLIENT_CLIENT_INTERNAL_H

#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/util/net/net_util.h"

namespace kudu {

class DnsResolver;
class HostPort;

namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
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

  Status IsCreateTableInProgress(const std::string& table_name,
                                 const MonoTime& deadline,
                                 bool *create_in_progress);
  Status IsAlterTableInProgress(const std::string& table_name,
                                const MonoTime& deadline,
                                bool *alter_in_progress);

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

  // Sets 'master_proxy_' to the proxy the leader master by cycling
  // through servers listed in 'master_server_addrs_' until one
  // responds with a quorum configuration that contains the leader
  // master or 'default_select_master_timeout_' passes.
  //
  // Works with both a distributed and non-distributed configuration.
  Status SetMasterServerProxy();

  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  gscoped_ptr<DnsResolver> dns_resolver_;
  scoped_refptr<internal::MetaCache> meta_cache_;

  // Set of hostnames and IPs on the local host.
  // This is initialized at client startup.
  std::tr1::unordered_set<std::string> local_host_names_;

  // Options the client was built with.
  std::vector<std::string> master_server_addrs_;
  MonoDelta default_admin_operation_timeout_;
  MonoDelta default_select_master_timeout_;

  // Proxy to the leader master.
  std::tr1::shared_ptr<master::MasterServiceProxy> master_proxy_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
