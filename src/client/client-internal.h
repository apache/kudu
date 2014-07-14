// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_CLIENT_INTERNAL_H
#define KUDU_CLIENT_CLIENT_INTERNAL_H

#include <string>

#include "client/client.h"
#include "util/net/net_util.h"

namespace kudu {

class DnsResolver;

namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
} // namespace rpc

namespace client {

class KuduClient::Data {
 public:
  explicit Data(const KuduClientOptions& options);
  ~Data();

  Status Init(const std::tr1::shared_ptr<KuduClient>& client);

  // Returns the ts that hosts a tablet with the given tablet ID, subject
  // to the given selection criteria.
  //
  // Note: failed replicas are ignored. If no appropriate replica could be
  // found, a non-OK status is returned and 'ts' is untouched.
  Status GetTabletServer(KuduClient* client,
                         const std::string& tablet_id,
                         ReplicaSelection selection,
                         RemoteTabletServer** ts);

  Status IsCreateTableInProgress(const std::string& table_name,
                                 const MonoTime& deadline,
                                 bool *create_in_progress);
  Status IsAlterTableInProgress(const std::string& table_name,
                                const MonoTime& deadline,
                                bool *alter_in_progress);

  Status InitLocalHostNames();

  bool IsLocalHostPort(const HostPort& hp) const;

  bool IsTabletServerLocal(const RemoteTabletServer& rts) const;

  // Returns the closest, non-failed replica to the client.
  //
  // Returns NULL if there are no tablet servers, or if they've all failed.
  // Given that the replica list may change at any time, callers should
  // always check the result against NULL.
  RemoteTabletServer* PickClosestReplica(const scoped_refptr<RemoteTablet>& rt) const;

  bool initted_;
  KuduClientOptions options_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;

  gscoped_ptr<DnsResolver> dns_resolver_;
  scoped_refptr<MetaCache> meta_cache_;

  // Set of hostnames and IPs on the local host.
  // This is initialized at client startup.
  std::tr1::unordered_set<std::string> local_host_names_;

  // Proxy to the master.
  std::tr1::shared_ptr<master::MasterServiceProxy> master_proxy_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
