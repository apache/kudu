// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// This header file contains generic helper utilities for writing tests against
// MiniClusters and ExternalMiniClusters. Ideally, the functions will be
// generic enough to use with either type of cluster, due to operating
// primarily through RPC-based APIs or through KuduClient.
// However, it's also OK to include common operations against a particular
// cluster type if it's general enough to use from multiple tests while not
// belonging in the MiniCluster / ExternalMiniCluster classes themselves. But
// consider just putting stuff like that in those classes.

#ifndef KUDU_INTEGRATION_TESTS_CLUSTER_ITEST_UTIL_H_
#define KUDU_INTEGRATION_TESTS_CLUSTER_ITEST_UTIL_H_

#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/master.pb.h"

namespace kudu {
class MonoDelta;
class Sockaddr;
class Status;

namespace client {
class KuduClient;
class KuduSchema;
class KuduTable;
}

namespace consensus {
class ConsensusServiceProxy;
class OpId;
}

namespace master {
class MasterServiceProxy;
}

namespace rpc {
class Messenger;
}

namespace tserver {
class TabletServerAdminServiceProxy;
class TabletServerServiceProxy;
}

namespace itest {

struct TServerDetails {
  NodeInstancePB instance_id;
  master::TSRegistrationPB registration;
  gscoped_ptr<tserver::TabletServerServiceProxy> tserver_proxy;
  gscoped_ptr<tserver::TabletServerAdminServiceProxy> tserver_admin_proxy;
  gscoped_ptr<consensus::ConsensusServiceProxy> consensus_proxy;

  // Convenience function to get the UUID from the instance_id struct.
  const std::string& uuid() const;

  std::string ToString() const;
};

// tablet_id -> replica map.
typedef std::tr1::unordered_multimap<std::string, TServerDetails*> TabletReplicaMap;

// uuid -> tablet server map.
typedef std::tr1::unordered_map<std::string, TServerDetails*> TabletServerMap;

// Returns possibly the simplest imaginable schema, with a single int key column.
client::KuduSchema SimpleIntKeyKuduSchema();

// Create a populated TabletServerMap by interrogating the master.
// Note: The bare-pointer TServerDetails values must be deleted by the caller!
// Consider using ValueDeleter (in gutil/stl_util.h) for that.
Status CreateTabletServerMap(master::MasterServiceProxy* master_proxy,
                             const std::tr1::shared_ptr<rpc::Messenger>& messenger,
                             std::tr1::unordered_map<std::string, TServerDetails*>* ts_map);

// Gets a vector containing the latest OpId for each of the given replicas.
// Returns a bad Status if any replica cannot be reached.
Status GetLastOpIdForEachReplica(const std::string& tablet_id,
                                 const std::vector<TServerDetails*>& replicas,
                                 std::vector<consensus::OpId>* op_ids);

// Like the above, but for a single replica.
Status GetLastOpIdForReplica(const std::string& tablet_id,
                             TServerDetails* replica,
                             consensus::OpId* op_id);

// Wait until all of the servers have converged on the same log index.
// The converged index must be at least equal to 'minimum_index'.
//
// Requires that all servers are running. Returns Status::TimedOut if the
// indexes do not converge within the given timeout.
Status WaitForServersToAgree(const MonoDelta& timeout,
                             const TabletServerMap& tablet_servers,
                             const std::string& tablet_id,
                             int64_t minimum_index);

// Wait until all specified replicas have logged at least the given index.
// Unlike WaitForServersToAgree(), the servers do not actually have to converge
// or quiesce. They only need to progress to or past the given index.
Status WaitUntilAllReplicasHaveOp(const int64_t log_index,
                                  const std::string& tablet_id,
                                  const std::vector<TServerDetails*>& replicas,
                                  const MonoDelta& timeout);

} // namespace itest
} // namespace kudu

#endif // KUDU_INTEGRATION_TESTS_CLUSTER_ITEST_UTIL_H_
