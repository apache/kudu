// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_INTEGRATION_TESTS_MINI_CLUSTER_H
#define KUDU_INTEGRATION_TESTS_MINI_CLUSTER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/macros.h"
#include "util/env.h"

namespace kudu {

namespace master {
class MiniMaster;
class TSDescriptor;
class TabletLocationsPB;
}

namespace tserver {
class MiniTabletServer;
}

// An in-process cluster with a MiniMaster and a configurable
// number of MiniTabletServers for use in tests.
class MiniCluster {
 public:
  MiniCluster(Env* env,
              const std::string& fs_root,
              int num_tablet_servers);
   ~MiniCluster();

   // Start a cluster with a Master and 'num_tablet_servers' TabletServers.
   // All servers run on the loopback interface with ephemeral ports.
  Status Start();

  // Like the previous method but performs initialization synchronously, i.e.
  // this will wait for all TS's to be started and initialized. Tests should
  // use this if they interact with tablets immediately after Start();
  Status StartSync();

  void Shutdown();

  // Add a new TS to the cluster. The new TS is started.
  // Requires that the master is already running.
  Status AddTabletServer();

  // Returns the Master for this MiniCluster.
  master::MiniMaster* mini_master() { return mini_master_.get(); }

  // Returns the TabletServer at index 'idx' of this MiniCluster.
  // 'idx' must be between 0 and 'num_tablet_servers' -1.
  tserver::MiniTabletServer* mini_tablet_server(int idx);

  string GetMasterFsRoot();

  string GetTabletServerFsRoot(int idx);

  // Wait for the given tablet to have 'expected_count' replicas
  // reported on the master.
  // Requires that the master has started.
  // Returns a bad Status if the tablet does not reach the required count
  // within kTabletReportWaitTimeSeconds.
  Status WaitForReplicaCount(const string& tablet_id, int expected_count);

  // Wait for the given tablet to have 'expected_count' replicas
  // reported on the master. Returns the locations in '*locations'.
  // Requires that the master has started;
  // Returns a bad Status if the tablet does not reach the required count
  // within kTabletReportWaitTimeSeconds.
  Status WaitForReplicaCount(const string& tablet_id,
                             int expected_count,
                             master::TabletLocationsPB* locations);

  // Wait until the number of registered tablet servers reaches the given
  // count. Returns Status::TimedOut if the desired count is not achieved
  // within kRegistrationWaitTimeSeconds.
  Status WaitForTabletServerCount(int count);
  Status WaitForTabletServerCount(int count,
                                  std::vector<std::tr1::shared_ptr<master::TSDescriptor> >* descs);

 private:
  enum {
    kTabletReportWaitTimeSeconds = 5,
    kRegistrationWaitTimeSeconds = 5
  };

  bool started_;

  Env* const env_;
  const std::string fs_root_;
  const int num_ts_initial_;

  gscoped_ptr<master::MiniMaster> mini_master_;
  std::vector<std::tr1::shared_ptr<tserver::MiniTabletServer> > mini_tablet_servers_;
};

} // namespace kudu

#endif /* KUDU_INTEGRATION_TESTS_MINI_CLUSTER_H */
