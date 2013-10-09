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
  Status Shutdown();

  // Returns the Master for this MiniCluster.
  master::MiniMaster* mini_master() { return mini_master_.get(); }

  // Returns the TabletServer at index 'idx' of this MiniCluster.
  // 'idx' must be between 0 and 'num_tablet_servers' -1.
  tserver::MiniTabletServer* mini_tablet_server(int idx);

  string GetMasterFsRoot();

  string GetTabletServerFsRoot(int idx);

 private:

  bool started_;

  Env* const env_;
  const std::string fs_root_;
  const int num_tablet_servers_;

  gscoped_ptr<master::MiniMaster> mini_master_;
  vector<std::tr1::shared_ptr<tserver::MiniTabletServer> > mini_tablet_servers_;
};

} // namespace kudu

#endif /* KUDU_INTEGRATION_TESTS_MINI_CLUSTER_H */
