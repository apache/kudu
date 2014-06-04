// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_INTEGRATION_TESTS_EXTERNAL_MINI_CLUSTER_H
#define KUDU_INTEGRATION_TESTS_EXTERNAL_MINI_CLUSTER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/ref_counted.h"
#include "util/monotime.h"
#include "util/status.h"

namespace kudu {

class ExternalDaemon;
class ExternalMaster;
class ExternalTabletServer;
class HostPort;
class NodeInstancePB;
class Sockaddr;
class Subprocess;

namespace client {
class KuduClient;
struct KuduClientOptions;
} // namespace client

namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
} // namespace rpc

namespace server {
class ServerStatusPB;
} // namespace server

struct ExternalMiniClusterOptions {
  ExternalMiniClusterOptions();
  ~ExternalMiniClusterOptions();

  // Number of TS to start.
  // Default: 1
  int num_tablet_servers;

  // Directory in which to store data.
  // Default: "", which auto-generates a unique path for this cluster.
  std::string data_root;

  // The path where the kudu daemons should be run from.
  // Default: "", which uses the same path as the currently running executable.
  // This works for unit tests, since they all end up in build/latest/.
  std::string daemon_bin_path;

  // Extra flags for tablet servers and masters respectively.
  //
  // In these flags, you may use the special string '${index}' which will
  // be substituted with the index of the tablet server or master.
  std::vector<std::string> extra_tserver_flags;
  std::vector<std::string> extra_master_flags;
};

// A mini-cluster made up of subprocesses running each of the daemons
// separately. This is useful for black-box or grey-box failure testing
// purposes -- it provides the ability to forcibly kill or stop particular
// cluster participants, which isn't feasible in the normal MiniCluster.
// On the other hand, there is little access to inspect the internal state
// of the daemons.
class ExternalMiniCluster {
 public:
  explicit ExternalMiniCluster(const ExternalMiniClusterOptions& opts);
  ~ExternalMiniCluster();

  // Start the cluster.
  Status Start();

  // Like the previous method but performs initialization synchronously, i.e.
  // this will wait for all TS's to be started and initialized. Tests should
  // use this if they interact with tablets immediately after Start();
  Status StartSync();

  // Add a new TS to the cluster. The new TS is started.
  // Requires that the master is already running.
  Status AddTabletServer();

  // Shuts down the cluster.
  // Currently, this uses SIGKILL on each daemon for a non-graceful shutdown.
  void Shutdown();

  // Return a pointer to the running master. This may be NULL if the cluster
  // is not started.
  ExternalMaster* master() { return master_.get(); }

  ExternalTabletServer* tablet_server(int idx) {
    CHECK_LT(idx, tablet_servers_.size());
    return tablet_servers_[idx].get();
  }

  // Return an RPC proxy to the running master. Requires that the master
  // is running.
  std::tr1::shared_ptr<master::MasterServiceProxy> master_proxy();

  // Wait until the number of registered tablet servers reaches the given count.
  // Returns Status::TimedOut if the desired count is not achieved with the given
  // timeout.
  Status WaitForTabletServerCount(int count, const MonoDelta& timeout);

  // Create a client configured to talk to this cluster.
  // Options may contain override options for the client. If no messenger is provided,
  // the internal messenger owned by this class is used. The master address will
  // be overridden to talk to the running master.
  //
  // REQUIRES: the cluster must have already been Start()ed.
  Status CreateClient(const client::KuduClientOptions& opts,
                      std::tr1::shared_ptr<client::KuduClient>* client);

 private:
  Status StartMaster();

  std::string GetBinaryPath(const std::string& binary) const;
  std::string GetDataPath(const std::string& daemon_id) const;

  Status DeduceBinRoot(std::string* ret);
  Status HandleOptions();

  const ExternalMiniClusterOptions opts_;

  // The root for binaries.
  std::string daemon_bin_path_;

  std::string data_root_;

  bool started_;

  scoped_refptr<ExternalMaster> master_;
  std::vector<scoped_refptr<ExternalTabletServer> > tablet_servers_;

  std::tr1::shared_ptr<rpc::Messenger> messenger_;

  DISALLOW_COPY_AND_ASSIGN(ExternalMiniCluster);
};

class ExternalDaemon : public base::RefCountedThreadSafe<ExternalDaemon> {
 public:
  ExternalDaemon(const std::string& exe, const std::string& data_dir,
                 const std::vector<std::string>& extra_flags);

  HostPort bound_rpc_hostport() const;
  Sockaddr bound_rpc_addr() const;
  HostPort bound_http_hostport() const;
  const NodeInstancePB& instance_id() const;

  virtual void Shutdown();

 protected:
  friend class base::RefCountedThreadSafe<ExternalDaemon>;
  virtual ~ExternalDaemon();
  Status StartProcess(const std::vector<std::string>& flags);

  const std::string exe_;
  const std::string data_dir_;
  const std::vector<std::string> extra_flags_;

  gscoped_ptr<Subprocess> process_;

  gscoped_ptr<server::ServerStatusPB> status_;

  DISALLOW_COPY_AND_ASSIGN(ExternalDaemon);
};


class ExternalMaster : public ExternalDaemon {
 public:
  ExternalMaster(const std::string& exe, const std::string& data_dir,
                 const std::vector<std::string>& extra_flags);

  Status Start();

 private:
  friend class base::RefCountedThreadSafe<ExternalMaster>;
  virtual ~ExternalMaster();
};

class ExternalTabletServer : public ExternalDaemon {
 public:
  ExternalTabletServer(const std::string& exe, const std::string& data_dir,
                       const HostPort& master_addr,
                       const std::vector<std::string>& extra_flags);

  Status Start();

 private:
  const std::string master_addr_;

  friend class base::RefCountedThreadSafe<ExternalTabletServer>;
  virtual ~ExternalTabletServer();
};

} // namespace kudu
#endif /* KUDU_INTEGRATION_TESTS_EXTERNAL_MINI_CLUSTER_H */
