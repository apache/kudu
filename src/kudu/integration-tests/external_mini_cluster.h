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

#pragma once

#include <sys/types.h>

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/optional.hpp>

#include "kudu/client/client.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {

class ExternalDaemon;
class ExternalMaster;
class ExternalTabletServer;
class HostPort;
class MetricPrototype;
class MetricEntityPrototype;
class NodeInstancePB;
class ScopedSubprocess;
class Sockaddr;
class Subprocess;

namespace server {
class ServerStatusPB;
} // namespace server

struct ExternalMiniClusterOptions {
  ExternalMiniClusterOptions();

  // Number of masters to start.
  // Default: 1
  int num_masters;

  // Number of TS to start.
  // Default: 1
  int num_tablet_servers;

  // Directory in which to store data.
  // Default: "", which auto-generates a unique path for this cluster.
  std::string data_root;

  MiniCluster::BindMode bind_mode;

  // The path where the kudu daemons should be run from.
  // Default: "", which uses the same path as the currently running executable.
  // This works for unit tests, since they all end up in build/latest/bin.
  std::string daemon_bin_path;

  // Number of data directories to be created for each daemon.
  // Default: 1
  int num_data_dirs;

  // Extra flags for tablet servers and masters respectively.
  //
  // In these flags, you may use the special string '${index}' which will
  // be substituted with the index of the tablet server or master.
  std::vector<std::string> extra_tserver_flags;
  std::vector<std::string> extra_master_flags;

  // If more than one master is specified, list of ports for the
  // masters in a consensus configuration. Port at index 0 is used for the leader
  // master.
  std::vector<uint16_t> master_rpc_ports;

  // Options to configure the MiniKdc before starting it up.
  // Only used when 'enable_kerberos' is 'true'.
  MiniKdcOptions mini_kdc_options;

  // If true, set up a KDC as part of this ExternalMiniCluster, generate keytabs for
  // the servers, and require Kerberos authentication from clients.
  //
  // Additionally, when the cluster is started, the environment of the
  // test process will be modified to include Kerberos credentials for
  // a principal named 'testuser'.
  bool enable_kerberos;

  // If true, sends logging output to stderr instead of a log file. Defaults to
  // true.
  bool logtostderr;

  // Amount of time that may elapse between the creation of a daemon process
  // and the process writing out its info file. Defaults to 30 seconds.
  MonoDelta start_process_timeout;
};

// A mini-cluster made up of subprocesses running each of the daemons
// separately. This is useful for black-box or grey-box failure testing
// purposes -- it provides the ability to forcibly kill or stop particular
// cluster participants, which isn't feasible in the normal InternalMiniCluster.
// On the other hand, there is little access to inspect the internal state
// of the daemons.
class ExternalMiniCluster : public MiniCluster {
 public:
  // Constructs a cluster with the default options.
  ExternalMiniCluster();

  // Constructs a cluster with options specified in 'opts'.
  explicit ExternalMiniCluster(ExternalMiniClusterOptions opts);

  // Destroys a cluster.
  virtual ~ExternalMiniCluster();

  // Start the cluster.
  Status Start() override;

  // Restarts the cluster. Requires that it has been Shutdown() first.
  Status Restart();

  // Add a new TS to the cluster. The new TS is started.
  // Requires that the master is already running.
  Status AddTabletServer();

  // Currently, this uses SIGKILL on each daemon for a non-graceful shutdown.
  void ShutdownNodes(ClusterNodes nodes) override;

  // Return the IP address that the tablet server with the given index will bind to.
  // If options.bind_to_unique_loopback_addresses is false, this will be 127.0.0.1
  // Otherwise, it is another IP in the local netblock.
  std::string GetBindIpForTabletServer(int index) const;

  // Same as above but for a master.
  std::string GetBindIpForMaster(int index) const;

  // Return a pointer to the running leader master. This may be NULL
  // if the cluster is not started.
  //
  // TODO: Use the appropriate RPC here to return the leader master,
  // to allow some of the existing tests (e.g., raft_consensus-itest)
  // to use multiple masters.
  ExternalMaster* leader_master() { return master(0); }

  // Perform an RPC to determine the leader of the external mini
  // cluster.  Set 'index' to the leader master's index (for calls to
  // to master() below).
  //
  // NOTE: if a leader election occurs after this method is executed,
  // the last result may not be valid.
  Status GetLeaderMasterIndex(int* idx);

  // If this cluster is configured for a single non-distributed
  // master, return the single master or NULL if the master is not
  // started. Exits with a CHECK failure if there are multiple
  // masters.
  ExternalMaster* master() const {
    CHECK_EQ(masters_.size(), 1)
        << "master() should not be used with multiple masters, use leader_master() instead.";
    return master(0);
  }

  // Return master at 'idx' or NULL if the master at 'idx' has not
  // been started.
  ExternalMaster* master(int idx) const {
    CHECK_LT(idx, masters_.size());
    return masters_[idx].get();
  }

  ExternalTabletServer* tablet_server(int idx) const {
    CHECK_LT(idx, tablet_servers_.size());
    return tablet_servers_[idx].get();
  }

  // Return ExternalTabletServer given its UUID. If not found, returns NULL.
  ExternalTabletServer* tablet_server_by_uuid(const std::string& uuid) const;

  // Return the index of the ExternalTabletServer that has the given 'uuid', or
  // -1 if no such UUID can be found.
  int tablet_server_index_by_uuid(const std::string& uuid) const;

  // Return all tablet servers and masters.
  std::vector<ExternalDaemon*> daemons() const;

  MiniKdc* kdc() const {
    return CHECK_NOTNULL(kdc_.get());
  }

  int num_tablet_servers() const override {
    return tablet_servers_.size();
  }

  int num_masters() const override {
    return masters_.size();
  }

  BindMode bind_mode() const override {
    return opts_.bind_mode;
  }

  std::vector<uint16_t> master_rpc_ports() const override {
    return opts_.master_rpc_ports;
  }

  std::vector<HostPort> master_rpc_addrs() const override;

  std::shared_ptr<rpc::Messenger> messenger() const override;
  std::shared_ptr<master::MasterServiceProxy> master_proxy() const override;
  std::shared_ptr<master::MasterServiceProxy> master_proxy(int idx) const override;

  // Wait until the number of registered tablet servers reaches the given count
  // on all of the running masters. Returns Status::TimedOut if the desired
  // count is not achieved with the given timeout.
  Status WaitForTabletServerCount(int count, const MonoDelta& timeout);

  // Runs gtest assertions that no servers have crashed.
  void AssertNoCrashes();

  // Wait until all tablets on the given tablet server are in the RUNNING
  // state. Returns Status::TimedOut if 'timeout' elapses and at least one
  // tablet is not yet RUNNING.
  //
  // If 'min_tablet_count' is not -1, will also wait for at least that many
  // RUNNING tablets to appear before returning (potentially timing out if that
  // number is never reached).
  Status WaitForTabletsRunning(ExternalTabletServer* ts, int min_tablet_count,
                               const MonoDelta& timeout);

  // Create a client configured to talk to this cluster.
  // Builder may contain override options for the client. The master address will
  // be overridden to talk to the running master.
  //
  // REQUIRES: the cluster must have already been Start()ed.
  Status CreateClient(client::KuduClientBuilder* builder,
                      client::sp::shared_ptr<client::KuduClient>* client) const override;

  // Sets the given flag on the given daemon, which must be running.
  //
  // This uses the 'force' flag on the RPC so that, even if the flag
  // is considered unsafe to change at runtime, it is changed.
  Status SetFlag(ExternalDaemon* daemon,
                 const std::string& flag,
                 const std::string& value) WARN_UNUSED_RESULT;

  // Set the path where daemon binaries can be found.
  // Overrides 'daemon_bin_path' set by ExternalMiniClusterOptions.
  // The cluster must be shut down before calling this method.
  void SetDaemonBinPath(std::string daemon_bin_path);

  // Returns the path where 'binary' is expected to live, based on
  // ExternalMiniClusterOptions.daemon_bin_path if it was provided, or on the
  // path of the currently running executable otherwise.
  std::string GetBinaryPath(const std::string& binary) const;

  // Returns the path where 'daemon_id' is expected to store its data, based on
  // ExternalMiniClusterOptions.data_root if it was provided, or on the
  // standard Kudu test directory otherwise.
  // 'dir_index' is an optional numeric suffix to be added to the default path.
  // If it is not specified, the cluster must be configured to use a single data dir.
  std::string GetDataPath(const std::string& daemon_id,
                          boost::optional<uint32_t> dir_index = boost::none) const;

  // Returns paths where 'daemon_id' is expected to store its data, each with a
  // numeric suffix appropriate for 'opts_.num_data_dirs'
  std::vector<std::string> GetDataPaths(const std::string& daemon_id) const;

  // Returns the path where 'daemon_id' is expected to store its wal, or other
  // files that reside in the wal dir.
  std::string GetWalPath(const std::string& daemon_id) const;

  // Returns the path where 'daemon_id' is expected to store its logs, or other
  // files that reside in the log dir.
  std::string GetLogPath(const std::string& daemon_id) const;

 private:
  FRIEND_TEST(MasterFailoverTest, TestKillAnyMaster);

  Status StartSingleMaster();

  Status StartDistributedMasters();

  Status DeduceBinRoot(std::string* ret);
  Status HandleOptions();

  const ExternalMiniClusterOptions opts_;

  // The root for binaries.
  std::string daemon_bin_path_;

  std::string data_root_;

  std::vector<scoped_refptr<ExternalMaster> > masters_;
  std::vector<scoped_refptr<ExternalTabletServer> > tablet_servers_;
  std::unique_ptr<MiniKdc> kdc_;

  std::shared_ptr<rpc::Messenger> messenger_;

  DISALLOW_COPY_AND_ASSIGN(ExternalMiniCluster);
};

struct ExternalDaemonOptions {
  explicit ExternalDaemonOptions(bool logtostderr)
      : logtostderr(logtostderr) {
  }

  bool logtostderr;
  std::shared_ptr<rpc::Messenger> messenger;
  std::string exe;
  HostPort rpc_bind_address;
  std::string wal_dir;
  std::vector<std::string> data_dirs;
  std::string log_dir;
  std::string perf_record_filename;
  std::vector<std::string> extra_flags;
  MonoDelta start_process_timeout;
};

class ExternalDaemon : public RefCountedThreadSafe<ExternalDaemon> {
 public:
  explicit ExternalDaemon(ExternalDaemonOptions opts);

  HostPort bound_rpc_hostport() const;
  Sockaddr bound_rpc_addr() const;

  // Return the host/port that this daemon is bound to for HTTP.
  // May return an uninitialized HostPort if HTTP is disabled.
  HostPort bound_http_hostport() const;

  const NodeInstancePB& instance_id() const;
  const std::string& uuid() const;

  // Return the pid of the running process.
  // Causes a CHECK failure if the process is not running.
  pid_t pid() const;

  // Return the pointer to the undelying Subprocess if it is set.
  // Otherwise, returns nullptr.
  Subprocess* process() const;

  // Set the path of the executable to run as a daemon.
  // Overrides the exe path specified in the constructor.
  // The daemon must be shut down before calling this method.
  void SetExePath(std::string exe);

  // Enable Kerberos for this daemon. This creates a Kerberos principal
  // and keytab, and sets the appropriate environment variables in the
  // subprocess such that the server will use Kerberos authentication.
  //
  // 'bind_host' is the hostname that will be used to generate the Kerberos
  // service principal.
  //
  // Must be called before 'StartProcess()'.
  Status EnableKerberos(MiniKdc* kdc, const std::string& bind_host);

  // Sends a SIGSTOP signal to the daemon.
  Status Pause() WARN_UNUSED_RESULT;

  // Sends a SIGCONT signal to the daemon.
  Status Resume() WARN_UNUSED_RESULT;

  // Return true if we have explicitly shut down the process.
  bool IsShutdown() const;

  // Return true if the process is still running.
  // This may return false if the process crashed, even if we didn't
  // explicitly call Shutdown().
  bool IsProcessAlive() const;

  // Wait for this process to crash due to a configured fault
  // injection, or the given timeout to elapse. If the process
  // crashes for some reason other than an injected fault, returns
  // Status::Aborted.
  //
  // If the process is already crashed, returns immediately.
  Status WaitForInjectedCrash(const MonoDelta& timeout) const;

  // Same as the above, but expects the process to crash due to a
  // LOG(FATAL) or CHECK failure. In other words, waits for it to
  // crash from SIGABRT.
  Status WaitForFatal(const MonoDelta& timeout) const;

  virtual void Shutdown();

  // Delete files specified by 'wal_dir_' and 'data_dirs_'.
  Status DeleteFromDisk() const WARN_UNUSED_RESULT;

  const std::string& wal_dir() const { return wal_dir_; }

  const std::string& data_dir() const {
    CHECK_EQ(1, data_dirs_.size());
    return data_dirs_[0];
  }

  const std::vector<std::string>& data_dirs() const { return data_dirs_; }

  // Returns the log dir of the external daemon.
  const std::string& log_dir() const { return log_dir_; }

  // Return a pointer to the flags used for this server on restart.
  // Modifying these flags will only take effect on the next restart.
  std::vector<std::string>* mutable_flags() { return &extra_flags_; }

  // Retrieve the value of a given metric from this server. The metric must
  // be of int64_t type.
  //
  // 'value_field' represents the particular field of the metric to be read.
  // For example, for a counter or gauge, this should be 'value'. For a
  // histogram, it might be 'total_count' or 'mean'.
  //
  // 'entity_id' may be NULL, in which case the first entity of the same type
  // as 'entity_proto' will be matched.
  Status GetInt64Metric(const MetricEntityPrototype* entity_proto,
                        const char* entity_id,
                        const MetricPrototype* metric_proto,
                        const char* value_field,
                        int64_t* value) const;

 protected:
  friend class RefCountedThreadSafe<ExternalDaemon>;
  virtual ~ExternalDaemon();

  // Starts a process with the given flags.
  Status StartProcess(const std::vector<std::string>& user_flags);

  // Wait for the process to exit, and then call 'wait_status_predicate'
  // on the resulting exit status. NOTE: this is not the return code, but
  // rather the value provided by waitpid(2): use WEXITSTATUS, etc.
  //
  // If the predicate matches, returns OK. Otherwise, returns an error.
  // 'crash_type_str' should be a descriptive name for the type of crash,
  // used in formatting the error message.
  Status WaitForCrash(const MonoDelta& timeout,
                      const std::function<bool(int)>& wait_status_predicate,
                      const char* crash_type_str) const;

  // In a code-coverage build, try to flush the coverage data to disk.
  // In a non-coverage build, this does nothing.
  void FlushCoverage();

  // In an LSAN build, ask the daemon to check for leaked memory, and
  // LOG(FATAL) if there are any leaks.
  void CheckForLeaks();

  // Get RPC bind address for daemon.
  const HostPort& rpc_bind_address() const {
    return rpc_bind_address_;
  }

  const std::shared_ptr<rpc::Messenger> messenger_;
  const std::string wal_dir_;
  std::vector<std::string> data_dirs_;
  const std::string log_dir_;
  const std::string perf_record_filename_;
  const MonoDelta start_process_timeout_;
  const bool logtostderr_;
  const HostPort rpc_bind_address_;
  std::string exe_;
  std::vector<std::string> extra_flags_;
  std::map<std::string, std::string> extra_env_;

  std::unique_ptr<Subprocess> process_;
  bool paused_ = false;

  std::unique_ptr<Subprocess> perf_record_process_;

  std::unique_ptr<server::ServerStatusPB> status_;

  // These capture the daemons parameters and running ports and
  // are used to Restart() the daemon with the same parameters.
  HostPort bound_rpc_;
  HostPort bound_http_;

  DISALLOW_COPY_AND_ASSIGN(ExternalDaemon);
};

// Resumes a daemon that was stopped with ExternalDaemon::Pause() upon
// exiting a scope.
class ScopedResumeExternalDaemon {
 public:
  // 'daemon' must remain valid for the lifetime of a
  // ScopedResumeExternalDaemon object.
  explicit ScopedResumeExternalDaemon(ExternalDaemon* daemon);

  // Resume 'daemon_'.
  ~ScopedResumeExternalDaemon();

 private:
  ExternalDaemon* daemon_;

  DISALLOW_COPY_AND_ASSIGN(ScopedResumeExternalDaemon);
};

class ExternalMaster : public ExternalDaemon {
 public:
  explicit ExternalMaster(ExternalDaemonOptions opts);

  Status Start();

  // Restarts the daemon.
  // Requires that it has previously been shutdown.
  Status Restart() WARN_UNUSED_RESULT;

  // Blocks until the master's catalog manager is initialized and responding to
  // RPCs.
  Status WaitForCatalogManager() WARN_UNUSED_RESULT;

 private:
  std::vector<std::string> GetCommonFlags() const;

  friend class RefCountedThreadSafe<ExternalMaster>;
  virtual ~ExternalMaster();
};

class ExternalTabletServer : public ExternalDaemon {
 public:
  ExternalTabletServer(ExternalDaemonOptions opts,
                       std::vector<HostPort> master_addrs);

  Status Start();

  // Restarts the daemon.
  // Requires that it has previously been shutdown.
  Status Restart() WARN_UNUSED_RESULT;

 private:
  const std::vector<HostPort> master_addrs_;

  friend class RefCountedThreadSafe<ExternalTabletServer>;
  virtual ~ExternalTabletServer();
};

} // namespace kudu
