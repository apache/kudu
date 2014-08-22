// Copyright (c) 2014, Cloudera, inc.
//
// Ksck, a tool to run a Kudu System Check.

#ifndef KUDU_TOOLS_KSCK_H
#define KUDU_TOOLS_KSCK_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {
namespace tools {

// Representation of a tablet replica on a tablet server.
class KsckTabletReplica {
 public:
  KsckTabletReplica(const std::string ts_uuid, const bool is_leader, const bool is_follower)
      : is_leader_(is_leader),
        is_follower_(is_follower),
        ts_uuid_(ts_uuid) {
  }

  const bool& is_leader() const {
    return is_leader_;
  }

  const bool& is_follower() const {
    return is_follower_;
  }

  const std::string& ts_uuid() const {
    return ts_uuid_;
  }

 private:
  const bool is_leader_;
  const bool is_follower_;
  const std::string ts_uuid_;
  DISALLOW_COPY_AND_ASSIGN(KsckTabletReplica);
};

// Representation of a tablet belonging to a table. The tablet is composed of replicas.
class KsckTablet {
 public:
  // TODO add start/end keys, stale.
  explicit KsckTablet(const std::string& id)
      : id_(id) {
  }

  const std::string& id() const {
    return id_;
  }

  const std::vector<std::tr1::shared_ptr<KsckTabletReplica> >& replicas() const {
    return replicas_;
  }

  void set_replicas(std::vector<std::tr1::shared_ptr<KsckTabletReplica> >& replicas) {
    replicas_.assign(replicas.begin(), replicas.end());
  }
 private:
  const std::string id_;
  std::vector<std::tr1::shared_ptr<KsckTabletReplica> > replicas_;
  DISALLOW_COPY_AND_ASSIGN(KsckTablet);
};

// Representation of a table. Composed of tablets.
class KsckTable {
 public:
  KsckTable(const std::string& name, int num_replicas)
      : name_(name),
        num_replicas_(num_replicas) {
  }

  const std::string& name() const {
    return name_;
  }

  int num_replicas() const {
    return num_replicas_;
  }

  void set_tablets(std::vector<std::tr1::shared_ptr<KsckTablet> >& tablets) {
    tablets_.assign(tablets.begin(), tablets.end());
  }

  std::vector<std::tr1::shared_ptr<KsckTablet> >& tablets() {
    return tablets_;
  }

 private:
  const std::string name_;
  const int num_replicas_;
  std::vector<std::tr1::shared_ptr<KsckTablet> > tablets_;
  DISALLOW_COPY_AND_ASSIGN(KsckTable);
};

// The following two classes must be extended in order to communicate with their respective
// components. The two main use cases envisioned for this are:
// - To be able to mock a cluster to more easily test the Ksck checks.
// - To be able to communicate with a real Kudu cluster.

// Class that must be extended to represent a tablet server.
class KsckTabletServer {
 public:
  explicit KsckTabletServer(const std::string& uuid)
      : uuid_(uuid) {
  }
  virtual ~KsckTabletServer() { }

  // Connects to the configured Tablet Server.
  virtual Status Connect() = 0;

  const std::string& uuid() const {
    return uuid_;
  }

 private:
  const std::string uuid_;
  DISALLOW_COPY_AND_ASSIGN(KsckTabletServer);
};

// Class that must be extended to represent a master.
class KsckMaster {
 public:
  KsckMaster() { }
  virtual ~KsckMaster() { }

  // Connects to the configured Master.
  virtual Status Connect() = 0;

  // Gets the list of Tablet Servers from the Master and stores it in the passed vector.
  // tablet_servers is only modified if this method returns OK.
  virtual Status RetrieveTabletServersList(
      std::vector<std::tr1::shared_ptr<KsckTabletServer> >& tablet_servers) = 0;

  // Gets the list of tables from the Master and stores it in the passed vector.
  // tables is only modified if this method returns OK.
  virtual Status RetrieveTablesList(
      std::vector<std::tr1::shared_ptr<KsckTable> >& tables) = 0;

  // Gets the list of tablets for the specified table and stores the list in it.
  // The table's tablet list is only modified if this method returns OK.
  virtual Status RetrieveTabletsList(const std::tr1::shared_ptr<KsckTable>& table) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(KsckMaster);
};

// Class used to communicate with the cluster. It bootstraps this by using the provided master.
class KsckCluster {
 public:
  explicit KsckCluster(const std::tr1::shared_ptr<KsckMaster>& master)
      : master_(master) {
  }
  ~KsckCluster();

  // Gets the list of tablet servers from the Master.
  Status RetrieveTabletServersList();

  // Gets the list of tables from the Master.
  Status RetrieveTablesList();

  Status RetrieveTabletsList(const std::tr1::shared_ptr<KsckTable>& table);

  const std::tr1::shared_ptr<KsckMaster>& master() {
    return master_;
  }

  const std::vector<std::tr1::shared_ptr<KsckTabletServer> >& tablet_servers() {
    return tablet_servers_;
  }

  const std::vector<std::tr1::shared_ptr<KsckTable> >& tables() {
    return tables_;
  }

 private:
  const std::tr1::shared_ptr<KsckMaster> master_;
  std::vector<std::tr1::shared_ptr<KsckTabletServer> > tablet_servers_;
  std::vector<std::tr1::shared_ptr<KsckTable> > tables_;
  DISALLOW_COPY_AND_ASSIGN(KsckCluster);
};

// Externally facing class to run checks against the provided cluster.
class Ksck {
 public:
  explicit Ksck(const std::tr1::shared_ptr<KsckCluster>& cluster)
      : cluster_(cluster) {
  }
  ~Ksck();

  // Verifies that it can connect to the Master.
  Status CheckMasterRunning();

  // Verifies that it can connect to all the Tablet Servers reported by the master.
  // Depends on: CheckMasterRunning().
  Status CheckTabletServersRunning();

  // Establishes a connection with the specified Tablet Server.
  Status ConnectToTabletServer(const std::tr1::shared_ptr<KsckTabletServer>& ts);

  // Verifies that all the tables have contiguous tablets and that each tablet has enough replicas
  // and a leader.
  // Depends on: CheckMasterRunning().
  Status CheckTablesConsistency();

  // Verifies that the assignments reported by the master are the same reported by the
  // Tablet Servers.
  // Depends on: CheckTablesConsistency().
  Status CheckAssignments();

 private:
  bool VerifyTable(const std::tr1::shared_ptr<KsckTable>& table);
  bool VerifyTablet(const std::tr1::shared_ptr<KsckTablet>& tablet, int table_num_replicas);

  const std::tr1::shared_ptr<KsckCluster> cluster_;
  DISALLOW_COPY_AND_ASSIGN(Ksck);
};
} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_KSCK_H
