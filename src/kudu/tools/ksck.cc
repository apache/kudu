// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tools/ksck.h"
#include <boost/foreach.hpp>
#include <glog/logging.h>
#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace tools {

using std::string;
using std::tr1::shared_ptr;
using strings::Substitute;

KsckCluster::~KsckCluster() {
}

Status KsckCluster::RetrieveTabletServersList() {
  return master_->RetrieveTabletServersList(tablet_servers_);
}

Status KsckCluster::RetrieveTablesList() {
  return master_->RetrieveTablesList(tables_);
}

Status KsckCluster::RetrieveTabletsList(const shared_ptr<KsckTable>& table) {
  return master_->RetrieveTabletsList(table);
}

Ksck::~Ksck() {
}

Status Ksck::CheckMasterRunning() {
  VLOG(1) << "Connecting to the Master";
  Status s = cluster_->master()->Connect();
  if (s.ok()) {
    LOG(INFO) << "Connected to the Master";
  } else {
    LOG(ERROR) << "Unable to connect to the Master: " << s.ToString();
  }
  return s;
}

Status Ksck::CheckTabletServersRunning() {
  VLOG(1) << "Getting the Tablet Servers list";
  RETURN_NOT_OK(cluster_->RetrieveTabletServersList());
  int servers_count = cluster_->tablet_servers().size();
  VLOG(1) << Substitute("List of $0 Tablet Servers retrieved", servers_count);

  if (servers_count == 0) {
    LOG(WARNING) << "The Master reports 0 Tablet Servers";
    return Status::OK();
  }

  int bad_servers = 0;
  VLOG(1) << "Connecting to all the Tablet Servers";
  BOOST_FOREACH(const shared_ptr<KsckTabletServer>& ts, cluster_->tablet_servers()) {
    Status s = ConnectToTabletServer(ts);
    if (!s.ok()) {
      bad_servers++;
    }
  }
  if (bad_servers == 0) {
    LOG(INFO) << Substitute("Connected to all $0 Tablet Servers", servers_count);
    return Status::OK();
  } else {
    LOG(WARNING) << Substitute("Connected to $0 Tablet Servers, $1 weren't reachable",
                               servers_count - bad_servers, bad_servers);
    return Status::NetworkError("Not all Tablet Servers are reachable");
  }
}

Status Ksck::ConnectToTabletServer(const shared_ptr<KsckTabletServer>& ts) {
  VLOG(1) << "Going to connect to Tablet Server: " << ts->uuid();
  Status s = ts->Connect();
  if (s.ok()) {
    VLOG(1) << "Connected to Tablet Server: " << ts->uuid();
  } else {
    LOG(WARNING) << Substitute("Unable to connect to Tablet Server $0 because $1",
                               ts->uuid(), s.ToString());
  }
  return s;
}

Status Ksck::CheckTablesConsistency() {
  VLOG(1) << "Getting the tables list";
  RETURN_NOT_OK(cluster_->RetrieveTablesList());
  int tables_count = cluster_->tables().size();
  VLOG(1) << Substitute("List of $0 tables retrieved", tables_count);

  if (tables_count == 0) {
    LOG(INFO) << "The cluster doesn't have any tables";
    return Status::OK();
  }

  VLOG(1) << "Verifying each table";
  int bad_tables_count = 0;
  BOOST_FOREACH(const shared_ptr<KsckTable> &table, cluster_->tables()) {
    if (!VerifyTable(table)) {
      bad_tables_count++;
    }
  }
  if (bad_tables_count == 0) {
    LOG(INFO) << Substitute("The metadata for $0 tables is HEALTHY", tables_count);
    return Status::OK();
  } else {
    LOG(WARNING) << Substitute("$0 out of $1 tables are not in a healthy state",
                               bad_tables_count, tables_count);
    return Status::Corruption(Substitute("$0 tables are bad", bad_tables_count));
  }
}

bool Ksck::VerifyTable(const shared_ptr<KsckTable>& table) {
  bool good_table = true;
  VLOG(1) << "Getting the tablets' metadata for table " << table->name();
  Status s = cluster_->RetrieveTabletsList(table);
  if (!s.ok()) {
    LOG(WARNING) << Substitute("Couldn't get the list of tablets for table $0 because: $1",
                               table->name(), s.ToString());
    return false;
  }
  vector<shared_ptr<KsckTablet> > tablets = table->tablets();
  int tablets_count = tablets.size();
  if (tablets_count == 0) {
    LOG(WARNING) << Substitute("Table $0 has 0 tablets", table->name());
    return false;
  }
  int table_num_replicas = table->num_replicas();
  VLOG(1) << Substitute("Verifying $0 tablets for table $1 configured with num_replicas = $2",
                        tablets_count, table->name(), table_num_replicas);
  int bad_tablets_count = 0;
  // TODO check if the tablets are contiguous and in order.
  BOOST_FOREACH(const shared_ptr<KsckTablet> &tablet, tablets) {
    if (!VerifyTablet(tablet, table_num_replicas)) {
      bad_tablets_count++;
    }
  }
  if (bad_tablets_count == 0) {
    VLOG(1) << Substitute("Table $0 is HEALTHY", table->name());
  } else {
    LOG(WARNING) << Substitute("Table $0 has $1 bad tablets", table->name(), bad_tablets_count);
    good_table = false;
  }
  return good_table;
}

bool Ksck::VerifyTablet(const shared_ptr<KsckTablet>& tablet, int table_num_replicas) {
  vector<shared_ptr<KsckTabletReplica> > replicas = tablet->replicas();
  bool good_tablet = true;
  if (replicas.size() != table_num_replicas) {
    LOG(WARNING) << Substitute("Tablet $0 is missing $1 replicas",
                               tablet->id(), table_num_replicas - replicas.size());
    good_tablet = false;
  }
  int leaders_count = 0;
  int followers_count = 0;
  BOOST_FOREACH(const shared_ptr<KsckTabletReplica> replica, replicas) {
    if (replica->is_leader()) {
      VLOG(1) << Substitute("Replica at $0 is a LEADER", replica->ts_uuid());
      leaders_count++;
    } else if (replica->is_follower()) {
      VLOG(1) << Substitute("Replica at $0 is a FOLLOWER", replica->ts_uuid());
      followers_count++;
    }
  }
  if (leaders_count == 0) {
    LOG(WARNING) << Substitute("Tablet $0 doesn't have a leader", tablet->id());
    good_tablet = false;
  }
  VLOG(1) << Substitute("Tablet $0 has $1 leader and $2 followers",
                        tablet->id(), leaders_count, followers_count);
  return good_tablet;
}

Status Ksck::CheckAssignments() {
  // TODO
  return Status::NotSupported("CheckAssignments hasn't been implemented");
}

} // namespace tools
} // namespace kudu
