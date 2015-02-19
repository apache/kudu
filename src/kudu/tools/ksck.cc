// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <iostream>
#include <tr1/unordered_set>

#include "kudu/tools/ksck.h"

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/shared_ptr_util.h"

namespace kudu {
namespace tools {

using std::cerr;
using std::cout;
using std::endl;
using std::ostream;
using std::string;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using strings::Substitute;

// Print an informational message to cerr.
static ostream& Info() {
  cerr << "INFO: ";
  return cerr;
}

// Print a warning message to cerr.
static ostream& Warn() {
  cerr << "WARNING: ";
  return cerr;
}

// Print an error message to cerr.
static ostream& Error() {
  cerr << "ERROR: ";
  return cerr;
}

KsckCluster::~KsckCluster() {
}

Status KsckCluster::FetchTableAndTabletInfo() {
  RETURN_NOT_OK(RetrieveTablesList());
  RETURN_NOT_OK(RetrieveTabletServers());
  BOOST_FOREACH(const shared_ptr<KsckTable>& table, tables()) {
    RETURN_NOT_OK(RetrieveTabletsList(table));
  }
  return Status::OK();
}

// Gets the list of tablet servers from the Master.
Status KsckCluster::RetrieveTabletServers() {
  RETURN_NOT_OK(master_->EnsureConnected());
  return master_->RetrieveTabletServers(&tablet_servers_);
}

// Gets the list of tables from the Master.
Status KsckCluster::RetrieveTablesList() {
  RETURN_NOT_OK(master_->EnsureConnected());
  return master_->RetrieveTablesList(&tables_);
}

Status KsckCluster::RetrieveTabletsList(const shared_ptr<KsckTable>& table) {
  RETURN_NOT_OK(master_->EnsureConnected());
  return master_->RetrieveTabletsList(table);
}

Status Ksck::CheckMasterRunning() {
  VLOG(1) << "Connecting to the Master";
  Status s = cluster_->master()->Connect();
  if (s.ok()) {
    Info() << "Connected to the Master" << endl;
  }
  return s;
}

Status Ksck::FetchTableAndTabletInfo() {
  return cluster_->FetchTableAndTabletInfo();
}

Status Ksck::CheckTabletServersRunning() {
  VLOG(1) << "Getting the Tablet Servers list";
  int servers_count = cluster_->tablet_servers().size();
  VLOG(1) << Substitute("List of $0 Tablet Servers retrieved", servers_count);

  if (servers_count == 0) {
    return Status::NotFound("No tablet servers found");
  }

  int bad_servers = 0;
  VLOG(1) << "Connecting to all the Tablet Servers";
  BOOST_FOREACH(const KsckMaster::TSMap::value_type& entry, cluster_->tablet_servers()) {
    Status s = ConnectToTabletServer(entry.second);
    if (!s.ok()) {
      bad_servers++;
    }
  }
  if (bad_servers == 0) {
    Info() << Substitute("Connected to all $0 Tablet Servers", servers_count) << endl;
    return Status::OK();
  } else {
    Warn() << Substitute("Connected to $0 Tablet Servers, $1 weren't reachable",
                         servers_count - bad_servers, bad_servers) << endl;
    return Status::NetworkError("Not all Tablet Servers are reachable");
  }
}

Status Ksck::ConnectToTabletServer(const shared_ptr<KsckTabletServer>& ts) {
  VLOG(1) << "Going to connect to Tablet Server: " << ts->uuid();
  Status s = ts->Connect();
  if (s.ok()) {
    VLOG(1) << "Connected to Tablet Server: " << ts->uuid();
  } else {
    Warn() << Substitute("Unable to connect to Tablet Server $0 because $1",
                         ts->uuid(), s.ToString()) << endl;
  }
  return s;
}

Status Ksck::CheckTablesConsistency() {
  VLOG(1) << "Getting the tables list";
  int tables_count = cluster_->tables().size();
  VLOG(1) << Substitute("List of $0 tables retrieved", tables_count);

  if (tables_count == 0) {
    Info() << "The cluster doesn't have any tables" << endl;
    return Status::OK();
  }

  VLOG(1) << "Verifying each table";
  int bad_tables_count = 0;
  BOOST_FOREACH(const shared_ptr<KsckTable> &table, cluster_->tables()) {
    if (!VerifyTableWithTimeout(table,
                                MonoDelta::FromSeconds(2),
                                MonoDelta::FromMilliseconds(100))) {
      bad_tables_count++;
    }
  }
  if (bad_tables_count == 0) {
    Info() << Substitute("The metadata for $0 tables is HEALTHY", tables_count) << endl;
    return Status::OK();
  } else {
    Warn() << Substitute("$0 out of $1 tables are not in a healthy state",
                         bad_tables_count, tables_count) << endl;
    return Status::Corruption(Substitute("$0 tables are bad", bad_tables_count));
  }
}

ChecksumResultReporter::ChecksumResultReporter(int num_tablet_replicas)
    : responses_(num_tablet_replicas) {
}

void ChecksumResultReporter::HandleResponse(const string& tablet_id,
                                            const string& replica_uuid,
                                            const Status& status,
                                            uint64_t checksum) {
  lock_guard<simple_spinlock> guard(&lock_);
  unordered_map<string, ResultPair>& replica_results =
      LookupOrInsert(&checksums_, tablet_id, unordered_map<string, ResultPair>());
  InsertOrDie(&replica_results, replica_uuid, ResultPair(status, checksum));
  responses_.CountDown();
}

ChecksumResultReporter::TabletResultMap ChecksumResultReporter::checksums() const {
  lock_guard<simple_spinlock> guard(&lock_);
  return checksums_;
}

Status Ksck::ChecksumData(const vector<string>& tables,
                          const vector<string>& tablets,
                          const MonoDelta& timeout) {
  const unordered_set<string> tables_filter(tables.begin(), tables.end());
  const unordered_set<string> tablets_filter(tablets.begin(), tablets.end());

  typedef unordered_map<shared_ptr<KsckTablet>, shared_ptr<KsckTable>,
                        SharedPtrHashFunctor<KsckTablet> > TabletTableMap;
  TabletTableMap tablet_table_map;

  int num_tablet_replicas = 0;
  BOOST_FOREACH(const shared_ptr<KsckTable>& table, cluster_->tables()) {
    VLOG(1) << "Table: " << table->name();
    if (!tables_filter.empty() && !ContainsKey(tables_filter, table->name())) continue;
    BOOST_FOREACH(const shared_ptr<KsckTablet>& tablet, table->tablets()) {
      VLOG(1) << "Tablet: " << tablet->id();
      if (!tablets_filter.empty() && !ContainsKey(tablets_filter, tablet->id())) continue;
      InsertOrDie(&tablet_table_map, tablet, table);
      num_tablet_replicas += tablet->replicas().size();
    }
  }
  if (num_tablet_replicas == 0) {
    string msg = "No tablet replicas found.";
    if (!tables.empty() || !tablets.empty()) {
      msg += " Filter: ";
      if (!tables.empty()) {
        msg += "tables=" + JoinStrings(tables, ",") + ".";
      }
      if (!tablets.empty()) {
        msg += "tablets=" + JoinStrings(tablets, ",") + ".";
      }

    }
    return Status::NotFound(msg);
  }
  shared_ptr<ChecksumResultReporter> reporter(new ChecksumResultReporter(num_tablet_replicas));

  // Kick off the scans in parallel.
  // TODO: Add some way to throttle requests on big clusters.
  BOOST_FOREACH(const TabletTableMap::value_type& entry, tablet_table_map) {
    const shared_ptr<KsckTablet>& tablet = entry.first;
    const shared_ptr<KsckTable>& table = entry.second;
    BOOST_FOREACH(const shared_ptr<KsckTabletReplica>& replica, tablet->replicas()) {
      const shared_ptr<KsckTabletServer>& ts =
          FindOrDie(cluster_->tablet_servers(), replica->ts_uuid());
      Status s = ts->RunTabletChecksumScanAsync(tablet->id(), table->schema(), reporter);
      if (!s.ok()) {
        reporter->ReportError(tablet->id(), replica->ts_uuid(), s);
      }
    }
  }

  bool timed_out = false;
  if (!reporter->WaitFor(timeout)) {
    timed_out = true;
  }
  ChecksumResultReporter::TabletResultMap checksums = reporter->checksums();

  int num_errors = 0;
  int num_mismatches = 0;
  int num_results = 0;
  BOOST_FOREACH(const shared_ptr<KsckTable>& table, cluster_->tables()) {
    bool printed_table_name = false;
    BOOST_FOREACH(const shared_ptr<KsckTablet>& tablet, table->tablets()) {
      if (ContainsKey(checksums, tablet->id())) {
        if (!printed_table_name) {
          printed_table_name = true;
          cout << "-----------------------" << endl;
          cout << table->name() << endl;
          cout << "-----------------------" << endl;
        }
        bool seen_first_replica = false;
        uint64_t first_checksum = 0;

        BOOST_FOREACH(const ChecksumResultReporter::ReplicaResultMap::value_type& r,
                      FindOrDie(checksums, tablet->id())) {
          const string& replica_uuid = r.first;

          shared_ptr<KsckTabletServer> ts = FindOrDie(cluster_->tablet_servers(), replica_uuid);
          const ChecksumResultReporter::ResultPair& result = r.second;
          const Status& status = result.first;
          uint64_t checksum = result.second;
          string status_str = (status.ok()) ? Substitute("Checksum: $0", checksum)
                                            : Substitute("Error: $0", status.ToString());
          cout << Substitute("T $0 P $1 ($2): $3", tablet->id(), ts->uuid(), ts->address(),
                                                   status_str) << endl;
          if (!status.ok()) {
            num_errors++;
          } else if (!seen_first_replica) {
            seen_first_replica = true;
            first_checksum = checksum;
          } else if (checksum != first_checksum) {
            num_mismatches++;
            Error() << ">> Mismatch found in table " << table->name()
                    << " tablet " << tablet->id() << endl;
          }
          num_results++;
        }
      }
    }
    if (printed_table_name) cout << endl;
  }
  if (num_results != num_tablet_replicas) {
    CHECK(timed_out) << Substitute("Unexpected error: only got $0 out of $1 replica results",
                                   num_results, num_tablet_replicas);
    return Status::TimedOut(Substitute("Checksum scan did not complete within the timeout of $0: "
                                       "Received results for $1 out of $2 expected replicas",
                                       timeout.ToString(), num_results, num_tablet_replicas));
  }
  if (num_mismatches != 0) {
    return Status::Corruption(Substitute("$0 checksum mismatches were detected", num_mismatches));
  }
  if (num_errors != 0) {
    return Status::Aborted(Substitute("$0 errors were detected", num_errors));
  }

  return Status::OK();
}

bool Ksck::VerifyTableWithTimeout(const shared_ptr<KsckTable>& table,
                                  const MonoDelta& timeout,
                                  const MonoDelta& retry_interval) {

  MonoTime deadline = MonoTime::Now(MonoTime::COARSE);
  deadline.AddDelta(timeout);
  while (true) {
    if (VerifyTable(table)) {
      break;
    }
    if (deadline.ComesBefore(MonoTime::Now(MonoTime::COARSE))) {
      return false;
    }
    SleepFor(retry_interval);
  }
  return true;
}

bool Ksck::VerifyTable(const shared_ptr<KsckTable>& table) {
  bool good_table = true;
  vector<shared_ptr<KsckTablet> > tablets = table->tablets();
  int tablets_count = tablets.size();
  if (tablets_count == 0) {
    Warn() << Substitute("Table $0 has 0 tablets", table->name()) << endl;
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
    Info() << Substitute("Table $0 is HEALTHY", table->name()) << endl;
  } else {
    Warn() << Substitute("Table $0 has $1 bad tablets", table->name(), bad_tablets_count) << endl;
    good_table = false;
  }
  return good_table;
}

bool Ksck::VerifyTablet(const shared_ptr<KsckTablet>& tablet, int table_num_replicas) {
  vector<shared_ptr<KsckTabletReplica> > replicas = tablet->replicas();
  bool good_tablet = true;
  if (replicas.size() != table_num_replicas) {
    Warn() << Substitute("Tablet $0 is missing $1 replicas",
                         tablet->id(), table_num_replicas - replicas.size()) << endl;
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
    Warn() << Substitute("Tablet $0 doesn't have a leader", tablet->id()) << endl;
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
