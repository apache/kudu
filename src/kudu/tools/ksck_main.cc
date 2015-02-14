// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Command line tool to run Ksck against a cluster. Defaults to running against a local Master
// on the default RPC port. It verifies that all the reported Tablet Servers are running and that
// the tablets are in a consistent state.

#include <iostream>

#include "kudu/gutil/strings/split.h"
#include "kudu/tools/ksck_remote.h"

#define PUSH_PREPEND_NOT_OK(s, statuses, msg) do { \
  ::kudu::Status _s = (s); \
  if (PREDICT_FALSE(!_s.ok())) { \
    statuses.push_back(string(msg) + ": " + _s.ToString()); \
  } \
} while (0);

using std::cerr;
using std::cout;
using std::endl;
using std::tr1::shared_ptr;

DEFINE_string(master_address, "localhost",
              "Address of master server to run against");

DEFINE_bool(checksum_scan, false,
            "Perform a checksum scan on data in the cluster");

DEFINE_int32(checksum_timeout_sec, 120,
             "Maximum total time that we will wait for a checksum scan to complete");

DEFINE_string(tables, "",
              "Tables to check (comma-separated list of names). "
              "If not specified, checks all tables.");

DEFINE_string(tablets, "",
              "Tablets to check (comma-separated list of IDs) "
              "If not specified, checks all tablets.");

namespace kudu {
namespace tools {

void PrintUsage(char** argv) {
  cerr << "usage: " << argv[0] << " [--master_address=<address>]"
       << " [--checksum_scan [--tables table1,table2] [--tablets tablet1,tablet2]"
       << " [--checksum_timeout_sec TIMEOUT] ]"
       << endl
       << "The tool defaults to running against a local Master." << endl
       << "Using --vmodule=ksck=1 gives more details at runtime." << endl;
}

bool IsOkElseLog(const Status& s) {
  if (!s.ok()) {
    LOG(WARNING) << s.ToString();
  }
  return s.ok();
}

static int KsckMain(int argc, char** argv) {
  shared_ptr<KsckMaster> master(new RemoteKsckMaster(FLAGS_master_address));
  shared_ptr<KsckCluster> cluster(new KsckCluster(master));
  shared_ptr<Ksck> ksck(new Ksck(cluster));
  vector<string> error_messages;
  if (!IsOkElseLog(ksck->CheckMasterRunning())) {
    // If we couldn't connect to the Master it might just be that the wrong address was given
    // so we print the usage.
    PrintUsage(argv);
    return 1;
  }
  if (!IsOkElseLog(ksck->FetchTableAndTabletInfo())) {
    return 1;
  }
  PUSH_PREPEND_NOT_OK(ksck->CheckTabletServersRunning(), error_messages,
                      "Tablet server aliveness check error");

  // TODO: Add support for tables / tablets filter in the consistency check.
  PUSH_PREPEND_NOT_OK(ksck->CheckTablesConsistency(), error_messages,
                      "Table consistency check error");

  if (FLAGS_checksum_scan) {
    vector<string> tables = strings::Split(FLAGS_tables, ",", strings::SkipEmpty());
    vector<string> tablets = strings::Split(FLAGS_tablets, ",", strings::SkipEmpty());
    PUSH_PREPEND_NOT_OK(ksck->ChecksumData(tables, tablets,
        MonoDelta::FromSeconds(FLAGS_checksum_timeout_sec)),
        error_messages,
        "Checksum scan error");
  }

  // All good.
  if (error_messages.empty()) {
    cout << "OK" << endl;
    return 0;
  }

  // Something went wrong.
  cerr << "==================" << endl;
  cerr << "Errors:" << endl;
  cerr << "==================" << endl;
  BOOST_FOREACH(const string& s, error_messages) {
    cerr << s << endl;
  }
  cerr << endl;
  cerr << "FAILED" << endl;
  return 1;
}

} // namespace tools
} // namespace kudu

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = true;
  return kudu::tools::KsckMain(argc, argv);
}
