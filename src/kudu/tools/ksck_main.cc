// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Command line tool to run Ksck against a cluster. Defaults to running against a local Master
// on the default RPC port. It verifies that all the reported Tablet Servers are running and that
// the tablets are in a consistent state.

#include <glog/logging.h>
#include <google/gflags.h>
#include <iostream>

#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"

#define PUSH_PREPEND_NOT_OK(s, statuses, msg) do { \
  ::kudu::Status _s = (s); \
  if (PREDICT_FALSE(!_s.ok())) { \
    statuses->push_back(string(msg) + ": " + _s.ToString()); \
  } \
} while (0);

using std::cerr;
using std::cout;
using std::endl;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

DEFINE_string(master_address, "localhost",
              "Address of master server to run against");

DEFINE_bool(checksum_scan, false,
            "Perform a checksum scan on data in the cluster");

DEFINE_string(tables, "",
              "Tables to check (comma-separated list of names). "
              "If not specified, checks all tables.");

DEFINE_string(tablets, "",
              "Tablets to check (comma-separated list of IDs) "
              "If not specified, checks all tablets.");

namespace kudu {
namespace tools {

static string GetKsckUsage(const char* progname) {
  string msg = Substitute("Usage: $0\n", progname);
  msg += "The tool defaults to running against a local Master.\n";
  msg += "Using --vmodule=ksck=1 gives more details at runtime.\n";
  return msg;
}

// Run ksck.
// Error information is appended to the provided vector.
// If the vector is empty upon completion, ksck ran successfully.
static void RunKsck(vector<string>* error_messages) {
  vector<Sockaddr> master_addrs;
  PUSH_PREPEND_NOT_OK(ParseAddressList(FLAGS_master_address,
                                       master::Master::kDefaultPort,
                                       &master_addrs),
                      error_messages, "Unable to parse master address");

  shared_ptr<KsckMaster> master;
  PUSH_PREPEND_NOT_OK(RemoteKsckMaster::Build(master_addrs[0], &master),
                      error_messages, "Unable to build KsckMaster");
  if (!error_messages->empty()) return;
  shared_ptr<KsckCluster> cluster(new KsckCluster(master));
  shared_ptr<Ksck> ksck(new Ksck(cluster));

  // This is required for everything below.
  PUSH_PREPEND_NOT_OK(ksck->CheckMasterRunning(), error_messages,
                      "Master aliveness check error");
  if (!error_messages->empty()) return;

  // This is also required for everything below.
  PUSH_PREPEND_NOT_OK(ksck->FetchTableAndTabletInfo(), error_messages,
                      "Error fetching the cluster metadata from the Master server");
  if (!error_messages->empty()) return;

  PUSH_PREPEND_NOT_OK(ksck->CheckTabletServersRunning(), error_messages,
                      "Tablet server aliveness check error");

  // TODO: Add support for tables / tablets filter in the consistency check.
  PUSH_PREPEND_NOT_OK(ksck->CheckTablesConsistency(), error_messages,
                      "Table consistency check error");

  if (FLAGS_checksum_scan) {
    vector<string> tables = strings::Split(FLAGS_tables, ",", strings::SkipEmpty());
    vector<string> tablets = strings::Split(FLAGS_tablets, ",", strings::SkipEmpty());
    PUSH_PREPEND_NOT_OK(ksck->ChecksumData(tables, tablets, ChecksumOptions()),
                        error_messages, "Checksum scan error");
  }
}

} // namespace tools
} // namespace kudu

int main(int argc, char** argv) {
  google::SetUsageMessage(kudu::tools::GetKsckUsage(argv[0]));
  kudu::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = true;
  kudu::InitGoogleLoggingSafe(argv[0]);

  vector<string> error_messages;
  kudu::tools::RunKsck(&error_messages);

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
