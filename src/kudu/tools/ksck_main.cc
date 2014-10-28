// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Command line tool to run Ksck against a cluster. Defaults to running against a local Master
// on the default RPC port. It verifies that all the reported Tablet Servers are running and that
// the tablets are in a consistent state.

#include <iostream>

#include "kudu/tools/ksck_remote.h"

using std::tr1::shared_ptr;

DEFINE_string(master_address, "localhost",
                "Address of master server to run against");

namespace kudu {
namespace tools {

void PrintUsage(char** argv) {
  std::cerr << "usage: " << argv[0] << " [--master_address=<address>]" << std::endl;
  std::cerr << "The tool defaults to running against a local Master." << std::endl;
  std::cerr << "Using --vmodule=ksck=1 gives more details at runtime." << std::endl;
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
  if (!IsOkElseLog(ksck->CheckMasterRunning())) {
    // If we couldn't connect to the Master it might just be that the wrong address was given
    // so we print the usage.
    PrintUsage(argv);
    return 1;
  }
  if (!IsOkElseLog(ksck->CheckTabletServersRunning())) {
    return 1;
  }
  if (!IsOkElseLog(ksck->CheckTablesConsistency())) {
    return 1;
  }
  return 0;
}
} // namespace tools
} // namespace kudu

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return kudu::tools::KsckMain(argc, argv);
}
