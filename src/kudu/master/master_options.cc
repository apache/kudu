// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/master_options.h"

#include <gflags/gflags.h>

#include "kudu/master/master.h"
#include "kudu/util/env.h"

namespace kudu {
namespace master {

DEFINE_string(master_base_dir, "/tmp/kudu-master",
              "Base directory for kudu master server");

DEFINE_string(master_rpc_bind_addresses, "0.0.0.0:7051",
             "Comma-separated list of addresses for the Tablet Server"
              " to bind to for RPC connections");

DEFINE_string(master_quorum, "",
              "Comma-separated list of all the RPC addresses for Master quorum."
              " NOTE: if not specified, assumes a standalone Master.");

DEFINE_int32(master_web_port, Master::kDefaultWebPort,
             "Port to bind to for the Master web server");
DEFINE_int32(master_num_acceptors_per_address, 1,
             "Number of RPC acceptor threads for each bound address");
DEFINE_int32(master_num_service_threads, 10,
             "Number of RPC worker threads to run");


MasterOptions::MasterOptions() {
  rpc_opts.rpc_bind_addresses = FLAGS_master_rpc_bind_addresses;
  rpc_opts.num_acceptors_per_address = FLAGS_master_num_acceptors_per_address;
  rpc_opts.num_service_threads = FLAGS_master_num_service_threads;
  rpc_opts.default_port = Master::kDefaultPort;

  webserver_opts.port = FLAGS_master_web_port;
  // The rest of the web server options are not overridable on a per-tablet-server
  // basis.

  base_dir = FLAGS_master_base_dir;

  env = Env::Default();

  if (!FLAGS_master_quorum.empty()) {
    Status s = HostPort::ParseStrings(FLAGS_master_quorum, Master::kDefaultPort,
                                      &master_quorum);
    if (!s.ok()) {
      LOG(FATAL) << "Couldn't parse the master_quorum flag('" << FLAGS_master_quorum << "'): "
                 << s.ToString();
    }
    if (master_quorum.size() < 2) {
      LOG(FATAL) << "At least 2 masters are required for a distributed quorum, but "
          "master_quorum flag ('" << FLAGS_master_quorum << "') only specifies "
                 << master_quorum.size() << " masters.";
    }
    if (master_quorum.size() == 2) {
      LOG(WARNING) << "Only 2 masters are specified by master_quorum_flag ('" <<
          FLAGS_master_quorum << "'), but minimum of 3 are required to tolerate failures"
          " of any one master. It is recommended to use at least 3 masters.";
    }
  }
}

bool MasterOptions::IsDistributed() const {
  return !master_quorum.empty();
}

} // namespace master
} // namespace kudu
