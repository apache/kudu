// Copyright (c) 2013, Cloudera, inc.

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

// TODO Get rid of these flags (only have "--master_peers", and possibly
// "--initial_leader") once leader elections are implemented.
DEFINE_bool(leader, false,
            "If true, this server is the leader of a master quorum.");
DEFINE_string(leader_address, "",
              "Address of the leader of the master quorum (iff the node is a follower).");
DEFINE_string(follower_addresses, "",
              "Comma separated list of followers in the master quorum (not including this node).");

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

  leader = FLAGS_leader;

  if (FLAGS_leader_address != "") {
    Status s = leader_address.ParseString(FLAGS_leader_address, Master::kDefaultPort);
    if (!s.ok()) {
      LOG(FATAL) << "Couldn't parse the leader_address flag: " << s.ToString();
    }
  } else if (!leader && FLAGS_follower_addresses != "") {
    LOG(FATAL) << "Invalid configuration: leader flag is not set, leader_address is flag is not "
               << "specified, but distributed mode is requested (follower_addresses flag is set).";
  }

  // TODO: Currently we require at least three masters for a
  // distributed configuration. We should decide whether to support
  // even numbers of masters (including two) -- with the caveat being
  // that all these masters must remain available, or require that
  // number of masters must always be odd.
  if (FLAGS_follower_addresses != "") {
    Status s = HostPort::ParseStrings(FLAGS_follower_addresses, Master::kDefaultPort,
                                      &follower_addresses);
    if (!s.ok()) {
      LOG(FATAL) << "Couldn't parse the follower_addresses flag: " << s.ToString();
    }
  } else if (leader || FLAGS_leader_address != "") {
    LOG(FATAL) << "Master is started in distributed mode, but follower_addresses flag is not "
        "specified.";
  }
}

bool MasterOptions::IsDistributed() const {
  return leader || leader_address.host() != "";
}

} // namespace master
} // namespace kudu
