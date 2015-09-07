// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/master_options.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/master/master.h"
#include "kudu/util/flag_tags.h"

namespace kudu {
namespace master {

DEFINE_string(master_addresses, "",
              "Comma-separated list of all the RPC addresses for Master config. "
              "This is used to configure the replicated Master process "
              "(currently considered experimental). "
              "NOTE: if not specified, configures a non-replicated Master.");
TAG_FLAG(master_addresses, experimental);

MasterOptions::MasterOptions() {
  rpc_opts.default_port = Master::kDefaultPort;

  if (!FLAGS_master_addresses.empty()) {
    Status s = HostPort::ParseStrings(FLAGS_master_addresses, Master::kDefaultPort,
                                      &master_addresses);
    if (!s.ok()) {
      LOG(FATAL) << "Couldn't parse the master_addresses flag('" << FLAGS_master_addresses << "'): "
                 << s.ToString();
    }
    if (master_addresses.size() < 2) {
      LOG(FATAL) << "At least 2 masters are required for a distributed config, but "
          "master_addresses flag ('" << FLAGS_master_addresses << "') only specifies "
                 << master_addresses.size() << " masters.";
    }
    if (master_addresses.size() == 2) {
      LOG(WARNING) << "Only 2 masters are specified by master_addresses_flag ('" <<
          FLAGS_master_addresses << "'), but minimum of 3 are required to tolerate failures"
          " of any one master. It is recommended to use at least 3 masters.";
    }
  }
}

bool MasterOptions::IsDistributed() const {
  return !master_addresses.empty();
}

} // namespace master
} // namespace kudu
