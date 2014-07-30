// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MASTER_OPTIONS_H
#define KUDU_MASTER_MASTER_OPTIONS_H

#include "kudu/server/server_base_options.h"

namespace kudu {
namespace master {

// Options for constructing the master.
// These are filled in by gflags by default -- see the .cc file for
// the list of options and corresponding flags.
struct MasterOptions : public server::ServerBaseOptions {
  MasterOptions();
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_MASTER_OPTIONS_H */
