// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MASTER_PATH_HANDLERS_H
#define KUDU_MASTER_MASTER_PATH_HANDLERS_H

#include "gutil/macros.h"
#include "server/webserver.h"

#include <sstream>

namespace kudu {
namespace master {

class Master;

// Web page support for the master.
class MasterPathHandlers {
 public:
  explicit MasterPathHandlers(Master* master)
    : master_(master) {
  }

  ~MasterPathHandlers();

  Status Register(Webserver* server);

  void HandleTabletServers(const Webserver::ArgumentMap& args,
                           std::stringstream* output);

 private:
  Master* master_;
  DISALLOW_COPY_AND_ASSIGN(MasterPathHandlers);
};

void HandleTabletServersPage(const Webserver::ArgumentMap& args, std::stringstream* output);

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_MASTER_PATH_HANDLERS_H */
