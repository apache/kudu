// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_HEARTBEATER_H
#define KUDU_TSERVER_HEARTBEATER_H

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "util/status.h"

namespace kudu {
namespace tserver {

class TabletServer;
struct TabletServerOptions;

// Component of the Tablet Server which is responsible for heartbeating to the
// master.
class Heartbeater {
 public:
  Heartbeater(const TabletServerOptions& options, TabletServer* server);
  Status Start();
  Status Stop();

  ~Heartbeater();

 private:
  class Thread;
  gscoped_ptr<Thread> thread_;
  DISALLOW_COPY_AND_ASSIGN(Heartbeater);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_HEARTBEATER_H */
