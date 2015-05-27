// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_HEARTBEATER_H
#define KUDU_TSERVER_HEARTBEATER_H

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tserver {

class TabletServer;
struct TabletServerOptions;

// Component of the Tablet Server which is responsible for heartbeating to the
// leader master.
//
// TODO: send heartbeats to non-leader masters.
class Heartbeater {
 public:
  Heartbeater(const TabletServerOptions& options, TabletServer* server);
  Status Start();
  Status Stop();

  // Trigger a heartbeat as soon as possible, even if the normal
  // heartbeat interval has not expired.
  void TriggerASAP();

  ~Heartbeater();

 private:
  class Thread;
  gscoped_ptr<Thread> thread_;
  DISALLOW_COPY_AND_ASSIGN(Heartbeater);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_HEARTBEATER_H */
