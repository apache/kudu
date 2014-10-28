// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_UTIL_RESETTABLE_HEARTBEATER_H_
#define KUDU_UTIL_RESETTABLE_HEARTBEATER_H_

#include <boost/function.hpp>
#include <string>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"

namespace kudu {
class MonoDelta;
class Status;
class ResettableHeartbeaterThread;

typedef boost::function<Status()> HeartbeatFunction;

// A resettable hearbeater that takes a function and calls
// it to perform a regular heartbeat, unless Reset() is called
// in which case the heartbeater resets the heartbeat period.
// The point is to send "I'm Alive" heartbeats only if no regular
// messages are sent in the same period.
//
// TODO Eventually this should be used instead of the master heartbeater
// as it shares a lot of logic with the exception of the specific master
// stuff (and the fact that it is resettable).
//
// TODO We'll have a lot of these per server, so eventually we need
// to refactor this so that multiple heartbeaters share something like
// java's ScheduledExecutor.
//
// TODO Do something about failed hearbeats, right now this is just
// logging. Probably could take more arguments and do more of an
// exponential backoff.
//
// This class is thread safe.
class ResettableHeartbeater {
 public:
  ResettableHeartbeater(const std::string& name,
                        MonoDelta period,
                        HeartbeatFunction function);

  // Starts the heartbeater
  Status Start();

  // Stops the hearbeater
  Status Stop();

  // Resets the heartbeat period.
  void Reset();

  ~ResettableHeartbeater();
 private:
  gscoped_ptr<ResettableHeartbeaterThread> thread_;

  DISALLOW_COPY_AND_ASSIGN(ResettableHeartbeater);
};

}  // namespace kudu

#endif /* KUDU_UTIL_RESETTABLE_HEARTBEATER_H_ */
