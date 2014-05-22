// Copyright (c) 2014, Cloudera, inc.

#ifndef KUDU_UTIL_FAILURE_DETECTOR_H_
#define KUDU_UTIL_FAILURE_DETECTOR_H_

#include <tr1/unordered_map>
#include <string>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/monotime.h"
#include "kudu/util/locks.h"
#include "kudu/util/status_callback.h"

// The minimum time the FailureMonitor will wait.
const int64_t kMinWakeUpTimeMillis = 10;

namespace kudu {
class MonoDelta;
class MonoTime;
class Status;
class RandomizedFailureMonitorThread;

// A generic interface for failure detector implementations.
// A failure detector is responsible for deciding whether a certain server is dead or alive.
class FailureDetector : public RefCountedThreadSafe<FailureDetector> {
 public:
  enum NodeStatus {
    DEAD,
    ALIVE
  };
  typedef std::tr1::unordered_map<std::string, NodeStatus> StatusMap;

  typedef Callback<void(const std::string& name,
                        const Status& status)> FailureDetectedCallback;

  virtual ~FailureDetector() {}

  // Registers a node with 'name' in the failure detector.
  //
  // If it returns Status::OK() the failure detector will from now
  // expect messages from the machine with 'name' and will trigger
  // 'callback' if a failure is detected.
  //
  // Returns Status::AlreadyPresent() if a machine with 'name' is
  // already registered in this failure detector.
  virtual Status Track(const MonoTime& now,
                       const std::string& name,
                       FailureDetectedCallback callback) = 0;

  // Stops tracking node with 'name'.
  virtual void UnTrack(const std::string& name) = 0;

  // Records that a message from machine with 'name' was received at 'now'.
  virtual void MessageFrom(const std::string& name, const MonoTime& now) = 0;

  // Updates the failure status of the tracked nodes.
  virtual void UpdateStatuses(const MonoTime& now) = 0;
};

// A simple failure detector implementation that considers a node dead
// when they have not reported by a certain time interval.
class TimedFailureDetector : public FailureDetector {
 public:
  struct Node {
    std::string permanent_name;
    MonoTime last_heard_of;
    FailureDetectedCallback callback;
    NodeStatus status;
  };

  virtual ~TimedFailureDetector() OVERRIDE;

  explicit TimedFailureDetector(MonoDelta failure_period);

  virtual Status Track(const MonoTime& now,
                       const std::string& name,
                       FailureDetectedCallback callback) OVERRIDE;

  virtual void UnTrack(const std::string& name) OVERRIDE;

  virtual void MessageFrom(const std::string& name, const MonoTime& now) OVERRIDE;

  virtual void UpdateStatuses(const MonoTime& now) OVERRIDE;
 private:
  typedef std::tr1::unordered_map<std::string, Node*> NodeMap;

  virtual FailureDetector::NodeStatus UpdateStatus(const std::string& name,
                                                   const MonoTime& now);

  const MonoDelta failure_period_;
  NodeMap nodes_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(TimedFailureDetector);
};

// A randomized failure monitor that wakes up in semi-regular intervals, runs
// FailureDetector::UpdateStatuses() for all nodes and triggers the failure
// callbacks if necessary.
//
// The wake up interval is defined by a normal distribution with median =
// 'median_nanos' and stddev = 'stddev_nanos', but with min values truncated
// to kMinWakeUpTime.
//
// We vary the wake up interval so that, if multiple nodes are started
// at the same time and one fails other nodes do not detect the failure
// at the same time.
//
// TODO potentially this could take a set of FDs instead of a single one, as long
// as they can use the same median/stddev. That would avoid us having one/two fd
// threads per node.
class RandomizedFailureMonitor {
 public:
  RandomizedFailureMonitor(int64_t median_nanos,
                           int64_t stdde);

  ~RandomizedFailureMonitor();

  // Starts the failure monitor.
  Status Start();
  // Stops the failure monitor.
  Status Stop();

  // Adds a failure detector to be monitored.
  void MonitorFailureDetector(const std::string& name,
                              const scoped_refptr<FailureDetector>& fd);

  // Unmonitors the failure detector with the provided
  void UnmonitorFailureDetector(const std::string& name);
 private:
  gscoped_ptr<RandomizedFailureMonitorThread> thread_;

  DISALLOW_COPY_AND_ASSIGN(RandomizedFailureMonitor);
};

}  // namespace kudu

#endif /* KUDU_UTIL_FAILURE_DETECTOR_H_ */
