// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/failure_detector.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

namespace kudu {

using strings::Substitute;

TimedFailureDetector::TimedFailureDetector(MonoDelta failure_period)
    : failure_period_(failure_period) {
}

TimedFailureDetector::~TimedFailureDetector() {
  STLDeleteValues(&nodes_);
}

Status TimedFailureDetector::Track(const MonoTime& now,
                                   const string& name,
                                   FailureDetectedCallback callback) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  gscoped_ptr<Node> node(new Node);
  node->permanent_name = name;
  node->callback = callback;
  node->last_heard_of = now;
  node->status = ALIVE;
  if (!InsertIfNotPresent(&nodes_, name, node.get())) {
    return Status::AlreadyPresent(
        Substitute("Node with name: $0 is already being monitored.", name));
  }
  ignore_result(node.release());
  return Status::OK();
}

void TimedFailureDetector::UnTrack(const string& name) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  delete EraseKeyReturnValuePtr(&nodes_, name);
}

void TimedFailureDetector::MessageFrom(const std::string& name, const MonoTime& now) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  Node* node = FindPtrOrNull(nodes_, name);
  if (node == NULL) {
    VLOG(1) << "Not tracking node: " << name;
    return;
  }
  node->last_heard_of = now;
  node->status = ALIVE;
}

FailureDetector::NodeStatus TimedFailureDetector::UpdateStatus(const std::string& name,
                                                               const MonoTime& now) {
  Node* node = FindOrDie(nodes_, name);
  if (now.GetDeltaSince(node->last_heard_of).MoreThan(failure_period_)) {
    node->status = DEAD;
    node->callback.Run(name, Status::RemoteError(Substitute("Node: $0 failed.", name)));
  }
  return node->status;
}

void TimedFailureDetector::UpdateStatuses(const MonoTime& now) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  BOOST_FOREACH(const NodeMap::value_type& entry, nodes_) {
    UpdateStatus(entry.first, now);
  }
}

class RandomizedFailureMonitorThread {
 public:
  RandomizedFailureMonitorThread(int64_t period_median_nanos,
                                 int64_t period_stddev_nanos);

  Status Start();
  Status Stop();

  bool MonitorFailureDetector(const string& name,
                              const scoped_refptr<FailureDetector>& fd);

  void UnmonitorFailureDetector(const string& name);

 private:
  typedef std::tr1::unordered_map<string, scoped_refptr<FailureDetector> > FDMap;
  void RunThread();
  bool IsCurrentThread() const;

  // The period.
  int64_t period_median_nanos_;
  int64_t period_stddev_nanos_;

  // The actual running thread (NULL before it is started)
  scoped_refptr<kudu::Thread> thread_;

  CountDownLatch run_latch_;

  // Whether the failure monitor should shutdown.
  bool shutdown_;

  FDMap fds_;

  mutable simple_spinlock lock_;
  DISALLOW_COPY_AND_ASSIGN(RandomizedFailureMonitorThread);
};

RandomizedFailureMonitor::RandomizedFailureMonitor(int64_t median_nanos,
                                                   int64_t stddev_nanos)
    : thread_(new RandomizedFailureMonitorThread(median_nanos, stddev_nanos)) {
}

RandomizedFailureMonitor::~RandomizedFailureMonitor() {
  WARN_NOT_OK(Stop(), "Unable to stop failure monitor thread");
}

Status RandomizedFailureMonitor::Start() {
  return thread_->Start();
}

Status RandomizedFailureMonitor::Stop() {
  return thread_->Stop();
}

// Adds a failure detector to be monitored.
void RandomizedFailureMonitor::MonitorFailureDetector(
    const std::string& name,
    const scoped_refptr<FailureDetector>& fd) {
  thread_->MonitorFailureDetector(name, fd);
}

// Unmonitors the failure detector with the provided name.
void RandomizedFailureMonitor::UnmonitorFailureDetector(const std::string& name) {
  thread_->UnmonitorFailureDetector(name);
}

RandomizedFailureMonitorThread::RandomizedFailureMonitorThread(
    int64_t period_median_nanos,
    int64_t period_stddev_nanos)
    : period_median_nanos_(period_median_nanos),
      period_stddev_nanos_(period_stddev_nanos),
      run_latch_(0),
      shutdown_(false) {
}

void RandomizedFailureMonitorThread::RunThread() {
  CHECK(IsCurrentThread());
  VLOG(1) << "Failure monitor thread starting";

  while (true) {
    int64_t nanos = NormalDist(period_median_nanos_, period_stddev_nanos_);
    if (nanos * 1000 * 1000 < kMinWakeUpTimeMillis) {
      nanos = kMinWakeUpTimeMillis  * 1000 * 1000;
    }

    MonoDelta delta = MonoDelta::FromNanoseconds(nanos);
    VLOG(3) << "Failure monitor sleeping for: " << delta.ToString();
    if (run_latch_.WaitFor(delta)) {
      // CountDownLatch reached 0
      lock_guard<simple_spinlock> lock(&lock_);
      // check if we were told to shutdown
      if (shutdown_) {
        // Latch fired -- exit loop
        VLOG(1) << "FailureMonitorThread  thread finished";
        return;
      }
    }

    MonoTime now = MonoTime::Now(MonoTime::FINE);
    // take a copy of the FD map under the lock.
    FDMap fds_copy;
    {
      lock_guard<simple_spinlock> l(&lock_);
      fds_copy = fds_;
    }
    BOOST_FOREACH(const FDMap::value_type& entry, fds_copy) {
      entry.second->UpdateStatuses(now);
    }
  }
}

bool RandomizedFailureMonitorThread::IsCurrentThread() const {
  return thread_.get() == kudu::Thread::current_thread();
}

Status RandomizedFailureMonitorThread::Start() {
  CHECK(thread_ == NULL);
  run_latch_.Reset(1);
  return kudu::Thread::Create("failure-monitors", "failure-monitor",
                              &RandomizedFailureMonitorThread::RunThread,
                              this, &thread_);
}

Status RandomizedFailureMonitorThread::Stop() {
  if (!thread_) {
    return Status::OK();
  }

  {
    lock_guard<simple_spinlock> l(&lock_);
    if (shutdown_) {
      return Status::OK();
    }
    shutdown_ = true;
  }

  run_latch_.CountDown();
  RETURN_NOT_OK(ThreadJoiner(thread_.get()).Join());
  return Status::OK();
}

bool RandomizedFailureMonitorThread::MonitorFailureDetector(
    const string& name,
    const scoped_refptr<FailureDetector>& fd) {
  {
    lock_guard<simple_spinlock> l(&lock_);
    return InsertIfNotPresent(&fds_, name, fd);
  }
}

void RandomizedFailureMonitorThread::UnmonitorFailureDetector(const string& name) {
  {
    lock_guard<simple_spinlock> l(&lock_);
    fds_.erase(name);
  }
}

}  // namespace kudu
