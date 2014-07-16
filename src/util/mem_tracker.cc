// Copyright (c) 2014, Cloudera, inc.

#include "util/mem_tracker.h"

#include <algorithm>
#include <boost/thread/locks.hpp>
#include <google/malloc_extension.h>
#include <limits>

#include "gutil/strings/join.h"
#include "gutil/strings/human_readable.h"
#include "gutil/strings/substitute.h"
#include "util/debug-util.h"
#include "util/mutex.h"
#include "util/status.h"

namespace kudu {

// NOTE: this class has been adapted from Impala, so the code style varies
// somewhat from kudu.

using std::string;
using std::stringstream;
using std::tr1::shared_ptr;
using std::vector;

using strings::Substitute;

MemTracker::TrackerMap MemTracker::id_to_mem_trackers_;
Mutex MemTracker::static_mem_trackers_lock_;
Atomic64 MemTracker::released_memory_since_gc_;

namespace {

void InitiateHighWaterMark(const string& id,
                           const string& descr,
                           gscoped_ptr<HighWaterMark<int64_t> >* hwm) {
  GaugePrototype<int64_t> proto(id.c_str(), MetricUnit::kBytes, descr.c_str());
  hwm->reset(new HighWaterMark<int64_t>(proto, 0));
}

} // anonymous namespace

shared_ptr<MemTracker> MemTracker::CreateTracker(int64_t byte_limit,
                                                 const string& id,
                                                 MemTracker* parent) {
  shared_ptr<MemTracker> tracker(new MemTracker(byte_limit,
                                                id,
                                                parent));
  AddToTrackerMap(id, tracker);
  return tracker;
}

shared_ptr<MemTracker> MemTracker::CreateTracker(FunctionGauge<uint64_t>* consumption_metric,
                                                 int64_t byte_limit,
                                                 const std::string& id) {
  shared_ptr<MemTracker> tracker(new MemTracker(consumption_metric,
                                                byte_limit,
                                                id));
  AddToTrackerMap(id, tracker);
  return tracker;
}

MemTracker::MemTracker(int64_t byte_limit, const string& id,
                       MemTracker* parent)
    : limit_(byte_limit),
      id_(id),
      descr_(Substitute("memory consumption for $0", id)),
      parent_(parent),
      consumption_metric_(NULL),
      auto_unregister_(false),
      enable_logging_(false),
      log_stack_(false) {
  InitiateHighWaterMark(id_, descr_, &consumption_);
  if (parent != NULL) {
    parent->AddChildTracker(this);
  }
  Init();
}

MemTracker::MemTracker(FunctionGauge<uint64_t>* consumption_metric,
                       int64_t byte_limit, const string& id)
  : limit_(byte_limit),
    id_(id),
    descr_(Substitute("memory consumption for $0", id)),
    parent_(NULL),
    consumption_metric_(consumption_metric),
    auto_unregister_(false),
    enable_logging_(false),
    log_stack_(false) {
  InitiateHighWaterMark(id_, descr_, &consumption_);
  Init();
}

MemTracker::~MemTracker() {
  MutexLock l(static_mem_trackers_lock_);
  if (auto_unregister_) {
    UnregisterFromParent();
  }
  TrackerMap::iterator it = id_to_mem_trackers_.find(id_);
  if (it != id_to_mem_trackers_.end()) {
    id_to_mem_trackers_.erase(it);
  }
}

void MemTracker::UnregisterFromParent() {
  DCHECK(parent_ != NULL);
  MutexLock l(parent_->child_trackers_lock_);
  parent_->child_trackers_.erase(child_tracker_it_);
  child_tracker_it_ = parent_->child_trackers_.end();
}

void MemTracker::AddToTrackerMap(const string& id, const shared_ptr<MemTracker>& tracker) {
  MutexLock l(static_mem_trackers_lock_);
  id_to_mem_trackers_[id] = tracker;
}

bool MemTracker::FindTracker(const string& id, shared_ptr<MemTracker>* tracker) {
  MutexLock l(static_mem_trackers_lock_);
  TrackerMap::iterator it = id_to_mem_trackers_.find(id);
  if (it != id_to_mem_trackers_.end()) {
    *tracker = it->second.lock();
    return true;
  }
  return false;
}

shared_ptr<MemTracker> MemTracker::FindOrCreateTracker(int64_t byte_limit,
                                                       const std::string& id,
                                                       MemTracker* parent) {
  MutexLock l(static_mem_trackers_lock_);
  TrackerMap::iterator it = id_to_mem_trackers_.find(id);
  if (it != id_to_mem_trackers_.end()) {
    return it->second.lock();
  }
  shared_ptr<MemTracker> ret(new MemTracker(byte_limit, id, parent));
  id_to_mem_trackers_[id] = ret;
  return ret;
}

void MemTracker::ListTrackers(vector<shared_ptr<MemTracker> >* trackers) {
  MutexLock l(static_mem_trackers_lock_);
  for (TrackerMap::iterator it = id_to_mem_trackers_.begin();
       it != id_to_mem_trackers_.end();
       ++it) {
    trackers->push_back(it->second.lock());
  }
}

void MemTracker::UpdateConsumption() {
  DCHECK(consumption_metric_ != NULL);
  DCHECK(parent_ == NULL);
  consumption_->set_value(consumption_metric_->value());
}

// TODO Use HighWaterMark for 'consumption_metric', then if
// 'consumption_metric' is not null, simply make calls to
// consumption() forward to consumption_metric_ instead.
void MemTracker::Consume(int64_t bytes) {
  if (bytes < 0) {
    Release(-bytes);
    return;
  }

  if (consumption_metric_ != NULL) {
    UpdateConsumption();
    return;
  }
  if (bytes == 0) {
    return;
  }
  if (PREDICT_FALSE(enable_logging_)) {
    LogUpdate(true, bytes);
  }
  for (std::vector<MemTracker*>::iterator tracker = all_trackers_.begin();
       tracker != all_trackers_.end(); ++tracker) {
    (*tracker)->consumption_->IncrementBy(bytes);
    if ((*tracker)->consumption_metric_ == NULL) {
      DCHECK_GE((*tracker)->consumption_->current_value(), 0);
    }
  }
}

bool MemTracker::TryConsume(int64_t bytes) {
  if (consumption_metric_ != NULL) {
    UpdateConsumption();
  }
  if (bytes <= 0) {
    return true;
  }
  if (PREDICT_FALSE(enable_logging_)) {
    LogUpdate(true, bytes);
  }

  int i = 0;
  // Walk the tracker tree top-down, to avoid expanding a limit on a child whose parent
  // won't accommodate the change.
  for (i = all_trackers_.size() - 1; i >= 0; --i) {
    if (all_trackers_[i]->limit_ < 0) {
      all_trackers_[i]->consumption_->IncrementBy(bytes);
    } else {
      if (!all_trackers_[i]->consumption_->TryIncrementBy(bytes, all_trackers_[i]->limit_)) {
        // One of the trackers failed, attempt to GC memory or expand our limit. If that
        // succeeds, TryUpdate() again. Bail if either fails.
        if (!all_trackers_[i]->GcMemory(all_trackers_[i]->limit_ - bytes) ||
            all_trackers_[i]->ExpandLimit(bytes)) {
          if (!all_trackers_[i]->consumption_->TryIncrementBy(
                  bytes, all_trackers_[i]->limit_)) {
            break;
          }
        } else {
          break;
        }
      }
    }
  }
  // Everyone succeeded, return.
  if (i == -1) {
    return true;
  }

  // Someone failed, roll back the ones that succeeded.
  // TODO: this doesn't roll it back completely since the max values for
  // the updated trackers aren't decremented. The max values are only used
  // for error reporting so this is probably okay. Rolling those back is
  // pretty hard; we'd need something like 2PC.
  //
  // TODO: This might leave us with an allocated resource that we can't use. Do we need
  // to adjust the consumption of the query tracker to stop the resource from never
  // getting used by a subsequent TryConsume()?
  for (int j = all_trackers_.size() - 1; j > i; --j) {
    all_trackers_[j]->consumption_->IncrementBy(-bytes);
  }
  return false;
}

void MemTracker::Release(int64_t bytes) {
  if (bytes < 0) {
    Consume(-bytes);
    return;
  }

  if (PREDICT_FALSE(base::subtle::Barrier_AtomicIncrement(&released_memory_since_gc_, bytes) >
                    GC_RELEASE_SIZE)) {
    GcTcmalloc();
  }

  if (consumption_metric_ != NULL) {
    UpdateConsumption();
    return;
  }

  if (bytes == 0) {
    return;
  }
  if (PREDICT_FALSE(enable_logging_)) {
    LogUpdate(false, bytes);
  }

  for (std::vector<MemTracker*>::iterator tracker = all_trackers_.begin();
       tracker != all_trackers_.end(); ++tracker) {
    (*tracker)->consumption_->IncrementBy(-bytes);
    // If a UDF calls FunctionContext::TrackAllocation() but allocates less than the
    // reported amount, the subsequent call to FunctionContext::Free() may cause the
    // process mem tracker to go negative until it is synced back to the tcmalloc
    // metric. Don't blow up in this case. (Note that this doesn't affect non-process
    // trackers since we can enforce that the reported memory usage is internally
    // consistent.)
    if ((*tracker)->consumption_metric_ == NULL) {
      DCHECK_GE((*tracker)->consumption_->current_value(), 0);
    }
  }
}

bool MemTracker::AnyLimitExceeded() {
  for (std::vector<MemTracker*>::iterator tracker = limit_trackers_.begin();
       tracker != limit_trackers_.end(); ++tracker) {
    if ((*tracker)->LimitExceeded()) {
      return true;
    }
  }
  return false;
}

bool MemTracker::LimitExceeded() {
  if (PREDICT_FALSE(CheckLimitExceeded())) {
    return GcMemory(limit_);
  }
  return false;
}

int64_t MemTracker::SpareCapacity() const {
  int64_t result = std::numeric_limits<int64_t>::max();
  for (std::vector<MemTracker*>::const_iterator tracker = limit_trackers_.begin();
       tracker != limit_trackers_.end(); ++tracker) {
    int64_t mem_left = (*tracker)->limit() - (*tracker)->consumption();
    result = std::min(result, mem_left);
  }
  return result;
}

bool MemTracker::GcMemory(int64_t max_consumption) {
  DCHECK_GE(max_consumption, 0);
  boost::lock_guard<simple_spinlock> l(gc_lock_);
  if (consumption_metric_ != NULL) {
    UpdateConsumption();
  }
  uint64_t pre_gc_consumption = consumption();
  // Check if someone gc'd before us
  if (pre_gc_consumption < max_consumption) {
    return false;
  }

  // Try to free up some memory
  for (int i = 0; i < gc_functions_.size(); ++i) {
    gc_functions_[i]();
    if (consumption_metric_ != NULL) {
      UpdateConsumption();
    }
    if (consumption() <= max_consumption) {
      break;
    }
  }

  return consumption() > max_consumption;
}

void MemTracker::GcTcmalloc() {
#ifdef TCMALLOC_ENABLED
  released_memory_since_gc_ = 0;
  MallocExtension::instance()->ReleaseFreeMemory();
#else
  // Nothing to do if not using tcmalloc.
#endif
}

std::string MemTracker::LogUsage(const std::string& prefix) const {
  stringstream ss;
  ss << prefix << id_ << ":";
  if (CheckLimitExceeded()) {
    ss << " memory limit exceeded.";
  }
  if (limit_ > 0) {
    ss << " Limit=" << HumanReadableNumBytes::ToString(limit_);
  }
  ss << " Consumption=" << HumanReadableNumBytes::ToString(consumption());

  stringstream prefix_ss;
  prefix_ss << prefix << "  ";
  string new_prefix = prefix_ss.str();
  MutexLock l(child_trackers_lock_);
  if (!child_trackers_.empty()) {
    ss << "\n" << LogUsage(new_prefix, child_trackers_);
  }
  return ss.str();
}

void MemTracker::Init() {
  // populate all_trackers_ and limit_trackers_
  MemTracker* tracker = this;
  while (tracker != NULL) {
    all_trackers_.push_back(tracker);
    if (tracker->has_limit()) limit_trackers_.push_back(tracker);
    tracker = tracker->parent_;
  }
  DCHECK_GT(all_trackers_.size(), 0);
  DCHECK_EQ(all_trackers_[0], this);
}

void MemTracker::AddChildTracker(MemTracker* tracker) {
  MutexLock l(child_trackers_lock_);
  tracker->child_tracker_it_ = child_trackers_.insert(child_trackers_.end(), tracker);
}

void MemTracker::LogUpdate(bool is_consume, int64_t bytes) const {
  stringstream ss;
  ss << this << " " << (is_consume ? "Consume: " : "Release: ") << bytes
     << " Consumption: " << consumption() << " Limit: " << limit_;
  if (log_stack_) {
    ss << std::endl << GetStackTrace();
  }
  LOG(ERROR) << ss.str();
}

std::string MemTracker::LogUsage(const std::string& prefix,
                                 const std::list<MemTracker*>& trackers) {
  vector<string> usage_strings;
  for (std::list<MemTracker*>::const_iterator it = trackers.begin();
       it != trackers.end(); ++it) {
    usage_strings.push_back((*it)->LogUsage(prefix));
  }
  return JoinStrings(usage_strings, "\n");
}

} // namespace kudu
