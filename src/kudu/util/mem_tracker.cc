// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/mem_tracker.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <deque>
#include <gperftools/malloc_extension.h>
#include <limits>
#include <list>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/once.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mutex.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"

DEFINE_int64(memory_limit_hard_mb, 0,
             "Maximum amount of memory this daemon should use, in megabytes. "
             "A value of 0 autosizes based on the total system memory. "
             "A value of -1 disables all memory limiting.");
TAG_FLAG(memory_limit_hard_mb, stable);

DEFINE_int32(memory_limit_soft_percentage, 60,
             "Percentage of the hard memory limit that this daemon may "
             "consume before memory throttling of writes begins. The greater "
             "the excess, the higher the chance of throttling. In general, a "
             "lower soft limit leads to smoother write latencies but "
             "decreased throughput, and vice versa for a higher soft limit.");
TAG_FLAG(memory_limit_hard_mb, advanced);


namespace kudu {

// NOTE: this class has been adapted from Impala, so the code style varies
// somewhat from kudu.

using std::deque;
using std::list;
using std::string;
using std::stringstream;
using std::tr1::shared_ptr;
using std::vector;

using strings::Substitute;

// The ancestor for all trackers. Every tracker is visible from the root down.
static shared_ptr<MemTracker> root_tracker;
static GoogleOnceType root_tracker_once = GOOGLE_ONCE_INIT;

// Total amount of memory from calls to Release() since the last GC. If this
// is greater than GC_RELEASE_SIZE, this will trigger a tcmalloc gc.
static Atomic64 released_memory_since_gc;

// Validate that soft limit is a percentage.
static bool ValidateSoftLimit(const char* flagname, int value) {
  if (value >= 0 && value <= 100) {
    return true;
  }
  LOG(ERROR) << Substitute("$0 must be a percentage, value $1 is invalid",
                           flagname, value);
  return false;
}
static bool dummy = google::RegisterFlagValidator(
    &FLAGS_memory_limit_soft_percentage, &ValidateSoftLimit);

#ifdef TCMALLOC_ENABLED
static uint64_t GetTCMallocCurrentAllocatedBytes() {
  size_t value;
  if (!MallocExtension::instance()->GetNumericProperty(
      "generic.current_allocated_bytes", &value)) {
    LOG(DFATAL) << "Failed to get tcmalloc current allocated bytes";
  }
  return value;
}
#endif

void MemTracker::CreateRootTracker() {
  int64_t limit = FLAGS_memory_limit_hard_mb * 1024 * 1024;
  if (limit == 0) {
    // If no limit is provided, we'll use 80% of system RAM.
    int64_t total_ram;
    CHECK_OK(Env::Default()->GetTotalRAMBytes(&total_ram));
    limit = total_ram * 4;
    limit /= 5;
  }

  ConsumptionFunction f;
#ifdef TCMALLOC_ENABLED
  f = &GetTCMallocCurrentAllocatedBytes;
#endif
  root_tracker.reset(new MemTracker(f, limit, "root",
                                    shared_ptr<MemTracker>()));
  root_tracker->Init();
  LOG(INFO) << StringPrintf("MemTracker: hard memory limit is %.6f GB",
                            (static_cast<float>(limit) / (1024.0 * 1024.0 * 1024.0)));
  LOG(INFO) << StringPrintf("MemTracker: soft memory limit is %.6f GB",
                            (static_cast<float>(root_tracker->soft_limit_) /
                                (1024.0 * 1024.0 * 1024.0)));
}

shared_ptr<MemTracker> MemTracker::CreateTracker(int64_t byte_limit,
                                                 const string& id,
                                                 const shared_ptr<MemTracker>& parent) {
  shared_ptr<MemTracker> real_parent = parent ? parent : GetRootTracker();
  MutexLock l(real_parent->child_trackers_lock_);
  return CreateTrackerUnlocked(byte_limit, id, real_parent);
}

shared_ptr<MemTracker> MemTracker::CreateTrackerUnlocked(int64_t byte_limit,
                                                         const string& id,
                                                         const shared_ptr<MemTracker>& parent) {
  DCHECK(parent);
  shared_ptr<MemTracker> tracker(new MemTracker(ConsumptionFunction(), byte_limit, id, parent));
  parent->AddChildTrackerUnlocked(tracker.get());
  tracker->Init();

  return tracker;
}

MemTracker::MemTracker(const ConsumptionFunction& consumption_func,
                       int64_t byte_limit,
                       const string& id,
                       const shared_ptr<MemTracker>& parent)
    : limit_(byte_limit),
      id_(id),
      descr_(Substitute("memory consumption for $0", id)),
      parent_(parent),
      consumption_(0),
      consumption_func_(consumption_func),
      rand_(GetRandomSeed32()),
      enable_logging_(false),
      log_stack_(false) {
  VLOG(1) << "Creating tracker " << ToString();
  if (consumption_func_) {
    UpdateConsumption();
  }
  soft_limit_ = (limit_ == -1)
      ? -1 : (limit_ * FLAGS_memory_limit_soft_percentage) / 100;
}

MemTracker::~MemTracker() {
  VLOG(1) << "Destroying tracker " << ToString();
  if (parent_) {
    DCHECK(consumption() == 0) << "Memory tracker " << ToString()
        << " has unreleased consumption " << consumption();
    parent_->Release(consumption());
    UnregisterFromParent();
  }
}

void MemTracker::UnregisterFromParent() {
  DCHECK(parent_);
  MutexLock l(parent_->child_trackers_lock_);
  if (child_tracker_it_ != parent_->child_trackers_.end()) {
    parent_->child_trackers_.erase(child_tracker_it_);
    child_tracker_it_ = parent_->child_trackers_.end();
  }
}

string MemTracker::ToString() const {
  string s;
  const MemTracker* tracker = this;
  while (tracker) {
    if (s != "") {
      s += "->";
    }
    s += tracker->id();
    tracker = tracker->parent_.get();
  }
  return s;
}

bool MemTracker::FindTracker(const string& id,
                             shared_ptr<MemTracker>* tracker,
                             const shared_ptr<MemTracker>& parent) {
  shared_ptr<MemTracker> real_parent = parent ? parent : GetRootTracker();
  MutexLock l(real_parent->child_trackers_lock_);
  return FindTrackerUnlocked(id, tracker, real_parent);
}

bool MemTracker::FindTrackerUnlocked(const string& id,
                                     shared_ptr<MemTracker>* tracker,
                                     const shared_ptr<MemTracker>& parent) {
  DCHECK(parent != NULL);
  parent->child_trackers_lock_.AssertAcquired();
  BOOST_FOREACH(MemTracker* child, parent->child_trackers_) {
    if (child->id() == id) {
      *tracker = child->shared_from_this();
      return true;
    }
  }
  return false;
}

shared_ptr<MemTracker> MemTracker::FindOrCreateTracker(int64_t byte_limit,
                                                       const string& id,
                                                       const shared_ptr<MemTracker>& parent) {
  shared_ptr<MemTracker> real_parent = parent ? parent : GetRootTracker();
  MutexLock l(real_parent->child_trackers_lock_);
  shared_ptr<MemTracker> found;
  if (FindTrackerUnlocked(id, &found, real_parent)) {
    return found;
  }
  return CreateTrackerUnlocked(byte_limit, id, real_parent);
}

void MemTracker::ListTrackers(vector<shared_ptr<MemTracker> >* trackers) {
  trackers->clear();
  deque<shared_ptr<MemTracker> > to_process;
  to_process.push_front(GetRootTracker());
  while (!to_process.empty()) {
    shared_ptr<MemTracker> t = to_process.back();
    to_process.pop_back();

    trackers->push_back(t);
    {
      MutexLock l(t->child_trackers_lock_);
      BOOST_FOREACH(MemTracker* child, t->child_trackers_) {
        to_process.push_back(child->shared_from_this());
      }
    }
  }
}

void MemTracker::UpdateConsumption() {
  DCHECK(!consumption_func_.empty());
  DCHECK(parent_.get() == NULL);
  consumption_.set_value(consumption_func_());
}

void MemTracker::Consume(int64_t bytes) {
  if (bytes < 0) {
    Release(-bytes);
    return;
  }

  if (!consumption_func_.empty()) {
    UpdateConsumption();
    return;
  }
  if (bytes == 0) {
    return;
  }
  if (PREDICT_FALSE(enable_logging_)) {
    LogUpdate(true, bytes);
  }
  for (vector<MemTracker*>::iterator tracker = all_trackers_.begin();
       tracker != all_trackers_.end(); ++tracker) {
    (*tracker)->consumption_.IncrementBy(bytes);
    if (!(*tracker)->consumption_func_.empty()) {
      DCHECK_GE((*tracker)->consumption_.current_value(), 0);
    }
  }
}

bool MemTracker::TryConsume(int64_t bytes) {
  if (!consumption_func_.empty()) {
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
    MemTracker *tracker = all_trackers_[i];
    if (tracker->limit_ < 0) {
      tracker->consumption_.IncrementBy(bytes);
    } else {
      if (!tracker->consumption_.TryIncrementBy(bytes, tracker->limit_)) {
        // One of the trackers failed, attempt to GC memory or expand our limit. If that
        // succeeds, TryUpdate() again. Bail if either fails.
        if (!tracker->GcMemory(tracker->limit_ - bytes) ||
            tracker->ExpandLimit(bytes)) {
          if (!tracker->consumption_.TryIncrementBy(
                  bytes, tracker->limit_)) {
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
    all_trackers_[j]->consumption_.IncrementBy(-bytes);
  }
  return false;
}

void MemTracker::Release(int64_t bytes) {
  if (bytes < 0) {
    Consume(-bytes);
    return;
  }

  if (PREDICT_FALSE(base::subtle::Barrier_AtomicIncrement(&released_memory_since_gc, bytes) >
                    GC_RELEASE_SIZE)) {
    GcTcmalloc();
  }

  if (!consumption_func_.empty()) {
    UpdateConsumption();
    return;
  }

  if (bytes == 0) {
    return;
  }
  if (PREDICT_FALSE(enable_logging_)) {
    LogUpdate(false, bytes);
  }

  for (vector<MemTracker*>::iterator tracker = all_trackers_.begin();
       tracker != all_trackers_.end(); ++tracker) {
    (*tracker)->consumption_.IncrementBy(-bytes);
    // If a UDF calls FunctionContext::TrackAllocation() but allocates less than the
    // reported amount, the subsequent call to FunctionContext::Free() may cause the
    // process mem tracker to go negative until it is synced back to the tcmalloc
    // metric. Don't blow up in this case. (Note that this doesn't affect non-process
    // trackers since we can enforce that the reported memory usage is internally
    // consistent.)
    if (!(*tracker)->consumption_func_.empty()) {
      DCHECK_GE((*tracker)->consumption_.current_value(), 0);
    }
  }
}

bool MemTracker::AnyLimitExceeded() {
  for (vector<MemTracker*>::iterator tracker = limit_trackers_.begin();
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

bool MemTracker::SoftLimitExceeded(double* current_capacity_pct) {
  // Did we exceed the actual limit?
  if (LimitExceeded()) {
    if (current_capacity_pct) {
      *current_capacity_pct =
          static_cast<double>(consumption()) / limit() * 100;
    }
    return true;
  }

  // No soft limit defined.
  if (!has_limit() || limit_ == soft_limit_) {
    return false;
  }

  // Are we under the soft limit threshold?
  int64_t usage = consumption();
  if (usage < soft_limit_) {
    return false;
  }

  // We're over the threshold; were we randomly chosen to be over the soft limit?
  if (usage + rand_.Uniform64(limit_ - soft_limit_) > limit_) {
    bool exceeded = GcMemory(soft_limit_);
    if (exceeded && current_capacity_pct) {
      *current_capacity_pct =
          static_cast<double>(consumption()) / limit() * 100;
    }
    return exceeded;
  }
  return false;
}

bool MemTracker::AnySoftLimitExceeded(double* current_capacity_pct) {
  BOOST_FOREACH(MemTracker* t, limit_trackers_) {
    if (t->SoftLimitExceeded(current_capacity_pct)) {
      return true;
    }
  }
  return false;
}

int64_t MemTracker::SpareCapacity() const {
  int64_t result = std::numeric_limits<int64_t>::max();
  for (vector<MemTracker*>::const_iterator tracker = limit_trackers_.begin();
       tracker != limit_trackers_.end(); ++tracker) {
    int64_t mem_left = (*tracker)->limit() - (*tracker)->consumption();
    result = std::min(result, mem_left);
  }
  return result;
}

bool MemTracker::GcMemory(int64_t max_consumption) {
  if (max_consumption < 0) {
    // Impossible to GC enough memory to reach the goal.
    return true;
  }

  lock_guard<simple_spinlock> l(&gc_lock_);
  if (!consumption_func_.empty()) {
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
    if (!consumption_func_.empty()) {
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
  released_memory_since_gc = 0;
  MallocExtension::instance()->ReleaseFreeMemory();
#else
  // Nothing to do if not using tcmalloc.
#endif
}

string MemTracker::LogUsage(const string& prefix) const {
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
  while (tracker) {
    all_trackers_.push_back(tracker);
    if (tracker->has_limit()) limit_trackers_.push_back(tracker);
    tracker = tracker->parent_.get();
  }
  DCHECK_GT(all_trackers_.size(), 0);
  DCHECK_EQ(all_trackers_[0], this);
}

void MemTracker::AddChildTrackerUnlocked(MemTracker* tracker) {
  child_trackers_lock_.AssertAcquired();
#ifndef NDEBUG
  shared_ptr<MemTracker> found;
  CHECK(!FindTrackerUnlocked(tracker->id(), &found, shared_from_this()))
    << Substitute("Duplicate memory tracker (id $0) on parent $1",
                  tracker->id(), ToString());
#endif
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

string MemTracker::LogUsage(const string& prefix,
                            const list<MemTracker*>& trackers) {
  vector<string> usage_strings;
  BOOST_FOREACH(const MemTracker* child, trackers) {
    usage_strings.push_back(child->LogUsage(prefix));
  }
  return JoinStrings(usage_strings, "\n");
}

shared_ptr<MemTracker> MemTracker::GetRootTracker() {
  GoogleOnceInit(&root_tracker_once, &MemTracker::CreateRootTracker);
  return root_tracker;
}

} // namespace kudu
