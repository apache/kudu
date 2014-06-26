// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_UTIL_MEM_TRACKER_H
#define KUDU_UTIL_MEM_TRACKER_H

#include <boost/functional.hpp>
#include <boost/thread/mutex.hpp>
#include <list>
#include <stdint.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "util/locks.h"
#include "util/metrics.h"

namespace kudu {

class Status;
class MemTracker;

// A MemTracker tracks memory consumption; it contains an optional limit
// and can be arranged into a tree structure such that the consumption tracked
// by a MemTracker is also tracked by its ancestors.
//
// By default, memory consumption is tracked via calls to Consume()/Release(), either to
// the tracker itself or to one of its descendents. Alternatively, a consumption metric
// can specified, and then the metric's value is used as the consumption rather than the
// tally maintained by Consume() and Release(). A tcmalloc metric is used to track process
// memory consumption, since the process memory usage may be higher than the computed
// total memory (tcmalloc does not release deallocated memory immediately).
//
// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
// reached. If LimitExceeded() is called and the limit is exceeded, it will first call the
// GcFunctions to try to free memory and recheck the limit. For example, the process
// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
// this will be called before the process limit is reported as exceeded. GcFunctions are
// called in the order they are added, so expensive functions should be added last.
//
// This class is thread-safe.
//
// NOTE: this class has been partially ported over from Impala with
// several changes, and as a result the style differs somewhat from
// the Kudu style (e.g., BOOST_FOREACH is not used).
//
// Changes from Impala:
// 1) Id a string vs. a TUniqueId
// 2) There is no concept of query trackers vs. pool trackers -- trackers are instead
//    associated with objects. Parent hierarchy is preserved, with the assumption that,
//    e.g., a tablet server's memtracker will have as its children the tablets' memtrackers,
//    which in turn will have memtrackers for their caches, logs, and so forth.
//
// TODO: this classes uses a lot of statics fields and methods, which
// isn't common in Kudu. It is probably wise to later move the
// 'registry' of trackers to a separate class, but it's better to
// start using the 'class' *first* and then change this functionality,
// depending on how MemTracker ends up being used in Kudu.
class MemTracker {
 public:

  // Signature for function that can be called to free some memory after limit is reached.
  typedef boost::function<void ()> GcFunction;

  ~MemTracker();

  // Removes this tracker from parent_->child_trackers_.
  void UnregisterFromParent();

  // Creates and adds the tracker to a static map so that it can be
  // retrieved with FindTracker/FindOrCreateTracker.
  //
  // byte_limit < 0 means no limit; 'id' is a used as a label for
  // LogUsage() and web UI and must be unique; set 'parent' to NULL if
  // there is no parent.
  static std::tr1::shared_ptr<MemTracker> CreateTracker(int64_t byte_limit,
                                                        const std::string& id,
                                                        MemTracker* parent);

  // Factory method for tracker that uses consumption_metric as the
  // consumption value.  Consume()/Release() can still be called.
  // Adds the tracker to a static map so that it can be retrieved with
  // FindTracker/FindOrCreateTracker.
  //
  // TODO Gauge-based memtrackers can't have parents (but can have
  // children). In the future, however, it may be very convenient to
  // use FunctionGauges to monitor memory consumed by e.g., various
  // per-tablet in-memory structures -- where it is logical to have an
  // umbrella per-tablet tracker as a parent.
  static std::tr1::shared_ptr<MemTracker> CreateTracker(FunctionGauge<uint64_t>* consumption_metric,
                                                        int64_t byte_limit,
                                                        const std::string& id);

  // If a tracker with the specified 'id' exists in the tracker map,
  // sets 'tracker' to reference that instance. Returns false if no
  // such tracker exists in the map.
  static bool FindTracker(const std::string& id,
                          std::tr1::shared_ptr<MemTracker>* tracker);

  // If a tracker with the specified 'id' exists in the tracker map,
  // returns a shared_ptr to that instance. Otherwise, creates a new
  // MemTracker with the specified byte_limit, id, and parent.
  static std::tr1::shared_ptr<MemTracker> FindOrCreateTracker(int64_t byte_limit,
                                                              const std::string& id,
                                                              MemTracker* parent);


  // Returns a list of all the valid trackers.
  static void ListTrackers(std::vector<std::tr1::shared_ptr<MemTracker> >* trackers);

  // Updates consumption from the consumption metric specified in the constructor.
  // NOTE: this method will crash if 'consumption_metric_' is not set.
  void UpdateConsumption();

  // Increases consumption of this tracker and its ancestors by 'bytes'.
  void Consume(int64_t bytes);

  // Try to expand the limit (by asking the resource broker for more memory) by at least
  // 'bytes'. Returns false if not possible, true if the request succeeded. May allocate
  // more memory than was requested.
  // TODO: always returns false for now, not yet implemented.
  bool ExpandLimit(int64_t /* unused: bytes */) { return false; }

  // Increases consumption of this tracker and its ancestors by 'bytes' only if
  // they can all consume 'bytes'. If this brings any of them over, none of them
  // are updated.
  // Returns true if the try succeeded.
  bool TryConsume(int64_t bytes);

  // Decreases consumption of this tracker and its ancestors by 'bytes'.
  void Release(int64_t bytes);

  // Returns true if a valid limit of this tracker or one of its ancestors is
  // exceeded.
  bool AnyLimitExceeded();

  // If this tracker has a limit, checks the limit and attempts to free up some memory if
  // the limit is exceeded by calling any added GC functions. Returns true if the limit is
  // exceeded after calling the GC functions. Returns false if there is no limit.
  bool LimitExceeded();

  // Returns the maximum consumption that can be made without exceeding the limit on
  // this tracker or any of its parents. Returns int64_t::max() if there are no
  // limits and a negative value if any limit is already exceeded.
  int64_t SpareCapacity() const;


  int64_t limit() const { return limit_; }
  bool has_limit() const { return limit_ >= 0; }
  const std::string& id() const { return id_; }

  // Returns the memory consumed in bytes.
  int64_t consumption() const {
    return consumption_->current_value();
  }

  // Note that if consumption_ is based on consumption_metric_, this
  // will be the max value we've recorded in consumption(), not
  // necessarily the highest value consumption_metric_ has ever
  // reached.
  int64_t peak_consumption() const { return consumption_->value(); }

  // Retrieve the parent tracker, or NULL If one is not set.
  MemTracker* parent() const { return parent_; }

  // Add a function 'f' to be called if the limit is reached.
  // 'f' does not need to be thread-safe as long as it is added to only one MemTracker.
  // Note that 'f' must be valid for the lifetime of this MemTracker.
  void AddGcFunction(GcFunction f) {
    gc_functions_.push_back(f);
  }

  // Logs the usage of this tracker and all of its children (recursively).
  std::string LogUsage(const std::string& prefix = "") const;

  void EnableLogging(bool enable, bool log_stack) {
    enable_logging_ = enable;
    log_stack_ = log_stack;
  }

 private:
  FRIEND_TEST(MemTrackerTest, SingleTrackerNoLimit);
  FRIEND_TEST(MemTrackerTest, SingleTrackerWithLimit);
  FRIEND_TEST(MemTrackerTest, TrackerHierarchy);
  FRIEND_TEST(MemTrackerTest, GcFunctions);

  // byte_limit < 0 means no limit
  // 'id' is the label for LogUsage() and web UI.
  MemTracker(int64_t byte_limit, const std::string& id,
             MemTracker* parent);

  // C'tor for tracker that uses consumption_metric as the consumption value.
  // Consume()/Release() can still be called.
  MemTracker(FunctionGauge<uint64_t>* consumption_metric,
             int64_t byte_limit, const std::string& id);

  // Adds the tracker for 'id' to a map so that trackers can be listed
  // and retrieved by other classes.
  //
  // NOTE: if CreateTracker factory methods are used, this is called automatically.
  // There is no need to manually remove the tracker from the map, this is done
  // by the destructor.
  static void AddToTrackerMap(const std::string& id,
                              const std::tr1::shared_ptr<MemTracker>& tracker);

  bool CheckLimitExceeded() const {
    return limit_ >= 0 && limit_ < consumption();
  }

  // If consumption is higher than max_consumption, attempts to free memory by calling any
  // added GC functions.  Returns true if max_consumption is still exceeded. Takes
  // gc_lock. Updates metrics if initialized.
  bool GcMemory(int64_t max_consumption);

  // Called when the total release memory is larger than GC_RELEASE_SIZE.
  // TcMalloc holds onto released memory and very slowly (if ever) releases it back to
  // the OS. This is problematic since it is memory we are not constantly tracking which
  // can cause us to go way over mem limits.
  void GcTcmalloc();

  // Walks the MemTracker hierarchy and populates all_trackers_ and
  // limit_trackers_
  void Init();

  // Adds tracker to child_trackers_
  void AddChildTracker(MemTracker* tracker);

  // Logs the stack of the current consume/release. Used for debugging only.
  void LogUpdate(bool is_consume, int64_t bytes) const;

  static std::string LogUsage(const std::string& prefix,
      const std::list<MemTracker*>& trackers);

  // Size, in bytes, that is considered a large value for Release() (or Consume() with
  // a negative value). If tcmalloc is used, this can trigger it to GC.
  // A higher value will make us call into tcmalloc less often (and therefore more
  // efficient). A lower value will mean our memory overhead is lower.
  // TODO: this is a stopgap.
  static const int64_t GC_RELEASE_SIZE = 128 * 1024L * 1024L;

  // Total amount of memory from calls to Release() since the last GC. If this
  // is greater than GC_RELEASE_SIZE, this will trigger a tcmalloc gc.
  static Atomic64 released_memory_since_gc_;

  simple_spinlock gc_lock_;

  static boost::mutex static_mem_trackers_lock_;

  typedef std::tr1::unordered_map<std::string, std::tr1::weak_ptr<MemTracker> > TrackerMap;

  static TrackerMap id_to_mem_trackers_;

  int64_t limit_;
  const std::string id_;
  const std::string descr_;
  MemTracker* parent_;

  gscoped_ptr<HighWaterMark<int64_t> > consumption_;

  FunctionGauge<uint64_t>* consumption_metric_;

  std::vector<MemTracker*> all_trackers_; // this tracker plus all of its ancestors
  std::vector<MemTracker*> limit_trackers_; // all_trackers_ with valid limits

  // All the child trackers of this tracker. Used for error reporting only.
  // i.e., Updating a parent tracker does not update the children.
  mutable boost::mutex child_trackers_lock_;
  std::list<MemTracker*> child_trackers_;

  // Iterator into parent_->child_trackers_ for this object. Stored to have O(1)
  // remove.
  std::list<MemTracker*>::iterator child_tracker_it_;

  // Functions to call after the limit is reached to free memory.
  std::vector<GcFunction> gc_functions_;

  // If true, calls UnregisterFromParent() in the dtor. This is only used for
  // the query wide trackers to remove it from the process mem tracker. The
  // process tracker never gets deleted so it is safe to reference it in the dtor.
  // The query tracker has lifetime shared by multiple plan fragments so it's hard
  // to do cleanup another way.
  bool auto_unregister_;

  // If true, logs to INFO every consume/release called. Used for debugging.
  bool enable_logging_;
  // If true, log the stack as well.
  bool log_stack_;
};

} // namespace kudu

#endif // KUDU_UTIL_MEM_TRACKER_H
