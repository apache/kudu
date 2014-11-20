// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CONSENSUS_LOG_CACHE_H
#define KUDU_CONSENSUS_LOG_CACHE_H

#include <map>
#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {

class MetricContext;
class MemTracker;

namespace log {
class AsyncLogReader;
class Log;
} // namespace log

namespace consensus {

class ReplicateMsg;

// The id for the server-wide log cache MemTracker.
extern const char kLogCacheTrackerId[];

// Read-through cache for the log.
//
// This stores a set of log messages by their index. New operations
// can be appended to the end as they are written to the log. Readers
// fetch entries that were explicitly appended, or they can fetch older
// entries which are asynchronously fetched from the disk.
class LogCache {
 public:
  LogCache(const MetricContext& metric_ctx,
           log::Log* log,
           const std::string& parent_tracker_id = kLogCacheTrackerId);
  ~LogCache();

  // Initialize the cache.
  //
  // 'preceding_op' is the current latest op. The next AppendOperation() call
  // must follow this op.
  //
  // Requires that the cache is empty.
  void Init(const OpId& preceding_op);

  // Read operations from the log, following 'after_op_index'.
  // The returned messages are owned by the log cache, and will be freed
  // upon AdvancePinnedOp() when the pin point is moved later than these messages.
  // Note that 'after_op_index' _must_ be pinned before calling this method.
  //
  // If such an op exists in the log, an OK result will always include at least one
  // operation.
  //
  // The result will be limited such that the total ByteSize() of the returned ops
  // is less than max_size_bytes, unless that would result in an empty result, in
  // which case exactly one op is returned.
  //
  // The OpId which precedes the returned ops is returned in *preceding_op.
  // The index of this OpId will match 'after_op_index'.
  //
  // If the ops being requested are not available in the log, this will asynchronously
  // enqueue a read for these ops into the cache, and return Status::Incomplete().
  //
  Status ReadOps(int64_t after_op_index,
                 int max_size_bytes,
                 std::vector<ReplicateMsg*>* messages,
                 OpId* preceding_op);

  // Append the operation directly into the cache.
  // NOTE: this will crash if an operation with the same index has already been
  // appended! TODO: change this when we support replacing log indexes.
  //
  // Returns false if the hard limit has been reached.
  // Takes ownership when it returns true.
  bool AppendOperation(gscoped_ptr<ReplicateMsg>* message);

  // Return true if the cache currently contains data for the given operation.
  bool HasOpIndex(int64_t log_index) const;

  // Change the pinned operation index..
  //
  // Any operations with an index >= the given 'id' are pinned in the cache.
  // Any operation with a lower index may be evicted based on memory pressure.
  //
  // The pin point may be lower than the lowest operation in the log -- this
  // doesn't imply that those ops will be eagerly loaded. Rather, it just enforces
  // that once they are loaded, they are not evicted.
  void SetPinnedOp(int64_t index);

  // Remove all entries from the cache.
  void Clear();

  // Closes the cache, making sure that any outstanding reader terminates and that
  // there are no outstanding operations in the cache that are not in the log.
  // This latter case may happen in the off chance that we're faster writing to
  // other nodes than to local disk.
  void Close();

  // Return the number of bytes of memory currently in use by the cache.
  int64_t BytesUsed() const;

  // Dump the current contents of the cache to the log.
  void DumpToLog() const;

  // Dumps the contents of the cache to the provided string vector.
  void DumpToStrings(std::vector<std::string>* lines) const;

  void DumpToHtml(std::ostream& out) const;

  std::string StatsString() const;

 private:
  FRIEND_TEST(LogCacheTest, TestGetMessages);

  // Evicts all operations from the cache which are not later than
  // min_pinned_op_index_.
  void Evict();

  // Check whether adding 'bytes' to the cache would violate
  // either the local (per-tablet) hard limit or the global
  // (server-wide) hard limit.
  bool WouldHardLimitBeViolated(size_t bytes) const;

  // Return a string with stats
  std::string StatsStringUnlocked() const;

  void EntriesLoadedCallback(int64_t after_op_index,
                             const Status& status,
                             const std::vector<ReplicateMsg*>& replicates);

  log::Log* const log_;

  mutable simple_spinlock lock_;

  // An ordered map that serves as the buffer for the cached messages.
  // Maps from log index -> ReplicateMsg
  typedef std::map<uint64_t, ReplicateMsg*> MessageCache;
  MessageCache cache_;

  // The OpId which comes before the first op in the cache.
  OpId preceding_first_op_;

  // Any operation with an index >= min_pinned_op_ may not be
  // evicted from the cache.
  // Protected by lock_.
  int64_t min_pinned_op_index;

  // The total size of consensus entries to keep in memory.
  // This is a hard limit, i.e. messages in the queue are always discarded
  // down to this limit. If a peer has not yet replicated the messages
  // selected to be discarded the peer will be evicted from the quorum.
  uint64_t max_ops_size_bytes_hard_;

  // Server-wide version of 'max_ops_size_bytes_hard_'.
  uint64_t global_max_ops_size_bytes_hard_;

  // Pointer to a parent memtracker for all log caches. This
  // exists to compute server-wide cache size and enforce a
  // server-wide memory limit.  When the first instance of a log
  // cache is created, a new entry is added to MemTracker's static
  // map; subsequent entries merely increment the refcount, so that
  // the parent tracker can be deleted if all log caches are
  // deleted (e.g., if all tablets are deleted from a server, or if
  // the server is shutdown).
  std::tr1::shared_ptr<MemTracker> parent_tracker_;

  // A MemTracker for this instance.
  std::tr1::shared_ptr<MemTracker> tracker_;

  // The log reader used to fill the cache when a caller requests older
  // entries.
  gscoped_ptr<log::AsyncLogReader> async_reader_;

  struct Metrics {
    explicit Metrics(const MetricContext& metric_ctx);

    // Keeps track of the total number of operations in the cache.
    AtomicGauge<int64_t>* log_cache_total_num_ops;

    // Keeps track of the memory consumed by the cache, in bytes.
    AtomicGauge<int64_t>* log_cache_size_bytes;
  };
  Metrics metrics_;

  enum State {
    kCacheOpen,
    kCacheClosed,
  };

  State state_;

  DISALLOW_COPY_AND_ASSIGN(LogCache);
};

} // namespace consensus
} // namespace kudu
#endif /* KUDU_CONSENSUS_LOG_CACHE_H */
