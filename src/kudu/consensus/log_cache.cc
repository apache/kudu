// Copyright (c) 2014, Cloudera, inc.

#include "kudu/consensus/log_cache.h"

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <map>
#include <vector>

#include "kudu/consensus/async_log_reader.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/locks.h"

DEFINE_int32(log_cache_size_soft_limit_mb, 128,
             "The total per-tablet size of consensus entries to keep in memory."
             " This is a soft limit, i.e. messages in the queue are discarded"
             " down to this limit only if no peer needs to replicate them.");
DEFINE_int32(log_cache_size_hard_limit_mb, 256,
             "The total per-tablet size of consensus entries to keep in memory."
             " This is a hard limit, i.e. messages in the queue are always discarded"
             " down to this limit. If a peer has not yet replicated the messages"
             " selected to be discarded the peer will be evicted from the quorum.");

DEFINE_int32(global_log_cache_size_soft_limit_mb, 1024,
             "Server-wide version of 'log_cache_size_soft_limit_mb'");
DEFINE_int32(global_log_cache_size_hard_limit_mb, 1024,
             "Server-wide version of 'log_cache_size_hard_limit_mb'");

using strings::Substitute;

namespace kudu {
namespace consensus {

METRIC_DEFINE_gauge_int64(log_cache_total_num_ops, MetricUnit::kCount,
                          "Total number of cached operations in the leader queue.");
METRIC_DEFINE_gauge_int64(log_cache_size_bytes, MetricUnit::kBytes,
                          "Number of operations in the log cache ack'd by all peers.");

const char kLogCacheTrackerId[] = "log_cache_parent";

LogCache::LogCache(const MetricContext& metric_ctx,
                   const std::string& parent_tracker_id)
  : preceding_first_op_(MinimumOpId()),
    min_pinned_op_index(0),
    max_ops_size_bytes_hard_(FLAGS_log_cache_size_hard_limit_mb * 1024 * 1024),
    global_max_ops_size_bytes_hard_(
      FLAGS_global_log_cache_size_hard_limit_mb * 1024 * 1024),
    metrics_(metric_ctx) {
  uint64_t max_ops_size_bytes_soft = FLAGS_log_cache_size_soft_limit_mb * 1024 * 1024;
  uint64_t global_max_ops_size_bytes_soft =
      FLAGS_global_log_cache_size_soft_limit_mb * 1024 * 1024;

  // If no tracker is registered for kConsensusQueueMemTrackerId,
  // create one using the global soft limit.
  parent_tracker_ = MemTracker::FindOrCreateTracker(global_max_ops_size_bytes_soft,
                                                    parent_tracker_id,
                                                    NULL);

  tracker_ = MemTracker::CreateTracker(max_ops_size_bytes_soft,
                                       Substitute("$0-$1", parent_tracker_id, metric_ctx.prefix()),
                                       parent_tracker_.get());
}

LogCache::~LogCache() {
  STLDeleteValues(&cache_);
}

void LogCache::Init(const OpId& preceding_op,
                    gscoped_ptr<log::AsyncLogReader> log_reader) {
  lock_guard<simple_spinlock> l(&lock_);
  CHECK(cache_.empty());
  preceding_first_op_ = preceding_op;
  async_reader_ = log_reader.Pass();
}

void LogCache::SetPinnedOp(int64_t index) {
  {
    lock_guard<simple_spinlock> l(&lock_);
    min_pinned_op_index = index;
  }

  Evict();
}

void LogCache::Clear() {
  lock_guard<simple_spinlock> l(&lock_);
  STLDeleteValues(&cache_);
  min_pinned_op_index = 0;
  preceding_first_op_ = MinimumOpId();
}


bool LogCache::AppendOperation(gscoped_ptr<ReplicateMsg>* message) {
  lock_guard<simple_spinlock> l(&lock_);
  ReplicateMsg* msg_ptr = DCHECK_NOTNULL(message->get());

  // In debug mode, check that the indexes in the queue are consecutive.
  if (!cache_.empty()) {
    int last_index = (*cache_.rbegin()).first;
    DCHECK_EQ(last_index + 1, msg_ptr->id().index())
      << "Last op in the queue had index " << last_index
      << ". Operation being appended: " << msg_ptr->id();
  }

  int mem_required = msg_ptr->SpaceUsed();
  // Once either the local or global soft limit is exceeded...
  if (!tracker_->TryConsume(mem_required)) {

    // Check if we'd hit the hard limit
    if (WouldHardLimitBeViolated(mem_required)) {
      return false;
    }

    // If we're under the hard limit, we can continue anyway.
    tracker_->Consume(mem_required);
  }
  metrics_.log_cache_size_bytes->IncrementBy(mem_required);
  metrics_.log_cache_total_num_ops->Increment();

  InsertOrDie(&cache_, msg_ptr->id().index(), message->release());
  return true;
}

bool LogCache::HasOpIndex(int64_t index) const {
  lock_guard<simple_spinlock> l(&lock_);
  return ContainsKey(cache_, index);
}

Status LogCache::ReadOps(int64_t after_op_index,
                         int max_size_bytes,
                         std::vector<ReplicateMsg*>* messages,
                         OpId* preceding_op) {

  lock_guard<simple_spinlock> l(&lock_);
  CHECK_GE(after_op_index, min_pinned_op_index)
    << "Cannot currently support reading non-pinned operations";

  // If the messages the peer needs haven't been loaded into the queue yet,
  // load them.
  if (after_op_index < preceding_first_op_.index()) {
    Status status = async_reader_->EnqueueAsyncRead(
      after_op_index, preceding_first_op_.index(),
      Bind(&LogCache::EntriesLoadedCallback, Unretained(this)));
    if (status.IsAlreadyPresent()) {
      // The log reader is already loading another part of the log. We'll try again at some
      // point.
      return Status::Incomplete("Cache already busy loading");
    } else if (status.ok()) {
      // Successfully enqueued.
      return Status::Incomplete("Asynchronously reading ops");
    }
    RETURN_NOT_OK_PREPEND(status, "Unable to enqueue async log read");
  }

  if (cache_.empty()) {
    return Status::NotFound("No ops in cache");
  }

  // We don't actually start sending on 'lower_bound' but we seek to
  // it to get the preceding_id.
  MessageCache::const_iterator iter = cache_.lower_bound(after_op_index);
  DCHECK(iter != cache_.end()) << after_op_index;

  int found_index = iter->first;
  if (found_index != after_op_index) {
    // If we were looking for exactly the op that precedes the beginning of the
    // queue, use our cached OpId
    if (after_op_index == preceding_first_op_.index()) {
      preceding_op->CopyFrom(preceding_first_op_);
    } else {
      LOG(FATAL) << "trying to read index " << after_op_index
                 << " but seeked to " << found_index;
    }
  } else {
    // ... otherwise 'preceding_id' is the first element in the iterator and we start sending
    // on the element after that.
    preceding_op->CopyFrom((*iter).second->id());
    iter++;
  }

  // Return as many operations as we can, up to the limit
  int total_size = 0;
  for (; iter != cache_.end(); iter++) {
    ReplicateMsg* msg = iter->second;
    int msg_size = google::protobuf::internal::WireFormatLite::LengthDelimitedSize(
      msg->ByteSize());
    msg_size += 1; // for the type tag
    if (total_size + msg_size > max_size_bytes && !messages->empty()) {
      break;
    }

    messages->push_back(msg);
    total_size += msg_size;
  }

  return Status::OK();
}

void LogCache::EntriesLoadedCallback(const Status& status,
                                     const vector<ReplicateMsg*>& replicates) {
  // TODO deal with errors when loading operations.
  CHECK_OK(status);

  // OK, we're all done, we can now bulk load the operations into the queue.

  // Note that we don't check queue limits. Were we to stop adding operations
  // in the middle of the sequence the queue would have holes so it is possible
  // that we're breaking queue limits right here.

  size_t total_size = 0;

  // TODO enforce some sort of limit on how much can be loaded from disk
  {
    lock_guard<simple_spinlock> lock(&lock_);

    // We were told to load ops after 'new_preceding_first_op_index' so we skip
    // the first one, whose OpId will become our new 'preceding_first_op_'
    vector<ReplicateMsg*>::const_iterator iter = replicates.begin();
    gscoped_ptr<ReplicateMsg> preceding_replicate(*iter);
    ++iter;

    for (; iter != replicates.end(); ++iter) {
      ReplicateMsg* replicate = *iter;
      InsertOrDie(&cache_, replicate->id().index(), replicate);
      size_t size = replicate->SpaceUsed();
      tracker_->Consume(size);
      total_size += size;
    }

    CHECK(OpIdEquals(replicates.back()->id(),
                     preceding_first_op_))
      << "Expected: " << preceding_first_op_.ShortDebugString()
      << " got: " << replicates.back()->id();

    preceding_first_op_ = preceding_replicate->id();
    LOG(INFO) << "Loaded operations into the cache from: "
        << replicates.front()->id().ShortDebugString()
        << " to: " << replicates.back()->id().ShortDebugString()
        << " for a total of: " << replicates.size();
  }

  metrics_.log_cache_total_num_ops->IncrementBy(replicates.size());
  metrics_.log_cache_size_bytes->IncrementBy(total_size);
}

void LogCache::Evict() {
  lock_guard<simple_spinlock> lock(&lock_);
  MessageCache::iterator iter = cache_.begin();

  VLOG(1) << "Evicting log cache: before stats: " << StatsStringUnlocked();
  while (iter != cache_.end() &&
         iter->first < min_pinned_op_index) {
    ReplicateMsg* msg = (*iter).second;
    preceding_first_op_ = msg->id();;
    tracker_->Release(msg->SpaceUsed());
    VLOG(1) << "Evicting cache. Deleting: " << msg->id().ShortDebugString();
    metrics_.log_cache_size_bytes->IncrementBy(-1 * msg->SpaceUsed());
    metrics_.log_cache_total_num_ops->Decrement();
    delete msg;
    cache_.erase(iter++);
  }
  VLOG(1) << "Evicting log cache: after stats: " << StatsStringUnlocked();
}

bool LogCache::WouldHardLimitBeViolated(size_t bytes) const {
  bool local_limit_violated = (bytes + tracker_->consumption()) > max_ops_size_bytes_hard_;
  bool global_limit_violated = (bytes + parent_tracker_->consumption())
      > global_max_ops_size_bytes_hard_;
#ifndef NDEBUG
  if (VLOG_IS_ON(1)) {
    DVLOG(1) << "global consumption: "
             << HumanReadableNumBytes::ToString(parent_tracker_->consumption());
    string human_readable_bytes = HumanReadableNumBytes::ToString(bytes);
    if (local_limit_violated) {
      DVLOG(1) << "adding " << human_readable_bytes
               << " would violate local hard limit ("
               << HumanReadableNumBytes::ToString(max_ops_size_bytes_hard_) << ").";
    }
    if (global_limit_violated) {
      DVLOG(1) << "adding " << human_readable_bytes
               << " would violate global hard limit ("
               << HumanReadableNumBytes::ToString(global_max_ops_size_bytes_hard_) << ").";
    }
  }
#endif
  return local_limit_violated || global_limit_violated;
}

int64_t LogCache::BytesUsed() const {
  return tracker_->consumption();
}

string LogCache::StatsString() const {
  lock_guard<simple_spinlock> lock(&lock_);
  return StatsStringUnlocked();
}

string LogCache::StatsStringUnlocked() const {
  return Substitute("LogCache(num_ops=$0, bytes=$1)",
                    metrics_.log_cache_total_num_ops->value(),
                    metrics_.log_cache_size_bytes->value());
}

void LogCache::DumpToLog() const {
  vector<string> strings;
  DumpToStrings(&strings);
  BOOST_FOREACH(const string& s, strings) {
    LOG(INFO) << s;
  }
}

void LogCache::DumpToStrings(vector<string>* lines) const {
  lock_guard<simple_spinlock> lock(&lock_);
  int counter = 0;
  lines->push_back("Messages:");
  BOOST_FOREACH(const MessageCache::value_type entry, cache_) {
    ReplicateMsg* msg = entry.second;
    lines->push_back(
      Substitute("Message[$0] $1.$2 : REPLICATE. Type: $3, Size: $4",
                 counter++, msg->id().term(), msg->id().index(),
                 OperationType_Name(msg->op_type()),
                 msg->ByteSize()));
  }
}

void LogCache::DumpToHtml(std::ostream& out) const {
  using std::endl;

  lock_guard<simple_spinlock> lock(&lock_);
  out << "<h3>Messages:</h3>" << endl;
  out << "<table>" << endl;
  out << "<tr><th>Entry</th><th>OpId</th><th>Type</th><th>Size</th><th>Status</th></tr>" << endl;

  int counter = 0;
  BOOST_FOREACH(const MessageCache::value_type entry, cache_) {
    ReplicateMsg* msg = entry.second;
    out << Substitute("<tr><th>$0</th><th>$1.$2</th><td>REPLICATE $3</td>"
                      "<td>$4</td><td>$5</td></tr>",
                      counter++, msg->id().term(), msg->id().index(),
                      OperationType_Name(msg->op_type()),
                      msg->ByteSize(), msg->id().ShortDebugString()) << endl;
  }
  out << "</table>";
}

#define INSTANTIATE_METRIC(x) \
  AtomicGauge<int64_t>::Instantiate(x, metric_ctx)
LogCache::Metrics::Metrics(const MetricContext& metric_ctx)
  : log_cache_total_num_ops(INSTANTIATE_METRIC(METRIC_log_cache_total_num_ops)),
    log_cache_size_bytes(INSTANTIATE_METRIC(METRIC_log_cache_size_bytes)) {
}
#undef INSTANTIATE_METRIC

} // namespace consensus
} // namespace kudu
