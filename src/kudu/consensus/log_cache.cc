// Copyright (c) 2014, Cloudera, inc.

#include "kudu/consensus/log_cache.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <map>
#include <vector>

#include "kudu/consensus/async_log_reader.h"
#include "kudu/consensus/log.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"

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

typedef vector<const ReplicateMsg*>::const_iterator MsgIter;

LogCache::LogCache(const MetricContext& metric_ctx,
                   log::Log* log,
                   const std::string& local_uuid,
                   const std::string& tablet_id,
                   const std::string& parent_tracker_id)
  : log_(log),
    local_uuid_(local_uuid),
    tablet_id_(tablet_id),
    preceding_first_op_(MinimumOpId()),
    min_pinned_op_index_(0),
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
  async_reader_.reset(new log::AsyncLogReader(log_->GetLogReader()));
}

LogCache::~LogCache() {
  async_reader_->Shutdown();

  cache_.clear();
}

void LogCache::Init(const OpId& preceding_op) {
  lock_guard<simple_spinlock> l(&lock_);
  CHECK(cache_.empty());
  preceding_first_op_ = preceding_op;
}

void LogCache::SetPinnedOp(int64_t index) {
  {
    lock_guard<simple_spinlock> l(&lock_);
    min_pinned_op_index_ = index;
  }

  Evict();
}

bool LogCache::AppendOperations(const vector<ReplicateRefPtr>& msgs,
                                const StatusCallback& callback) {
  unique_lock<simple_spinlock> l(&lock_);

  uint32_t first_idx_to_cache = 0;
  uint32_t size = msgs.size();

  int last_index = cache_.empty() ? preceding_first_op_.index() : (*cache_.rbegin()).first;

  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (last_index + 1 != msgs[0]->get()->id().index()) {
    // If the index is not consecutive then it must be lower than or equal
    // to the last index, i.e. we're overwriting.
    CHECK_LE(msgs[0]->get()->id().index(), last_index);

    // Now remove the overwritten operations.
    for (int64_t i = msgs[0]->get()->id().index(); i <= last_index; ++i) {
      ReplicateRefPtr msg = EraseKeyReturnValuePtr(&cache_, i);
      if (msg != NULL) {
        AccountForMessageRemovalUnlocked(msg);
      }
    }

    // Make sure we set the pin to the first op, if it was higher.
    if (min_pinned_op_index_ > msgs[0]->get()->id().index()) {
      min_pinned_op_index_ = msgs[0]->get()->id().index();
    }

    // Finally we make sure that the preceding id is still correct.
    // If we're writing messages that come before the current preceding
    // id, we make the first message's id the new preceding id (otherwise
    // we'd have to read from disk).
    if (preceding_first_op_.index() >= msgs[0]->get()->id().index()) {
      preceding_first_op_ = msgs[0]->get()->id();
      first_idx_to_cache++;
      size--;
    }
  }

  CHECK_GE(msgs[0]->get()->id().index(), min_pinned_op_index_) << LogPrefixUnlocked()
      << " Appending operation after pin. State: " << ToStringUnlocked();

  int64_t mem_required = 0;

  for (int i = first_idx_to_cache; i < msgs.size(); i++) {
    mem_required += msgs[i]->get()->SpaceUsed();
  }

  // Once either the local or global soft limit is exceeded...
  if (!tracker_->TryConsume(mem_required)) {

    // Check if we'd hit the hard limit
    if (WouldHardLimitBeViolated(mem_required)) {
      return false;
    }

    // If we're under the hard limit, we can continue anyway.
    tracker_->Consume(mem_required);
  }

  // We drop the lock during the AsyncAppendReplicates call, since it may block
  // if the queue is full, and the queue might not drain if it's trying to call
  // our callback and blocked on this lock.
  l.unlock();
  Status log_status = log_->AsyncAppendReplicates(msgs, callback);
  l.lock();
  if (!log_status.ok()) {
    LOG_WITH_PREFIX(WARNING) << "Couldn't append to log: " << log_status.ToString();
    tracker_->Release(mem_required);
    return false;
  }

  metrics_.log_cache_size_bytes->IncrementBy(mem_required);
  metrics_.log_cache_total_num_ops->IncrementBy(msgs.size() - first_idx_to_cache);

  for (int i = first_idx_to_cache; i < msgs.size(); i++) {
    InsertOrDie(&cache_,  msgs[i]->get()->id().index(), msgs[i]);
  }

  return true;
}

bool LogCache::HasOpIndex(int64_t index) const {
  lock_guard<simple_spinlock> l(&lock_);
  return ContainsKey(cache_, index);
}

Status LogCache::ReadOps(int64_t after_op_index,
                         int max_size_bytes,
                         std::vector<ReplicateRefPtr>* messages,
                         OpId* preceding_op) {
  lock_guard<simple_spinlock> l(&lock_);
  DCHECK_GE(after_op_index, 0);
  CHECK_GE(after_op_index, min_pinned_op_index_)
    << "Cannot currently support reading non-pinned operations";

  // If the messages the peer needs haven't been loaded into the queue yet,
  // load them.
  if (after_op_index < preceding_first_op_.index()) {
    // If after_op_index is 0, then we can't actually ask the log
    // to read index 0. Instead, we'll ask for index 1, and then
    // we special case this in the callback below.
    int64_t req_op_index = std::max<int64_t>(1, after_op_index);

    Status status = async_reader_->EnqueueAsyncRead(
      req_op_index, preceding_first_op_.index(),
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
  if (iter == cache_.end()) {
    return Status::NotFound("Op not in cache.");
  }

  int found_index = iter->first;
  if (found_index != after_op_index) {
    // If we were looking for exactly the op that precedes the beginning of the
    // queue, use our cached OpId
    if (after_op_index == preceding_first_op_.index()) {
      preceding_op->CopyFrom(preceding_first_op_);
    } else {
      LOG_WITH_PREFIX(FATAL) << "trying to read index " << after_op_index
          << " but seeked to " << found_index;
    }
  } else {
    // ... otherwise 'preceding_id' is the first element in the iterator and we start sending
    // on the element after that.
    preceding_op->CopyFrom((*iter).second->get()->id());
    iter++;

  }

  // Return as many operations as we can, up to the limit
  int total_size = 0;
  for (; iter != cache_.end(); iter++) {
    const ReplicateRefPtr& msg = iter->second;
    int msg_size = google::protobuf::internal::WireFormatLite::LengthDelimitedSize(
      msg->get()->ByteSize());
    msg_size += 1; // for the type tag
    if (total_size + msg_size > max_size_bytes && !messages->empty()) {
      break;
    }

    messages->push_back(msg);
    total_size += msg_size;
  }

  return Status::OK();
}

void LogCache::EntriesLoadedCallback(int64_t after_op_index,
                                     const Status& status,
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

    // If the pin has moved past the first message's id we no longer need them
    // and we skip adding them to the cache.
    if (replicates.front()->id().index() < min_pinned_op_index_) {
      LOG_WITH_PREFIX(INFO) << "Can't load operations into the queue, pin advanced"
          "past the first operation. Pin: " << min_pinned_op_index_ << " First requested"
          " op: " << replicates.front()->id();
      for (; iter != replicates.end(); ++iter) {
        delete (*iter);
      }
      return;
    }

    OpId preceding_id;
    // Special case when the caller requested all operations after MinimumOpId()
    // since that operation does not exist, we need to set it ourselves.
    if (after_op_index == 1 && replicates[0]->id().index() == 1) {
      preceding_id = MinimumOpId();
    // otherwise just skip the first operation, which will become the preceding id.
    } else {
      preceding_id = (*iter)->id();
      delete *iter;
      ++iter;
    }

    for (; iter != replicates.end(); ++iter) {
      ReplicateMsg* replicate = *iter;
      InsertOrDie(&cache_, replicate->id().index(), make_scoped_refptr_replicate(replicate));
      size_t size = replicate->SpaceUsed();
      tracker_->Consume(size);
      total_size += size;
    }

    CHECK(OpIdEquals(replicates.back()->id(), preceding_first_op_))
      << "Expected: " << preceding_first_op_.ShortDebugString()
      << " got: " << replicates.back()->id();

    preceding_first_op_ = preceding_id;
    LOG_WITH_PREFIX(INFO) << "Loaded operations into the cache after: "
        << preceding_id.ShortDebugString() << " to: "
        << replicates.back()->id().ShortDebugString()
        << " for a total of: " << replicates.size();
  }

  metrics_.log_cache_total_num_ops->IncrementBy(replicates.size());
  metrics_.log_cache_size_bytes->IncrementBy(total_size);
}

void LogCache::Evict() {
  lock_guard<simple_spinlock> lock(&lock_);
  MessageCache::iterator iter = cache_.begin();

  VLOG_WITH_PREFIX(1) << "Evicting log cache: before state: " << ToStringUnlocked();
  while (iter != cache_.end() &&
         iter->first < min_pinned_op_index_) {
    const ReplicateRefPtr& msg = (*iter).second;
    preceding_first_op_ = msg->get()->id();;
    VLOG_WITH_PREFIX(1) << "Evicting cache. Removing: " << msg->get()->id().ShortDebugString();
    AccountForMessageRemovalUnlocked(msg);
    cache_.erase(iter++);
  }
  VLOG_WITH_PREFIX(1) << "Evicting log cache: after state: " << ToStringUnlocked();
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

void LogCache::AccountForMessageRemovalUnlocked(const ReplicateRefPtr& msg) {
  tracker_->Release(msg->get()->SpaceUsed());
  metrics_.log_cache_size_bytes->DecrementBy(msg->get()->SpaceUsed());
  metrics_.log_cache_total_num_ops->Decrement();
}

int64_t LogCache::BytesUsed() const {
  return tracker_->consumption();
}

string LogCache::StatsString() const {
  lock_guard<simple_spinlock> lock(&lock_);
  return StatsStringUnlocked();
}

string LogCache::StatsStringUnlocked() const {
  return Substitute("LogCacheStats(num_ops=$0, bytes=$1)",
                    metrics_.log_cache_total_num_ops->value(),
                    metrics_.log_cache_size_bytes->value());
}

std::string LogCache::ToString() const {
  lock_guard<simple_spinlock> lock(&lock_);
  return ToStringUnlocked();
}

std::string LogCache::ToStringUnlocked() const {
  return Substitute("Preceding Op: $0, Pinned index: $1, $2",
                    OpIdToString(preceding_first_op_), min_pinned_op_index_,
                    StatsStringUnlocked());
}

std::string LogCache::LogPrefixUnlocked() const {
  return Substitute("T $0 P $1: ",
                    tablet_id_,
                    local_uuid_);
}

void LogCache::DumpToLog() const {
  vector<string> strings;
  DumpToStrings(&strings);
  BOOST_FOREACH(const string& s, strings) {
    LOG_WITH_PREFIX(INFO) << s;
  }
}

void LogCache::DumpToStrings(vector<string>* lines) const {
  lock_guard<simple_spinlock> lock(&lock_);
  int counter = 0;
  lines->push_back(ToStringUnlocked());
  lines->push_back(Substitute("Preceding Id: $0", preceding_first_op_.ShortDebugString()));
  lines->push_back("Messages:");
  BOOST_FOREACH(const MessageCache::value_type& entry, cache_) {
    const ReplicateMsg* msg = entry.second->get();
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
  BOOST_FOREACH(const MessageCache::value_type& entry, cache_) {
    const ReplicateMsg* msg = entry.second->get();
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
