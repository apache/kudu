// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/consensus/log_cache.h"

#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/wire_format_lite_inl.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mutex.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"

DEFINE_int32(log_cache_size_limit_mb, 128,
             "The total per-tablet size of consensus entries which may be kept in memory. "
             "The log cache attempts to keep all entries which have not yet been replicated "
             "to all followers in memory, but if the total size of those entries exceeds "
             "this limit within an individual tablet, the oldest will be evicted.");
TAG_FLAG(log_cache_size_limit_mb, advanced);

DEFINE_int32(global_log_cache_size_limit_mb, 1024,
             "Server-wide version of 'log_cache_size_limit_mb'. The total memory used for "
             "caching log entries across all tablets is kept under this threshold.");
TAG_FLAG(global_log_cache_size_limit_mb, advanced);

using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace consensus {

METRIC_DEFINE_gauge_int64(server, log_cache_num_ops, "Log Cache Operation Count",
                          MetricUnit::kOperations,
                          "Number of operations in the log cache.");
METRIC_DEFINE_gauge_int64(server, log_cache_size, "Log Cache Memory Usage",
                          MetricUnit::kBytes,
                          "Amount of memory in use for caching the local log.");
METRIC_DEFINE_gauge_int64(server, log_cache_msg_size, "Log Cache Message Size",
                          MetricUnit::kBytes,
                          "Size of the incoming uncompressed payload for the "
                          "messages in the log_cache");

static const char kParentMemTrackerId[] = "log_cache";

typedef vector<const ReplicateMsg*>::const_iterator MsgIter;

LogCache::LogCache(const scoped_refptr<MetricEntity>& metric_entity,
                   scoped_refptr<log::Log> log,
                   string local_uuid,
                   string tablet_id)
  : log_(std::move(log)),
    local_uuid_(std::move(local_uuid)),
    tablet_id_(std::move(tablet_id)),
    next_index_cond_(&lock_),
    next_sequential_op_index_(0),
    min_pinned_op_index_(0),
    metrics_(metric_entity),
    codec_(nullptr) {


  const int64_t max_ops_size_bytes = FLAGS_log_cache_size_limit_mb * 1024L * 1024L;
  const int64_t global_max_ops_size_bytes = FLAGS_global_log_cache_size_limit_mb * 1024L * 1024L;

  // Set up (or reuse) a tracker with the global limit. It is parented directly
  // to the root tracker so that it's always global.
  parent_tracker_ = MemTracker::FindOrCreateGlobalTracker(global_max_ops_size_bytes,
                                                          kParentMemTrackerId);

  // And create a child tracker with the per-tablet limit.
  tracker_ = MemTracker::CreateTracker(
      max_ops_size_bytes, Substitute("$0:$1:$2", kParentMemTrackerId,
                                     local_uuid, tablet_id),
      parent_tracker_);

  // Put a fake message at index 0, since this simplifies a lot of our
  // code paths elsewhere.
  auto zero_op = new ReplicateMsg();
  *zero_op->mutable_id() = MinimumOpId();
  InsertOrDie(&cache_, 0, { make_scoped_refptr_replicate(zero_op), zero_op->SpaceUsed() });
}

LogCache::~LogCache() {
  tracker_->Release(tracker_->consumption());
  cache_.clear();
}

void LogCache::Init(const OpId& preceding_op) {
  std::lock_guard<Mutex> l(lock_);
  CHECK_EQ(cache_.size(), 1)
    << "Cache should have only our special '0' op";
  next_sequential_op_index_ = preceding_op.index() + 1;
  min_pinned_op_index_ = next_sequential_op_index_;
}

Status LogCache::SetCompressionCodec(const std::string& codec) {
  if (codec.empty()) {
    LOG(INFO) << "Disabling compression";
    codec_.store(nullptr);
    return Status::OK();
  }

  const CompressionCodec* comp_codec = nullptr;
  auto codec_type = GetCompressionCodecType(codec);
  if (codec_type != NO_COMPRESSION) {
    auto status = GetCompressionCodec(codec_type, &comp_codec);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to set compression codec for log-cache";
      return status;
    }
  }

  LOG(INFO) << "Updating compression codec to: " << codec;
  codec_.store(comp_codec);
  return Status::OK();
}

void LogCache::TruncateOpsAfter(int64_t index) {
  {
    std::unique_lock<Mutex> l(lock_);
    TruncateOpsAfterUnlocked(index);
  }

  // In the base kuduraft implementation this is a no-op
  // because trimming is handled by resetting the append index.
  // In the MySQL case, we do actual trimming and this is
  // implemented in the binlog wrapper.
  // We don't append in async mode, so we cannot race with
  // AsyncAppendReplicates, where an append has not finished,
  // but the Truncate comes in.
  Status log_status = log_->TruncateOpsAfter(index);

  // We crash the server if Truncate fails, symmetric to
  // what happenes when AsyncAppendReplicates fails.
  CHECK_OK_PREPEND(log_status, Substitute("$0: cannot truncate ops after index $1",
                           log_status.ToString(), index));
}

void LogCache::TruncateOpsAfterUnlocked(int64_t index) {
  int64_t first_to_truncate = index + 1;
  // If the index is not consecutive then it must be lower than or equal
  // to the last index, i.e. we're overwriting.
  CHECK_LE(first_to_truncate, next_sequential_op_index_);

  // Now remove the overwritten operations.
  for (int64_t i = first_to_truncate; i < next_sequential_op_index_; ++i) {
    auto it = cache_.find(i);
    if (it != cache_.end()) {
      AccountForMessageRemovalUnlocked(it->second);
      cache_.erase(it);
    }
  }
  next_sequential_op_index_ = index + 1;
}

Status LogCache::UncompressMsg(const ReplicateRefPtr& msg,
                               faststring& buffer,
                               std::unique_ptr<ReplicateMsg>* uncompressed_msg) {
  const OperationType op_type = msg->get()->op_type();
  const WritePayloadPB& payload = msg->get()->write_payload();
  const CompressionType compression_codec = payload.compression_codec();
  const int64_t uncompressed_size = payload.uncompressed_size();
  const int64_t compressed_size = payload.payload().size();

  VLOG(2) << "Uncompressing messages before writing to log." <<
             " opid: " << msg->get()->id().ShortDebugString() <<
             " codec: " << compression_codec <<
             " op_type: " << op_type <<
             " compressed payload size: " << compressed_size <<
             " uncompressed payload size: " << uncompressed_size;

  // Resize buffer to hold uncompressed payload.
  // TODO: needs perf testing and maybe implement streaming (un)compression
  buffer.resize(uncompressed_size);
  Slice compressed_slice(payload.payload().c_str(), compressed_size);

  Status status;
  const CompressionCodec* codec = codec_.load();
  if (codec && codec->type() == compression_codec) {
    status = codec->Uncompress(
        compressed_slice, buffer.data(), uncompressed_size);
  }
  else {
    // Either compression is not enabled on this instance OR this message uses a
    // different compression codec. Get the right codec now
    status = GetCompressionCodec(compression_codec, &codec);
    if (status.ok()) {
      status = codec->Uncompress(
          compressed_slice, buffer.data(), uncompressed_size);
    }
  }

  // Return early if uncompression failed
  RETURN_NOT_OK_PREPEND(status,
      Substitute("Failed to uncompress OpId $0. Compression codec used: $1, "
                 "Operation type: $2, Compressed payload size: $3"
                 "Uncompressed payload size: $4",
                 msg->get()->id().ShortDebugString(), compression_codec,
                 op_type, compressed_size, uncompressed_size));

  // Now create a new ReplicateMsg and copy over the contents from the original
  // msg and the uncompressed payload
  std::unique_ptr<ReplicateMsg> rep_msg(new ReplicateMsg);
  *(rep_msg->mutable_id()) = msg->get()->id();
  rep_msg->set_timestamp(msg->get()->timestamp());
  rep_msg->set_op_type(msg->get()->op_type());

  WritePayloadPB* write_payload = rep_msg->mutable_write_payload();
  write_payload->set_payload(std::move(buffer.ToString()));

  uncompressed_msg->reset(rep_msg.release());
  return Status::OK();
}

Status LogCache::CompressMsg(const ReplicateRefPtr& msg,
                             faststring& buffer,
                             std::unique_ptr<ReplicateMsg>* compressed_msg) {
  // Grab the reference to the payload that needs to be compressed
  ReplicateMsg* original_msg = msg->get();
  const std::string& payload_str = original_msg->write_payload().payload();
  bool is_compressed =
    (original_msg->write_payload().compression_codec() != NO_COMPRESSION);

  if (PREDICT_FALSE(is_compressed)) {
    // msg is already compressed
    return Status::NotSupported("Double compression is not supported");
  }

  if (original_msg->op_type() != WRITE_OP_EXT) {
    // Compression of messages other than WRITE_OP_EXT is not supported
    return Status::NotSupported("Only write ops can be compressed");
  }

  const CompressionCodec* codec = codec_.load();
  if (!codec) {
    return Status::NotSupported("Compression Codec is not initialized");
  }

  Slice uncompressed_slice(payload_str.c_str(), payload_str.size());

  // Resize buffer to hold max possible compressed payload size
  // TODO: Needs perf testing and maybe add support for streaming compression
  buffer.resize(codec->MaxCompressedLength(uncompressed_slice.size()));

  size_t compressed_len = 0;
  auto status =
    codec->Compress(uncompressed_slice, &buffer[0], &compressed_len);

  if (!status.ok()) {
    LOG(ERROR) <<
      "Compression failed for OpId: " << msg->get()->id().ShortDebugString();
    return status;
  }

  // Resize buffer to the actual compressed length
  buffer.resize(compressed_len);
  VLOG(2) << "Compressed OpId: " <<  msg->get()->id().ShortDebugString() <<
             " original payload size: " << uncompressed_slice.size() <<
             " compressed payload size: " << compressed_len;

  // Now create a new replicate message and copy contents from original message
  // and compressed payload
  std::unique_ptr<ReplicateMsg> rep_msg(new ReplicateMsg);
  *(rep_msg->mutable_id()) = msg->get()->id();
  rep_msg->set_timestamp(msg->get()->timestamp());
  rep_msg->set_op_type(msg->get()->op_type());

  WritePayloadPB* write_payload = rep_msg->mutable_write_payload();
  write_payload->set_payload(std::move(buffer.ToString()));
  write_payload->set_compression_codec(codec->type());
  write_payload->set_uncompressed_size(payload_str.size());

  compressed_msg->reset(rep_msg.release());
  return Status::OK();
}

Status LogCache::AppendOperations(const vector<ReplicateRefPtr>& msgs,
                                  const StatusCallback& callback) {
  CHECK_GT(msgs.size(), 0);

  // SpaceUsed is relatively expensive, so do calculations outside the lock
  // and cache the result with each message.
  int64_t mem_required = 0;
  int64_t total_msg_size = 0;
  vector<CacheEntry> entries_to_insert;
  entries_to_insert.reserve(msgs.size());

  bool found_compressed_msgs = false;
  for (const auto& msg : msgs) {
    CacheEntry e;
    e.mem_usage = 0;
    e.msg_size = msg->get()->SpaceUsedLong();

    const auto op_type = msg->get()->op_type();
    bool is_compressed =
      (msg->get()->write_payload().compression_codec() != NO_COMPRESSION);

    if (is_compressed) {
      // This batch has a compressed message. Store this fact to be used later
      // when appending this batch to the log
      found_compressed_msgs = true;
    }

    // Try to compress the message if:
    // (1) this msg is not already compressed (identified by 'is_compressed').
    // This can happen when this AppendOperations() is called on a secondary and
    // the primary has already compressed this msg
    // (2) Compression is configured for this instance (codec_ should not be
    // nullptr)
    // (3) This is a 'WRITE_OP_EXT' type (config-change/no-op/rotates are not
    // compressed today - these messages are small and can be left uncompressed)
    const CompressionCodec* codec = codec_.load();
    if (!is_compressed && codec && op_type == WRITE_OP_EXT) {
      std::unique_ptr<ReplicateMsg> compressed_msg;
      auto status =
        CompressMsg(msg, log_cache_compression_buf_, &compressed_msg);
      if (status.ok()) {
        // Successfully compressed this msg. So, use the compressed msg to cache
        e.mem_usage = static_cast<int64_t>(compressed_msg->SpaceUsedLong());
        e.msg = make_scoped_refptr_replicate(compressed_msg.release());
      } else {
        KLOG_EVERY_N_SECS(WARNING, 300) << "Failed to compress replicate msg";
      }
    }

    if (e.mem_usage == 0) {
      // Need to store the original msg. One of the three checks mentioned above
      // was not true
      e.mem_usage = e.msg_size;
      e.msg = msg;
    }

    total_msg_size += e.msg_size;
    mem_required += e.mem_usage;
    entries_to_insert.emplace_back(std::move(e));
  }

  int64_t first_idx_in_batch = msgs.front()->get()->id().index();
  int64_t last_idx_in_batch = msgs.back()->get()->id().index();

  std::unique_lock<Mutex> l(lock_);
  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (first_idx_in_batch != next_sequential_op_index_) {
    TruncateOpsAfterUnlocked(first_idx_in_batch - 1);
  }

  // Try to consume the memory. If it can't be consumed, we may need to evict.
  bool borrowed_memory = false;
  if (!tracker_->TryConsume(mem_required)) {
    int spare = tracker_->SpareCapacity();
    int need_to_free = mem_required - spare;
    VLOG_WITH_PREFIX_UNLOCKED(1) << "Memory limit would be exceeded trying to append "
                        << HumanReadableNumBytes::ToString(mem_required)
                        << " to log cache (available="
                        << HumanReadableNumBytes::ToString(spare)
                        << "): attempting to evict some operations...";

    // TODO: we should also try to evict from other tablets - probably better to
    // evict really old ops from another tablet than evict recent ops from this one.
    EvictSomeUnlocked(min_pinned_op_index_, need_to_free);

    // Force consuming, so that we don't refuse appending data. We might
    // blow past our limit a little bit (as much as the number of tablets times
    // the amount of in-flight data in the log), but until implementing the above TODO,
    // it's difficult to solve this issue.
    tracker_->Consume(mem_required);

    borrowed_memory = parent_tracker_->LimitExceeded();
  }

  for (auto& e : entries_to_insert) {
    auto index = e.msg->get()->id().index();
    EmplaceOrDie(&cache_, index, std::move(e));
    next_sequential_op_index_ = index + 1;
  }

  // We drop the lock during the AsyncAppendReplicates call, since it may block
  // if the queue is full, and the queue might not drain if it's trying to call
  // our callback and blocked on this lock.
  l.unlock();

  metrics_.log_cache_size->IncrementBy(mem_required);
  metrics_.log_cache_msg_size->IncrementBy(total_msg_size);
  metrics_.log_cache_num_ops->IncrementBy(msgs.size());

  Status log_status;
  if (!found_compressed_msgs) {
    // No compressed messages are found, so appen orinals msgs to log
    log_status = log_->AsyncAppendReplicates(
      msgs, Bind(&LogCache::LogCallback,
                 Unretained(this),
                 last_idx_in_batch,
                 borrowed_memory,
                 callback));
  } else {
    // The batch contains some compressed msgs. It needs to be uncompressed
    // before appending to the log
    vector<ReplicateRefPtr> uncompressed_msgs;
    uncompressed_msgs.reserve(msgs.size());

    for (const auto& msg : msgs) {
      const auto compression_codec =
        msg->get()->write_payload().compression_codec();

      if (compression_codec == NO_COMPRESSION) {
        // msg is not compressed, use it to write to the log
        uncompressed_msgs.push_back(msg);
      } else {
        // msg needs to be uncompressed before writing to the log
        std::unique_ptr<ReplicateMsg> uncompressed_msg;
        auto status = UncompressMsg(
            msg, log_cache_compression_buf_, &uncompressed_msg);

        // Crash if uncompression failed
        CHECK_OK_PREPEND(status,
            Substitute("Uncompess failed when writing to log"));
        uncompressed_msgs.push_back(std::move(
              make_scoped_refptr_replicate(uncompressed_msg.release())));
      }
    }

    log_status = log_->AsyncAppendReplicates(
      uncompressed_msgs, Bind(&LogCache::LogCallback,
                              Unretained(this),
                              last_idx_in_batch,
                              borrowed_memory,
                              callback));
  }

  if (!log_status.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(ERROR) << "Couldn't append to log: " << log_status.ToString();
    tracker_->Release(mem_required);
    return log_status;
  }

  // Now signal any threads that might be waiting for Ops to be appended to the
  // log
  next_index_cond_.Broadcast();
  return Status::OK();
}

void LogCache::LogCallback(int64_t last_idx_in_batch,
                           bool borrowed_memory,
                           const StatusCallback& user_callback,
                           const Status& log_status) {
  if (log_status.ok()) {
    std::lock_guard<Mutex> l(lock_);
    if (min_pinned_op_index_ <= last_idx_in_batch) {
      VLOG_WITH_PREFIX_UNLOCKED(1) << "Updating pinned index to " << (last_idx_in_batch + 1);
      min_pinned_op_index_ = last_idx_in_batch + 1;
    }

    // If we went over the global limit in order to log this batch, evict some to
    // get back down under the limit.
    if (borrowed_memory) {
      int64_t spare_capacity = parent_tracker_->SpareCapacity();
      if (spare_capacity < 0) {
        EvictSomeUnlocked(min_pinned_op_index_, -spare_capacity);
      }
    }
  }
  user_callback.Run(log_status);
}

bool LogCache::HasOpBeenWritten(int64_t index) const {
  std::lock_guard<Mutex> l(lock_);
  return index < next_sequential_op_index_;
}

Status LogCache::LookupOpId(int64_t op_index, OpId* op_id) const {
  // First check the log cache itself.
  {
    std::lock_guard<Mutex> l(lock_);

    // We sometimes try to look up OpIds that have never been written
    // on the local node. In that case, don't try to read the op from
    // the log reader, since it might actually race against the writing
    // of the op.
    if (op_index >= next_sequential_op_index_) {
      return Status::Incomplete(Substitute("Op with index $0 is ahead of the local log "
                                           "(next sequential op: $1)",
                                           op_index, next_sequential_op_index_));
    }
    auto iter = cache_.find(op_index);
    if (iter != cache_.end()) {
      *op_id = iter->second.msg->get()->id();
      return Status::OK();
    }
  }

  // If it misses, read from the log.
  return log_->LookupOpId(op_index, op_id);
}

namespace {
// Calculate the total byte size that will be used on the wire to replicate
// this message as part of a consensus update request. This accounts for the
// length delimiting and tagging of the message.
int64_t TotalByteSizeForMessage(const ReplicateMsg& msg) {
  int msg_size = google::protobuf::internal::WireFormatLite::LengthDelimitedSize(
    msg.ByteSize());
  msg_size += 1; // for the type tag
  return msg_size;
}
} // anonymous namespace

Status LogCache::BlockingReadOps(int64_t after_op_index,
                                 int max_size_bytes,
                                 const ReadContext& context,
                                 int64_t max_duration_ms,
                                 std::vector<ReplicateRefPtr>* messages,
                                 OpId* preceding_op) {
  MonoTime deadline =
    MonoTime::Now() + MonoDelta::FromMilliseconds(max_duration_ms);

  {
    std::lock_guard<Mutex> l(lock_);
    while (after_op_index >= next_sequential_op_index_) {
      (void) next_index_cond_.WaitUntil(deadline);

      if (MonoTime::Now() > deadline)
        break;
    }

    if (after_op_index >= next_sequential_op_index_) {
      // Waited for max_duration_ms, but 'after_op_index' is still not available
      // in the local log
      return Status::Incomplete(Substitute("Op with index $0 is ahead of the local log "
                                           "(next sequential op: $1)",
                                           after_op_index, next_sequential_op_index_));
    }
  }

  return ReadOps(
      after_op_index,
      max_size_bytes,
      context,
      messages,
      preceding_op);
}

Status LogCache::ReadOps(int64_t after_op_index,
                         int max_size_bytes,
                         const ReadContext& context,
                         std::vector<ReplicateRefPtr>* messages,
                         OpId* preceding_op) {
  DCHECK_GE(after_op_index, 0);

  // Try to lookup the first OpId in index
  auto lookUpStatus = LookupOpId(after_op_index, preceding_op);
  if (!lookUpStatus.ok()) {
    // On error return early
    if (lookUpStatus.IsNotFound()) {
      // If it is a NotFound() error, then do a dummy call into
      // ReadReplicatesInRange() to read a single op. This is so that it gets a
      // chance to update the error manager and report the error to upper layer
      vector<ReplicateMsg*> raw_replicate_ptrs;
      log_->ReadReplicatesInRange(
          after_op_index, after_op_index + 1, max_size_bytes, context,
          &raw_replicate_ptrs);
      for (ReplicateMsg* msg : raw_replicate_ptrs) {
        delete msg;
      }
    }

    return lookUpStatus;
  }

  std::unique_lock<Mutex> l(lock_);
  int64_t next_index = after_op_index + 1;

  // Return as many operations as we can, up to the limit
  int64_t remaining_space = max_size_bytes;
  while (remaining_space > 0 && next_index < next_sequential_op_index_) {

    // If the messages the peer needs haven't been loaded into the queue yet,
    // load them.
    MessageCache::const_iterator iter = cache_.lower_bound(next_index);
    if (iter == cache_.end() || iter->first != next_index) {
      int64_t up_to;
      if (iter == cache_.end()) {
        // Read all the way to the current op
        up_to = next_sequential_op_index_ - 1;
      } else {
        // Read up to the next entry that's in the cache
        up_to = iter->first - 1;
      }

      l.unlock();

      vector<ReplicateMsg*> raw_replicate_ptrs;
      RETURN_NOT_OK_PREPEND(
        log_->ReadReplicatesInRange(
          next_index, up_to, remaining_space, context, &raw_replicate_ptrs),
        Substitute("Failed to read ops $0..$1", next_index, up_to));
      l.lock();
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Successfully read " << raw_replicate_ptrs.size() << " ops "
          << "from disk (" << next_index << ".."
          << (next_index + raw_replicate_ptrs.size() - 1) << ")";

      for (ReplicateMsg* msg : raw_replicate_ptrs) {
        CHECK_EQ(next_index, msg->id().index());

        remaining_space -= TotalByteSizeForMessage(*msg);
        if (remaining_space > 0 || messages->empty()) {
          messages->push_back(make_scoped_refptr_replicate(msg));
          next_index++;
        } else {
          delete msg;
        }
      }

    } else {
      // Pull contiguous messages from the cache until the size limit is achieved.
      for (; iter != cache_.end(); ++iter) {
        const ReplicateRefPtr& msg = iter->second.msg;
        int64_t index = msg->get()->id().index();
        if (index != next_index) {
          continue;
        }

        remaining_space -= TotalByteSizeForMessage(*msg->get());
        if (remaining_space < 0 && !messages->empty()) {
          break;
        }

        messages->push_back(msg);
        next_index++;
      }
    }
  }
  return Status::OK();
}


void LogCache::EvictThroughOp(int64_t index) {
  std::lock_guard<Mutex> lock(lock_);

  EvictSomeUnlocked(index, MathLimits<int64_t>::kMax);
}

void LogCache::EvictSomeUnlocked(int64_t stop_after_index, int64_t bytes_to_evict) {
  VLOG_WITH_PREFIX_UNLOCKED(2) << "Evicting log cache index <= "
                      << stop_after_index
                      << " or " << HumanReadableNumBytes::ToString(bytes_to_evict)
                      << ": before state: " << ToStringUnlocked();

  int64_t bytes_evicted = 0;
  for (auto iter = cache_.begin(); iter != cache_.end();) {
    const CacheEntry& entry = (*iter).second;
    const ReplicateRefPtr& msg = entry.msg;
    VLOG_WITH_PREFIX_UNLOCKED(2) << "considering for eviction: " << msg->get()->id();
    int64_t msg_index = msg->get()->id().index();
    if (msg_index == 0) {
      // Always keep our special '0' op.
      ++iter;
      continue;
    }

    if (msg_index > stop_after_index || msg_index >= min_pinned_op_index_) {
      break;
    }

    if (!msg->HasOneRef()) {
      VLOG_WITH_PREFIX_UNLOCKED(2) << "Evicting cache: cannot remove " << msg->get()->id()
                                   << " because it is in-use by a peer.";
      ++iter;
      continue;
    }

    VLOG_WITH_PREFIX_UNLOCKED(2) << "Evicting cache. Removing: " << msg->get()->id();
    AccountForMessageRemovalUnlocked(entry);
    bytes_evicted += entry.mem_usage;
    cache_.erase(iter++);

    if (bytes_evicted >= bytes_to_evict) {
      break;
    }
  }
  VLOG_WITH_PREFIX_UNLOCKED(1) << "Evicting log cache: after state: " << ToStringUnlocked();
}

void LogCache::AccountForMessageRemovalUnlocked(const LogCache::CacheEntry& entry) {
  tracker_->Release(entry.mem_usage);
  metrics_.log_cache_size->DecrementBy(entry.mem_usage);
  metrics_.log_cache_msg_size->DecrementBy(entry.msg_size);
  metrics_.log_cache_num_ops->Decrement();
}

int64_t LogCache::BytesUsed() const {
  return tracker_->consumption();
}

string LogCache::StatsString() const {
  std::lock_guard<Mutex> lock(lock_);
  return StatsStringUnlocked();
}

string LogCache::StatsStringUnlocked() const {
  return Substitute("LogCacheStats(num_ops=$0, bytes=$1)",
                    metrics_.log_cache_num_ops->value(),
                    metrics_.log_cache_size->value());
}

std::string LogCache::ToString() const {
  std::lock_guard<Mutex> lock(lock_);
  return ToStringUnlocked();
}

std::string LogCache::ToStringUnlocked() const {
  return Substitute("Pinned index: $0, $1",
                    min_pinned_op_index_,
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
  for (const string& s : strings) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << s;
  }
}

void LogCache::DumpToStrings(vector<string>* lines) const {
  std::lock_guard<Mutex> lock(lock_);
  int counter = 0;
  lines->push_back(ToStringUnlocked());
  lines->push_back("Messages:");
  for (const auto& entry : cache_) {
    const ReplicateMsg* msg = entry.second.msg->get();
    lines->push_back(
      Substitute("Message[$0] $1.$2 : REPLICATE. Type: $3, Size: $4",
                 counter++, msg->id().term(), msg->id().index(),
                 OperationType_Name(msg->op_type()),
                 msg->ByteSize()));
  }
}

void LogCache::DumpToHtml(std::ostream& out) const {
  using std::endl;

  std::lock_guard<Mutex> lock(lock_);
  out << "<h3>Messages:</h3>" << endl;
  out << "<table>" << endl;
  out << "<tr><th>Entry</th><th>OpId</th><th>Type</th><th>Size</th><th>Status</th></tr>" << endl;

  int counter = 0;
  for (const auto& entry : cache_) {
    const ReplicateMsg* msg = entry.second.msg->get();
    out << Substitute("<tr><th>$0</th><th>$1.$2</th><td>REPLICATE $3</td>"
                      "<td>$4</td><td>$5</td></tr>",
                      counter++, msg->id().term(), msg->id().index(),
                      OperationType_Name(msg->op_type()),
                      msg->ByteSize(), SecureShortDebugString(msg->id())) << endl;
  }
  out << "</table>";
}

#define INSTANTIATE_METRIC(x) \
  x.Instantiate(metric_entity, 0)
LogCache::Metrics::Metrics(const scoped_refptr<MetricEntity>& metric_entity)
  : log_cache_num_ops(INSTANTIATE_METRIC(METRIC_log_cache_num_ops)),
    log_cache_size(INSTANTIATE_METRIC(METRIC_log_cache_size)),
    log_cache_msg_size(INSTANTIATE_METRIC(METRIC_log_cache_msg_size)) {
}
#undef INSTANTIATE_METRIC

} // namespace consensus
} // namespace kudu
