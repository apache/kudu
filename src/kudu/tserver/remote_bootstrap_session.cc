// Copyright (c) 2014 Cloudera Inc.
#include "kudu/tserver/remote_bootstrap_session.h"

#include <algorithm>

#include "kudu/consensus/log.h"
#include "kudu/fs/block_id-inl.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/transfer.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/util/env_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tserver {

using consensus::OpId;
using log::MinimumOpId;
using log::OpIdAnchorRegistry;
using metadata::TabletMetadata;
using metadata::TabletSuperBlockPB;
using std::tr1::shared_ptr;
using strings::Substitute;
using tablet::TabletPeer;

RemoteBootstrapSession::RemoteBootstrapSession(const scoped_refptr<TabletPeer>& tablet_peer,
                                               const std::string& session_id,
                                               const std::string& requestor_uuid,
                                               FsManager* fs_manager)
  : tablet_peer_(tablet_peer),
    session_id_(session_id),
    requestor_uuid_(requestor_uuid),
    fs_manager_(fs_manager) {
}

RemoteBootstrapSession::~RemoteBootstrapSession() {
  // No lock taken in the destructor, should only be 1 thread with access now.
  CHECK_OK(UnregisterAnchorIfNeededUnlocked());
  blocks_.clear();
}

Status RemoteBootstrapSession::Init() {
  // Take locks to support re-initialization of the same session.
  boost::lock_guard<simple_spinlock> l(session_lock_);
  RETURN_NOT_OK(UnregisterAnchorIfNeededUnlocked());

  blocks_.clear();
  logs_.clear();

  const string& tablet_id = tablet_peer_->tablet()->tablet_id();

  // Look up the metadata.
  const TabletMetadata* metadata = tablet_peer_->shared_tablet()->metadata();
  shared_ptr<TabletSuperBlockPB> tablet_superblock;
  RETURN_NOT_OK_PREPEND(metadata->ToSuperBlock(&tablet_superblock),
                        Substitute("Unable to access superblock for tablet $0", tablet_id));
  tablet_superblock_ = *tablet_superblock.get();
  // TODO: Anchor blocks once block GC is implemented. See KUDU-452.

  // Look up the log segments. To avoid races, we do a 2-phase thing where we
  // first anchor all the logs, get a list of the logs available, and then
  // atomically re-anchor on the minimum OpId in that set.
  // TODO: Implement one-shot anchoring through the Log API. See KUDU-284.
  OpIdAnchorRegistry* registry = tablet_peer_->tablet()->opid_anchor_registry();
  string anchor_owner_token = Substitute("RemoteBootstrap-$0", session_id_);

  registry->Register(MinimumOpId(), anchor_owner_token, &log_anchor_);
  tablet_peer_->log()->GetReadableLogSegments(&log_segments_);
  // Re-anchor on the earliest OpId that we actually need.
  if (!log_segments_.empty()) {
    // Anchor everything in log_segments.
    const OpId& earliest = log_segments_.begin()->first;
    RETURN_NOT_OK(registry->UpdateRegistration(earliest, anchor_owner_token, &log_anchor_));
  } else {
    // No log segments returned, so no log anchors needed.
    RETURN_NOT_OK(registry->Unregister(&log_anchor_));
  }

  return Status::OK();
}

const std::string& RemoteBootstrapSession::tablet_id() const {
  return tablet_peer_->tablet()->tablet_id();
}

const std::string& RemoteBootstrapSession::requestor_uuid() const {
  return requestor_uuid_;
}

// Determine the length of the data chunk to return to the client.
static int64_t DetermineReadLength(int64_t bytes_remaining, int64_t requested_len) {
  // Determine the size of the chunks we want to read.
  // Choose "system max" as a multiple of typical HDD block size (4K) with 4K to
  // spare for other stuff in the message, like headers, other protobufs, etc.
  const int32_t kSpareBytes = 4096;
  const int32_t kDiskSectorSize = 4096;
  int32_t system_max_chunk_size =
      ((FLAGS_rpc_max_message_size - kSpareBytes) / kDiskSectorSize) * kDiskSectorSize;
  CHECK_GT(system_max_chunk_size, 0) << "rpc_max_message_size is too low to transfer data: "
                                     << FLAGS_rpc_max_message_size;

  // The min of the {requested, system} maxes is the effective max.
  int64_t maxlen = (requested_len > 0) ? std::min<int64_t>(requested_len, system_max_chunk_size) :
                                        system_max_chunk_size;
  return std::min(bytes_remaining, maxlen);
}

// Calculate the size of the data to return given a maximum client message
// length, the file itself, and the offset into the file to be read from.
static Status GetResponseDataSize(const ImmutableRandomAccessFileInfo& file_info,
                                  uint64_t offset, int64_t client_maxlen,
                                  RemoteBootstrapErrorPB::Code* error_code, int64_t* data_size) {
  // If requested offset is off the end of the file, bail.
  if (offset >= file_info.file_size) {
    *error_code = RemoteBootstrapErrorPB::INVALID_REMOTE_BOOTSTRAP_REQUEST;
    return Status::InvalidArgument(
        Substitute("Requested offset ($0) is beyond the data size ($1)",
                   offset, file_info.file_size));
  }

  int64_t bytes_remaining = file_info.file_size - offset;

  *data_size = DetermineReadLength(bytes_remaining, client_maxlen);
  DCHECK_GT(*data_size, 0);
  if (client_maxlen > 0) {
    DCHECK_LE(*data_size, client_maxlen);
  }

  return Status::OK();
}

// Read a chunk of a file into a buffer.
// data_name provides a string for the block/log to be used in error messages.
static Status ReadFileChunkToBuf(const ImmutableRandomAccessFileInfo& file_info,
                                 uint64_t offset, int64_t client_maxlen,
                                 const string& data_name,
                                 string* data, int64_t* file_size,
                                 RemoteBootstrapErrorPB::Code* error_code) {
  int64_t response_data_size = 0;
  RETURN_NOT_OK_PREPEND(GetResponseDataSize(file_info, offset, client_maxlen, error_code,
                                            &response_data_size),
                        Substitute("Error reading $0", data_name));

  Stopwatch chunk_timer(Stopwatch::THIS_THREAD);
  chunk_timer.start();

  // Writing into a std::string buffer is basically guaranteed to work on C++11,
  // however any modern compiler should be compatible with it.
  // Violates the API contract, but avoids excessive copies.
  data->resize(response_data_size);
  uint8_t* buf = reinterpret_cast<uint8_t*>(const_cast<char*>(data->data()));
  Slice slice;
  Status s = env_util::ReadFully(file_info.file.get(), offset, response_data_size,
                                 &slice, buf);
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend(
        Substitute("Unable to read existing file for $0", data_name));
    LOG(WARNING) << s.ToString();
    *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    return s;
  }
  // Figure out if Slice points to buf or if Slice points to the mmap.
  // If it points to the mmap then copy into buf.
  if (slice.data() != buf) {
    memcpy(buf, slice.data(), slice.size());
  }
  chunk_timer.stop();
  TRACE("Remote bootstrap: $0: $1 total bytes read. Total time elapsed: $2",
        data_name, response_data_size, chunk_timer.elapsed().ToString());

  *file_size = file_info.file_size;
  return Status::OK();
}

Status RemoteBootstrapSession::GetBlockPiece(const BlockId& block_id,
                                             uint64_t offset, int64_t client_maxlen,
                                             string* data, int64_t* block_file_size,
                                             RemoteBootstrapErrorPB::Code* error_code) {
  ImmutableRandomAccessFileInfo file_info;
  RETURN_NOT_OK(FindOrOpenBlock(block_id, &file_info, error_code));

  RETURN_NOT_OK(ReadFileChunkToBuf(file_info, offset, client_maxlen,
                                      Substitute("block $0", block_id.ToString()),
                                      data, block_file_size, error_code));

  // Eagerly close the block file if we just read the last byte.
  // This optimizes for the sequential-read case by freeing resources.
  if (offset + data->size() == file_info.file_size) {
    Status s = CloseBlock(block_id);
    if (!s.ok()) {
      // Since we allow parallel readers to read the same block, this may race
      // and the close may fail.
      VLOG(1) << Substitute("Remote bootstrap: Closing block after read failed with code $0: $1",
                            RemoteBootstrapErrorPB::Code_Name(*error_code),
                            s.ToString());
    }
  }

  return Status::OK();
}

Status RemoteBootstrapSession::GetLogSegmentPiece(const consensus::OpId& segment_first_op_id,
                                                  uint64_t offset, int64_t client_maxlen,
                                                  std::string* data, int64_t* block_file_size,
                                                  RemoteBootstrapErrorPB::Code* error_code) {
  ImmutableRandomAccessFileInfo file_info;
  RETURN_NOT_OK(FindLogSegment(segment_first_op_id, &file_info, error_code));
  RETURN_NOT_OK(ReadFileChunkToBuf(file_info, offset, client_maxlen,
                                      Substitute("log segment $0",
                                                 segment_first_op_id.ShortDebugString()),
                                      data, block_file_size, error_code));

  // Note: We do not eagerly close log segment files, since we share ownership
  // of the LogSegment objects with the Log itself.

  return Status::OK();
}

bool RemoteBootstrapSession::IsBlockOpenForTests(const BlockId& block_id) const {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  return ContainsKey(blocks_, block_id);
}

Status RemoteBootstrapSession::CloseBlock(const BlockId& block_id) {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  if (!blocks_.erase(block_id)) {
    return Status::NotFound("Block is not open", block_id.ToString());
  }
  return Status::OK();
}

// Add a file to the cache and populate the given ImmutableRandomAcccessFileInfo
// object with the file ref and size.
template <class Collection, class Key>
static Status AddToCacheUnlocked(Collection* const cache,
                                 const Key& key,
                                 const shared_ptr<RandomAccessFile> file,
                                 ImmutableRandomAccessFileInfo* file_info,
                                 RemoteBootstrapErrorPB::Code* error_code) {
  uint64_t file_size;
  Status s = file->Size(&file_size);
  if (PREDICT_FALSE(!s.ok())) {
    *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    return s.CloneAndPrepend("Unable to get size of file");
  }

  // Sanity check for 0-length files.
  if (file_size == 0) {
    *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    return Status::Corruption("Found 0-length file");
  }

  // Looks good, add it to the cache.
  file_info->file = file;
  file_info->file_size = file_size;
  InsertOrDie(cache, key, *file_info);

  return Status::OK();
}

Status RemoteBootstrapSession::FindOrOpenBlock(const BlockId& block_id,
                                               ImmutableRandomAccessFileInfo* file_info,
                                               RemoteBootstrapErrorPB::Code* error_code) {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  if (FindCopy(blocks_, block_id, file_info)) {
    return Status::OK();
  }

  shared_ptr<RandomAccessFile> file;
  Status s = fs_manager_->OpenBlock(block_id, &file);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Unable to open requested (existing) block file: "
                 << block_id.ToString() << ": " << s.ToString();
    if (s.IsNotFound()) {
      *error_code = RemoteBootstrapErrorPB::BLOCK_NOT_FOUND;
    } else {
      *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    }
    return s.CloneAndPrepend(Substitute("Unable to open block file for block $0",
                                        block_id.ToString()));
  }

  s = AddToCacheUnlocked(&blocks_, block_id, file, file_info, error_code);
  if (!s.ok()) {
    s = s.CloneAndPrepend(Substitute("Error accessing data for block $0", block_id.ToString()));
    LOG(DFATAL) << "Data block disappeared: " << s.ToString();
  }
  return s;
}

Status RemoteBootstrapSession::FindLogSegment(const consensus::OpId& segment_first_op_id,
                                              ImmutableRandomAccessFileInfo* file_info,
                                              RemoteBootstrapErrorPB::Code* error_code) {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  if (FindCopy(logs_, segment_first_op_id, file_info)) {
    return Status::OK();
  }

  scoped_refptr<log::ReadableLogSegment> log_segment;
  if (!FindCopy(log_segments_, segment_first_op_id, &log_segment)) {
    *error_code = RemoteBootstrapErrorPB::WAL_SEGMENT_NOT_FOUND;
    return Status::NotFound(Substitute("Segment with first OpId $0 not found",
                                       segment_first_op_id.ShortDebugString()));
  }

  Status s = AddToCacheUnlocked(&logs_, segment_first_op_id, log_segment->readable_file(),
                                         file_info, error_code);
  if (!s.ok()) {
    s = s.CloneAndPrepend(
            Substitute("Error accessing data for log segment with seqno $0 and first OpId $1",
                       log_segment->header().sequence_number(),
                       segment_first_op_id.ShortDebugString()));
    LOG(INFO) << s.ToString();
  }
  return s;
}

Status RemoteBootstrapSession::UnregisterAnchorIfNeededUnlocked() {
  OpIdAnchorRegistry* registry = tablet_peer_->tablet()->opid_anchor_registry();
  if (registry->IsRegistered(&log_anchor_)) {
    RETURN_NOT_OK(registry->Unregister(&log_anchor_));
  }
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
