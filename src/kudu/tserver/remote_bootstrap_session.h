// Copyright (c) 2014 Cloudera Inc.
#ifndef KUDU_TSERVER_REMOTE_BOOTSTRAP_SESSION_H_
#define KUDU_TSERVER_REMOTE_BOOTSTRAP_SESSION_H_

#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <vector>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tserver/remote_bootstrap.pb.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace tablet {
class TabletPeer;
} // namespace tablet

namespace tserver {

class TabletPeerLookupIf;

// Caches file size and holds a shared_ptr reference to a RandomAccessFile.
// Assumes that the file underlying the RandomAccessFile is immutable.
struct ImmutableRandomAccessFileInfo {
  std::tr1::shared_ptr<RandomAccessFile> file;
  int64_t file_size;
};

// A potential Learner must establish a RemoteBootstrapSession with the leader in order
// to fetch the needed superblock, blocks, and log segments.
// This class is refcounted to make it easy to remove it from the session map
// on expiration while it is in use by another thread.
class RemoteBootstrapSession : public RefCountedThreadSafe<RemoteBootstrapSession> {
 public:
  RemoteBootstrapSession(const scoped_refptr<tablet::TabletPeer>& tablet_peer,
                         const std::string& session_id,
                         const std::string& requestor_uuid,
                         FsManager* fs_manager);

  // Initialize the session, including anchoring files (TODO) and fetching the
  // tablet superblock and list of WAL segments.
  Status Init();

  // Return ID of tablet corresponding to this session.
  const std::string& tablet_id() const;

  // Return UUID of the requestor that initiated this session.
  const std::string& requestor_uuid() const;

  // Open block for reading, if it's not already open, and read some of it.
  // If maxlen is 0, we use a system-selected length for the data piece.
  // *data is set to a std::string containing the data. Ownership of this object
  // is passed to the caller. A string is used because the RPC interface is
  // sending data serialized as protobuf and we want to minimize copying.
  // On error, Status is set to a non-OK value and error_code is filled in.
  //
  // This method is thread-safe.
  Status GetBlockPiece(const BlockId& block_id, size_t offset, int64_t client_maxlen,
                       std::string* data, int64_t* block_file_size,
                       RemoteBootstrapErrorPB::Code* error_code);

  // Get a piece of a log segment.
  // The behavior and params are very similar to GetBlockPiece(), but this one
  // is only for sending WAL segment files.
  Status GetLogSegmentPiece(const consensus::OpId& segment_first_op_id,
                            size_t offset, int64_t client_maxlen,
                            std::string* data, int64_t* log_file_size,
                            RemoteBootstrapErrorPB::Code* error_code);

  const tablet::TabletSuperBlockPB& tablet_superblock() const { return tablet_superblock_; }

  const log::ReadableLogSegmentMap& log_segments() const { return log_segments_; }

  // Check if a block is currently open.
  bool IsBlockOpenForTests(const BlockId& block_id) const;

 private:
  friend class RefCountedThreadSafe<RemoteBootstrapSession>;

  typedef std::tr1::unordered_map<BlockId, ImmutableRandomAccessFileInfo,
                                  BlockIdHash> BlockMap;
  typedef std::tr1::unordered_map<consensus::OpId, ImmutableRandomAccessFileInfo,
                                  consensus::OpIdHashFunctor, consensus::OpIdEqualsFunctor> LogMap;

  ~RemoteBootstrapSession();

  // Close the specified block file for read.
  // If it is not open, return an error.
  Status CloseBlock(const BlockId& block_id);

  // Open block or look up cached block info.
  Status FindOrOpenBlock(const BlockId& block_id,
                         ImmutableRandomAccessFileInfo* file_info,
                         RemoteBootstrapErrorPB::Code* error_code);

  // Look up log segment in cache or log segment map.
  Status FindLogSegment(const consensus::OpId& segment_first_op_id,
                        ImmutableRandomAccessFileInfo* file_info,
                        RemoteBootstrapErrorPB::Code* error_code);

  // Unregister log anchor, if it's registered.
  Status UnregisterAnchorIfNeededUnlocked();

  scoped_refptr<tablet::TabletPeer> tablet_peer_;
  const std::string session_id_;
  const std::string requestor_uuid_;
  FsManager* const fs_manager_;

  mutable simple_spinlock session_lock_;

  BlockMap blocks_; // Protected by session_lock_.
  LogMap logs_;     // Protected by session_lock_.
  tablet::TabletSuperBlockPB tablet_superblock_;
  log::ReadableLogSegmentMap log_segments_;
  log::OpIdAnchor log_anchor_;

  DISALLOW_COPY_AND_ASSIGN(RemoteBootstrapSession);
};

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_REMOTE_BOOTSTRAP_SESSION_H_
