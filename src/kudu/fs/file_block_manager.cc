// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/file_block_manager.h"

#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "kudu/fs/block_id-inl.h"
#include "kudu/fs/block_manager_util.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(enable_data_block_fsync);

namespace kudu {
namespace fs {
namespace internal {

////////////////////////////////////////////////////////////
// FileBlockLocation
////////////////////////////////////////////////////////////

// Logical location of a block in the file block manager.
//
// A block ID uniquely locates a block. Every ID is composed of two UUIDs
// appended together: a path UUID and a block UUID. The former uniquely
// identifies a mountpoint on the filesystem while the latter uniquely
// identifies a file within that mountpoint.
//
// The FileBlockLocation abstraction provides easy access to the different
// components of the block ID. Within the class, a "full block ID" refers
// to the 64 byte block ID that includes all components, and a "sub block
// ID" refers to the 32 byte block ID that is just the block UUID.
//
// A location is constructed via FromParts() or FromBlockId() and is
// copyable and assignable.
class FileBlockLocation {
 public:
  // Empty constructor
  FileBlockLocation() {
  }

  // Construct a location from its constituent parts.
  //
  // On success, 'location' is overwritten with the new location.
  static Status FromParts(const string& root_path,
                          const string& root_path_uuid,
                          const string& block_uuid,
                          FileBlockLocation* location);

  // Construct a location from a full block ID. The file block manager is
  // is used to convert the root path's UUID into the actual root path.
  //
  // On success, 'location' is overwritten with the new location.
  static Status FromBlockId(const FileBlockManager& block_manager,
                            const BlockId& block_id,
                            FileBlockLocation* location);

  // Returns the full filesystem path for this location.
  string GetFullPath() const;

  // Create all subdirectories needed for this location.
  //
  // On success, 'created_dirs' contains the directories that were actually
  // created (as opposed to those that were reused).
  Status CreateBlockDir(Env* env, vector<string>* created_dirs);

  // Writes all parent directories that are part of this location to
  // 'parent_dirs'.
  //
  // The directories are written in "fsync order"; that is, the order in
  // which they should be fsynced to make them durable.
  void GetAllParentDirs(vector<string>* parent_dirs) const;

  // Simple accessors.
  const string& root_path() const { return root_path_; }
  const BlockId& full_block_id() const { return full_block_id_; }

 private:
  static const int kRootPathUuidSize = 32;
  static const int kBlockUuidSize = 32;

  FileBlockLocation(const string& root_path,
                    const BlockId& full_block_id,
                    const BlockId& sub_block_id)
    : root_path_(root_path),
      full_block_id_(full_block_id),
      sub_block_id_(sub_block_id) {
  }

  static Status CreateDirIfMissing(Env* env, const string& path, bool* created);

  string root_path_;
  BlockId full_block_id_;
  BlockId sub_block_id_;
};

Status FileBlockLocation::FromParts(const string& root_path,
                                    const string& root_path_uuid,
                                    const string& block_uuid,
                                    FileBlockLocation* location) {
  if (root_path_uuid.size() != kRootPathUuidSize) {
    return Status::InvalidArgument("Bad root path UUID", root_path_uuid);
  }
  if (block_uuid.size() != kBlockUuidSize) {
    return Status::InvalidArgument("Bad block UUID", block_uuid);
  }

  *location = FileBlockLocation(root_path,
                                BlockId(root_path_uuid + block_uuid),
                                BlockId(block_uuid));
  return Status::OK();
}

Status FileBlockLocation::FromBlockId(const FileBlockManager& block_manager,
                                      const BlockId& block_id,
                                      FileBlockLocation* location) {
  if (block_id.id_.size() != kRootPathUuidSize + kBlockUuidSize) {
    return Status::InvalidArgument("Bad block ID", block_id.ToString());
  }
  string root_path_uuid = block_id.id_.substr(0, kRootPathUuidSize);
  string block_uuid = block_id.id_.substr(kRootPathUuidSize, kBlockUuidSize);
  string root_path;
  if (!block_manager.FindRootPath(root_path_uuid, &root_path)) {
    return Status::NotFound("Root path UUID not found", root_path_uuid);
  }

  *location = FileBlockLocation(root_path, block_id, BlockId(block_uuid));
  return Status::OK();
}

string FileBlockLocation::GetFullPath() const {
  string p = root_path_;
  p = JoinPathSegments(p, sub_block_id_.hash0());
  p = JoinPathSegments(p, sub_block_id_.hash1());
  p = JoinPathSegments(p, sub_block_id_.hash2());
  p = JoinPathSegments(p, sub_block_id_.ToString());
  return p;
}

Status FileBlockLocation::CreateBlockDir(Env* env,
                                         vector<string>* created_dirs) {
  DCHECK(env->FileExists(root_path_));

  bool path0_created;
  string path0 = JoinPathSegments(root_path_, sub_block_id_.hash0());
  RETURN_NOT_OK(CreateDirIfMissing(env, path0, &path0_created));

  bool path1_created;
  string path1 = JoinPathSegments(path0, sub_block_id_.hash1());
  RETURN_NOT_OK(CreateDirIfMissing(env, path1, &path1_created));

  bool path2_created;
  string path2 = JoinPathSegments(path1, sub_block_id_.hash2());
  RETURN_NOT_OK(CreateDirIfMissing(env, path2, &path2_created));

  if (path2_created) {
    created_dirs->push_back(path1);
  }
  if (path1_created) {
    created_dirs->push_back(path0);
  }
  if (path0_created) {
    created_dirs->push_back(root_path_);
  }
  return Status::OK();
}

void FileBlockLocation::GetAllParentDirs(vector<string>* parent_dirs) const {
  string path0 = JoinPathSegments(root_path_, sub_block_id_.hash0());
  string path1 = JoinPathSegments(path0, sub_block_id_.hash1());
  string path2 = JoinPathSegments(path1, sub_block_id_.hash2());

  // This is the order in which the parent directories should be
  // synchronized to disk.
  parent_dirs->push_back(path2);
  parent_dirs->push_back(path1);
  parent_dirs->push_back(path0);
  parent_dirs->push_back(root_path_);
}

Status FileBlockLocation::CreateDirIfMissing(Env* env,
                                             const string& path,
                                             bool* created) {
  Status s = env->CreateDir(path);
  if (created) {
    if (s.ok()) {
      *created = true;
    } else if (s.IsAlreadyPresent()) {
      *created = false;
    }
  }
  return s.IsAlreadyPresent() ? Status::OK() : s;
}

////////////////////////////////////////////////////////////
// FileWritableBlock
////////////////////////////////////////////////////////////

// A file-backed block that has been opened for writing.
//
// Contains a pointer to the block manager as well as a FileBlockLocation
// so that dirty metadata can be synced via BlockManager::SyncMetadata()
// at Close() time. Embedding a FileBlockLocation (and not a simpler
// BlockId) consumes more memory, but the number of outstanding
// FileWritableBlock instances is expected to be low.
class FileWritableBlock : public WritableBlock {
 public:
  FileWritableBlock(FileBlockManager* block_manager,
                    const FileBlockLocation& location,
                    const shared_ptr<WritableFile>& writer);

  virtual ~FileWritableBlock();

  virtual Status Close() OVERRIDE;

  virtual Status Abort() OVERRIDE;

  virtual BlockManager* block_manager() const OVERRIDE;

  virtual const BlockId& id() const OVERRIDE;

  virtual Status Append(const Slice& data) OVERRIDE;

  virtual Status FlushDataAsync() OVERRIDE;

  virtual size_t BytesAppended() const OVERRIDE;

  virtual State state() const OVERRIDE;

 private:
  enum SyncMode {
    SYNC,
    NO_SYNC
  };

  // Close the block, optionally synchronizing dirty data and metadata.
  Status Close(SyncMode mode);

  // Back pointer to the block manager.
  //
  // Should remain alive for the lifetime of this block.
  FileBlockManager* block_manager_;

  // The block's location.
  const FileBlockLocation location_;

  // The underlying opened file backing this block.
  shared_ptr<WritableFile> writer_;

  State state_;

  // The number of bytes successfully appended to the block.
  size_t bytes_appended_;

  DISALLOW_COPY_AND_ASSIGN(FileWritableBlock);
};

FileWritableBlock::FileWritableBlock(FileBlockManager* block_manager,
                                     const FileBlockLocation& location,
                                     const shared_ptr<WritableFile>& writer) :
  block_manager_(block_manager),
  location_(location),
  writer_(writer),
  state_(CLEAN),
  bytes_appended_(0) {
}

FileWritableBlock::~FileWritableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  id().ToString()));
}

Status FileWritableBlock::Close() {
  return Close(SYNC);
}

Status FileWritableBlock::Abort() {
  RETURN_NOT_OK(Close(NO_SYNC));
  return block_manager()->DeleteBlock(id());
}

BlockManager* FileWritableBlock::block_manager() const {
  return block_manager_;
}

const BlockId& FileWritableBlock::id() const {
  return location_.full_block_id();
}

Status FileWritableBlock::Append(const Slice& data) {
  DCHECK(state_ == CLEAN || state_ == DIRTY)
      << "Invalid state: " << state_;

  RETURN_NOT_OK(writer_->Append(data));
  state_ = DIRTY;
  bytes_appended_ += data.size();
  return Status::OK();
}

Status FileWritableBlock::FlushDataAsync() {
  DCHECK(state_ == CLEAN || state_ == DIRTY || state_ == FLUSHING)
      << "Invalid state: " << state_;
  if (state_ == DIRTY) {
    VLOG(3) << "Flushing block " << id();
    RETURN_NOT_OK(writer_->Flush(WritableFile::FLUSH_ASYNC));
  }

  state_ = FLUSHING;
  return Status::OK();
}

size_t FileWritableBlock::BytesAppended() const {
  return bytes_appended_;
}

WritableBlock::State FileWritableBlock::state() const {
  return state_;
}

Status FileWritableBlock::Close(SyncMode mode) {
  if (state_ == CLOSED) {
    return Status::OK();
  }

  Status sync;
  if (mode == SYNC &&
      (state_ == CLEAN || state_ == DIRTY || state_ == FLUSHING)) {
    // Safer to synchronize data first, then metadata.
    VLOG(3) << "Syncing block " << id();
    if (FLAGS_enable_data_block_fsync) {
      sync = writer_->Sync();
    }
    if (sync.ok()) {
      sync = block_manager_->SyncMetadata(location_);
    }
    WARN_NOT_OK(sync, Substitute("Failed to sync when closing block $0",
                                 id().ToString()));
  }
  Status close = writer_->Close();

  state_ = CLOSED;
  writer_.reset();

  // Prefer the result of Close() to that of Sync().
  return !close.ok() ? close : sync;
}

////////////////////////////////////////////////////////////
// FileReadableBlock
////////////////////////////////////////////////////////////

// A file-backed block that has been opened for reading.
//
// There may be millions of instances of FileReadableBlock outstanding, so
// great care must be taken to reduce its size. To that end, it does _not_
// embed a FileBlockLocation, using the simpler BlockId instead.
class FileReadableBlock : public ReadableBlock {
 public:
  FileReadableBlock(const BlockId& block_id,
                    const shared_ptr<RandomAccessFile>& reader);

  virtual ~FileReadableBlock();

  virtual Status Close() OVERRIDE;

  virtual const BlockId& id() const OVERRIDE;

  virtual Status Size(size_t* sz) const OVERRIDE;

  virtual Status Read(uint64_t offset, size_t length,
                      Slice* result, uint8_t* scratch) const OVERRIDE;

 private:
  // The block's identifier.
  const BlockId block_id_;

  // The underlying opened file backing this block.
  shared_ptr<RandomAccessFile> reader_;

  // Whether or not this block has been closed. Close() is thread-safe, so
  // this must be an atomic primitive.
  AtomicBool closed_;

  DISALLOW_COPY_AND_ASSIGN(FileReadableBlock);
};

FileReadableBlock::FileReadableBlock(const BlockId& block_id,
                                     const shared_ptr<RandomAccessFile>& reader) :
  block_id_(block_id),
  reader_(reader),
  closed_(false) {
}

FileReadableBlock::~FileReadableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  id().ToString()));
}

Status FileReadableBlock::Close() {
  if (closed_.CompareAndSwap(false, true)) {
    reader_.reset();
  }

  return Status::OK();
}

const BlockId& FileReadableBlock::id() const {
  return block_id_;
}

Status FileReadableBlock::Size(size_t* sz) const {
  DCHECK(!closed_.Load());

  return reader_->Size(sz);
}

Status FileReadableBlock::Read(uint64_t offset, size_t length,
                               Slice* result, uint8_t* scratch) const {
  DCHECK(!closed_.Load());

  return env_util::ReadFully(reader_.get(), offset, length, result, scratch);
}

} // namespace internal

////////////////////////////////////////////////////////////
// FileBlockManager
////////////////////////////////////////////////////////////

static const char* kBlockManagerType = "file";

Status FileBlockManager::SyncMetadata(const internal::FileBlockLocation& location) {
  vector<string> parent_dirs;
  location.GetAllParentDirs(&parent_dirs);

  // Figure out what directories to sync.
  vector<string> to_sync;
  {
    lock_guard<simple_spinlock> l(&lock_);
    BOOST_FOREACH(const string& parent_dir, parent_dirs) {
      if (dirty_dirs_.erase(parent_dir)) {
        to_sync.push_back(parent_dir);
      }
    }
  }

  // Sync them.
  if (FLAGS_enable_data_block_fsync) {
    BOOST_FOREACH(const string& s, to_sync) {
      RETURN_NOT_OK(env_->SyncDir(s));
    }
  }
  return Status::OK();
}

bool FileBlockManager::FindRootPath(const string& root_path_uuid,
                                    string* root_path) const {
  return FindCopy(root_paths_by_uuid_, root_path_uuid, root_path);
}

FileBlockManager::FileBlockManager(Env* env,
                                   const vector<string>& root_paths)
  : env_(env),
    root_paths_(root_paths) {
  DCHECK_GT(root_paths.size(), 0);
}

FileBlockManager::~FileBlockManager() {
}

Status FileBlockManager::Create() {
  BOOST_FOREACH(const string& root_path, root_paths_) {
    Status s = env_->CreateDir(root_path);
    if (!s.ok() && !s.IsAlreadyPresent()) {
      return s;
    }
    string instance_filename = JoinPathSegments(
        root_path, kInstanceMetadataFileName);

    PathInstanceMetadataFile metadata(env_, kBlockManagerType,
                                      instance_filename);
    RETURN_NOT_OK_PREPEND(metadata.Create(), instance_filename);
  }
  return Status::OK();
}

Status FileBlockManager::Open() {
  BOOST_FOREACH(const string& root_path, root_paths_) {
    if (!env_->FileExists(root_path)) {
      return Status::NotFound(Substitute(
          "FileBlockManager at $0 not found", root_path));
    }
    string instance_filename = JoinPathSegments(
        root_path, kInstanceMetadataFileName);
    PathInstanceMetadataPB new_instance;
    PathInstanceMetadataFile metadata(env_, kBlockManagerType,
                                      instance_filename);
    RETURN_NOT_OK_PREPEND(metadata.Open(&new_instance), instance_filename);
    InsertOrDie(&root_paths_by_uuid_, new_instance.uuid(), root_path);
  }
  next_root_path_ = root_paths_by_uuid_.begin();
  return Status::OK();
}

Status FileBlockManager::CreateBlock(const CreateBlockOptions& opts,
                                     gscoped_ptr<WritableBlock>* block) {

  // Pick a root path using a simple round-robin block placement strategy.
  string root_uuid;
  string root_path;
  {
    lock_guard<simple_spinlock> l(&lock_);
    root_uuid = next_root_path_->first;
    root_path = next_root_path_->second;
    next_root_path_++;
    if (next_root_path_ == root_paths_by_uuid_.end()) {
      next_root_path_ = root_paths_by_uuid_.begin();
    }
  }

  string path;
  vector<string> created_dirs;
  Status s;
  internal::FileBlockLocation location;
  shared_ptr<WritableFile> writer;

  // Repeat in case of block id collisions (unlikely).
  do {
    created_dirs.clear();
    RETURN_NOT_OK(internal::FileBlockLocation::FromParts(
        root_path, root_uuid, oid_generator_.Next(), &location));
    path = location.GetFullPath();
    RETURN_NOT_OK_PREPEND(location.CreateBlockDir(env_, &created_dirs), path);
    WritableFileOptions wr_opts;
    wr_opts.mode = WritableFileOptions::CREATE_NON_EXISTING;
    s = env_util::OpenFileForWrite(wr_opts, env_, path, &writer);
  } while (PREDICT_FALSE(s.IsAlreadyPresent()));
  if (s.ok()) {
    VLOG(1) << "Creating new block " << location.full_block_id().ToString() << " at " << path;
    {
      // Update dirty_dirs_ with those provided as well as the block's
      // directory, which may not have been created but is definitely dirty
      // (because we added a file to it).
      lock_guard<simple_spinlock> l(&lock_);
      BOOST_FOREACH(const string& created, created_dirs) {
        dirty_dirs_.insert(created);
      }
      dirty_dirs_.insert(DirName(path));
    }
    block->reset(new internal::FileWritableBlock(this, location, writer));
  }
  return s;
}

Status FileBlockManager::CreateBlock(gscoped_ptr<WritableBlock>* block) {
  return CreateBlock(CreateBlockOptions(), block);
}

Status FileBlockManager::OpenBlock(const BlockId& block_id,
                                   gscoped_ptr<ReadableBlock>* block) {
  internal::FileBlockLocation location;
  RETURN_NOT_OK(internal::FileBlockLocation::FromBlockId(*this, block_id, &location));
  string path = location.GetFullPath();
  VLOG(1) << "Opening block with id " << block_id.ToString() << " at " << path;

  shared_ptr<RandomAccessFile> reader;
  RETURN_NOT_OK(env_util::OpenFileForRandom(env_, path, &reader));
  block->reset(new internal::FileReadableBlock(location.full_block_id(), reader));
  return Status::OK();
}

Status FileBlockManager::DeleteBlock(const BlockId& block_id) {
  internal::FileBlockLocation location;
  RETURN_NOT_OK(internal::FileBlockLocation::FromBlockId(*this, block_id, &location));
  string path = location.GetFullPath();
  RETURN_NOT_OK(env_->DeleteFile(path));

  // We don't bother fsyncing the parent directory as there's nothing to be
  // gained by ensuring that the deletion is made durable. Even if we did
  // fsync it, we'd need to account for garbage at startup time (in the
  // event that we crashed just before the fsync), and with such accounting
  // fsync-as-you-delete is unnecessary.
  //
  // The block's directory hierarchy is left behind. We could prune it if
  // it's empty, but that's racy and leaving it isn't much overhead.

  return Status::OK();
}

Status FileBlockManager::CloseBlocks(const vector<WritableBlock*>& blocks) {
  VLOG(3) << "Closing " << blocks.size() << " blocks";
  if (FLAGS_block_coalesce_close) {
    // Ask the kernel to begin writing out each block's dirty data. This is
    // done up-front to give the kernel opportunities to coalesce contiguous
    // dirty pages.
    BOOST_FOREACH(WritableBlock* block, blocks) {
      RETURN_NOT_OK(block->FlushDataAsync());
    }
  }

  // Now close each block, waiting for each to become durable.
  BOOST_FOREACH(WritableBlock* block, blocks) {
    RETURN_NOT_OK(block->Close());
  }
  return Status::OK();
}

} // namespace fs
} // namespace kudu
