// Copyright (c) 2014, Cloudera, inc.

#include "kudu/fs/file_block_manager.h"

#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "kudu/fs/block_id-inl.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace fs {

////////////////////////////////////////////////////////////
// FileWritableBlock
////////////////////////////////////////////////////////////

FileWritableBlock::FileWritableBlock(FileBlockManager* block_manager,
                                     bool sync_on_close,
                                     const BlockId& block_id,
                                     const shared_ptr<WritableFile>& writer) :
  block_manager_(block_manager),
  sync_on_close_(sync_on_close),
  block_id_(block_id),
  writer_(writer),
  closed_(false),
  bytes_appended_(0) {
}

FileWritableBlock::~FileWritableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  block_id_.ToString()));
}

Status FileWritableBlock::Close() {
  if (closed_) {
    return Status::OK();
  }

  Status sync;
  if (sync_on_close_) {
    sync = Sync();
    WARN_NOT_OK(sync, Substitute("Failed to sync when closing block $0",
                                 block_id_.ToString()));
  }
  Status close = writer_->Close();

  closed_ = true;
  writer_.reset();

  // Prefer the result of Close() to that of Sync().
  return !close.ok() ? close : sync;
}

const BlockId& FileWritableBlock::id() const {
  return block_id_;
}

Status FileWritableBlock::Append(const Slice& data) {
  DCHECK(!closed_);

  RETURN_NOT_OK(writer_->Append(data));
  bytes_appended_ += data.size();
  return Status::OK();
}

Status FileWritableBlock::Sync() {
  DCHECK(!closed_);

  // Safer to synchronize data first, then metadata.
  RETURN_NOT_OK(writer_->Sync());
  return block_manager_->SyncMetadata(block_id_);
}

size_t FileWritableBlock::BytesAppended() const {
  return bytes_appended_;
}

////////////////////////////////////////////////////////////
// FileReadableBlock
////////////////////////////////////////////////////////////

FileReadableBlock::FileReadableBlock(const BlockId& block_id,
                                     const shared_ptr<RandomAccessFile>& reader) :
  block_id_(block_id),
  reader_(reader),
  closed_(false) {
}

FileReadableBlock::~FileReadableBlock() {
  Close();
}

Status FileReadableBlock::Close() {
  if (closed_) {
    return Status::OK();
  }

  closed_ = true;
  reader_.reset();
  return Status::OK();
}

const BlockId& FileReadableBlock::id() const {
  return block_id_;
}

Status FileReadableBlock::Size(size_t* sz) const {
  DCHECK(!closed_);

  return reader_->Size(sz);
}

Status FileReadableBlock::Read(uint64_t offset, size_t length,
                               Slice* result, uint8_t* scratch) const {
  DCHECK(!closed_);

  return env_util::ReadFully(reader_.get(), offset, length, result, scratch);
}

////////////////////////////////////////////////////////////
// FileBlockManager
////////////////////////////////////////////////////////////

Status FileBlockManager::CreateBlockDir(const BlockId& block_id, vector<string>* created_dirs) {
  CHECK(!block_id.IsNull());
  DCHECK(env_->FileExists(root_path_));

  bool path0_created;
  string path0 = JoinPathSegments(root_path_, block_id.hash0());
  RETURN_NOT_OK(CreateDirIfMissing(path0, &path0_created));

  bool path1_created;
  string path1 = JoinPathSegments(path0, block_id.hash1());
  RETURN_NOT_OK(CreateDirIfMissing(path1, &path1_created));

  bool path2_created;
  string path2 = JoinPathSegments(path1, block_id.hash2());
  RETURN_NOT_OK(CreateDirIfMissing(path2, &path2_created));

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

string FileBlockManager::GetBlockPath(const BlockId& block_id) const {
  CHECK(!block_id.IsNull());
  string path = root_path_;
  path = JoinPathSegments(path, block_id.hash0());
  path = JoinPathSegments(path, block_id.hash1());
  path = JoinPathSegments(path, block_id.hash2());
  path = JoinPathSegments(path, block_id.ToString());
  return path;
}

Status FileBlockManager::CreateDirIfMissing(const string& path, bool* created) {
  Status s = env_->CreateDir(path);
  if (created) {
    if (s.ok()) {
      *created = true;
    } else if (s.IsAlreadyPresent()) {
      *created = false;
    }
  }
  return s.IsAlreadyPresent() ? Status::OK() : s;
}

Status FileBlockManager::SyncMetadata(const BlockId& block_id) {
  CHECK(!block_id.IsNull());

  string path0 = JoinPathSegments(root_path_, block_id.hash0());
  string path1 = JoinPathSegments(path0, block_id.hash1());
  string path2 = JoinPathSegments(path1, block_id.hash2());

  // Figure out what directories to sync. Order is important.
  vector<string> to_sync;
  {
    lock_guard<simple_spinlock> l(&lock_);
    if (dirty_dirs_.erase(path2)) {
      to_sync.push_back(path2);
    }
    if (dirty_dirs_.erase(path1)) {
      to_sync.push_back(path1);
    }
    if (dirty_dirs_.erase(path0)) {
      to_sync.push_back(path0);
    }
    if (dirty_dirs_.erase(root_path_)) {
      to_sync.push_back(root_path_);
    }
  }

  // Sync them.
  BOOST_FOREACH(const string& s, to_sync) {
    RETURN_NOT_OK(env_->SyncDir(s));
  }
  return Status::OK();
}

void FileBlockManager::CreateBlock(const BlockId& block_id,
                                   const string& path,
                                   const vector<string>& created_dirs,
                                   const shared_ptr<WritableFile>& writer,
                                   const CreateBlockOptions& opts,
                                   gscoped_ptr<WritableBlock>* block) {
  VLOG(1) << "Creating new block " << block_id.ToString() << " at " << path;

  // Update dirty_dirs_ with those provided as well as the block's
  // directory, which may not have been created but is definitely dirty
  // (because we added a file to it).
  lock_guard<simple_spinlock> l(&lock_);
  BOOST_FOREACH(const string& created, created_dirs) {
    dirty_dirs_.insert(created);
  }
  dirty_dirs_.insert(DirName(path));

  block->reset(new FileWritableBlock(this, opts.sync_on_close, block_id, writer));
}

Status FileBlockManager::Create(Env* env, const string& root_path,
                                gscoped_ptr<BlockManager>* block_manager) {
  gscoped_ptr<FileBlockManager> bm(new FileBlockManager(env, root_path));
  RETURN_NOT_OK(bm->CreateDirIfMissing(root_path));
  block_manager->reset(bm.release());
  return Status::OK();
}

FileBlockManager::FileBlockManager(Env* env,
                                   const string& root_path) :
  env_(env),
  root_path_(root_path) {
}

FileBlockManager::~FileBlockManager() {
}

Status FileBlockManager::CreateAnonymousBlock(gscoped_ptr<WritableBlock>* block,
                                              CreateBlockOptions opts) {
  string path;
  vector<string> created_dirs;
  Status s;
  BlockId block_id;
  shared_ptr<WritableFile> writer;

  // Repeat in case of block id collisions (unlikely).
  do {
    created_dirs.clear();
    block_id.SetId(oid_generator_.Next());
    RETURN_NOT_OK(CreateBlockDir(block_id, &created_dirs));
    path = GetBlockPath(block_id);
    WritableFileOptions wr_opts;
    wr_opts.mmap_file = false;
    wr_opts.overwrite_existing = false;
    s = env_util::OpenFileForWrite(wr_opts, env_, path, &writer);
  } while (PREDICT_FALSE(s.IsAlreadyPresent()));
  if (s.ok()) {
    CreateBlock(block_id, path, created_dirs, writer, opts, block);
  }
  return s;
}

Status FileBlockManager::CreateNamedBlock(const BlockId& block_id,
                                          gscoped_ptr<WritableBlock>* block,
                                          CreateBlockOptions opts) {
  string path = GetBlockPath(block_id);
  VLOG(1) << "Creating new block with predetermined id "
          << block_id.ToString() << " at " << path;

  vector<string> created_dirs;
  RETURN_NOT_OK(CreateBlockDir(block_id, &created_dirs));
  shared_ptr<WritableFile> writer;
  WritableFileOptions wr_opts;
  wr_opts.mmap_file = false;
  wr_opts.overwrite_existing = false;
  RETURN_NOT_OK(env_util::OpenFileForWrite(wr_opts, env_, path, &writer));
  CreateBlock(block_id, path, created_dirs, writer, opts, block);
  return Status::OK();
}

Status FileBlockManager::OpenBlock(const BlockId& block_id,
                                   gscoped_ptr<ReadableBlock>* block) {
  string path = GetBlockPath(block_id);
  VLOG(1) << "Opening block with id " << block_id.ToString() << " at " << path;

  shared_ptr<RandomAccessFile> reader;
  RETURN_NOT_OK(env_util::OpenFileForRandom(env_, path, &reader));
  block->reset(new FileReadableBlock(block_id, reader));
  return Status::OK();
}

Status FileBlockManager::DeleteBlock(const BlockId& block_id) {
  string path = GetBlockPath(block_id);
  RETURN_NOT_OK(env_->DeleteFile(path));
  WARN_NOT_OK(env_->SyncDir(DirName(path)),
              "Failed to sync parent directory when deleting block");

  // The block's directory hierarchy is left behind. We could prune it if
  // it's empty, but that's racy and leaving it isn't much overhead.

  return Status::OK();
}

} // namespace fs
} // namespace kudu
