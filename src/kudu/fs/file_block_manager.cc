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

DEFINE_bool(block_coalesce_sync, true,
            "Coalesce synchronization of data during SyncBlocks()");

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
  return SyncMetadata();
}

size_t FileWritableBlock::BytesAppended() const {
  return bytes_appended_;
}

Status FileWritableBlock::SyncMetadata() {
  DCHECK(!closed_);

  string path2 = DirName(block_manager_->GetBlockPath(id()));
  RETURN_NOT_OK(block_manager_->env()->SyncDir(path2));

  string path1 = DirName(path2);
  RETURN_NOT_OK(block_manager_->env()->SyncDir(path1));

  string path0 = DirName(path1);
  RETURN_NOT_OK(block_manager_->env()->SyncDir(path0));

  string root_path = DirName(path0);
  return block_manager_->env()->SyncDir(root_path);
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

Status FileBlockManager::CreateBlockDir(const BlockId& block_id) {
  CHECK(!block_id.IsNull());
  DCHECK(env_->FileExists(root_path_));

  string path0 = JoinPathSegments(root_path_, block_id.hash0());
  RETURN_NOT_OK(CreateDirIfMissing(path0));

  string path1 = JoinPathSegments(path0, block_id.hash1());
  RETURN_NOT_OK(CreateDirIfMissing(path1));

  string path2 = JoinPathSegments(path1, block_id.hash2());
  RETURN_NOT_OK(CreateDirIfMissing(path2));

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

Status FileBlockManager::CreateDirIfMissing(const string& path) {
  Status s = env_->CreateDir(path);
  return s.IsAlreadyPresent() ? Status::OK() : s;
}

void FileBlockManager::CreateBlock(const BlockId& block_id,
                                   const string& path,
                                   const shared_ptr<WritableFile>& writer,
                                   const CreateBlockOptions& opts,
                                   gscoped_ptr<WritableBlock>* block) {
  VLOG(1) << "Creating new block " << block_id.ToString() << " at " << path;
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
  Status s;
  BlockId block_id;
  shared_ptr<WritableFile> writer;

  // Repeat in case of block id collisions (unlikely).
  do {
    block_id.SetId(oid_generator_.Next());
    RETURN_NOT_OK(CreateBlockDir(block_id));
    path = GetBlockPath(block_id);
    WritableFileOptions wr_opts;
    wr_opts.mmap_file = false;
    wr_opts.overwrite_existing = false;
    s = env_util::OpenFileForWrite(wr_opts, env_, path, &writer);
  } while (PREDICT_FALSE(s.IsAlreadyPresent()));
  if (s.ok()) {
    CreateBlock(block_id, path, writer, opts, block);
  }
  return s;
}

Status FileBlockManager::CreateNamedBlock(const BlockId& block_id,
                                          gscoped_ptr<WritableBlock>* block,
                                          CreateBlockOptions opts) {
  string path = GetBlockPath(block_id);
  VLOG(1) << "Creating new block with predetermined id "
          << block_id.ToString() << " at " << path;

  RETURN_NOT_OK(CreateBlockDir(block_id));
  shared_ptr<WritableFile> writer;
  WritableFileOptions wr_opts;
  wr_opts.mmap_file = false;
  wr_opts.overwrite_existing = false;
  RETURN_NOT_OK(env_util::OpenFileForWrite(wr_opts, env_, path, &writer));
  CreateBlock(block_id, path, writer, opts, block);
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

Status FileBlockManager::SyncBlocks(const vector<WritableBlock*>& blocks) {
  if (FLAGS_block_coalesce_sync) {
    // Ask the kernel to begin writing out each block's dirty data. This is
    // done up-front to give the kernel opportunities to coalesce contiguous
    // dirty pages.
    BOOST_FOREACH(WritableBlock* block, blocks) {
      FileWritableBlock* fwb = down_cast<FileWritableBlock*>(block);
      RETURN_NOT_OK(fwb->writer_->Flush(WritableFile::FLUSH_ASYNC));
    }
  }

  // Now wait for each block to actually become durable.
  BOOST_FOREACH(WritableBlock* block, blocks) {
    RETURN_NOT_OK(block->Sync());
  }
  return Status::OK();
}

} // namespace fs
} // namespace kudu
