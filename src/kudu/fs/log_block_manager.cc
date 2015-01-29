// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/log_block_manager.h"

#include <boost/foreach.hpp>

#include "kudu/fs/block_id-inl.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/mutex.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"

// TODO: How should this be configured? Should provide some guidance.
DEFINE_uint64(log_container_max_size, 10LU * 1024 * 1024 * 1024,
              "Maximum size (soft) of a log container");

DECLARE_bool(enable_data_block_fsync);

using std::tr1::unordered_set;
using strings::Substitute;
using kudu::fs::internal::LogBlock;
using kudu::fs::internal::LogBlockContainer;
using kudu::pb_util::ReadablePBContainerFile;
using kudu::pb_util::WritablePBContainerFile;

namespace kudu {

namespace fs {

namespace internal {

////////////////////////////////////////////////////////////
// LogBlockContainer
////////////////////////////////////////////////////////////

// A single block container belonging to the log-backed block manager.
//
// A container may only be used to write one WritableBlock at a given time.
// However, existing blocks may be deleted concurrently. As such, almost
// all container functions must be reentrant, even if the container itself
// is logically thread unsafe (i.e. multiple clients calling AppendData()
// concurrently will produce nonsensical container data). Thread unsafe
// functions are marked explicitly.
class LogBlockContainer {
 public:
  static const std::string kMetadataFileSuffix;
  static const std::string kDataFileSuffix;
  static const char* kMagic;

  // Creates a new block container in 'dir'.
  static Status Create(LogBlockManager* block_manager,
                       const std::string& dir,
                       gscoped_ptr<LogBlockContainer>* container);

  // Opens an existing block container in 'dir'.
  //
  // Every container is comprised of two files: "<dir>/<id>.data" and
  // "<dir>/<id>.metadata". Together, 'dir' and 'id' fully describe both files.
  static Status Open(LogBlockManager* block_manager,
                     const std::string& dir,
                     const std::string& id,
                     gscoped_ptr<LogBlockContainer>* container);

  // Indicates that the writing of 'block' is finished. If successful,
  // adds the block to the block manager's in-memory maps.
  //
  // Returns a status that is either the same as 's' (if !s.ok()) or
  // potentially different (if s.ok() and FinishBlock() failed).
  //
  // After returning, this container has been released to the block manager
  // and may no longer be used in the context of writing 'block'.
  Status FinishBlock(const Status& s, WritableBlock* block);

  // Frees the space associated with a block at 'offset' and 'length'. This
  // is a physical operation, not a logical one; a separate AppendMetadata()
  // is required to record the deletion in container metadata.
  //
  // The on-disk effects of this call are made durable only after SyncData().
  Status DeleteBlock(int64_t offset, int64_t length);

  // Appends 'data' to this container's data file.
  //
  // The on-disk effects of this call are made durable only after SyncData().
  Status AppendData(const Slice& data);

  // Appends 'pb' to this container's metadata file.
  //
  // The on-disk effects of this call are made durable only after SyncMetadata().
  Status AppendMetadata(const BlockRecordPB& pb);

  // Asynchronously flush this container's data file from 'offset' through
  // to 'length'.
  //
  // Does not guarantee data durability; use SyncData() for that.
  Status FlushData(int64_t offset, int64_t length);

  // Asynchronously flush this container's metadata file (all dirty bits).
  //
  // Does not guarantee metadata durability; use SyncMetadata() for that.
  //
  // TODO: Add support to just flush a range.
  Status FlushMetadata();

  // Synchronize this container's data file with the disk. On success,
  // guarantees that the data is made durable.
  //
  // TODO: Add support to synchronize just a range.
  Status SyncData();

  // Synchronize this container's metadata file with the disk. On success,
  // guarantees that the metadata is made durable.
  //
  // TODO: Add support to synchronize just a range.
  Status SyncMetadata();

  // Updates 'total_bytes_written_', marking this container as full if
  // needed.
  //
  // This function is thread unsafe.
  void UpdateBytesWritten(int64_t more_bytes);

  // Simple accessors.
  std::string dir() const { return DirName(path_); }
  const std::string& ToString() const { return path_; }
  LogBlockManager* block_manager() const { return block_manager_; }
  RandomAccessFile* data_reader() const { return data_reader_.get(); }
  int64_t total_bytes_written() const { return total_bytes_written_; }
  bool full() const {
    return total_bytes_written_ >=  FLAGS_log_container_max_size;
  }

 private:
  // RAII-style class for finishing containers in FinishBlock().
  class ScopedFinisher {
   public:
    // 'container' must outlive the finisher.
    explicit ScopedFinisher(LogBlockContainer* container) :
      container_(container) {
    }
    ~ScopedFinisher() {
      container_->block_manager()->MakeContainerAvailable(container_);
    }
   private:
    LogBlockContainer* container_;
  };

  LogBlockContainer(LogBlockManager* block_manager,
                    const std::string& path,
                    gscoped_ptr<WritablePBContainerFile> metadata_writer,
                    gscoped_ptr<WritableFile> data_writer,
                    gscoped_ptr<RandomAccessFile> data_reader);

  // Reads the container's metadata from disk, creating LogBlocks as needed
  // and populating the block manager's in-memory maps.
  Status LoadContainer();

  // The owning block manager. Must outlive the container itself.
  LogBlockManager* block_manager_;

  // The path to the container's files. Equivalent to "<dir>/<id>" (see the
  // container constructor).
  const std::string path_;

  // Opened file handles to the container's files.
  //
  // WritableFile is not thread safe so access to each writer must be
  // serialized through a (sleeping) mutex. We use different mutexes to
  // avoid contention in cases where only one writer is needed.
  gscoped_ptr<WritablePBContainerFile> metadata_pb_writer_;
  Mutex metadata_pb_writer_lock_;
  gscoped_ptr<WritableFile> data_writer_;
  Mutex data_writer_lock_;
  gscoped_ptr<RandomAccessFile> data_reader_;

  // The amount of data written thus far in the container.
  int64_t total_bytes_written_;

  DISALLOW_COPY_AND_ASSIGN(LogBlockContainer);
};

const std::string LogBlockContainer::kMetadataFileSuffix(".metadata");
const std::string LogBlockContainer::kDataFileSuffix(".data");

LogBlockContainer::LogBlockContainer(LogBlockManager* block_manager,
                                     const string& path,
                                     gscoped_ptr<WritablePBContainerFile> metadata_writer,
                                     gscoped_ptr<WritableFile> data_writer,
                                     gscoped_ptr<RandomAccessFile> data_reader)
  : block_manager_(block_manager),
    path_(path),
    metadata_pb_writer_(metadata_writer.Pass()),
    data_writer_(data_writer.Pass()),
    data_reader_(data_reader.Pass()),
    total_bytes_written_(0) {
}

Status LogBlockContainer::Create(LogBlockManager* block_manager,
                                 const string& dir,
                                 gscoped_ptr<LogBlockContainer>* container) {
  string common_path;
  string metadata_path;
  string data_path;
  Status metadata_status;
  Status data_status;
  gscoped_ptr<WritableFile> metadata_writer;
  gscoped_ptr<WritableFile> data_writer;
  WritableFileOptions wr_opts;
  wr_opts.mode = WritableFileOptions::CREATE_NON_EXISTING;

  // When running on XFS and using PosixMmapFile for data files, the reader
  // threads in block_manager-stress-test sometimes read garbage out of the
  // container. It's some kind of interaction between mmap-based writing
  // and the fallocate(2) performed during block deletion.
  //
  // TODO: until we figure it out, we cannot safely use mmap-based writes.
  // See KUDU-596 for more details.
  wr_opts.mmap_file = false;

  // Repeat in the event of a container id collision (unlikely).
  //
  // When looping, we delete any created-and-orphaned files.
  do {
    if (metadata_writer) {
      block_manager->env()->DeleteFile(metadata_path);
    }
    common_path = JoinPathSegments(dir, block_manager->oid_generator()->Next());
    metadata_path = StrCat(common_path, kMetadataFileSuffix);
    metadata_status = block_manager->env()->NewWritableFile(wr_opts,
                                                            metadata_path,
                                                            &metadata_writer);
    if (data_writer) {
      block_manager->env()->DeleteFile(data_path);
    }
    data_path = StrCat(common_path, kDataFileSuffix);
    data_status = block_manager->env()->NewWritableFile(wr_opts,
                                                        data_path,
                                                        &data_writer);
  } while (PREDICT_FALSE(metadata_status.IsAlreadyPresent() ||
                         data_status.IsAlreadyPresent()));
  if (metadata_status.ok() && data_status.ok()) {
    gscoped_ptr<WritablePBContainerFile> metadata_pb_writer(
        new WritablePBContainerFile(metadata_writer.Pass()));
    RETURN_NOT_OK(metadata_pb_writer->Init(BlockRecordPB()));

    // The data file may increase in size as more blocks as written, so we
    // cannot use mmapped-files (which assume that the file is immutable).
    gscoped_ptr<RandomAccessFile> reader;
    RandomAccessFileOptions rd_opts;
    rd_opts.mmap_file = false;
    RETURN_NOT_OK(block_manager->env()->NewRandomAccessFile(rd_opts, data_path, &reader));
    container->reset(new LogBlockContainer(block_manager, common_path,
                                           metadata_pb_writer.Pass(),
                                           data_writer.Pass(),
                                           reader.Pass()));
    VLOG(1) << "Created log block container " << (*container)->ToString();
  }

  // Prefer metadata status (arbitrarily).
  return !metadata_status.ok() ? metadata_status : data_status;
}

Status LogBlockContainer::Open(LogBlockManager* block_manager,
                               const string& dir, const string& id,
                               gscoped_ptr<LogBlockContainer>* container) {
  string common_path = JoinPathSegments(dir, id);

  // Open the existing metadata and data files for writing.
  //
  // The comment in Create() explains why we're not using mmap-based writes.
  string metadata_path = StrCat(common_path, kMetadataFileSuffix);
  gscoped_ptr<WritableFile> metadata_writer;
  WritableFileOptions wr_opts;
  wr_opts.mode = WritableFileOptions::OPEN_EXISTING;
  wr_opts.mmap_file = false;
  RETURN_NOT_OK(block_manager->env()->NewWritableFile(wr_opts,
                                                      metadata_path,
                                                      &metadata_writer));
  gscoped_ptr<WritablePBContainerFile> metadata_pb_writer(
      new WritablePBContainerFile(metadata_writer.Pass()));
  // No call to metadata_pb_writer->Init() because we're reopening an
  // existing pb container (that should already have a valid header).

  string data_path = StrCat(common_path, kDataFileSuffix);
  gscoped_ptr<WritableFile> data_writer;
  RETURN_NOT_OK(block_manager->env()->NewWritableFile(wr_opts,
                                                      data_path,
                                                      &data_writer));
  int64_t existing_data_size = data_writer->Size();

  // Open the existing data file for reading.
  //
  // The comment in Create() explains why we're not using mmap-based reads.
  gscoped_ptr<RandomAccessFile> reader;
  RandomAccessFileOptions rd_opts;
  rd_opts.mmap_file = false;
  RETURN_NOT_OK(block_manager->env()->NewRandomAccessFile(rd_opts, data_path, &reader));

  // Create the in-memory container and populate it.
  gscoped_ptr<LogBlockContainer> open_container(new LogBlockContainer(block_manager,
                                                                      common_path,
                                                                      metadata_pb_writer.Pass(),
                                                                      data_writer.Pass(),
                                                                      reader.Pass()));
  open_container->UpdateBytesWritten(existing_data_size);
  RETURN_NOT_OK(open_container->LoadContainer());

  VLOG(1) << "Opened log block container " << open_container->ToString();
  container->reset(open_container.release());
  return Status::OK();
}

Status LogBlockContainer::LoadContainer() {
  string metadata_path = StrCat(path_, kMetadataFileSuffix);
  gscoped_ptr<RandomAccessFile> metadata_reader;
  RETURN_NOT_OK(block_manager()->env()->NewRandomAccessFile(metadata_path, &metadata_reader));
  ReadablePBContainerFile pb_reader(metadata_reader.Pass());
  RETURN_NOT_OK(pb_reader.Init());
  Status read_status;
  while (true) {
    BlockRecordPB record;
    read_status = pb_reader.ReadNextPB(&record);
    if (!read_status.ok()) {
      break;
    }
    block_manager()->ProcessBlockRecord(this, record);
  }
  Status close_status = pb_reader.Close();
  return !read_status.IsEndOfFile() ? read_status : close_status;
}


Status LogBlockContainer::FinishBlock(const Status& s, WritableBlock* block) {
  ScopedFinisher finisher(this);
  if (!s.ok()) {
    // Early return; 'finisher' makes the container available again.
    return s;
  }

  // A failure when syncing the container means the container (and its new
  // blocks) may be missing the next time the on-disk state is reloaded.
  //
  // As such, it's not correct to add the block to in-memory state unless
  // synchronization succeeds. In the worst case, this means the data file
  // will have written some garbage that can be expunged during a GC.
  RETURN_NOT_OK(block_manager()->SyncContainer(*this));

  CHECK(block_manager()->AddLogBlock(this, block->id(),
                                     total_bytes_written_, block->BytesAppended()));
  UpdateBytesWritten(block->BytesAppended());
  return Status::OK();
}

Status LogBlockContainer::DeleteBlock(int64_t offset, int64_t length) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);

  // It is invalid to punch a zero-size hole.
  if (length) {
    lock_guard<Mutex> l(&data_writer_lock_);
    return data_writer_->PunchHole(offset, length);
  }
  return Status::OK();
}

Status LogBlockContainer::AppendData(const Slice& data) {
  lock_guard<Mutex> l(&data_writer_lock_);
  return data_writer_->Append(data);
}

Status LogBlockContainer::AppendMetadata(const BlockRecordPB& pb) {
  lock_guard<Mutex> l(&metadata_pb_writer_lock_);
  return metadata_pb_writer_->Append(pb);
}

Status LogBlockContainer::FlushData(int64_t offset, int64_t length) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);

  lock_guard<Mutex> l(&data_writer_lock_);
  return data_writer_->FlushRange(WritableFile::FLUSH_ASYNC, offset, length);
}

Status LogBlockContainer::FlushMetadata() {
  lock_guard<Mutex> l(&metadata_pb_writer_lock_);
  return metadata_pb_writer_->Flush();
}

Status LogBlockContainer::SyncData() {
  lock_guard<Mutex> l(&data_writer_lock_);
  return data_writer_->Sync();
}

Status LogBlockContainer::SyncMetadata() {
  lock_guard<Mutex> l(&metadata_pb_writer_lock_);
  return metadata_pb_writer_->Sync();
}

void LogBlockContainer::UpdateBytesWritten(int64_t more_bytes) {
  DCHECK_GE(more_bytes, 0);

  total_bytes_written_ += more_bytes;
  if (full()) {
    VLOG(1) << "Container " << ToString() << " with size "
            << total_bytes_written_ << " is now full, max size is "
            << FLAGS_log_container_max_size;
  }
}

////////////////////////////////////////////////////////////
// LogBlock
////////////////////////////////////////////////////////////

// The persistent metadata that describes a logical block.
//
// A block grows a LogBlock when its data has been synchronized with
// the disk. That's when it's fully immutable (i.e. none of its metadata
// can change), and when it becomes readable and persistent.
//
// LogBlocks are reference counted to simplify support for deletion with
// outstanding readers. All refcount increments are performed with the
// block manager lock held, as are deletion-based decrements. However,
// no lock is held when ~LogReadableBlock decrements the refcount, thus it
// must be made thread safe (by extending RefCountedThreadSafe instead of
// the simpler RefCounted).
class LogBlock : public RefCountedThreadSafe<LogBlock> {
 public:
  LogBlock(LogBlockContainer* container, const BlockId& block_id,
           int64_t offset, int64_t length);
  ~LogBlock();

  const BlockId& block_id() const { return block_id_; }
  LogBlockContainer* container() const { return container_; }
  int64_t offset() const { return offset_; }
  int64_t length() const { return length_; }

  // Delete the block. Actual deletion takes place when the
  // block is destructed.
  void Delete();

 private:
  // The owning container. Must outlive the LogBlock.
  LogBlockContainer* container_;

  // The block identifier.
  const BlockId block_id_;

  // The block's offset in the container.
  const int64_t offset_;

  // The block's length.
  const int64_t length_;

  // Whether the block has been marked for deletion.
  bool deleted_;

  DISALLOW_COPY_AND_ASSIGN(LogBlock);
};

LogBlock::LogBlock(LogBlockContainer* container,
                   const BlockId& block_id,
                   int64_t offset, int64_t length)
  : container_(container),
    block_id_(block_id),
    offset_(offset),
    length_(length),
    deleted_(false) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);
}

LogBlock::~LogBlock() {
  if (deleted_) {
    // We don't call SyncData() to synchronize the deletion because it's
    // expensive, and in the worst case, we'll just leave orphaned data
    // behind to be cleaned up in the next GC.
    VLOG(3) << "Freeing space belonging to block " << block_id_;
    WARN_NOT_OK(container_->DeleteBlock(offset_, length_),
                Substitute("Could not delete block $0", block_id_.ToString()));
  }
}

void LogBlock::Delete() {
  DCHECK(!deleted_);
  deleted_ = true;
}

////////////////////////////////////////////////////////////
// LogWritableBlock
////////////////////////////////////////////////////////////

// A log-backed block that has been opened for writing.
//
// There's no reference to a LogBlock as this block has yet to be
// persisted.
class LogWritableBlock : public WritableBlock {
 public:
  enum SyncMode {
    SYNC,
    NO_SYNC
  };

  LogWritableBlock(LogBlockContainer* container, const BlockId& block_id,
                   int64_t block_offset);

  virtual ~LogWritableBlock();

  virtual Status Close() OVERRIDE;

  virtual Status Abort() OVERRIDE;

  virtual const BlockId& id() const OVERRIDE;

  virtual BlockManager* block_manager() const OVERRIDE;

  virtual Status Append(const Slice& data) OVERRIDE;

  virtual Status FlushDataAsync() OVERRIDE;

  virtual size_t BytesAppended() const OVERRIDE;

  virtual State state() const OVERRIDE;

  // Actually close the block, possibly synchronizing its dirty data and
  // metadata to disk.
  Status DoClose(SyncMode mode);

  // Write this block's metadata to disk.
  //
  // Does not synchronize the written data; that takes place in Close().
  Status AppendMetadata();

 private:

  // RAII-style class for finishing writable blocks in DoClose().
  class ScopedFinisher {
   public:
    // Both 'block' and 's' must outlive the finisher.
    ScopedFinisher(LogWritableBlock* block, Status* s) :
      block_(block),
      status_(s) {
    }
    ~ScopedFinisher() {
      block_->state_ = CLOSED;
      *status_ = block_->container_->FinishBlock(*status_, block_);
    }
   private:
    LogWritableBlock* block_;
    Status* status_;
  };

  // The owning container. Must outlive the block.
  LogBlockContainer* container_;

  // The block's identifier.
  const BlockId block_id_;

  // The block's offset within the container. Known from the moment the
  // block is created.
  const int64_t block_offset_;

  // The block's length. Changes with each Append().
  int64_t block_length_;

  // The state of the block describing where it is in the write lifecycle,
  // for example, has it been synchronized to disk?
  WritableBlock::State state_;

  DISALLOW_COPY_AND_ASSIGN(LogWritableBlock);
};

LogWritableBlock::LogWritableBlock(LogBlockContainer* container,
                                   const BlockId& block_id,
                                   int64_t block_offset)
  : container_(container),
    block_id_(block_id),
    block_offset_(block_offset),
    block_length_(0),
    state_(CLEAN) {
  DCHECK_GE(block_offset, 0);
}

LogWritableBlock::~LogWritableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  block_id_.ToString()));
}

Status LogWritableBlock::Close() {
  return DoClose(SYNC);
}

Status LogWritableBlock::Abort() {
  RETURN_NOT_OK(DoClose(NO_SYNC));

  // DoClose() has unlocked the container; it may be locked by someone else.
  // But block_manager_ is immutable, so this is safe.
  return container_->block_manager()->DeleteBlock(block_id_);
}

const BlockId& LogWritableBlock::id() const {
  return block_id_;
}

BlockManager* LogWritableBlock::block_manager() const {
  return container_->block_manager();
}

Status LogWritableBlock::Append(const Slice& data) {
  DCHECK(state_ == CLEAN || state_ == DIRTY)
      << "Invalid state: " << state_;

  // The metadata change is deferred to Close() or FlushDataAsync(),
  // whichever comes first. We can't do it now because the block's
  // length is still in flux.
  RETURN_NOT_OK(container_->AppendData(data));

  block_length_ += data.size();
  state_ = DIRTY;
  return Status::OK();
}

Status LogWritableBlock::FlushDataAsync() {
  DCHECK(state_ == CLEAN || state_ == DIRTY || state_ == FLUSHING)
      << "Invalid state: " << state_;
  if (state_ == DIRTY) {
    VLOG(3) << "Flushing block " << id();
    RETURN_NOT_OK(container_->FlushData(block_offset_, block_length_));

    RETURN_NOT_OK(AppendMetadata());

    // TODO: Flush just the range we care about.
    RETURN_NOT_OK(container_->FlushMetadata());
  }

  state_ = FLUSHING;
  return Status::OK();
}

size_t LogWritableBlock::BytesAppended() const {
  return block_length_;
}


WritableBlock::State LogWritableBlock::state() const {
  return state_;
}

Status LogWritableBlock::DoClose(SyncMode mode) {
  if (state_ == CLOSED) {
    return Status::OK();
  }

  // Tracks the first failure (if any).
  //
  // It's important that any subsequent failures mutate 's' before
  // returning. Otherwise 'finisher' won't properly provide the first
  // failure to LogBlockContainer::FinishBlock().
  //
  // Note also that when 'finisher' goes out of scope it may mutate 's'.
  Status s;
  {
    ScopedFinisher finisher(this, &s);

    // FlushDataAsync() was not called; append the metadata now.
    if (state_ == CLEAN || state_ == DIRTY) {
      s = AppendMetadata();
      RETURN_NOT_OK(s);
    }

    if (mode == SYNC &&
        (state_ == CLEAN || state_ == DIRTY || state_ == FLUSHING) &&
        FLAGS_enable_data_block_fsync) {
      VLOG(3) << "Syncing block " << id();

      // TODO: Sync just this block's dirty data.
      s = container_->SyncData();
      RETURN_NOT_OK(s);

      // TODO: Sync just this block's dirty metadata.
      s = container_->SyncMetadata();
      RETURN_NOT_OK(s);
    }
  }

  return s;
}

Status LogWritableBlock::AppendMetadata() {
  BlockRecordPB record;
  block_id_.CopyToPB(record.mutable_block_id());
  record.set_op_type(CREATE);
  record.set_offset(block_offset_);
  record.set_length(block_length_);
  return container_->AppendMetadata(record);
}

////////////////////////////////////////////////////////////
// LogReadableBlock
////////////////////////////////////////////////////////////

// A log-backed block that has been opened for reading.
//
// Refers to a LogBlock representing the block's persisted metadata.
class LogReadableBlock : public ReadableBlock {
 public:
  LogReadableBlock(LogBlockContainer* container,
                   const scoped_refptr<LogBlock>& log_block);

  virtual ~LogReadableBlock();

  virtual Status Close() OVERRIDE;

  virtual const BlockId& id() const OVERRIDE;

  virtual Status Size(size_t* sz) const OVERRIDE;

  virtual Status Read(uint64_t offset, size_t length,
                      Slice* result, uint8_t* scratch) const OVERRIDE;

 private:

  // The owning container. Must outlive this block.
  LogBlockContainer* container_;

  // A reference to this block's metadata.
  scoped_refptr<internal::LogBlock> log_block_;

  // Whether the block has been closed.
  bool closed_;

  DISALLOW_COPY_AND_ASSIGN(LogReadableBlock);
};

LogReadableBlock::LogReadableBlock(LogBlockContainer* container,
                                   const scoped_refptr<LogBlock>& log_block)
  : container_(container),
    log_block_(log_block),
    closed_(false) {
}

LogReadableBlock::~LogReadableBlock() {
  Close();
}

Status LogReadableBlock::Close() {
  if (closed_) {
    return Status::OK();
  }

  closed_ = true;
  return Status::OK();
}

const BlockId& LogReadableBlock::id() const {
  return log_block_->block_id();
}

Status LogReadableBlock::Size(size_t* sz) const {
  DCHECK(!closed_);

  *sz = log_block_->length();
  return Status::OK();
}

Status LogReadableBlock::Read(uint64_t offset, size_t length,
                              Slice* result, uint8_t* scratch) const {
  DCHECK(!closed_);

  uint64_t read_offset = log_block_->offset() + offset;
  if (log_block_->length() < offset + length) {
    return Status::IOError("Out-of-bounds read",
                           Substitute("read of [$0-$1) in block [$2-$3)",
                                      read_offset,
                                      read_offset + length,
                                      log_block_->offset(),
                                      log_block_->offset() + log_block_->length()));
  }
  return env_util::ReadFully(container_->data_reader(), read_offset, length,
                             result, scratch);
}

} // namespace internal

////////////////////////////////////////////////////////////
// LogBlockManager
////////////////////////////////////////////////////////////

LogBlockManager::LogBlockManager(Env* env, const string& root_path)
  : env_(env),
    root_path_(root_path) {
}

LogBlockManager::~LogBlockManager() {
  STLDeleteElements(&all_containers_);
}

Status LogBlockManager::Create() {
    return env_->CreateDir(root_path_);
}

Status LogBlockManager::Open() {
  if (!env_->FileExists(root_path_)) {
    return Status::NotFound(Substitute("LogBlockManager at $0 not found",
                                       root_path_));
  }

  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(root_path_, &children));

  BOOST_FOREACH(const string& child, children) {
    string id;
    if (!TryStripSuffixString(child, LogBlockContainer::kMetadataFileSuffix, &id)) {
      continue;
    }
    gscoped_ptr<LogBlockContainer> container;
    RETURN_NOT_OK(LogBlockContainer::Open(this, root_path_, id, &container));
    {
      lock_guard<simple_spinlock> l(&lock_);
      AddNewContainerUnlocked(container.get());
      MakeContainerAvailableUnlocked(container.release());
    }
  }
  return Status::OK();
}


Status LogBlockManager::CreateBlock(const CreateBlockOptions& opts,
                                    gscoped_ptr<WritableBlock>* block) {
  // Find a free container. If one cannot be found, create a new one.
  //
  // TODO: should we cap the number of outstanding containers and force
  // callers to block if we've reached it?
  LogBlockContainer* container = GetAvailableContainer();
  if (!container) {
    gscoped_ptr<LogBlockContainer> new_container;
    RETURN_NOT_OK(LogBlockContainer::Create(this, root_path_, &new_container));
    container = new_container.release();
    {
      lock_guard<simple_spinlock> l(&lock_);
      dirty_dirs_.insert(root_path_);
      AddNewContainerUnlocked(container);
    }
  }

  // Generate a free block ID.
  BlockId new_block_id;
  do {
    new_block_id.SetId(oid_generator()->Next());
  } while (!TryUseBlockId(new_block_id));

  block->reset(new internal::LogWritableBlock(container,
                                              new_block_id,
                                              container->total_bytes_written()));
  VLOG(3) << "Created block " << (*block)->id() << " in container "
          << container->ToString();
  return Status::OK();
}

Status LogBlockManager::CreateBlock(gscoped_ptr<WritableBlock>* block) {
  return CreateBlock(CreateBlockOptions(), block);
}

Status LogBlockManager::OpenBlock(const BlockId& block_id,
                                  gscoped_ptr<ReadableBlock>* block) {
  scoped_refptr<LogBlock> lb;
  {
    lock_guard<simple_spinlock> l(&lock_);
    lb = FindPtrOrNull(blocks_by_block_id_, block_id);
  }
  if (!lb) {
    return Status::NotFound("Can't find block", block_id.ToString());
  }

  block->reset(new internal::LogReadableBlock(lb->container(),
                                              lb.get()));
  VLOG(3) << "Opened block " << (*block)->id()
          << " from container " << lb->container()->ToString();
  return Status::OK();
}

Status LogBlockManager::DeleteBlock(const BlockId& block_id) {
  scoped_refptr<LogBlock> lb(RemoveLogBlock(block_id));
  if (!lb) {
    return Status::NotFound("Can't find block", block_id.ToString());
  }
  VLOG(3) << "Deleting block " << block_id;
  lb->Delete();

  // Record the on-disk deletion.
  //
  // TODO: what if this fails? Should we restore the in-memory block?
  BlockRecordPB record;
  block_id.CopyToPB(record.mutable_block_id());
  record.set_op_type(DELETE);
  RETURN_NOT_OK(lb->container()->AppendMetadata(record));
  if (FLAGS_enable_data_block_fsync) {
    RETURN_NOT_OK(lb->container()->SyncMetadata());
  }

  return Status::OK();
}

Status LogBlockManager::CloseBlocks(const std::vector<WritableBlock*>& blocks) {
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

void LogBlockManager::AddNewContainerUnlocked(LogBlockContainer* container) {
  DCHECK(lock_.is_locked());
  all_containers_.push_back(container);
}

LogBlockContainer* LogBlockManager::GetAvailableContainer() {
  LogBlockContainer* container = NULL;
  lock_guard<simple_spinlock> l(&lock_);
  if (!available_containers_.empty()) {
    container = available_containers_.front();
    available_containers_.pop_front();
  }
  return container;
}

void LogBlockManager::MakeContainerAvailable(LogBlockContainer* container) {
  lock_guard<simple_spinlock> l(&lock_);
  MakeContainerAvailableUnlocked(container);
}

void LogBlockManager::MakeContainerAvailableUnlocked(LogBlockContainer* container) {
  DCHECK(lock_.is_locked());
  if (container->full()) {
    return;
  }
  available_containers_.push_back(container);
}

Status LogBlockManager::SyncContainer(const LogBlockContainer& container) {
  Status s;
  bool to_sync = false;
  {
    lock_guard<simple_spinlock> l(&lock_);
    to_sync = dirty_dirs_.erase(container.dir());
  }

  if (to_sync) {
    s = env_->SyncDir(container.dir());

    // If SyncDir fails, the container directory must be restored to
    // dirty_dirs_. Otherwise a future successful LogWritableBlock::Close()
    // on this container won't call SyncDir again, and the container might
    // be lost on crash.
    //
    // In the worst case (another block synced this container as we did),
    // we'll sync it again needlessly.
    if (!s.ok()) {
      lock_guard<simple_spinlock> l(&lock_);
      dirty_dirs_.insert(container.dir());
    }
  }
  return s;
}

void LogBlockManager::ProcessBlockRecord(LogBlockContainer* container,
                                         const BlockRecordPB& record) {
  BlockId block_id(BlockId::FromPB(record.block_id()));
  switch (record.op_type()) {
    case CREATE: {
      if (!record.has_offset() ||
          !record.has_length() ||
          record.offset() < 0  ||
          record.length() < 0  ||
          record.offset() + record.length() > container->total_bytes_written()) {
        LOG(FATAL) << "Found malformed block record: "
                   << record.DebugString();
        break;
      }
      if (!AddLogBlock(container, block_id,
                       record.offset(), record.length())) {
        LOG(FATAL) << "Found already existent block record: "
                   << record.DebugString();
        break;
      }
      VLOG(2) << Substitute("Found CREATE block $0 at offset $1 with length $2",
                            block_id.ToString(),
                            record.offset(), record.length());
      break;
    }
    case DELETE:
      if (!RemoveLogBlock(block_id)) {
        LOG(FATAL) << "Found already non-existent block record: "
                   << record.DebugString();
        break;
      }
      VLOG(2) << Substitute("Found DELETE block $0", block_id.ToString());
      break;
    default:
      LOG(FATAL) << "Found unknown op type in block record: "
                 << record.DebugString();
      break;
  }
}

bool LogBlockManager::TryUseBlockId(const BlockId& block_id) {
  lock_guard<simple_spinlock> l(&lock_);
  if (ContainsKey(blocks_by_block_id_, block_id)) {
    return false;
  }
  return InsertIfNotPresent(&open_block_ids_, block_id);
}

bool LogBlockManager::AddLogBlock(LogBlockContainer* container, const BlockId& block_id,
                                  int64_t offset, int64_t length) {
  scoped_refptr<LogBlock> lb(new LogBlock(container, block_id, offset, length));
  lock_guard<simple_spinlock> l(&lock_);
  if (!InsertIfNotPresent(&blocks_by_block_id_, block_id, lb)) {
    return false;
  }

  // There may already be an entry in open_block_ids_ (e.g. we just
  // finished writing out a block).
  open_block_ids_.erase(block_id);
  return true;
}

scoped_refptr<LogBlock> LogBlockManager::RemoveLogBlock(const BlockId& block_id) {
  lock_guard<simple_spinlock> l(&lock_);
  return EraseKeyReturnValuePtr(&blocks_by_block_id_, block_id);
}

} // namespace fs
} // namespace kudu
