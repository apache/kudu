// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log.h"


#include "consensus/log_reader.h"
#include "gutil/strings/substitute.h"
#include "server/fsmanager.h"
#include "util/coding.h"
#include "util/env_util.h"
#include "util/pb_util.h"
#include "util/stopwatch.h"

namespace kudu {
namespace log {

using consensus::OpId;

Status Log::Open(const LogOptions &options,
                 FsManager *fs_manager,
                 const metadata::TabletSuperBlockPB& super_block,
                 const consensus::OpId& current_id,
                 gscoped_ptr<Log> *log) {

  gscoped_ptr<LogSegmentHeader> header(new LogSegmentHeader);
  header->set_major_version(kLogMajorVersion);
  header->set_minor_version(kLogMinorVersion);
  header->mutable_tablet_meta()->CopyFrom(super_block);
  header->mutable_initial_id()->CopyFrom(current_id);

  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(fs_manager->GetWalsRootDir()));

  string tablet_wal_path = fs_manager->env()->JoinPathSegments(
      fs_manager->GetWalsRootDir(),
      super_block.oid());

  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(tablet_wal_path));

  gscoped_ptr<Log> new_log(new Log(options,
                                   fs_manager,
                                   tablet_wal_path,
                                   header.Pass()));
  RETURN_NOT_OK(new_log->Init());
  log->reset(new_log.release());
  return Status::OK();
}

Log::Log(const LogOptions &options,
         FsManager *fs_manager,
         const string& log_path,
         gscoped_ptr<LogSegmentHeader> header)
: options_(options),
  fs_manager_(fs_manager),
  log_dir_(log_path),
  next_segment_header_(header.Pass()),
  header_size_(0),
  max_segment_size_(options_.segment_size_mb * 1024 * 1024),
  state_(kLogInitialized) {
}

Status Log::Init() {
  CHECK_EQ(state_, kLogInitialized);

  RETURN_NOT_OK(LogReader::Open(fs_manager_,
                                next_segment_header_->tablet_meta().oid(),
                                &reader_));

  // the case where we are continuing an existing log
  if (reader_->size() != 0) {
    previous_ = reader_->segments();
    VLOG(1) << "Using existing " << previous_.size()
            << " segments from path: " << fs_manager_->GetWalsRootDir();
  }

  // we always create a new segment when the log starts.
  RETURN_NOT_OK(CreateNewSegment(*next_segment_header_.get()));
  state_ = kLogWriting;
  return Status::OK();
}

Status Log::RollOver() {
  CHECK_EQ(state_, kLogWriting);
  RETURN_NOT_OK(current_->writable_file()->Close());
  RETURN_NOT_OK(CreateNewSegment(*next_segment_header_.get()));
  return Status::OK();
}

Status Log::Append(const LogEntry& entry) {
  CHECK_EQ(state_, kLogWriting);

  // update the current header
  switch (entry.type()) {
    case REPLICATE: {
      next_segment_header_->mutable_initial_id()->CopyFrom(entry.msg().id());
      break;
    }
    case COMMIT: {
      next_segment_header_->mutable_initial_id()->CopyFrom(entry.commit().id());
      break;
    }
    case TABLET_METADATA: {
      next_segment_header_->mutable_tablet_meta()->CopyFrom(entry.tablet_meta());
      break;
    }
    default: {
      LOG(FATAL) << "Unexpected log entry type: " << entry.DebugString();
    }
  }

  uint32_t entry_size = entry.ByteSize();

  // if the size of this entry overflows the current segment, get a new one
  if ((current_->writable_file()->Size() + entry_size + 4) > max_segment_size_) {
    RETURN_NOT_OK(RollOver());
    LOG(INFO) << "Max segment size reached. Rolled over to new segment: "
              << current_->path();
  }

  RETURN_NOT_OK(current_->writable_file()->Append(Slice(reinterpret_cast<uint8_t *>(&entry_size), 4)));
  if (!pb_util::SerializeToWritableFile(entry, current_->writable_file().get())) {
    return Status::Corruption("Unable  to serialize entry to file");
  }

  if (options_.force_fsync_all) {
    RETURN_NOT_OK(current_->writable_file()->Sync());
  }
  return Status::OK();
}

Status Log::GC() {
  LOG(INFO) << "Running Log GC on " << log_dir_;
  LOG_TIMING(INFO, "Log GC") {
    uint32_t num_stale_segments = 0;
    RETURN_NOT_OK(FindStaleSegmentsPrefixSize(previous_,
                                              next_segment_header_->tablet_meta(),
                                              &num_stale_segments));
    if (num_stale_segments > 0) {
      LOG(INFO) << "Found " << num_stale_segments << " stale segments.";
      // delete the files and erase the segments from previous_
      for (int i = 0; i < num_stale_segments; i++) {
        LOG(INFO) << "Deleting Log file in path: " << previous_[i]->path();
        RETURN_NOT_OK(fs_manager_->env()->DeleteFile(previous_[i]->path()));
      }
      previous_.erase(previous_.begin(),
                      previous_.begin() + num_stale_segments);
    }
  }
  return Status::OK();
}

Status Log::Close() {
  if (state_ == kLogWriting) {
    RETURN_NOT_OK(current_->writable_file()->Close());
    state_ = kLogClosed;
    VLOG(1) << "Log Closed()";
    return Status::OK();
  }
  if (state_ == kLogClosed) {
    VLOG(1) << "Log already Closed()";
    return Status::OK();
  }
  return Status::IllegalState(strings::Substitute("Bad state for Close() $0", state_));
}

Status Log::CreateNewSegment(const LogSegmentHeader& header) {
  // create a new segment file and pre-allocate the space
  shared_ptr<WritableFile> sink;
  string fqp = CreateSegmentFileName(header.initial_id());

  LOG(INFO) << "Creating new segment: " << fqp;
  RETURN_NOT_OK(env_util::OpenFileForWrite(fs_manager_->env(), fqp, &sink));
  RETURN_NOT_OK(sink->PreAllocate(max_segment_size_));

  // create a new segment
  gscoped_ptr<WritableLogSegment> new_segment(new WritableLogSegment(header, fqp, sink));
  // ... and write and sync the header
  uint32_t pb_size = new_segment->header().ByteSize();
  faststring buf;
  // First the magic.
  buf.append(kMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, pb_size);
  if (!pb_util::AppendToString(new_segment->header(), &buf)) {
    return Status::Corruption("unable to encode header");
  }

  RETURN_NOT_OK(new_segment->writable_file()->Append(Slice(buf)));
  uint32_t new_header_size_ = kMagicAndHeaderLength + pb_size;
  RETURN_NOT_OK(new_segment->writable_file()->Sync());

  // transform the current segment into a readable one (we might need to replay
  // stuff for other peers)
  if (current_.get() != NULL) {
    shared_ptr<RandomAccessFile> source;
    RETURN_NOT_OK(env_util::OpenFileForRandom(fs_manager_->env(), current_->path(), &source));
    shared_ptr<ReadableLogSegment> readable_segment(
        new ReadableLogSegment(current_->header(),
                               current_->path(),
                               header_size_,
                               current_->writable_file()->Size(),
                               source));
    previous_.push_back(readable_segment);
  }

  // now set 'current_' to the new segment
  header_size_ = new_header_size_;
  current_.reset(new_segment.release());

  return Status::OK();
}

string Log::CreateSegmentFileName(const OpId &id) {
  return fs_manager_->env()->
      JoinPathSegments(log_dir_, strings::Substitute("$0-$1-$2",
                                                     kLogPrefix,
                                                     id.term(),
                                                     id.index()));
}

Log::~Log() {
}

}  // namespace log
}  // namespace kudu

