// Copyright (c) 2013, Cloudera, inc.

#include "kudu/consensus/log_util.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <iostream>
#include <limits>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/coding.h"
#include "kudu/util/env_util.h"
#include "kudu/util/pb_util.h"

DEFINE_int32(log_segment_size_mb, 64,
             "The default segment size for log roll-overs, in MB");

DEFINE_bool(log_force_fsync_all, false,
            "Whether the Log/WAL should explicitly call fsync() after each write.");

DEFINE_bool(log_preallocate_segments, true,
            "Whether the WAL should preallocate the entire segment before writing to it");

DEFINE_bool(log_async_preallocate_segments, true,
            "Whether the WAL segments preallocation should happen asynchronously");

namespace kudu {
namespace log {

using consensus::OpId;
using env_util::ReadFully;
using std::vector;
using std::tr1::shared_ptr;
using strings::Substitute;
using strings::SubstituteAndAppend;

const char kTmpSuffix[] = ".tmp";

const char kLogSegmentHeaderMagicString[] = "kudulogf";

// A magic that is written as the very last thing when a segment is closed.
// Segments that were not closed (usually the last one being written) will not
// have this magic.
const char kLogSegmentFooterMagicString[] = "closedls";

// Header is prefixed with the header magic (8 bytes) and the header length (4 bytes).
const size_t kLogSegmentHeaderMagicAndHeaderLength = 12;

// Footer is suffixed with the footer magic (8 bytes) and the footer length (4 bytes).
const size_t kLogSegmentFooterMagicAndFooterLength  = 12;

// Nulls the length of kLogSegmentMagicAndHeaderLength.
// This is used to check the case where we have a nonzero-length empty log file.
const char kLogSegmentNullHeader[] =
           { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 };

const size_t kEntryLengthSize = 4;

const int kLogMajorVersion = 1;
const int kLogMinorVersion = 0;

// Maximum log segment header/footer size, in bytes (8 MB).
const uint32_t kLogSegmentMaxHeaderOrFooterSize = 8 * 1024 * 1024;

LogOptions::LogOptions()
: segment_size_mb(FLAGS_log_segment_size_mb),
  force_fsync_all(FLAGS_log_force_fsync_all),
  preallocate_segments(FLAGS_log_preallocate_segments),
  async_preallocate_segments(FLAGS_log_async_preallocate_segments) {
}

Status ReadableLogSegment::Open(Env* env,
                                const string& path,
                                scoped_refptr<ReadableLogSegment>* segment) {
  VLOG(1) << "Parsing wal segment: " << path;
  shared_ptr<RandomAccessFile> readable_file;
  RETURN_NOT_OK_PREPEND(env_util::OpenFileForRandom(env, path, &readable_file),
                        "Unable to open file for reading");

  segment->reset(new ReadableLogSegment(path, readable_file));
  RETURN_NOT_OK_PREPEND((*segment)->Init(), "Unable to initialize segment");
  return Status::OK();
}

ReadableLogSegment::ReadableLogSegment(const std::string &path,
                                       const shared_ptr<RandomAccessFile>& readable_file)
  : path_(path),
    readable_file_(readable_file),
    is_initialized_(false),
    is_corrupted_(false) {
}

Status ReadableLogSegment::Init(const LogSegmentHeaderPB& header,
                                const LogSegmentFooterPB& footer,
                                uint64_t first_entry_offset) {
  DCHECK(!IsInitialized()) << "Can only call Init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";
  DCHECK(footer.IsInitialized()) << "Log segment footer must be initialized";

  RETURN_NOT_OK(ReadFileSize());

  header_.CopyFrom(header);
  footer_.CopyFrom(footer);
  first_entry_offset_ = first_entry_offset;
  is_initialized_ = true;

  return Status::OK();
}

Status ReadableLogSegment::Init(const LogSegmentHeaderPB& header,
                                uint64_t first_entry_offset) {
  DCHECK(!IsInitialized()) << "Can only call Init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";

  RETURN_NOT_OK(ReadFileSize());

  header_.CopyFrom(header);
  first_entry_offset_ = first_entry_offset;
  is_initialized_ = true;

  return Status::OK();
}

Status ReadableLogSegment::Init() {
  DCHECK(!IsInitialized()) << "Can only call Init() once";

  RETURN_NOT_OK(ReadFileSize());

  RETURN_NOT_OK(ReadHeader());

  Status s = ReadFooter();
  if (!s.ok()) {
    LOG(WARNING) << "Could not read footer for segment: " << path_
        << ". Status: " << s.ToString();
  }

  is_initialized_ = true;

  return Status::OK();
}

Status ReadableLogSegment::RebuildFooterByScanning() {
  DCHECK(!footer_.IsInitialized());
  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  Status s = ReadEntries(&entries);

  footer_.set_num_entries(entries.size());

  // Rebuild the index, right now we're just keeping the first entry.
  BOOST_FOREACH(const LogEntryPB* entry, entries) {
    if (entry->has_replicate()) {
      SegmentIdxPosPB* idx_pos = footer_.add_idx_entry();
      idx_pos->mutable_id()->CopyFrom(entry->replicate().id());
      break;
    }
  }

  // If we couldn't read the entries either (maybe some were
  // corrupted) log that in WARNING, but return OK.
  if (!s.ok()) {
    LOG(WARNING) << "Segment: " << path_ << " had corrupted entries, "
        "could not fully rebuild index. Only read " << entries.size() << " entries."
        << " Status: " << s.ToString();
    is_corrupted_ = true;
  }
  DCHECK(footer_.IsInitialized());
  DCHECK_EQ(entries.size(), footer_.num_entries());

  LOG(INFO) << "Successfully rebuilt footer for segment: " << path_ << ".";
  return Status::OK();
}

Status ReadableLogSegment::ReadFileSize() {
  // Check the size of the file.
  RETURN_NOT_OK_PREPEND(readable_file_->Size(&file_size_), "Unable to read file size");
  if (file_size_ == 0) {
    VLOG(1) << "Log segment file $0 is zero-length: " << path();
    return Status::OK();
  }
  return Status::OK();
}

Status ReadableLogSegment::ReadHeader() {
  uint32_t header_size;
  RETURN_NOT_OK(ReadHeaderMagicAndHeaderLength(&header_size));
  if (header_size == 0) {
    // If a log file has been pre-allocated but not initialized, then
    // 'header_size' will be 0 even the file size is > 0; in this
    // case, 'is_initialized_' remains set to false and return
    // Status::OK() early. LogReader ignores segments where
    // IsInitialized() returns false.
    return Status::OK();
  }

  if (header_size > kLogSegmentMaxHeaderOrFooterSize) {
    return Status::Corruption(
        Substitute("File is corrupted. "
                   "Parsed header size: $0 is zero or bigger than max header size: $1",
                   header_size, kLogSegmentMaxHeaderOrFooterSize));
  }

  uint8_t header_space[header_size];
  Slice header_slice;
  LogSegmentHeaderPB header;

  // Read and parse the log segment header.
  RETURN_NOT_OK_PREPEND(ReadFully(readable_file_.get(), kLogSegmentHeaderMagicAndHeaderLength,
                                  header_size, &header_slice, header_space),
                        "Unable to read fully");

  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&header,
                                                header_slice.data(),
                                                header_size),
                        "Unable to parse protobuf");

  header_.CopyFrom(header);
  first_entry_offset_ = header_size + kLogSegmentHeaderMagicAndHeaderLength;

  return Status::OK();
}


Status ReadableLogSegment::ReadHeaderMagicAndHeaderLength(uint32_t *len) {
  uint8_t scratch[kLogSegmentHeaderMagicAndHeaderLength];
  Slice slice;
  RETURN_NOT_OK(ReadFully(readable_file_.get(), 0, kLogSegmentHeaderMagicAndHeaderLength,
                          &slice, scratch));
  RETURN_NOT_OK(ParseHeaderMagicAndHeaderLength(slice, len));
  return Status::OK();
}

Status ReadableLogSegment::ParseHeaderMagicAndHeaderLength(const Slice &data,
                                                           uint32_t *parsed_len) {
  RETURN_NOT_OK_PREPEND(data.check_size(kLogSegmentHeaderMagicAndHeaderLength),
                        "Log segment file is too small to contain initial magic number");

  if (memcmp(kLogSegmentHeaderMagicString, data.data(),
             strlen(kLogSegmentHeaderMagicString)) != 0) {
    // As a special case, we check whether the file was allocated but no header
    // was written. We treat that case as an uninitialized file, much in the
    // same way we treat zero-length files.
    // Note: While the above comparison checks 8 bytes, this one checks the full 12
    // to ensure we have a full 12 bytes of NULL data.
    if (memcmp(kLogSegmentNullHeader, data.data(),
               strlen(kLogSegmentNullHeader)) == 0) {
      // 12 bytes of NULLs, good enough for us to consider this a file that
      // was never written to (but apparently preallocated).
      LOG(WARNING) << "Log segment file " << path() << " has 12 initial NULL bytes instead of "
                   << "magic and header length: " << data.ToDebugString()
                   << " and will be treated as a blank segment.";
      *parsed_len = 0;
      return Status::OK();
    }
    // If no magic and not uninitialized, the file is considered corrupt.
    return Status::Corruption(Substitute("Invalid log segment file $0: Bad magic. $1",
                                         path(), data.ToDebugString()));
  }

  *parsed_len = DecodeFixed32(data.data() + strlen(kLogSegmentHeaderMagicString));
  return Status::OK();
}

Status ReadableLogSegment::ReadFooter() {
  uint32_t footer_size;
  RETURN_NOT_OK(ReadFooterMagicAndFooterLength(&footer_size));

  if (footer_size == 0 || footer_size > kLogSegmentMaxHeaderOrFooterSize) {
    return Status::NotFound(
        Substitute("File is corrupted. "
                   "Parsed header size: $0 is zero or bigger than max header size: $1",
                   footer_size, kLogSegmentMaxHeaderOrFooterSize));
  }

  if (footer_size > (file_size_ - first_entry_offset_)) {
    return Status::NotFound("Footer not found. File corrupted. "
        "Decoded footer length pointed at a footer before the first entry.");
  }

  uint8_t footer_space[footer_size];
  Slice footer_slice;

  uint64_t footer_offset = file_size_ - kLogSegmentFooterMagicAndFooterLength - footer_size;

  LogSegmentFooterPB footer;

  // Read and parse the log segment footer.
  RETURN_NOT_OK_PREPEND(ReadFully(readable_file_.get(), footer_offset,
                                  footer_size, &footer_slice, footer_space),
                        "Footer not found. Could not read fully.");

  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&footer,
                                                footer_slice.data(),
                                                footer_size),
                        "Unable to parse protobuf");

  footer_.Swap(&footer);
  return Status::OK();
}

Status ReadableLogSegment::ReadFooterMagicAndFooterLength(uint32_t *len) {
  uint8_t scratch[kLogSegmentFooterMagicAndFooterLength];
  Slice slice;

  CHECK_GT(file_size_, kLogSegmentFooterMagicAndFooterLength);
  RETURN_NOT_OK(ReadFully(readable_file_.get(),
                          file_size_ - kLogSegmentFooterMagicAndFooterLength,
                          kLogSegmentFooterMagicAndFooterLength,
                          &slice,
                          scratch));

  RETURN_NOT_OK(ParseFooterMagicAndFooterLength(slice, len));
  return Status::OK();
}

Status ReadableLogSegment::ParseFooterMagicAndFooterLength(const Slice &data,
                                                           uint32_t *parsed_len) {
  RETURN_NOT_OK_PREPEND(data.check_size(kLogSegmentFooterMagicAndFooterLength),
                        "Slice is too small to contain final magic number");

  if (memcmp(kLogSegmentFooterMagicString, data.data(),
             strlen(kLogSegmentFooterMagicString)) != 0) {
    return Status::NotFound("Footer not found. Footer magic doesn't match");
  }

  *parsed_len = DecodeFixed32(data.data() + strlen(kLogSegmentFooterMagicString));
  return Status::OK();
}

Status ReadableLogSegment::ReadEntries(vector<LogEntryPB*>* entries) {
  vector<int64_t> recent_offsets(4, -1);
  int batches_read = 0;

  // If we don't have a footer, it's likely this is the segment that
  // we're currently writing to. We should refresh the size since it may
  // have grown since last we read it.
  if (!footer_.IsInitialized()) {
    VLOG(1) << "Refreshing file size to read in-progress log segment "
            << path_;
    RETURN_NOT_OK_PREPEND(ReadFileSize(),
                          "Could not refresh file size");
  }

  uint64_t offset = first_entry_offset();
  VLOG(1) << "Reading segment entries from "
          << path_ << ": offset=" << offset << " file_size="
          << file_size();
  faststring tmp_buf;

  // If we have a footer we only read up to it. If we don't we likely crashed
  // and always read to the end.
  uint64_t read_up_to = footer_.IsInitialized() && !is_corrupted_ ?
      file_size() - footer_.ByteSize() - kLogSegmentFooterMagicAndFooterLength :
      file_size();

  int num_entries_read = 0;
  while (offset < read_up_to) {
    const uint64_t this_batch_offset = offset;
    recent_offsets[batches_read++ % recent_offsets.size()] = offset;

    gscoped_ptr<LogEntryBatchPB> current_batch;

    // Read the entry length first, if we get 0 back that just means that
    // the log hasn't been ftruncated().
    uint32_t length;
    Status status = ReadEntryLength(&offset, &length);
    if (status.ok()) {
      if (length == 0) {
        // EOF
        return Status::OK();
      }
      status = ReadEntryBatch(&offset, length, &tmp_buf, &current_batch);
    }

    if (status.ok()) {
      if (VLOG_IS_ON(3)) {
        VLOG(3) << "Read Log entry batch: " << current_batch->DebugString();
      }
      for (size_t i = 0; i < current_batch->entry_size(); ++i) {
        entries->push_back(current_batch->mutable_entry(i));
        num_entries_read++;
      }
      current_batch->mutable_entry()->ExtractSubrange(0,
                                                      current_batch->entry_size(),
                                                      NULL);
    } else {
      string err = "Log file corrupted. ";
      SubstituteAndAppend(&err, "Failed trying to read batch #$0 at offset $1 for segment at $2. ",
                          batches_read, this_batch_offset, path_);
      err.append("Prior batch offsets:");
      std::sort(recent_offsets.begin(), recent_offsets.end());
      BOOST_FOREACH(int64_t offset, recent_offsets) {
        if (offset >= 0) {
          SubstituteAndAppend(&err, " $0", offset);
        }
      }

      RETURN_NOT_OK_PREPEND(status, err);
    }
  }

  if (footer_.num_entries() != num_entries_read) {
    return Status::Corruption(
      Substitute("Read $0 log entries from $1, but expected $2 based on the footeR",
                 num_entries_read, path_, footer_.num_entries()));
  }

  return Status::OK();
}

Status ReadableLogSegment::ReadEntryLength(uint64_t *offset, uint32_t *len) {
  uint8_t scratch[kEntryLengthSize];
  Slice slice;
  Status s = ReadFully(readable_file().get(), *offset, kEntryLengthSize,
                       &slice, scratch);
  if (!s.ok()) return Status::Corruption(Substitute("Could not read entry length. Cause: $0",
                                                    s.ToString()));
  *offset += kEntryLengthSize;
  *len = DecodeFixed32(slice.data());
  return Status::OK();
}

Status ReadableLogSegment::ReadEntryBatch(uint64_t *offset,
                                          uint32_t length,
                                          faststring *tmp_buf,
                                          gscoped_ptr<LogEntryBatchPB> *entry_batch) {

  if (length == 0 || length > file_size() - *offset) {
    return Status::Corruption(StringPrintf("Invalid entry length %d.", length));
  }

  tmp_buf->clear();
  tmp_buf->resize(length);
  Slice entry_batch_slice;

  Status s =  readable_file()->Read(*offset,
                                    length,
                                    &entry_batch_slice,
                                    tmp_buf->data());

  if (!s.ok()) return Status::Corruption(Substitute("Could not read entry. Cause: $0",
                                                    s.ToString()));

  gscoped_ptr<LogEntryBatchPB> read_entry_batch(new LogEntryBatchPB());
  s = pb_util::ParseFromArray(read_entry_batch.get(),
                              entry_batch_slice.data(),
                              length);

  if (!s.ok()) return Status::Corruption(Substitute("Could parse PB. Cause: $0",
                                                    s.ToString()));

  *offset += length;
  entry_batch->reset(read_entry_batch.release());
  return Status::OK();
}


WritableLogSegment::WritableLogSegment(
    const string &path,
    const shared_ptr<WritableFile>& writable_file)
: path_(path),
  writable_file_(writable_file),
  is_header_written_(false),
  is_footer_written_(false),
  written_offset_(0) {
}

Status WritableLogSegment::WriteHeaderAndOpen(const LogSegmentHeaderPB& new_header) {
  DCHECK(!IsHeaderWritten()) << "Can only call WriteHeader() once";
  DCHECK(new_header.IsInitialized())
      << "Log segment header must be initialized" << new_header.InitializationErrorString();
  faststring buf;

  // First the magic.
  buf.append(kLogSegmentHeaderMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, new_header.ByteSize());
  // Then Serialize the PB.
  if (!pb_util::AppendToString(new_header, &buf)) {
    return Status::Corruption("unable to encode header");
  }
  RETURN_NOT_OK(writable_file()->Append(Slice(buf)));

  header_.CopyFrom(new_header);
  first_entry_offset_ = buf.size();
  written_offset_ = first_entry_offset_;
  is_header_written_ = true;

  return Status::OK();
}

Status WritableLogSegment::WriteFooterAndClose(const LogSegmentFooterPB& footer) {
  DCHECK(IsHeaderWritten());
  DCHECK(!IsFooterWritten());
  DCHECK(footer.IsInitialized());

  faststring buf;

  if (!pb_util::AppendToString(footer, &buf)) {
    return Status::Corruption("unable to encode header");
  }

  buf.append(kLogSegmentFooterMagicString);
  PutFixed32(&buf, footer.ByteSize());

  RETURN_NOT_OK_PREPEND(writable_file()->Append(Slice(buf)), "Could not write the footer");

  footer_.CopyFrom(footer);
  is_footer_written_ = true;

  RETURN_NOT_OK(writable_file_->Close());

  written_offset_ += buf.size();

  return Status::OK();
}

void CreateBatchFromAllocatedOperations(const consensus::ReplicateMsg* const* msgs,
                                        int num_msgs,
                                        gscoped_ptr<LogEntryBatchPB>* batch) {
  gscoped_ptr<LogEntryBatchPB> entry_batch(new LogEntryBatchPB);
  entry_batch->mutable_entry()->Reserve(num_msgs);
  for (size_t i = 0; i < num_msgs; i++) {
    // We want to re-use the existing objects here, so const-casting allows
    // us to put a reference in the new PB.
    consensus::ReplicateMsg* msg = const_cast<consensus::ReplicateMsg*>(msgs[i]);
    LogEntryPB* entry_pb = entry_batch->add_entry();
    entry_pb->set_type(log::REPLICATE);
    entry_pb->set_allocated_replicate(msg);
  }
  batch->reset(entry_batch.release());
}

bool IsLogFileName(const string& fname) {
  if (HasPrefixString(fname, ".")) {
    // Hidden file or ./..
    VLOG(1) << "Ignoring hidden file: " << fname;
    return false;
  }

  if (HasSuffixString(fname, kTmpSuffix)) {
    LOG(WARNING) << "Ignoring tmp file: " << fname;
    return false;
  }

  vector<string> v = strings::Split(fname, "-");
  if (v.size() != 2 || v[0] != FsManager::kWalFileNamePrefix) {
    VLOG(1) << "Not a log file: " << fname;
    return false;
  }

  return true;
}

}  // namespace log
}  // namespace kudu
