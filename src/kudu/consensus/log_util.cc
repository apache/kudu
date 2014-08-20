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

const char kLogSegmentMagicString[] = "kudulogf";

// Header is prefixed with the magic (8 bytes) and the header length (4 bytes).
const size_t kLogSegmentMagicAndHeaderLength = 12;

// Nulls the length of kLogSegmentMagicAndHeaderLength.
// This is used to check the case where we have a nonzero-length empty log file.
const char kLogSegmentNullHeader[] =
           { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 };

const size_t kEntryLengthSize = 4;

const int kLogMajorVersion = 1;
const int kLogMinorVersion = 0;

// Maximum log segment header size, in bytes (8 MB).
const uint32_t kLogSegmentMaxHeaderSize = 8 * 1024 * 1024;

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
  uint64_t file_size;
  RETURN_NOT_OK_PREPEND(env->GetFileSize(path, &file_size), "Unable to read file size");
  shared_ptr<RandomAccessFile> readable_file;
  RETURN_NOT_OK_PREPEND(env_util::OpenFileForRandom(env, path, &readable_file),
                        "Unable to open file for reading");

  segment->reset(new ReadableLogSegment(path, file_size, readable_file));
  RETURN_NOT_OK_PREPEND((*segment)->Init(), "Unable to initialize segment");
  return Status::OK();
}

ReadableLogSegment::ReadableLogSegment(const std::string &path,
                                       uint64_t file_size,
                                       const shared_ptr<RandomAccessFile>& readable_file)
  : path_(path),
    file_size_(file_size),
    readable_file_(readable_file),
    is_initialized_(false) {
}

void ReadableLogSegment::Init(const LogSegmentHeaderPB& header, uint64_t first_entry_offset) {
  DCHECK(!IsInitialized()) << "Can only call Init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";
  header_.CopyFrom(header);
  first_entry_offset_ = first_entry_offset;
  is_initialized_ = true;
}

Status ReadableLogSegment::Init() {
  DCHECK(!IsInitialized()) << "Can only call Init() once";

  // Check the size of the file.
  // If it is zero, return Status::OK() early.
  uint64_t file_size = 0;
  RETURN_NOT_OK_PREPEND(readable_file_->Size(&file_size), "Unable to read file size");
  if (file_size == 0) {
    VLOG(1) << "Log segment file $0 is zero-length: " << path();
    return Status::OK();
  }

  uint32_t header_size = 0;
  RETURN_NOT_OK(ReadMagicAndHeaderLength(&header_size));
  if (header_size == 0) {
    // If a log file has been pre-allocated but not initialized, then
    // 'header_size' will be 0 even the file size is > 0; in this
    // case, 'is_initialized_' remains set to false and return
    // Status::OK() early. LogReader ignores segments where
    // IsInitialized() returns false.
    return Status::OK();
  }

  if (header_size > kLogSegmentMaxHeaderSize) {
    return Status::Corruption(
        Substitute("File is corrupted. "
                   "Parsed header size: $0 is zero or bigger than max header size: $1",
                   header_size, kLogSegmentMaxHeaderSize));
  }

  uint8_t header_space[header_size];
  Slice header_slice;
  LogSegmentHeaderPB header;

  // Read and parse the log segment header.
  RETURN_NOT_OK_PREPEND(ReadFully(readable_file_.get(), kLogSegmentMagicAndHeaderLength,
                                  header_size, &header_slice, header_space),
                        "Unable to read fully");

  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&header,
                                                header_slice.data(),
                                                header_size),
                        "Unable to parse protobuf");

  header_.CopyFrom(header);
  first_entry_offset_ = header_size + kLogSegmentMagicAndHeaderLength;
  is_initialized_ = true;

  return Status::OK();
}

Status ReadableLogSegment::ReadEntries(vector<LogEntryPB*>* entries) {
  vector<int64_t> recent_offsets(4, -1);
  int batches_read = 0;

  uint64_t offset = first_entry_offset();
  VLOG(1) << "Reading segment entries offset: " << offset << " file size: "
          << file_size();
  faststring tmp_buf;
  while (offset < file_size()) {
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
      }
      current_batch->mutable_entry()->ExtractSubrange(0,
                                                      current_batch->entry_size(),
                                                      NULL);
    } else {
      string err = "Log file corrupted. ";
      SubstituteAndAppend(&err, "Failed trying to read batch #$0 at offset $1. ",
                          batches_read, this_batch_offset);
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
  return Status::OK();
}

Status ReadableLogSegment::ReadEntryLength(uint64_t *offset, uint32_t *len) {
  uint8_t scratch[kEntryLengthSize];
  Slice slice;
  RETURN_NOT_OK(ReadFully(readable_file().get(), *offset, kEntryLengthSize,
                          &slice, scratch));
  RETURN_NOT_OK(slice.check_size(kEntryLengthSize));
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
  RETURN_NOT_OK(readable_file()->Read(*offset,
                                      length,
                                      &entry_batch_slice,
                                      tmp_buf->data()));

  gscoped_ptr<LogEntryBatchPB> read_entry_batch(new LogEntryBatchPB());
  RETURN_NOT_OK(pb_util::ParseFromArray(read_entry_batch.get(),
                                        entry_batch_slice.data(),
                                        length));
  *offset += length;
  entry_batch->reset(read_entry_batch.release());
  return Status::OK();
}

Status ReadableLogSegment::ReadMagicAndHeaderLength(uint32_t *len) {
  uint8_t scratch[kLogSegmentMagicAndHeaderLength];
  Slice slice;
  RETURN_NOT_OK(ReadFully(readable_file_.get(), 0, kLogSegmentMagicAndHeaderLength,
                          &slice, scratch));
  RETURN_NOT_OK(ParseMagicAndLength(slice, len));
  return Status::OK();
}

Status ReadableLogSegment::ParseMagicAndLength(const Slice &data, uint32_t *parsed_len) {
  RETURN_NOT_OK_PREPEND(data.check_size(kLogSegmentMagicAndHeaderLength),
                        "Log segment file is too small to contain initial magic number");

  if (memcmp(kLogSegmentMagicString, data.data(), strlen(kLogSegmentMagicString)) != 0) {
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

  *parsed_len = DecodeFixed32(data.data() + strlen(kLogSegmentMagicString));
  return Status::OK();
}


WritableLogSegment::WritableLogSegment(
    const string &path,
    const shared_ptr<WritableFile>& writable_file)
: path_(path),
  writable_file_(writable_file),
  is_header_written_(false) {
}

Status WritableLogSegment::WriteHeader(const LogSegmentHeaderPB& new_header) {
  DCHECK(!IsHeaderWritten()) << "Can only call WriteHeader() once";
  DCHECK(new_header.IsInitialized())
      << "Log segment header must be initialized" << new_header.InitializationErrorString();
  faststring buf;

  // First the magic.
  buf.append(kLogSegmentMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, new_header.ByteSize());
  // Then Serialize the PB.
  if (!pb_util::AppendToString(new_header, &buf)) {
    return Status::Corruption("unable to encode header");
  }
  RETURN_NOT_OK(writable_file()->Append(Slice(buf)));

  header_.CopyFrom(new_header);
  first_entry_offset_ = buf.size();
  is_header_written_ = true;

  return Status::OK();
}

size_t FindStaleSegmentsPrefixSize(const ReadableLogSegmentMap& segment_map,
                                   const consensus::OpId& earliest_needed_opid,
                                   OpIdRange* initial_op_id_range) {
  DCHECK(initial_op_id_range);
  // We iterate in reverse order.
  // Keep the 1st log segment with initial OpId less than or equal to the
  // earliest needed OpId, and delete all the log segments preceding it
  // (preceding meaning in natural order).
  size_t num_stale_segments = 0;
  bool seen_earlier_opid = false;
  BOOST_REVERSE_FOREACH(const ReadableLogSegmentMap::value_type& entry, segment_map) {
    const OpId& initial_op_id_in_segment = entry.first;
    const scoped_refptr<ReadableLogSegment>& segment = entry.second;
    if (consensus::OpIdLessThan(initial_op_id_in_segment, earliest_needed_opid) ||
        consensus::OpIdEquals(initial_op_id_in_segment, earliest_needed_opid)) {
      if (!seen_earlier_opid) {
        // earliest_needed_opid may be in the middle of this segment, do not
        // delete it (but earlier ones can go).
        seen_earlier_opid = true;
        initial_op_id_range->second = initial_op_id_in_segment;
        initial_op_id_range->first = initial_op_id_in_segment;
      } else {
        // All the earlier logs can go.
        num_stale_segments++;
        initial_op_id_range->first = initial_op_id_in_segment;
      }
    } else {
      CHECK(!seen_earlier_opid)
          << Substitute("Greater OpId found in previous log segment, segments"
                        " out of order! current: %s in %s, earliest needed: %s",
                        segment->header().initial_id().ShortDebugString(),
                        segment->path(),
                        earliest_needed_opid.ShortDebugString());
    }
  }

  return num_stale_segments;
}

void CreateBatchFromAllocatedOperations(const consensus::OperationPB* const* ops,
                                        int num_ops,
                                        gscoped_ptr<LogEntryBatchPB>* batch) {
  gscoped_ptr<LogEntryBatchPB> entry_batch(new LogEntryBatchPB);
  entry_batch->mutable_entry()->Reserve(num_ops);
  for (size_t i = 0; i < num_ops; i++) {
    // We want to re-use the existing objects here, so const-casting allows
    // us to put a reference in the new PB.
    consensus::OperationPB* op = const_cast<consensus::OperationPB*>(ops[i]);
    LogEntryPB* entry_pb = entry_batch->add_entry();
    entry_pb->set_type(log::OPERATION);
    entry_pb->set_allocated_operation(op);
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
