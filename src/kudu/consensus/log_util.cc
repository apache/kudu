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

#include "kudu/consensus/log_util.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/array_view.h" // IWYU pragma: keep
#include "kudu/util/coding-inl.h"
#include "kudu/util/coding.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"

DEFINE_int32(log_segment_size_mb, 8,
             "The default size for log segments, in MB");
TAG_FLAG(log_segment_size_mb, advanced);

DEFINE_bool(log_force_fsync_all, false,
            "Whether the Log/WAL should explicitly call fsync() after each write.");
TAG_FLAG(log_force_fsync_all, stable);

DEFINE_bool(log_preallocate_segments, true,
            "Whether the WAL should preallocate the entire segment before writing to it");
TAG_FLAG(log_preallocate_segments, advanced);

DEFINE_bool(log_async_preallocate_segments, true,
            "Whether the WAL segments preallocation should happen asynchronously");
TAG_FLAG(log_async_preallocate_segments, advanced);

DEFINE_double(fault_crash_before_write_log_segment_header, 0.0,
              "Fraction of the time we will crash just before writing the log segment header");
TAG_FLAG(fault_crash_before_write_log_segment_header, unsafe);

using kudu::consensus::OpId;
using std::string;
using std::shared_ptr;
using std::vector;
using std::unique_ptr;
using strings::Substitute;
using strings::SubstituteAndAppend;

namespace kudu {
namespace log {

const char kLogSegmentHeaderMagicString[] = "kudulogf";

// A magic that is written as the very last thing when a segment is closed.
// Segments that were not closed (usually the last one being written) will not
// have this magic.
const char kLogSegmentFooterMagicString[] = "closedls";

// Header is prefixed with the header magic (8 bytes) and the header length (4 bytes).
const size_t kLogSegmentHeaderMagicAndHeaderLength = 12;

// Footer is suffixed with the footer magic (8 bytes) and the footer length (4 bytes).
const size_t kLogSegmentFooterMagicAndFooterLength  = 12;

// Versions of Kudu <= 1.2  used a 12-byte entry header.
const size_t kEntryHeaderSizeV1 = 12;
// Later versions, which added support for compression, use a 16-byte header.
const size_t kEntryHeaderSizeV2 = 16;

// Maximum log segment header/footer size, in bytes (8 MB).
const uint32_t kLogSegmentMaxHeaderOrFooterSize = 8 * 1024 * 1024;

LogOptions::LogOptions()
: segment_size_mb(FLAGS_log_segment_size_mb),
  force_fsync_all(FLAGS_log_force_fsync_all),
  preallocate_segments(FLAGS_log_preallocate_segments),
  async_preallocate_segments(FLAGS_log_async_preallocate_segments) {
}

////////////////////////////////////////////////////////////
// LogEntryReader
////////////////////////////////////////////////////////////

LogEntryReader::LogEntryReader(ReadableLogSegment* seg)
    : seg_(seg),
      num_batches_read_(0),
      num_entries_read_(0),
      offset_(seg_->first_entry_offset()) {

  int64_t readable_to_offset = seg_->readable_to_offset_.Load();

  // If we have a footer we only read up to it. If we don't we likely crashed
  // and always read to the end.
  read_up_to_ = (seg_->footer_.IsInitialized() && !seg_->footer_was_rebuilt_) ?
      seg_->file_size() - seg_->footer_.ByteSize() - kLogSegmentFooterMagicAndFooterLength :
      readable_to_offset;
  VLOG(1) << "Reading segment entries from "
          << seg_->path_ << ": offset=" << offset_ << " file_size="
          << seg_->file_size() << " readable_to_offset=" << readable_to_offset;
}

LogEntryReader::~LogEntryReader() {}

Status LogEntryReader::ReadNextEntry(unique_ptr<LogEntryPB>* entry) {
  // Refill pending_entries_ if none are available.
  while (pending_entries_.empty()) {

    // If we are done reading, check that we got the expected number of entries
    // and return EOF.
    if (offset_ >= read_up_to_) {
      if (seg_->footer_.IsInitialized() && seg_->footer_.num_entries() != num_entries_read_) {
        return Status::Corruption(
            Substitute("Read $0 log entries from $1, but expected $2 based on the footer",
                       num_entries_read_, seg_->path_, seg_->footer_.num_entries()));
      }

      return Status::EndOfFile("Reached end of log");
    }

    // We still expect to have more entries in the log.
    unique_ptr<LogEntryBatchPB> current_batch;

    // Read and validate the entry header first.
    Status s;
    EntryHeaderStatus s_detail = EntryHeaderStatus::OTHER_ERROR;
    if (offset_ + seg_->entry_header_size() < read_up_to_) {
      s = seg_->ReadEntryHeaderAndBatch(&offset_, &tmp_buf_, &current_batch, &s_detail);
    } else {
      s = Status::Corruption(Substitute("Truncated log entry at offset $0", offset_));
    }

    if (PREDICT_FALSE(!s.ok())) {
      return HandleReadError(s, s_detail);
    }

    // Add the entries from this batch to our pending queue.
    for (int i = 0; i < current_batch->entry_size(); i++) {
      auto entry = current_batch->mutable_entry(i);
      pending_entries_.emplace_back(entry);
      num_entries_read_++;

      // Record it in the 'recent entries' deque.
      OpId op_id;
      if (entry->type() == log::REPLICATE && entry->has_replicate()) {
        op_id = entry->replicate().id();
      } else if (entry->has_commit() && entry->commit().has_commited_op_id()) {
        op_id = entry->commit().commited_op_id();
      }
      if (recent_entries_.size() == kNumRecentEntries) {
        recent_entries_.pop_front();
      }
      recent_entries_.push_back({ offset_, entry->type(), op_id });
    }
    current_batch->mutable_entry()->ExtractSubrange(
        0, current_batch->entry_size(), nullptr);
  }

  *entry = std::move(pending_entries_.front());
  pending_entries_.pop_front();
  return Status::OK();
}

Status LogEntryReader::HandleReadError(const Status& s, EntryHeaderStatus status_detail) const {
  if (!s.IsCorruption()) {
    // IO errors should always propagate back
    return s.CloneAndPrepend(Substitute("error reading from log $0", seg_->path_));
  }
  Status corruption_status = MakeCorruptionStatus(s);

  // If we have a valid footer in the segment, then the segment was correctly
  // closed, and we shouldn't see any corruption anywhere (including the last
  // batch).
  if (seg_->HasFooter() && !seg_->footer_was_rebuilt_) {
    LOG(WARNING) << "Found a corruption in a closed log segment: "
                 << corruption_status.ToString();
    return corruption_status;
  }

  // If we read a corrupt entry, but we don't have a footer, then it's
  // possible that we crashed in the middle of writing an entry.
  // In this case, we scan forward to see if there are any more valid looking
  // entries after this one in the file. If there are, it's really a corruption.
  // if not, we just WARN it, since it's OK for the last entry to be partially
  // written.
  bool has_valid_entries;
  RETURN_NOT_OK_PREPEND(seg_->ScanForValidEntryHeaders(offset_ + seg_->entry_header_size(),
                                                       &has_valid_entries),
                        "Scanning forward for valid entries");
  if (has_valid_entries) {
    return corruption_status;
  }

  CHECK(status_detail != EntryHeaderStatus::OK);
  if (status_detail == EntryHeaderStatus::ALL_ZEROS) {
    // In the common case of hitting the end of valid entries, we'll read a header which
    // is all zero bytes, and find no more entries following it. This isn't really a
    // "Corruption" so much as an expected EOF-type condition, so we'll just log
    // at VLOG(1) instead of INFO.
    VLOG(1) << "Reached preallocated space while reading log segment " << seg_->path_;
  } else {
    LOG(INFO) << "Ignoring log segment corruption in " << seg_->path_ << " because "
              << "there are no log entries following the corrupted one. "
              << "The server probably crashed in the middle of writing an entry "
              << "to the write-ahead log or downloaded an active log via tablet copy. "
              << "Error detail: " << corruption_status.ToString();
  }
  return Status::EndOfFile("");
}

Status LogEntryReader::MakeCorruptionStatus(const Status& status) const {

  string err = "Log file corruption detected. ";
  SubstituteAndAppend(&err, "Failed trying to read batch #$0 at offset $1 for log segment $2: ",
                      num_batches_read_, offset_, seg_->path_);
  err.append("Prior entries:");

  for (const auto& r : recent_entries_) {
    if (r.offset >= 0) {
      SubstituteAndAppend(&err, " [off=$0 $1 ($2)]",
                          r.offset, LogEntryTypePB_Name(r.type),
                          OpIdToString(r.op_id));
    }
  }

  return status.CloneAndAppend(err);
}

////////////////////////////////////////////////////////////
// ReadableLogSegment
////////////////////////////////////////////////////////////

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

ReadableLogSegment::ReadableLogSegment(
    std::string path, shared_ptr<RandomAccessFile> readable_file)
    : path_(std::move(path)),
      file_size_(0),
      readable_to_offset_(0),
      readable_file_(std::move(readable_file)),
      codec_(nullptr),
      is_initialized_(false),
      footer_was_rebuilt_(false) {}

Status ReadableLogSegment::Init(const LogSegmentHeaderPB& header,
                                const LogSegmentFooterPB& footer,
                                int64_t first_entry_offset) {
  DCHECK(!IsInitialized()) << "Can only call Init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";
  DCHECK(footer.IsInitialized()) << "Log segment footer must be initialized";

  RETURN_NOT_OK(ReadFileSize());

  header_.CopyFrom(header);
  RETURN_NOT_OK(InitCompressionCodec());

  footer_.CopyFrom(footer);
  first_entry_offset_ = first_entry_offset;
  is_initialized_ = true;
  readable_to_offset_.Store(file_size());

  return Status::OK();
}

Status ReadableLogSegment::Init(const LogSegmentHeaderPB& header,
                                int64_t first_entry_offset) {
  DCHECK(!IsInitialized()) << "Can only call Init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";

  RETURN_NOT_OK(ReadFileSize());

  header_.CopyFrom(header);
  first_entry_offset_ = first_entry_offset;
  RETURN_NOT_OK(InitCompressionCodec());
  is_initialized_ = true;

  // On a new segment, we don't expect any readable entries yet.
  readable_to_offset_.Store(first_entry_offset);

  return Status::OK();
}

Status ReadableLogSegment::Init() {
  DCHECK(!IsInitialized()) << "Can only call Init() once";

  RETURN_NOT_OK(ReadFileSize());

  RETURN_NOT_OK(ReadHeader());
  RETURN_NOT_OK(InitCompressionCodec());

  Status s = ReadFooter();
  if (!s.ok()) {
    if (s.IsNotFound()) {
      VLOG(1) << "Log segment " << path_ << " has no footer. This segment was likely "
              << "being written when the server previously shut down.";
    } else {
      LOG(WARNING) << "Could not read footer for segment: " << path_
          << ": " << s.ToString();
      return s;
    }
  }

  is_initialized_ = true;

  readable_to_offset_.Store(file_size());

  return Status::OK();
}

Status ReadableLogSegment::InitCompressionCodec() {
  // Init the compression codec.
  if (header_.has_compression_codec() && header_.compression_codec() != NO_COMPRESSION) {
    RETURN_NOT_OK_PREPEND(GetCompressionCodec(header_.compression_codec(), &codec_),
                          "could not init compression codec");
  }
  return Status::OK();
}

const int64_t ReadableLogSegment::readable_up_to() const {
  return readable_to_offset_.Load();
}

void ReadableLogSegment::UpdateReadableToOffset(int64_t readable_to_offset) {
  readable_to_offset_.Store(readable_to_offset);
  file_size_.StoreMax(readable_to_offset);
}

Status ReadableLogSegment::RebuildFooterByScanning() {
  TRACE_EVENT1("log", "ReadableLogSegment::RebuildFooterByScanning",
               "path", path_);

  DCHECK(!footer_.IsInitialized());

  LogEntryReader reader(this);

  LogSegmentFooterPB new_footer;
  int num_entries = 0;
  while (true) {
    unique_ptr<LogEntryPB> entry;
    Status s = reader.ReadNextEntry(&entry);
    if (s.IsEndOfFile()) break;
    RETURN_NOT_OK(s);

    DCHECK(entry);
    if (entry->has_replicate()) {
      UpdateFooterForReplicateEntry(*entry, &new_footer);
    }
    num_entries++;
  }

  new_footer.set_num_entries(num_entries);
  footer_ = new_footer;
  DCHECK(footer_.IsInitialized());
  footer_was_rebuilt_ = true;
  readable_to_offset_.Store(reader.offset());

  VLOG(1) << "Successfully rebuilt footer for segment: " << path_
          << " (valid entries through byte offset " << reader.offset() << ")";
  return Status::OK();
}

Status ReadableLogSegment::ReadFileSize() {
  // Check the size of the file.
  // Env uses uint here, even though we generally prefer signed ints to avoid
  // underflow bugs. Use a local to convert.
  uint64_t size;
  RETURN_NOT_OK_PREPEND(readable_file_->Size(&size), "Unable to read file size");
  file_size_.Store(size);
  if (size == 0) {
    VLOG(1) << "Log segment file $0 is zero-length: " << path();
    return Status::OK();
  }
  return Status::OK();
}

Status ReadableLogSegment::ReadHeader() {
  uint32_t header_size;
  RETURN_NOT_OK(ReadHeaderMagicAndHeaderLength(&header_size));

  if (header_size > kLogSegmentMaxHeaderOrFooterSize) {
    return Status::Corruption(
        Substitute("File is corrupted. "
                   "Parsed header size: $0 is zero or bigger than max header size: $1",
                   header_size, kLogSegmentMaxHeaderOrFooterSize));
  }

  uint8_t header_space[header_size];
  Slice header_slice(header_space, header_size);
  LogSegmentHeaderPB header;

  // Read and parse the log segment header.
  RETURN_NOT_OK_PREPEND(readable_file_->Read(kLogSegmentHeaderMagicAndHeaderLength,
                                             header_slice),
                        "Unable to read fully");

  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&header,
                                                header_slice.data(),
                                                header_size),
                        "Unable to parse protobuf");

  if (header.incompatible_features_size() > 0) {
    return Status::NotSupported("log segment uses a feature not supported by this version "
                                "of Kudu");
  }

  header_.Swap(&header);
  first_entry_offset_ = header_size + kLogSegmentHeaderMagicAndHeaderLength;

  return Status::OK();
}


Status ReadableLogSegment::ReadHeaderMagicAndHeaderLength(uint32_t *len) {
  uint8_t scratch[kLogSegmentHeaderMagicAndHeaderLength];
  Slice slice(scratch, kLogSegmentHeaderMagicAndHeaderLength);
  RETURN_NOT_OK(readable_file_->Read(0, slice));
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
    if (IsAllZeros(data)) {
      // 12 bytes of NULLs, good enough for us to consider this a file that
      // was never written to (but apparently preallocated).
      LOG(WARNING) << "Log segment file " << path() << " has 12 initial NULL bytes instead of "
                   << "magic and header length: " << KUDU_REDACT(data.ToDebugString())
                   << " and will be treated as a blank segment.";
      return Status::Uninitialized("log magic and header length are all NULL bytes");
    }
    // If no magic and not uninitialized, the file is considered corrupt.
    return Status::Corruption(Substitute("Invalid log segment file $0: Bad magic. $1",
                                         path(), KUDU_REDACT(data.ToDebugString())));
  }

  *parsed_len = DecodeFixed32(data.data() + strlen(kLogSegmentHeaderMagicString));
  return Status::OK();
}

Status ReadableLogSegment::ReadFooter() {
  uint32_t footer_size = 0;
  RETURN_NOT_OK(ReadFooterMagicAndFooterLength(&footer_size));

  if (footer_size == 0 || footer_size > kLogSegmentMaxHeaderOrFooterSize) {
    return Status::Corruption(
        Substitute("File is corrupted. "
                   "Parsed header size: $0 is zero or bigger than max header size: $1",
                   footer_size, kLogSegmentMaxHeaderOrFooterSize));
  }

  if (footer_size > (file_size() - first_entry_offset_)) {
    return Status::Corruption("Footer not found. File corrupted. "
        "Decoded footer length pointed at a footer before the first entry.");
  }

  uint8_t footer_space[footer_size];
  Slice footer_slice(footer_space, footer_size);

  int64_t footer_offset = file_size() - kLogSegmentFooterMagicAndFooterLength - footer_size;

  LogSegmentFooterPB footer;

  // Read and parse the log segment footer.
  RETURN_NOT_OK_PREPEND(readable_file_->Read(footer_offset, footer_slice),
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
  Slice slice(scratch, kLogSegmentFooterMagicAndFooterLength);

  CHECK_GT(file_size(), kLogSegmentFooterMagicAndFooterLength);
  RETURN_NOT_OK(readable_file_->Read(file_size() - kLogSegmentFooterMagicAndFooterLength,
                                     slice));

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

Status ReadableLogSegment::ReadEntries(LogEntries* entries) {
  TRACE_EVENT1("log", "ReadableLogSegment::ReadEntries",
               "path", path_);
  LogEntryReader reader(this);

  while (true) {
    unique_ptr<LogEntryPB> entry;
    Status s = reader.ReadNextEntry(&entry);
    if (s.IsEndOfFile()) {
      break;
    }
    RETURN_NOT_OK(s);
    DCHECK(entry);
    entries->emplace_back(std::move(entry));
  }

  return Status::OK();
}

size_t ReadableLogSegment::entry_header_size() const {
  DCHECK(is_initialized_);
  return header_.has_deprecated_major_version() ? kEntryHeaderSizeV1 : kEntryHeaderSizeV2;
}

Status ReadableLogSegment::ScanForValidEntryHeaders(int64_t offset, bool* has_valid_entries) {
  TRACE_EVENT1("log", "ReadableLogSegment::ScanForValidEntryHeaders",
               "path", path_);
  VLOG(1) << "Scanning " << path_ << " for valid entry headers "
          << "following offset " << offset << "...";
  *has_valid_entries = false;

  constexpr auto kChunkSize = 1024 * 1024;
  unique_ptr<uint8_t[]> buf(new uint8_t[kChunkSize]);

  // We overlap the reads by the size of the header, so that if a header
  // spans chunks, we don't miss it.
  for (;
       offset < file_size() - entry_header_size();
       offset += kChunkSize - entry_header_size()) {
    int rem = std::min<int64_t>(file_size() - offset, kChunkSize);
    Slice chunk(buf.get(), rem);
    RETURN_NOT_OK(readable_file()->Read(offset, chunk));

    // Optimization for the case where a chunk is all zeros -- this is common in the
    // case of pre-allocated files. This avoids a lot of redundant CRC calculation.
    if (IsAllZeros(chunk)) {
      continue;
    }

    // Check if this chunk has a valid entry header.
    for (int off_in_chunk = 0;
         off_in_chunk < chunk.size() - entry_header_size();
         off_in_chunk++) {
      Slice potential_header = Slice(&chunk[off_in_chunk], entry_header_size());

      EntryHeader header;
      if (DecodeEntryHeader(potential_header, &header) == EntryHeaderStatus::OK) {
        VLOG(1) << "Found a valid entry header at offset " << (offset + off_in_chunk);
        *has_valid_entries = true;
        return Status::OK();
      }
    }
  }

  VLOG(1) << "Found no log entry headers";
  return Status::OK();
}

Status ReadableLogSegment::ReadEntryHeaderAndBatch(int64_t* offset, faststring* tmp_buf,
                                                   unique_ptr<LogEntryBatchPB>* batch,
                                                   EntryHeaderStatus* status_detail) {
  int64_t cur_offset = *offset;
  EntryHeader header;
  RETURN_NOT_OK(ReadEntryHeader(&cur_offset, &header, status_detail));
  Status s = ReadEntryBatch(&cur_offset, header, tmp_buf, batch);
  if (PREDICT_FALSE(!s.ok())) {
    // If we failed to actually decode the batch, make sure to set status_detail to
    // non-OK.
    *status_detail = EntryHeaderStatus::OTHER_ERROR;
    return s;
  }
  *offset = cur_offset;
  return Status::OK();
}

Status ReadableLogSegment::ReadEntryHeader(int64_t *offset, EntryHeader* header,
                                           EntryHeaderStatus* status_detail) {
  const size_t header_size = entry_header_size();
  uint8_t scratch[header_size];
  Slice slice(scratch, header_size);
  RETURN_NOT_OK_PREPEND(readable_file()->Read(*offset, slice),
                        "Could not read log entry header");

  *status_detail = DecodeEntryHeader(slice, header);
  switch (*status_detail) {
    case EntryHeaderStatus::CRC_MISMATCH:
      return Status::Corruption("CRC mismatch in log entry header");
    case EntryHeaderStatus::ALL_ZEROS:
      return Status::Corruption("preallocated space found");
    case EntryHeaderStatus::OK:
      break;
    default:
      LOG(FATAL) << "unexpected result from decoding";
      return Status::Corruption("unexpected result from decoded");
  }

  *offset += slice.size();
  return Status::OK();
}

EntryHeaderStatus ReadableLogSegment::DecodeEntryHeader(
    const Slice& data, EntryHeader* header) {
  uint32_t computed_header_crc;
  if (entry_header_size() == kEntryHeaderSizeV2) {
    header->msg_length_compressed = DecodeFixed32(&data[0]);
    header->msg_length = DecodeFixed32(&data[4]);
    header->msg_crc    = DecodeFixed32(&data[8]);
    header->header_crc = DecodeFixed32(&data[12]);
    computed_header_crc = crc::Crc32c(&data[0], 12);
  } else {
    DCHECK_EQ(kEntryHeaderSizeV1, data.size());
    header->msg_length = DecodeFixed32(&data[0]);
    header->msg_length_compressed = header->msg_length;
    header->msg_crc    = DecodeFixed32(&data[4]);
    header->header_crc = DecodeFixed32(&data[8]);
    computed_header_crc = crc::Crc32c(&data[0], 8);
  }

  // Verify the header.
  if (computed_header_crc == header->header_crc) {
    return EntryHeaderStatus::OK;
  }
  if (IsAllZeros(data)) {
    return EntryHeaderStatus::ALL_ZEROS;
  }
  return EntryHeaderStatus::CRC_MISMATCH;
}


Status ReadableLogSegment::ReadEntryBatch(int64_t* offset,
                                          const EntryHeader& header,
                                          faststring* tmp_buf,
                                          unique_ptr<LogEntryBatchPB>* entry_batch) {
  TRACE_EVENT2("log", "ReadableLogSegment::ReadEntryBatch",
               "path", path_,
               "range", Substitute("offset=$0 entry_len=$1",
                                   *offset, header.msg_length));

  if (header.msg_length == 0) {
    return Status::Corruption("Invalid 0 entry length");
  }
  int64_t limit = readable_up_to();
  if (PREDICT_FALSE(header.msg_length_compressed + *offset > limit)) {
    // The log was likely truncated during writing.
    return Status::Corruption(
        Substitute("Could not read $0-byte log entry from offset $1 in $2: "
                   "log only readable up to offset $3",
                   header.msg_length_compressed, *offset, path_, limit));
  }

  tmp_buf->clear();
  size_t buf_len = header.msg_length_compressed;
  if (codec_) {
    // Reserve some space for the decompressed copy as well.
    buf_len += header.msg_length;
  }
  tmp_buf->resize(buf_len);
  Slice entry_batch_slice(tmp_buf->data(), header.msg_length_compressed);
  Status s =  readable_file()->Read(*offset, entry_batch_slice);

  if (!s.ok()) return Status::IOError(Substitute("Could not read entry. Cause: $0",
                                                 s.ToString()));

  // Verify the CRC.
  uint32_t read_crc = crc::Crc32c(entry_batch_slice.data(), entry_batch_slice.size());
  if (PREDICT_FALSE(read_crc != header.msg_crc)) {
    return Status::Corruption(Substitute("Entry CRC mismatch in byte range $0-$1: "
                                         "expected CRC=$2, computed=$3",
                                         *offset, *offset + header.msg_length,
                                         header.msg_crc, read_crc));
  }

  // If it was compressed, decompress it.
  if (codec_) {
    // We pre-reserved space for the decompression up above.
    uint8_t* uncompress_buf = &(*tmp_buf)[header.msg_length_compressed];
    RETURN_NOT_OK_PREPEND(codec_->Uncompress(entry_batch_slice, uncompress_buf, header.msg_length),
                          "failed to uncompress entry");
    entry_batch_slice = Slice(uncompress_buf, header.msg_length);
  }

  unique_ptr<LogEntryBatchPB> read_entry_batch(new LogEntryBatchPB);
  s = pb_util::ParseFromArray(read_entry_batch.get(),
                              entry_batch_slice.data(),
                              header.msg_length);

  if (!s.ok()) {
    return Status::Corruption(Substitute("Could not parse PB. Cause: $0", s.ToString()));
  }

  *offset += header.msg_length_compressed;
  entry_batch->reset(read_entry_batch.release());
  return Status::OK();
}

WritableLogSegment::WritableLogSegment(string path,
                                       shared_ptr<WritableFile> writable_file)
    : path_(std::move(path)),
      writable_file_(std::move(writable_file)),
      is_header_written_(false),
      is_footer_written_(false),
      written_offset_(0) {}

Status WritableLogSegment::WriteHeaderAndOpen(const LogSegmentHeaderPB& new_header) {
  MAYBE_FAULT(FLAGS_fault_crash_before_write_log_segment_header);

  DCHECK(!IsHeaderWritten()) << "Can only call WriteHeader() once";
  DCHECK(new_header.IsInitialized())
      << "Log segment header must be initialized" << new_header.InitializationErrorString();
  faststring buf;

  // First the magic.
  buf.append(kLogSegmentHeaderMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, new_header.ByteSize());
  // Then Serialize the PB.
  pb_util::AppendToString(new_header, &buf);
  RETURN_NOT_OK(writable_file()->Append(Slice(buf)));

  header_.CopyFrom(new_header);
  first_entry_offset_ = buf.size();
  written_offset_ = first_entry_offset_;
  is_header_written_ = true;

  return Status::OK();
}

Status WritableLogSegment::WriteFooterAndClose(const LogSegmentFooterPB& footer) {
  TRACE_EVENT1("log", "WritableLogSegment::WriteFooterAndClose",
               "path", path_);
  DCHECK(IsHeaderWritten());
  DCHECK(!IsFooterWritten());
  DCHECK(footer.IsInitialized()) << footer.InitializationErrorString();

  faststring buf;
  pb_util::AppendToString(footer, &buf);
  buf.append(kLogSegmentFooterMagicString);
  PutFixed32(&buf, footer.ByteSize());

  RETURN_NOT_OK_PREPEND(writable_file()->Append(Slice(buf)), "Could not write the footer");

  footer_.CopyFrom(footer);
  is_footer_written_ = true;

  RETURN_NOT_OK(writable_file_->Close());

  written_offset_ += buf.size();

  return Status::OK();
}

Status WritableLogSegment::WriteEntryBatch(const Slice& data,
                                           const CompressionCodec* codec) {
  DCHECK(is_header_written_);
  DCHECK(!is_footer_written_);
  uint8_t header_buf[kEntryHeaderSizeV2];

  const uint32_t uncompressed_len = data.size();

  // If necessary, compress the data.
  Slice data_to_write;
  if (codec) {
    DCHECK_NE(header_.compression_codec(), NO_COMPRESSION);
    compress_buf_.resize(codec->MaxCompressedLength(uncompressed_len));
    size_t compressed_len;
    RETURN_NOT_OK(codec->Compress(data, &compress_buf_[0], &compressed_len));
    compress_buf_.resize(compressed_len);
    data_to_write = Slice(compress_buf_.data(), compress_buf_.size());
  } else {
    data_to_write = data;
  }

  // Fill in the header.
  InlineEncodeFixed32(&header_buf[0], data_to_write.size());
  InlineEncodeFixed32(&header_buf[4], uncompressed_len);
  InlineEncodeFixed32(&header_buf[8], crc::Crc32c(data_to_write.data(), data_to_write.size()));
  InlineEncodeFixed32(&header_buf[12], crc::Crc32c(&header_buf[0], kEntryHeaderSizeV2 - 4));

  // Write the header to the file, followed by the batch data itself.
  Slice slices[2] = {
    Slice(header_buf, arraysize(header_buf)),
    data_to_write };
  RETURN_NOT_OK(writable_file_->AppendV(slices));
  written_offset_ += arraysize(header_buf) + data_to_write.size();
  return Status::OK();
}

void WritableLogSegment::GoIdle() {
  compress_buf_.clear();
  compress_buf_.shrink_to_fit();
}

unique_ptr<LogEntryBatchPB> CreateBatchFromAllocatedOperations(
    const vector<consensus::ReplicateRefPtr>& msgs) {
  unique_ptr<LogEntryBatchPB> entry_batch(new LogEntryBatchPB);
  entry_batch->mutable_entry()->Reserve(msgs.size());
  for (const auto& msg : msgs) {
    LogEntryPB* entry_pb = entry_batch->add_entry();
    entry_pb->set_type(log::REPLICATE);
    entry_pb->set_allocated_replicate(msg->get());
  }
  return entry_batch;
}

bool IsLogFileName(const string& fname) {
  if (HasPrefixString(fname, ".")) {
    // Hidden file or ./..
    VLOG(1) << "Ignoring hidden file: " << fname;
    return false;
  }

  vector<string> v = strings::Split(fname, "-");
  if (v.size() != 2 || v[0] != FsManager::kWalFileNamePrefix) {
    VLOG(1) << "Not a log file: " << fname;
    return false;
  }

  return true;
}

void UpdateFooterForReplicateEntry(const LogEntryPB& entry_pb,
                                   LogSegmentFooterPB* footer) {
  DCHECK(entry_pb.has_replicate());
  int64_t index = entry_pb.replicate().id().index();
  if (!footer->has_min_replicate_index() ||
      index < footer->min_replicate_index()) {
    footer->set_min_replicate_index(index);
  }
  if (!footer->has_max_replicate_index() ||
      index > footer->max_replicate_index()) {
    footer->set_max_replicate_index(index);
  }
}

}  // namespace log
}  // namespace kudu
