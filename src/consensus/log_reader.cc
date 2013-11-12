// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log_reader.h"

#include <boost/foreach.hpp>
#include <algorithm>

#include "gutil/stl_util.h"
#include "gutil/strings/util.h"
#include "gutil/strings/substitute.h"
#include "util/coding.h"
#include "util/env_util.h"
#include "util/hexdump.h"
#include "util/pb_util.h"

namespace kudu {
namespace log {

using consensus::OpId;
using env_util::ReadFully;
using strings::Substitute;

// the maximum header size (8 MB)
const uint32_t kMaxHeaderSize = 8 * 1024 * 1024;

static bool CompareSegments(const shared_ptr<ReadableLogSegment>& i,
                            const shared_ptr<ReadableLogSegment>& j) {
  const OpId& i_id = i->header().initial_id();
  const OpId& j_id = j->header().initial_id();
  return i_id.term() < j_id.term() ? true :
         i_id.term() > j_id.term() ? false :
         i_id.index() < j_id.index();
}

Status LogReader::Open(FsManager *fs_manager,
                       const string& tablet_oid,
                       gscoped_ptr<LogReader> *reader) {
  gscoped_ptr<LogReader> log_reader(new LogReader(fs_manager, tablet_oid));


  string tablet_wal_path = fs_manager->GetTabletWalDir(tablet_oid);

  RETURN_NOT_OK(log_reader->Init(tablet_wal_path))
  reader->reset(log_reader.release());
  return Status::OK();
}

Status LogReader::Open(FsManager *fs_manager,
                       const string& tablet_oid,
                       uint64_t recovery_ts,
                       gscoped_ptr<LogReader>* reader) {
  gscoped_ptr<LogReader> log_reader(new LogReader(fs_manager, tablet_oid));

  string tablet_wal_path = fs_manager->GetTabletWalRecoveryDir(tablet_oid, recovery_ts);

  RETURN_NOT_OK(log_reader->Init(tablet_wal_path))
  reader->reset(log_reader.release());
  return Status::OK();
}

LogReader::LogReader(FsManager *fs_manager,
                     const string& tablet_oid)
: fs_manager_(fs_manager),
  tablet_oid_(tablet_oid),
  state_(kLogReaderInitialized) {
}

Status LogReader::Init(const string& tablet_wal_path) {
  CHECK_EQ(state_, kLogReaderInitialized) << "bad state for Init(): " << state_;
  VLOG(1) << "Reading wal from path:" << tablet_wal_path;

  Env* env = fs_manager_->env();

  if (!fs_manager_->Exists(tablet_wal_path)) {
    return Status::IllegalState("Cannot find wal location at", tablet_wal_path);
  }

  VLOG(1) << "Parsing segments from path: " << tablet_wal_path;
  // list existing segment files
  vector<string> log_files;

  RETURN_NOT_OK(env->GetChildren(tablet_wal_path,
                                 &log_files));

  // build a log segment from each file
  BOOST_FOREACH(const string &log_file, log_files) {
    if (HasPrefixString(log_file, kLogPrefix)) {
      string fqp = env->JoinPathSegments(tablet_wal_path, log_file);
      shared_ptr<ReadableLogSegment> segment;
      RETURN_NOT_OK(InitSegment(env, fqp, &segment));
      DCHECK(segment);
      segments_.push_back(segment);
    }
  }

  // sort the segments
  sort(segments_.begin(), segments_.end(), CompareSegments);
  state_ = kLogReaderReading;
  return Status::OK();
}

Status LogReader::InitSegment(Env* env,
                              const string &log_file,
                              shared_ptr<ReadableLogSegment>* segment) {
  VLOG(1) << "Parsing segment: " << log_file;
  uint64_t file_size;
  RETURN_NOT_OK(env->GetFileSize(log_file, &file_size));
  shared_ptr<RandomAccessFile> file;
  RETURN_NOT_OK(kudu::env_util::OpenFileForRandom(env, log_file, &file));
  RETURN_NOT_OK(ParseHeaderAndBuildSegment(file_size,
                                           log_file,
                                           file,
                                           segment));
  return Status::OK();
}

Status LogReader::ReadEntries(const shared_ptr<ReadableLogSegment> &segment,
                              vector<LogEntry* >* entries) {
  uint64_t offset = segment->first_entry_offset();
  VLOG(1) << "Reading segment entries offset: " << offset << " file size: "
          << segment->file_size();
  faststring tmp_buf;
  while (offset < segment->file_size()) {
    gscoped_ptr<LogEntry> current;

    // Read the entry length first, if we get 0 back that just means that
    // the log hasn't been ftruncated().
    uint32_t length;
    RETURN_NOT_OK(ReadEntryLength(segment, &offset, &length));
    if (length == 0) {
      return Status::OK();
    }

    Status status = ReadEntry(segment, &tmp_buf, &offset, length, &current);
    if (status.ok()) {
      if (VLOG_IS_ON(3)) {
        VLOG(3) << "Read Log entry: " << current->DebugString();
      }
      entries->push_back(current.release());
    } else {
      RETURN_NOT_OK_PREPEND(status, strings::Substitute("Log File corrupted, "
          "cannot read further. 'entries' contains log entries read up to $0"
          " bytes", offset));
    }
  }
  return Status::OK();
}

const uint32_t LogReader::size() {
  return segments_.size();
}

Status LogReader::ParseMagicAndLength(const Slice &data,
                                      uint32_t *parsed_len) {
  RETURN_NOT_OK(data.check_size(kMagicAndHeaderLength));

  if (memcmp(kMagicString, data.data(), strlen(kMagicString)) != 0) {
    return Status::Corruption("Bad magic. " + data.ToDebugString());
  }

  *parsed_len = DecodeFixed32(data.data() + strlen(kMagicString));
  return Status::OK();
}

Status LogReader::ReadMagicAndHeaderLength(const shared_ptr<RandomAccessFile> &file,
                                           uint32_t *len) {
  uint8_t scratch[kMagicAndHeaderLength];
  Slice slice;
  RETURN_NOT_OK(ReadFully(file.get(), 0, kMagicAndHeaderLength, &slice, scratch));
  RETURN_NOT_OK(ParseMagicAndLength(slice, len));
  return Status::OK();
}

Status LogReader::ParseHeaderAndBuildSegment(
    const uint64_t file_size,
    const string &path,
    const shared_ptr<RandomAccessFile> &file,
    shared_ptr<ReadableLogSegment> *segment) {

  uint32_t header_size = 0;
  RETURN_NOT_OK(ReadMagicAndHeaderLength(file, &header_size));
  if (header_size == 0 || header_size > kMaxHeaderSize) {
    return Status::Corruption(strings::Substitute("File is corrupted. "
        "Parsed header size: $0 is zero or bigger than max header size: $1",
        header_size, kMaxHeaderSize));
  }

  uint8_t header_space[header_size];
  Slice header_slice;
  LogSegmentHeader header;

  RETURN_NOT_OK(ReadFully(file.get(), kMagicAndHeaderLength, header_size,
                          &header_slice, header_space));

  RETURN_NOT_OK(pb_util::ParseFromArray(&header,
                                        header_slice.data(),
                                        header_size));

  segment->reset(new ReadableLogSegment(header,
                                        path,
                                        header_size + kMagicAndHeaderLength,
                                        file_size,
                                        file));
  return Status::OK();
}

Status LogReader::ParseEntryLength(const Slice &data,
                                   uint32_t *parsed_len) {

  RETURN_NOT_OK(data.check_size(kEntryLengthSize));

  *parsed_len = DecodeFixed32(data.data());
  return Status::OK();
}

Status LogReader::ReadEntryLength(
    const shared_ptr<ReadableLogSegment> &segment,
    uint64_t *offset,
    uint32_t *len) {
  uint8_t scratch[kEntryLengthSize];
  Slice slice;
  RETURN_NOT_OK(ReadFully(segment->readable_file().get(), *offset, kEntryLengthSize,
                          &slice, scratch));
  *offset += kEntryLengthSize;
  return ParseEntryLength(slice, len);
}

Status LogReader::ReadEntry(const shared_ptr<ReadableLogSegment> &segment,
                            faststring *tmp_buf,
                            uint64_t *offset,
                            uint32_t length,
                            gscoped_ptr<LogEntry> *entry) {

  if (length == 0 || length > segment->file_size() - *offset) {
    return Status::Corruption(StringPrintf("Invalid entry length %d.", length));
  }

  tmp_buf->clear();
  tmp_buf->resize(length);
  Slice entry_slice;
  RETURN_NOT_OK(segment->readable_file()->Read(*offset,
                                               length,
                                               &entry_slice,
                                               tmp_buf->data()));

  gscoped_ptr<LogEntry> read_entry(new LogEntry());
  RETURN_NOT_OK(pb_util::ParseFromArray(read_entry.get(),
                                        entry_slice.data(),
                                        length));
  *offset += length;
  entry->reset(read_entry.release());
  return Status::OK();
}

}  // namespace log
}  // namespace kudu
