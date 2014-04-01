// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOG_UTIL_H_
#define KUDU_CONSENSUS_LOG_UTIL_H_

#include <string>
#include <vector>
#include <tr1/memory>

#include "consensus/log.pb.h"
#include "gutil/macros.h"
#include "util/env.h"

namespace kudu {
namespace log {

// Suffix for temprorary files
extern const char kTmpSuffix[];

// Logs start with "log-" prefix.
extern const char kLogPrefix[];

// Each log entry is prefixed by its length (4 bytes).
extern const size_t kEntryLengthSize;

extern const int kLogMajorVersion;
extern const int kLogMinorVersion;

// Options for the State Machine/Write Ahead Log
struct LogOptions {

  // The size of a Log segment
  // Logs will rollover upon reaching this size (default 64 MB)
  size_t segment_size_mb;

  // Whether to call fsync on every call to Append().
  bool force_fsync_all;

  // Whether to fallocate segments before writing to them.
  bool preallocate_segments;

  // Whether the allocation should happen asynchronously.
  bool async_preallocate_segments;

  LogOptions();
};

// A segment of the log, can either be a ReadableLogSegment (for replay and
// consensus catch-up) or a WritableLogSegment (where the Log actually stores
// state). LogSegments have a maximum size defined in LogOptions (set from the
// log_segment_size_mb flag, which defaults to 64). Upon reaching this size
// segments are rolled over and the Log continues in a new segment.


// A readable log segment for recovery and follower catch-up.
class ReadableLogSegment {
 public:
  // Build a readable segment to read entries from the provided path.
  ReadableLogSegment(
      const std::string &path,
      uint64_t file_size,
      const std::tr1::shared_ptr<RandomAccessFile>& readable_file);

  // Initialize the ReadableLogSegment.
  // This initializer provides methods for avoiding disk IO when creating a
  // ReadableLogSegment from a WritableLogSegment (i.e. for log rolling).
  // Note: This returns void, however will throw an assertion via DCHECK
  // if the provided header PB is not initialized.
  void Init(const LogSegmentHeaderPB& header,
            uint64_t first_entry_offset);

  // Initialize the ReadableLogSegment.
  // This initializer will parse the log segment header.
  // Note: This returns Status and may fail.
  Status Init();

  bool IsInitialized() const {
    return is_initialized_;
  }

  // Returns the parent directory where log segments are stored.
  const std::string &path() const {
    return path_;
  }

  const LogSegmentHeaderPB& header() const {
    DCHECK(IsInitialized());
    return header_;
  }

  const std::tr1::shared_ptr<RandomAccessFile> readable_file() const {
    return readable_file_;
  }

  const uint64_t file_size() const {
    return file_size_;
  }

  const uint64_t first_entry_offset() const {
    return first_entry_offset_;
  }

 private:
  // Helper functions called by Init().
  Status ReadMagicAndHeaderLength(uint32_t *len);
  Status ParseMagicAndLength(const Slice &data, uint32_t *parsed_len);

  const std::string path_;

  // the size of the readable file
  const uint64_t file_size_;

  // a readable file for a log segment (used on replay)
  const std::tr1::shared_ptr<RandomAccessFile> readable_file_;

  bool is_initialized_;

  LogSegmentHeaderPB header_;

  // the offset of the first entry in the log
  uint64_t first_entry_offset_;

  DISALLOW_COPY_AND_ASSIGN(ReadableLogSegment);
};

// A writable log segment where state data is stored.
class WritableLogSegment {
 public:
  WritableLogSegment(const std::string &path,
                     const std::tr1::shared_ptr<WritableFile>& writable_file);

  // Writes out the segment header. Stores bytes written into header_bytes.
  // Does _not_ sync the bytes written to disk.
  Status WriteHeader(const LogSegmentHeaderPB& new_header);

  // Returns true if the segment header has already been written to disk.
  bool IsHeaderWritten() const {
    return is_header_written_;
  }

  const LogSegmentHeaderPB& header() const {
    DCHECK(IsHeaderWritten());
    return header_;
  }

  // Returns the parent directory where log segments are stored.
  const std::string &path() const {
    return path_;
  }

  const uint64_t first_entry_offset() const {
    return first_entry_offset_;
  }

  const std::tr1::shared_ptr<WritableFile>& writable_file() const {
    return writable_file_;
  }

 private:
  // The path to the log file.
  const std::string path_;

  // The writable file to which this LogSegment will be written.
  const std::tr1::shared_ptr<WritableFile> writable_file_;

  bool is_header_written_;

  LogSegmentHeaderPB header_;

  // the offset of the first entry in the log
  uint64_t first_entry_offset_;

  DISALLOW_COPY_AND_ASSIGN(WritableLogSegment);
};

// Returns true iff left == right.
bool OpIdEquals(const consensus::OpId& left, const consensus::OpId& right);

// Returns true iff left < right.
bool OpIdLessThan(const consensus::OpId& left, const consensus::OpId& right);

// OpId hash functor. Suitable for use with std::unordered_map.
struct OpIdHashFunctor {
  size_t operator() (const consensus::OpId& id) const;
};

// OpId equals functor. Suitable for use with std::unordered_map.
struct OpIdEqualsFunctor {
  bool operator() (const consensus::OpId& left, const consensus::OpId& right) const;
};

// OpId compare() functor. Suitable for use with std::sort and std::map.
struct OpIdCompareFunctor {
  // Returns true iff left < right.
  bool operator() (const consensus::OpId& left, const consensus::OpId& right) const;
};

// Return the minimum possible OpId.
consensus::OpId MinimumOpId();

// Return the maximum possible OpId.
consensus::OpId MaximumOpId();

// Find WAL segments for GC whose highest initial OpId is less than
// earliest_needed_opid. We can approximate this by taking all segments that
// are at least 2 segments _before_ the segment with an initial_opid strictly
// greater than the earliest needed OpId.
// i.e. if we need 7, and segments start with 0, 5 & 10, then we must retain
// the logs starting with 10 and 5, but we can GC the log starting with OpId 0.
Status FindStaleSegmentsPrefixSize(
    const std::vector<std::tr1::shared_ptr<ReadableLogSegment> > &segments,
    const consensus::OpId& earliest_needed_opid,
    uint32_t *prefix_size);

// Checks if 'fname' is a correctly formatted name of log segment
// file.
bool IsLogFileName(const std::string& fname);

}  // namespace log
}  // namespace kudu

#endif /* KUDU_CONSENSUS_LOG_UTIL_H_ */
