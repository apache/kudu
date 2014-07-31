// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOG_UTIL_H_
#define KUDU_CONSENSUS_LOG_UTIL_H_

#include <iosfwd>
#include <map>
#include <string>
#include <tr1/memory>
#include <utility>
#include <vector>

#include "kudu/consensus/log.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/env.h"

namespace kudu {
namespace log {

// Suffix for temprorary files
extern const char kTmpSuffix[];

// Each log entry is prefixed by its length (4 bytes).
extern const size_t kEntryLengthSize;

extern const int kLogMajorVersion;
extern const int kLogMinorVersion;

class ReadableLogSegment;
struct OpIdCompareFunctor;

typedef std::map<consensus::OpId, scoped_refptr<ReadableLogSegment>, OpIdCompareFunctor>
        ReadableLogSegmentMap;

// Range of OpIds. The first item is inclusive, the second item is exclusive,
// i.e.: [first, second).
typedef std::pair<consensus::OpId, consensus::OpId> OpIdRange;

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

// A segment of the log can either be a ReadableLogSegment (for replay and
// consensus catch-up) or a WritableLogSegment (where the Log actually stores
// state). LogSegments have a maximum size defined in LogOptions (set from the
// log_segment_size_mb flag, which defaults to 64). Upon reaching this size
// segments are rolled over and the Log continues in a new segment.

// A readable log segment for recovery and follower catch-up.
class ReadableLogSegment : public RefCountedThreadSafe<ReadableLogSegment> {
 public:
  // Factory method to construct a ReadableLogSegment from a file on the FS.
  static Status Open(Env* env,
                     const std::string& path,
                     scoped_refptr<ReadableLogSegment>* segment);

  // Build a readable segment to read entries from the provided path.
  ReadableLogSegment(const std::string &path,
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

  // Reads all entries of the provided segment & adds them the 'entries' vector.
  // The 'entries' vector owns the read entries.
  // If the log is corrupted (i.e. the returned 'Status' is 'Corruption') all
  // the log entries read up to the corrupted are returned in the 'entries'
  // vector.
  Status ReadEntries(std::vector<LogEntryPB*>* entries);

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
  friend class RefCountedThreadSafe<ReadableLogSegment>;
  ~ReadableLogSegment() {}

  // Helper functions called by Init().
  Status ReadMagicAndHeaderLength(uint32_t *len);
  Status ParseMagicAndLength(const Slice &data, uint32_t *parsed_len);

  // Reads length from the segment and sets *len to this value.
  // Also increments the passed offset* by the length of the entry.
  Status ReadEntryLength(uint64_t *offset, uint32_t *len);

  // Reads a log entry batch from the provided readable segment, which gets decoded
  // into 'entry_batch' and increments 'offset' by the batch's length.
  Status ReadEntryBatch(uint64_t *offset,
                        uint32_t length,
                        faststring* tmp_buf,
                        gscoped_ptr<LogEntryBatchPB>* entry_batch);

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

// TODO: move these functors into their own file named opid_functors.h
// Returns true iff left == right.
bool OpIdEquals(const consensus::OpId& left, const consensus::OpId& right);

// Returns true iff left < right.
bool OpIdLessThan(const consensus::OpId& left, const consensus::OpId& right);

// Copies to_compare into target under the following conditions:
// - If to_compare is initialized and target is not.
// - If they are both initialized and to_compare is less than target.
// Otherwise, does nothing.
// If to_compare is copied into target, returns true, else false.
bool CopyIfOpIdLessThan(const consensus::OpId& to_compare, consensus::OpId* target);

// Return -1, 0, or 1.
int OpIdCompare(const consensus::OpId& left, const consensus::OpId& right);

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

// OpId comparison functor that returns true iff left > right. Suitable for use
// td::sort and std::map to sort keys in increasing order.]
struct OpIdBiggerThanFunctor {
  bool operator() (const consensus::OpId& left, const consensus::OpId& right) const;
};

// Return the minimum possible OpId.
consensus::OpId MinimumOpId();

// Return the maximum possible OpId.
consensus::OpId MaximumOpId();

// Find WAL segments for deletion whose largest contained OpId is less than
// earliest_needed_opid. We never identify the latest segment as stale.
// For example, if we need to retain OpId 7, and the rolled log segments start
// with 0, 5, and 10, respectively, then we must retain the logs starting with
// 10 and 5, but we can GC the log segment starting with OpId 0.
// See comments in the implementation file for more details on the algorithm.
//
// Returns number of stale segments found.
//
// If the returned number of stale segments is non-zero, 'stale_op_id_range'
// will be an interval of stale OpIds (the end being exclusive), starting at
// the initial OpId of the first stale segment and ending at the initial OpId
// of the first non-stale segment.
//
// Note: The range is based on initial OpId per segment because that is how each
// segment is keyed in the ReadableLogSegmentMap.
size_t FindStaleSegmentsPrefixSize(const ReadableLogSegmentMap& segment_map,
                                   const consensus::OpId& earliest_needed_opid,
                                   OpIdRange* initial_op_id_range);

// Checks if 'fname' is a correctly formatted name of log segment
// file.
bool IsLogFileName(const std::string& fname);

}  // namespace log

// This has to go in namespace 'consensus' for argument-dependent lookup to work.
// TODO: We should probably move all of the OpId*Functors into the consensus namespace
// to match where OpId itself is.
namespace consensus {
std::ostream& operator<<(std::ostream& os, const consensus::OpId& op_id);
} // namespace consensus

}  // namespace kudu

#endif /* KUDU_CONSENSUS_LOG_UTIL_H_ */
