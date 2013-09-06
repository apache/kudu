// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOG_UTIL_H_
#define KUDU_CONSENSUS_LOG_UTIL_H_

#include <string>
#include <vector>
#include <tr1/memory>

#include "gutil/macros.h"

#include "consensus/log.pb.h"
#include "util/env.h"

namespace kudu {
namespace log {

extern const char kMagicString[];

// logs start with "log-" prefix and end with the term and index of the initial
// log entry.
extern const char kLogPrefix[];

// header is prefixed with the magic (8 bytes) and the header length (4 bytes).
extern const size_t kMagicAndHeaderLength;
// each log entry is prefixed by its length (4 bytes)
extern const size_t kEntryLengthSize;

extern const int kLogMajorVersion;
extern const int kLogMinorVersion;

// Options for the State Machine/Write Ahead Log
struct LogOptions {

  // The size of a Log segment
  // Logs will rollover upon reaching this size (default 64 MB)
  size_t segment_size_mb;

  LogOptions();
};

// A segment of the log, can either be a ReadableLogSegment (for replay and
// consensus catch-up) or a WritableLogSegment (where the Log actually stores
// state). LogSegments have a maximum size defined in LogOptions (set from the
// log_segment_size_mb flag, which defaults to 64). Upon reaching this size
// segments are rolled over and the Log continues in a new segment.
class LogSegment {
 protected:
  LogSegment(const LogSegmentHeader &header,
             const std::string &path);

  LogSegment(const LogSegmentHeader &header,
             const std::string &path,
             const std::vector<LogEntry*> &entries);

 public:
  // Returns the header for this log segment, including initial index, term,
  // configuration and initial active MRSSet
  const LogSegmentHeader &header() const {
    return header_;
  }

  // Returns the parent directory where log segments are stored.
  const std::string &path() const {
    return path_;
  }

  virtual ~LogSegment() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(LogSegment);

  const LogSegmentHeader header_;
  const std::string path_;
};

// A readable log segment for recovery and follower catch-up.
class ReadableLogSegment : public LogSegment {
 public:

  // build a readable segment to read entries from the provided path.
  ReadableLogSegment(
      const LogSegmentHeader &header,
      const std::string &path,
      uint64_t entries_offset,
      uint64_t file_size,
      const std::tr1::shared_ptr<RandomAccessFile>& readable_file);

  // Used by WritableLogSegment to transform into a ReadableLogSegment
  // without re-reading all the entries.
  ReadableLogSegment(const LogSegmentHeader &header,
                     const std::string &path,
                     const std::vector<LogEntry*> &entries);

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
  DISALLOW_COPY_AND_ASSIGN(ReadableLogSegment);

  // the offset of the first entry in the log
  const uint64_t first_entry_offset_;

  // the size of the readable file
  const uint64_t file_size_;

  // a readable file for a log segment (used on replay)
  const std::tr1::shared_ptr<RandomAccessFile> readable_file_;
};

// A writable log segment where state data is stored.
class WritableLogSegment : public LogSegment {
 public:

  WritableLogSegment(const LogSegmentHeader &header,
                     const std::string &path,
                     const std::tr1::shared_ptr<WritableFile>& writable_file);

  const std::tr1::shared_ptr<WritableFile>& writable_file() const {
    return writable_file_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(WritableLogSegment);

  // the writable file to which this LogSegment will be written
  std::tr1::shared_ptr<WritableFile> writable_file_;
};

// Checks if the two superblocks have no memstores in common.
// This method assumes that for each durable store present in the superblock
// there is successor memstore in memory and that this store has data.
// For two superblocks to have no common stores the two superblocks must have
// different 'last_durable_mrs_id' and for each rowset, must also have a higher
// delta index. That is the 'newer' superblock must have a new MemRowSet and must
// have flushed the DeltaMemStore for each of the row sets.
//
// Note the order of the superblocks (older, newer) is relevant
bool HasNoCommonMemStores(const metadata::TabletSuperBlockPB &older,
                          const metadata::TabletSuperBlockPB &newer);

// Searches for the number of segments that have no entries that go into any
// memory store in ActiveMemStores. Sets prefix_size to the size of the prefix
// of segments that can be discarded safely.
Status FindStaleSegmentsPrefixSize(
    const std::vector<std::tr1::shared_ptr<ReadableLogSegment> > &segments,
    const metadata::TabletSuperBlockPB &metadata,
    uint32_t *prefix_size);

}  // namespace log
}  // namespace kudu

#endif /* KUDU_CONSENSUS_LOG_UTIL_H_ */
