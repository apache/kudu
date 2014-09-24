// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_LOG_LOG_READER_H_
#define KUDU_LOG_LOG_READER_H_

#include <gtest/gtest.h>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"

namespace kudu {
namespace log {
class Log;

// Reads a set of segments from a given path. Segment headers and footers
// are read and parsed, but entries are not.
// NOTE: This class is not thread safe.
class LogReader {
 public:
  // Opens a LogReader on the default tablet log directory, and sets
  // 'reader' to the newly created LogReader.
  static Status Open(FsManager *fs_manager,
                     const std::string& tablet_oid,
                     gscoped_ptr<LogReader> *reader);

  // Opens a LogReader on a specific tablet log recovery directory, and sets
  // 'reader' to the newly created LogReader.
  static Status OpenFromRecoveryDir(FsManager *fs_manager,
                                    const std::string& tablet_oid,
                                    gscoped_ptr<LogReader> *reader);

  // TODO this clears 'map' and inserts an 'old' index format (new index is
  // reversed and cannot be used without the segment sequence).
  // This is here for backwards compatibility so that patches can be split
  // Subsequent patches will remove this.
  void GetOldIndexFormat(ReadableLogSegmentMap* map);

  // Returns the biggest prefix of segments, from the current sequence, guaranteed
  // not to include 'opid'.
  Status GetSegmentPrefixNotIncluding(const consensus::OpId& opid,
                                      SegmentSequence* segments) const;

  // Returns the smallest suffix of segments, from the current sequence, guaranteed
  // to include 'opid'.
  Status GetSegmentSuffixIncluding(const consensus::OpId& opid,
                                   SegmentSequence* segments) const;

  // Copies a snapshot of the current sequence of segments into 'segments'.
  // 'segments' will be cleared first.
  Status GetSegmentsSnapshot(SegmentSequence* segments) const;

  // Returns the number of segments.
  const uint32_t num_segments() const;

  std::string ToString() const;

 private:
  FRIEND_TEST(LogTest, TestLogReader);
  friend class Log;
  friend class LogTest;

  // Simple struct that wraps SegmentIdxPosPB and adds the segment's sequence number
  // so that it can be used in the index.
  struct SegmentIdxPos {
    SegmentIdxPosPB entry_pb;
    uint64_t entry_segment_seqno;
  };

  // The index of op_ids to segments.
  // This is stored in reverse order so that if we query it for some operation
  // it returns the segment that will contain it or the segment before
  // (versus the segment after).
  // Example -
  // Index entries (first op in the segment, segment number):
  // - {0.40, seg004}
  // - {0.20, seg003}
  // - {0.10, seg002}
  //
  // Example queries:
  // - Segment that includes 0.15 (index.lower_bound(0.15)) -> {0.10, seg002}
  // - Segment that includes 0.10 (index.lower_bound(0.10)) -> {0.10, seg002}
  // - Segment that includes 0.1 (index.lower_bound(0.1)) -> end()
  // - Segment that includes 0.100 (index.lower_bound(0.100)) -> {0.40, seg004}
  //
  typedef std::map<consensus::OpId,
                   SegmentIdxPos,
                   consensus::OpIdBiggerThanFunctor> ReadableLogSegmentIndex;

  typedef ReadableLogSegmentIndex::value_type SegIdxEntry;

  // Appends 'segment' to the segments available for read by this reader.
  // Index entries in 'segment's footer will be added to the index.
  // If the segment has no footer it will be scanned so this should not be used
  // for new segments.
  Status AppendSegment(const scoped_refptr<ReadableLogSegment>& segment);

  // Same as above but for segments without any entries.
  // Used by the Log to add "empty" segments.
  Status AppendEmptySegment(const scoped_refptr<ReadableLogSegment>& segment);

  // Removes segments with sequence numbers less than or equal to 'seg_seqno' from this reader.
  Status TrimSegmentsUpToAndIncluding(uint64_t seg_seqno);

  // Replaces the last segment in the reader with 'segment'.
  // Used to replace a segment that was still in the process of being written
  // with its complete version which has a footer and index entries.
  // Requires that the last segment in 'segments_' has the same sequence
  // number as 'segment'.
  // Expects 'segment' to be properly closed and to have footer.
  Status ReplaceLastSegment(const scoped_refptr<ReadableLogSegment>& segment);

  DISALLOW_COPY_AND_ASSIGN(LogReader);

  LogReader(FsManager *fs_manager,
            const std::string& tablet_name);

  // Reads the headers of all segments in 'path_'.
  Status Init(const std::string& path_);

  // Initializes an 'empty' reader for tests, i.e. does not scan a path looking for segments.
  Status InitEmptyReaderForTests();

  FsManager *fs_manager_;
  const std::string tablet_oid_;

  // The sequence of all current log segments in increasing sequence number
  // order.
  // Note that not all segments in 'segments_' must be referred to in
  // 'segments_idx_' (e.g. a segment with only entries without ids is
  // present in 'segments_' but not present in 'segments_idx_').
  // To be sure to read all entries from/up to a point, the correct
  // segment should be looked up in the index, but then 'segments_' should
  // be used to actually read the entries.
  SegmentSequence segments_;

  // A sparse index of OpIds to segment sequence number and offset
  // within the segment. Not all segments must be mapped here (for
  // instance a segment that only contains operations without ids
  // is present in 'segments_' but not here).
  ReadableLogSegmentIndex segments_idx_;

  enum State {
    kLogReaderInitialized,
    kLogReaderReading,
    kLogReaderClosed
  };

  State state_;
};

}  // namespace log
}  // namespace kudu

#endif /* KUDU_LOG_LOG_READER_H_ */
