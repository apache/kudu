// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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
#include "kudu/gutil/spinlock.h"

namespace kudu {
namespace log {
class Log;

// Reads a set of segments from a given path. Segment headers and footers
// are read and parsed, but entries are not.
// This class is thread safe.
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

  // Returns the biggest prefix of segments, from the current sequence, guaranteed
  // not to include any replicate messages with indexes >= 'index'.
  Status GetSegmentPrefixNotIncluding(int64_t index,
                                      SegmentSequence* segments) const;

  // Returns the smallest suffix of segments, from the current sequence, which might
  // contain REPLICATE messages with the given index.
  Status GetSegmentSuffixIncluding(int64_t index,
                                   SegmentSequence* segments) const;

  // Copies a snapshot of the current sequence of segments into 'segments'.
  // 'segments' will be cleared first.
  Status GetSegmentsSnapshot(SegmentSequence* segments) const;

  // Reads all ReplicateMsgs from 'starting_after' exclusive, to 'up_to' inclusive.
  Status ReadAllReplicateEntries(
      const int64_t starting_after,
      const int64_t up_to,
      std::vector<consensus::ReplicateMsg*>* replicates) const;

  // Returns the number of segments.
  const int num_segments() const;

  std::string ToString() const;

 private:
  FRIEND_TEST(LogTest, TestLogReader);
  friend class Log;
  friend class LogTest;

  enum State {
    kLogReaderInitialized,
    kLogReaderReading,
    kLogReaderClosed
  };

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

  // Appends 'segment' to the segment sequence.
  // Assumes that the segment was scanned, if no footer was found.
  // To be used only internally, clients of this class with private access (i.e. friends)
  // should use the thread safe version, AppendSegment(), which will also scan the segment
  // if no footer is present.
  Status AppendSegmentUnlocked(const scoped_refptr<ReadableLogSegment>& segment);

  // Used by Log to update its LogReader on how far it is possible to read
  // the current segment. Requires that the reader has at least one segment
  // and that the last segment has no footer, meaning it is currently being
  // written to.
  void UpdateLastSegmentOffset(uint64_t readable_to_offset);

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
  SegmentSequence segments_;

  mutable simple_spinlock lock_;

  State state_;

  DISALLOW_COPY_AND_ASSIGN(LogReader);
};

}  // namespace log
}  // namespace kudu

#endif /* KUDU_LOG_LOG_READER_H_ */
