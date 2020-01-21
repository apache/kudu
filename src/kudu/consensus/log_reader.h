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
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/consensus/log_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/status.h"

namespace kudu {

class Counter;
class Env;
class FileCache;
class FsManager;
class Histogram;
class MetricEntity;
class faststring;

namespace consensus {
class OpId;
class ReplicateMsg;
} // namespace consensus

namespace log {
class LogEntryBatchPB;
class LogIndex;
struct LogIndexEntry;

// Reads a set of segments from a given path. Segment headers and footers are
// read and parsed, but entries are not.
//
// Once initialized, care should be taken to avoid mutating the segments in a
// non-thread-safe manner. E.g. segment mutation should be made thread-safe with
// locking primitives, or done via a copy-then-replace access pattern.
// UpdateLastSegmentOffset and ReplaceLastSegment are examples of each these
// patterns respectively.
//
// This class is thread safe.
class LogReader : public enable_make_shared<LogReader> {
 public:
  ~LogReader();

  // Opens a LogReader on the tablet log directory specified by
  // 'tablet_wal_dir', and sets 'reader' to the newly created LogReader.
  //
  // 'index' may be NULL, but if it is, ReadReplicatesInRange() may not
  // be used.
  static Status Open(Env* env,
                     const std::string& tablet_wal_dir,
                     const scoped_refptr<LogIndex>& index,
                     const std::string& tablet_id,
                     const scoped_refptr<MetricEntity>& metric_entity,
                     FileCache* file_cache,
                     std::shared_ptr<LogReader>* reader);

  // Same as above, but will use `fs_manager` to determine the default WAL dir
  // for the tablet.
  static Status Open(FsManager* fs_manager,
                     const scoped_refptr<LogIndex>& index,
                     const std::string& tablet_id,
                     const scoped_refptr<MetricEntity>& metric_entity,
                     FileCache* file_cache,
                     std::shared_ptr<LogReader>* reader);

  // Return the minimum replicate index that is retained in the currently available
  // logs. May return -1 if no replicates have been logged.
  int64_t GetMinReplicateIndex() const;

  // Return a readable segment with the given sequence number, or NULL if it
  // cannot be found (e.g. if it has already been GCed).
  scoped_refptr<ReadableLogSegment> GetSegmentBySequenceNumber(int64_t seq) const;

  // Copies a snapshot of the current sequence of segments into 'segments'.
  // 'segments' will be cleared first.
  void GetSegmentsSnapshot(SegmentSequence* segments) const;

  // Reads all ReplicateMsgs from 'starting_at' to 'up_to' both inclusive.
  // The caller takes ownership of the returned ReplicateMsg objects.
  //
  // Will attempt to read no more than 'max_bytes_to_read', unless it is set to
  // LogReader::kNoSizeLimit. If the size limit would prevent reading any operations at
  // all, then will read exactly one operation.
  //
  // Requires that a LogIndex was passed into LogReader::Open().
  Status ReadReplicatesInRange(
      int64_t starting_at,
      int64_t up_to,
      int64_t max_bytes_to_read,
      std::vector<consensus::ReplicateMsg*>* replicates) const;
  static const int64_t kNoSizeLimit;

  // Look up the OpId for the given operation index.
  // Returns a bad Status if the log index fails to load (eg. due to an IO error).
  Status LookupOpId(int64_t op_index, consensus::OpId* op_id) const;

  // Returns the number of segments.
  int num_segments() const;

  std::string ToString() const;

 protected:
  LogReader(Env* env,
            scoped_refptr<LogIndex> index,
            std::string tablet_id,
            const scoped_refptr<MetricEntity>& metric_entity,
            FileCache* file_cache);

 private:
  FRIEND_TEST(LogTestOptionalCompression, TestLogReader);
  FRIEND_TEST(LogTestOptionalCompression, TestReadLogWithReplacedReplicates);
  friend class Log;
  friend class LogTest;
  friend class LogTestOptionalCompression;

  enum State {
    kLogReaderInitialized,
    kLogReaderReading,
    kLogReaderClosed
  };

  // Appends 'segment' to the segments available for read by this reader.
  // Index entries in 'segment's footer will be added to the index.
  // If the segment has no footer it will be scanned so this should not be used
  // for new segments.
  Status AppendSegment(scoped_refptr<ReadableLogSegment> segment);

  // Same as above but for segments without any entries.
  // Used by the Log to add "empty" segments.
  void AppendEmptySegment(scoped_refptr<ReadableLogSegment> segment);

  // Removes segments with sequence numbers less than or equal to
  // 'segment_sequence_number' from this reader.
  void TrimSegmentsUpToAndIncluding(int64_t segment_sequence_number);

  // Replaces the last segment in the reader with 'segment'.
  // Used to replace a segment that was still in the process of being written
  // with its complete version which has a footer and index entries.
  // Requires that the last segment in 'segments_' has the same sequence
  // number as 'segment'.
  // Expects 'segment' to be properly closed and to have footer.
  void ReplaceLastSegment(scoped_refptr<ReadableLogSegment> segment);

  // Appends 'segment' to the segment sequence.
  // Assumes that the segment was scanned, if no footer was found.
  // To be used only internally, clients of this class with private access (i.e. friends)
  // should use the thread safe version, AppendSegment(), which will also scan the segment
  // if no footer is present.
  void AppendSegmentUnlocked(scoped_refptr<ReadableLogSegment> segment);

  // Used by Log to update its LogReader on how far it is possible to read
  // the current segment. Requires that the reader has at least one segment
  // and that the last segment has no footer, meaning it is currently being
  // written to.
  void UpdateLastSegmentOffset(int64_t readable_to_offset);

  // Read the LogEntryBatchPB pointed to by the provided index entry.
  // 'tmp_buf' is used as scratch space to avoid extra allocation.
  Status ReadBatchUsingIndexEntry(const LogIndexEntry& index_entry,
                                  faststring* tmp_buf,
                                  std::unique_ptr<LogEntryBatchPB>* batch) const;

  // Reads the headers of all segments in 'tablet_wal_path'.
  Status Init(const std::string& tablet_wal_path);

  // Initializes an 'empty' reader for tests, i.e. does not scan a path looking for segments.
  void InitEmptyReaderForTests();

  Env* env_;
  FileCache* file_cache_;
  const scoped_refptr<LogIndex> log_index_;
  const std::string tablet_id_;

  // Metrics
  scoped_refptr<Counter> bytes_read_;
  scoped_refptr<Counter> entries_read_;
  scoped_refptr<Histogram> read_batch_latency_;

  // The sequence of all current log segments in increasing sequence number
  // order.
  SegmentSequence segments_;

  mutable simple_spinlock lock_;

  State state_;

  DISALLOW_COPY_AND_ASSIGN(LogReader);
};

}  // namespace log
}  // namespace kudu
