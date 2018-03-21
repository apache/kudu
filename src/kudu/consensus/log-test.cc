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

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/move.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/async_util.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/debug/sanitizer_scopes.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_batches, 10000,
             "Number of batches to write to/read from the Log in TestWriteManyBatches");

DECLARE_int32(log_min_segments_to_retain);
DECLARE_int32(log_max_segments_to_retain);
DECLARE_double(log_inject_io_error_on_preallocate_fraction);
DECLARE_int64(fs_wal_dir_reserved_bytes);
DECLARE_int64(disk_reserved_bytes_free_for_testing);
DECLARE_string(log_compression_codec);

namespace kudu {
namespace log {

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using consensus::CommitMsg;
using consensus::MakeOpId;
using consensus::NO_OP;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::WRITE_OP;
using strings::Substitute;

struct TestLogSequenceElem {
  enum ElemType {
    REPLICATE,
    COMMIT,
    ROLL
  };
  ElemType type;
  OpId id;
};

class LogTest : public LogTestBase {
 public:
  void CreateAndRegisterNewAnchor(int64_t log_index, vector<LogAnchor*>* anchors) {
    anchors->push_back(new LogAnchor());
    log_anchor_registry_->Register(log_index, CURRENT_TEST_NAME(), anchors->back());
  }

  // Create a series of NO_OP entries in the log.
  // Anchor each segment on the first OpId of each log segment,
  // and update op_id to point to the next valid OpId.
  Status AppendMultiSegmentSequence(int num_total_segments, int num_ops_per_segment,
                                    OpId* op_id, vector<LogAnchor*>* anchors) {
    CHECK(op_id->IsInitialized());
    for (int i = 0; i < num_total_segments - 1; i++) {
      if (anchors) {
        CreateAndRegisterNewAnchor(op_id->index(), anchors);
      }
      RETURN_NOT_OK(AppendNoOps(op_id, num_ops_per_segment));
      RETURN_NOT_OK(RollLog());
    }

    if (anchors) {
      CreateAndRegisterNewAnchor(op_id->index(), anchors);
    }
    RETURN_NOT_OK(AppendNoOps(op_id, num_ops_per_segment));
    return Status::OK();
  }

  Status AppendNewEmptySegmentToReader(int sequence_number,
                                       int first_repl_index,
                                       LogReader* reader) {
    string fqp = GetTestPath(strings::Substitute("wal-00000000$0", sequence_number));
    unique_ptr<WritableFile> w_log_seg;
    RETURN_NOT_OK(fs_manager_->env()->NewWritableFile(fqp, &w_log_seg));
    unique_ptr<RandomAccessFile> r_log_seg;
    RETURN_NOT_OK(fs_manager_->env()->NewRandomAccessFile(fqp, &r_log_seg));

    scoped_refptr<ReadableLogSegment> readable_segment(
        new ReadableLogSegment(fqp, shared_ptr<RandomAccessFile>(r_log_seg.release())));

    LogSegmentHeaderPB header;
    header.set_sequence_number(sequence_number);
    header.set_tablet_id(kTestTablet);
    SchemaToPB(GetSimpleTestSchema(), header.mutable_schema());

    LogSegmentFooterPB footer;
    footer.set_num_entries(10);
    footer.set_min_replicate_index(first_repl_index);
    footer.set_max_replicate_index(first_repl_index + 9L);

    RETURN_NOT_OK(readable_segment->Init(header, footer, 0));
    RETURN_NOT_OK(reader->AppendSegment(readable_segment));
    return Status::OK();
  }

  void GenerateTestSequence(Random* rng, int seq_len,
                            vector<TestLogSequenceElem>* ops,
                            vector<int64_t>* terms_by_index);
  void AppendTestSequence(const vector<TestLogSequenceElem>& seq);

  // Where to corrupt the log entry.
  enum CorruptionPosition {
    // Corrupt/truncate within the header.
    IN_HEADER,
    // Corrupt/truncate within the entry data itself.
    IN_ENTRY
  };

  void DoCorruptionTest(CorruptionType type, CorruptionPosition place,
                        const Status& expected_status, int expected_entries);

};

// For cases which should run both with and without compression.
class LogTestOptionalCompression : public LogTest,
                                   public testing::WithParamInterface<CompressionType> {
 public:
  LogTestOptionalCompression() {
    const auto& name = CompressionType_Name(GetParam());
    LOG(INFO) << "using compression type: " << name;
    FLAGS_log_compression_codec = name;
  }
};
INSTANTIATE_TEST_CASE_P(Codecs, LogTestOptionalCompression, ::testing::Values(NO_COMPRESSION, LZ4));

// If we write more than one entry in a batch, we should be able to
// read all of those entries back.
TEST_P(LogTestOptionalCompression, TestMultipleEntriesInABatch) {
  ASSERT_OK(BuildLog());

  OpId opid;
  opid.set_term(1);
  opid.set_index(1);

  AppendNoOpsToLogSync(clock_, log_.get(), &opid, 2);

  // RollOver() the batch so that we have a properly formed footer.
  ASSERT_OK(log_->AllocateSegmentAndRollOver());

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  SegmentSequence segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));

  ASSERT_OK(segments[0]->ReadEntries(&entries));

  ASSERT_EQ(2, entries.size());

  // Verify the index.
  {
    LogIndexEntry entry;
    ASSERT_OK(log_->log_index_->GetEntry(1, &entry));
    ASSERT_EQ(1, entry.op_id.term());
    ASSERT_EQ(1, entry.segment_sequence_number);
    int64_t offset = entry.offset_in_segment;

    ASSERT_OK(log_->log_index_->GetEntry(2, &entry));
    ASSERT_EQ(1, entry.op_id.term());
    ASSERT_EQ(1, entry.segment_sequence_number);
    int64_t second_offset = entry.offset_in_segment;

    // The second entry should be at the same offset as the first entry
    // since they were written in the same batch.
    ASSERT_EQ(second_offset, offset);
  }

  // Test LookupOpId
  {
    OpId loaded_op;
    ASSERT_OK(log_->reader()->LookupOpId(1, &loaded_op));
    ASSERT_EQ("1.1", OpIdToString(loaded_op));
    ASSERT_OK(log_->reader()->LookupOpId(2, &loaded_op));
    ASSERT_EQ("1.2", OpIdToString(loaded_op));
    Status s = log_->reader()->LookupOpId(3, &loaded_op);
    ASSERT_TRUE(s.IsNotFound()) << "unexpected status: " << s.ToString();
  }

  ASSERT_OK(log_->Close());
}

// Tests that everything works properly with fsync enabled:
// This also tests SyncDir() (see KUDU-261), which is called whenever
// a new log segment is initialized.
TEST_P(LogTestOptionalCompression, TestFsync) {
  options_.force_fsync_all = true;
  ASSERT_OK(BuildLog());

  OpId opid;
  opid.set_term(0);
  opid.set_index(1);

  AppendNoOp(&opid);

  ASSERT_OK(log_->Close());
}

// Regression test for part of KUDU-735:
// if a log is not preallocated, we should properly track its on-disk size as we append to
// it.
TEST_P(LogTestOptionalCompression, TestSizeIsMaintained) {
  options_.preallocate_segments = false;
  ASSERT_OK(BuildLog());

  OpId opid = MakeOpId(0, 1);
  AppendNoOp(&opid);

  SegmentSequence segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));
  int64_t orig_size = segments[0]->file_size();
  ASSERT_GT(orig_size, 0);

  AppendNoOp(&opid);

  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));
  int64_t new_size = segments[0]->file_size();
  ASSERT_GT(new_size, orig_size);

  ASSERT_OK(log_->Close());
}

// Test that the reader can read from the log even if it hasn't been
// properly closed.
TEST_P(LogTestOptionalCompression, TestLogNotTrimmed) {
  ASSERT_OK(BuildLog());

  OpId opid;
  opid.set_term(0);
  opid.set_index(1);

  AppendNoOp(&opid);

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  SegmentSequence segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));

  ASSERT_OK(segments[0]->ReadEntries(&entries));
  // Close after testing to ensure correct shutdown
  // TODO : put this in TearDown() with a test on log state?
  ASSERT_OK(log_->Close());
}

// Test that the reader will not fail if a log file is completely blank.
// This happens when it's opened but nothing has been written.
// The reader should gracefully handle this situation. See KUDU-140.
TEST_P(LogTestOptionalCompression, TestBlankLogFile) {
  ASSERT_OK(BuildLog());

  // The log's reader will have a segment...
  ASSERT_EQ(log_->reader()->num_segments(), 1);

  // ...and we're able to read from it.
  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  SegmentSequence segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));

  ASSERT_OK(segments[0]->ReadEntries(&entries));

  // ...It's just that it's empty.
  ASSERT_EQ(entries.size(), 0);
}

void LogTest::DoCorruptionTest(CorruptionType type, CorruptionPosition place,
                               const Status& expected_status, int expected_entries) {
  const int kNumEntries = 4;
  ASSERT_OK(BuildLog());
  OpId op_id = MakeOpId(1, 1);
  ASSERT_OK(AppendNoOps(&op_id, kNumEntries));

  // Find the entry that we want to corrupt before closing the log.
  LogIndexEntry entry;
  ASSERT_OK(log_->log_index_->GetEntry(4, &entry));

  ASSERT_OK(log_->Close());

  // Corrupt the log as specified.
  int offset;
  switch (place) {
    case IN_HEADER:
      offset = entry.offset_in_segment + 1;
      break;
    case IN_ENTRY:
      offset = entry.offset_in_segment + kEntryHeaderSizeV2 + 1;
      break;
  }
  ASSERT_OK(CorruptLogFile(env_, log_->ActiveSegmentPathForTests(), type, offset));

  // Open a new reader -- we don't reuse the existing LogReader from log_
  // because it has a cached header.
  shared_ptr<LogReader> reader;
  ASSERT_OK(LogReader::Open(fs_manager_.get(),
                            make_scoped_refptr(new LogIndex(log_->log_dir_)),
                            kTestTablet, nullptr, &reader));
  ASSERT_EQ(1, reader->num_segments());

  SegmentSequence segments;
  ASSERT_OK(reader->GetSegmentsSnapshot(&segments));
  Status s = segments[0]->ReadEntries(&entries_);
  ASSERT_EQ(s.CodeAsString(), expected_status.CodeAsString())
    << "Got unexpected status: " << s.ToString();

  // Last entry is ignored, but we should still see the previous ones.
  ASSERT_EQ(expected_entries, entries_.size());
}
// Tests that the log reader reads up until some truncated entry is found.
// It should still return OK, since on a crash, it's acceptable to have
// a partial entry at EOF.
TEST_P(LogTestOptionalCompression, TestTruncateLogInEntry) {
  DoCorruptionTest(TRUNCATE_FILE, IN_ENTRY, Status::OK(), 3);
}

// Same, but truncate in the middle of the header of that entry.
TEST_P(LogTestOptionalCompression, TestTruncateLogInHeader) {
  DoCorruptionTest(TRUNCATE_FILE, IN_HEADER, Status::OK(), 3);
}

// Similar to the above, except flips a byte. In this case, it should return
// a Corruption instead of an OK, because we still have a valid footer in
// the file (indicating that all of the entries should be valid as well).
TEST_P(LogTestOptionalCompression, TestCorruptLogInEntry) {
  DoCorruptionTest(FLIP_BYTE, IN_ENTRY, Status::Corruption(""), 3);
}

// Same, but corrupt in the middle of the header of that entry.
TEST_P(LogTestOptionalCompression, TestCorruptLogInHeader) {
  DoCorruptionTest(FLIP_BYTE, IN_HEADER, Status::Corruption(""), 3);
}

// Tests that segments roll over when max segment size is reached
// and that the player plays all entries in the correct order.
TEST_P(LogTestOptionalCompression, TestSegmentRollover) {
  ASSERT_OK(BuildLog());
  // Set a small segment size so that we have roll overs.
  log_->SetMaxSegmentSizeForTests(990);
  const int kNumEntriesPerBatch = 100;

  OpId op_id = MakeOpId(1, 1);
  int num_entries = 0;

  SegmentSequence segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));

  while (segments.size() < 3) {
    ASSERT_OK(AppendNoOps(&op_id, kNumEntriesPerBatch));
    num_entries += kNumEntriesPerBatch;
    // Update the segments
    ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));
  }

  ASSERT_FALSE(segments.back()->HasFooter());
  ASSERT_OK(log_->Close());

  shared_ptr<LogReader> reader;
  ASSERT_OK(LogReader::Open(fs_manager_.get(), NULL, kTestTablet, NULL, &reader));
  ASSERT_OK(reader->GetSegmentsSnapshot(&segments));

  ASSERT_TRUE(segments.back()->HasFooter());

  for (const scoped_refptr<ReadableLogSegment>& entry : segments) {
    Status s = entry->ReadEntries(&entries_);
    if (!s.ok()) {
      FAIL() << "Failed to read entries in segment: " << entry->path()
          << ". Status: " << s.ToString()
          << ".\nSegments: " << DumpSegmentsToString(segments);
    }
  }

  ASSERT_EQ(num_entries, entries_.size());
}

TEST_F(LogTest, TestWriteAndReadToAndFromInProgressSegment) {
  FLAGS_log_compression_codec = "none";

  const int kNumEntries = 4;
  ASSERT_OK(BuildLog());

  SegmentSequence segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(segments.size(), 1);
  scoped_refptr<ReadableLogSegment> readable_segment = segments[0];

  int header_size = log_->active_segment_->written_offset();
  ASSERT_GT(header_size, 0);
  readable_segment->UpdateReadableToOffset(header_size);

  vector<LogEntryPB*> entries;

  // Reading the readable segment now should return OK but yield no
  // entries.
  ASSERT_OK(readable_segment->ReadEntries(&entries));
  ASSERT_EQ(entries.size(), 0);

  // Dummy add_entry to help us estimate the size of what
  // gets written to disk.
  LogEntryBatchPB batch;
  OpId op_id = MakeOpId(1, 1);
  LogEntryPB* log_entry = batch.add_entry();
  log_entry->set_type(REPLICATE);
  ReplicateMsg* repl = log_entry->mutable_replicate();
  repl->mutable_id()->CopyFrom(op_id);
  repl->set_op_type(NO_OP);
  repl->set_timestamp(0L);

  // Entries are prefixed with a header.
  int64_t single_entry_size = batch.ByteSize() + kEntryHeaderSizeV2;

  int written_entries_size = header_size;
  ASSERT_OK(AppendNoOps(&op_id, kNumEntries, &written_entries_size));
  ASSERT_EQ(single_entry_size * kNumEntries + header_size, written_entries_size);
  ASSERT_EQ(written_entries_size, log_->active_segment_->written_offset());

  // Updating the readable segment with the offset of the first entry should
  // make it read a single entry even though there are several in the log.
  readable_segment->UpdateReadableToOffset(header_size + single_entry_size);
  ASSERT_OK(readable_segment->ReadEntries(&entries));
  ASSERT_EQ(entries.size(), 1);
  STLDeleteElements(&entries);

  // Now append another entry so that the Log sets the correct readable offset
  // on the reader.
  ASSERT_OK(AppendNoOps(&op_id, 1, &written_entries_size));

  // Now the reader should be able to read all 5 entries.
  ASSERT_OK(readable_segment->ReadEntries(&entries));
  ASSERT_EQ(entries.size(), 5);
  STLDeleteElements(&entries);

  // Offset should get updated for an additional entry.
  ASSERT_EQ(single_entry_size * (kNumEntries + 1) + header_size,
            written_entries_size);
  ASSERT_EQ(written_entries_size, log_->active_segment_->written_offset());

  // When we roll it should go back to the header size.
  ASSERT_OK(log_->AllocateSegmentAndRollOver());
  ASSERT_EQ(header_size, log_->active_segment_->written_offset());
  written_entries_size = header_size;

  // Now that we closed the original segment. If we get a segment from the reader
  // again, we should get one with a footer and we should be able to read all entries.
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(segments.size(), 2);
  readable_segment = segments[0];
  ASSERT_OK(readable_segment->ReadEntries(&entries));
  ASSERT_EQ(entries.size(), 5);
  STLDeleteElements(&entries);

  // Offset should get updated for an additional entry, again.
  ASSERT_OK(AppendNoOp(&op_id, &written_entries_size));
  ASSERT_EQ(single_entry_size  + header_size, written_entries_size);
  ASSERT_EQ(written_entries_size, log_->active_segment_->written_offset());
}

// Tests that segments can be GC'd while the log is running.
TEST_P(LogTestOptionalCompression, TestGCWithLogRunning) {
  FLAGS_log_min_segments_to_retain = 2;
  ASSERT_OK(BuildLog());

  vector<LogAnchor*> anchors;
  ElementDeleter deleter(&anchors);

  SegmentSequence segments;

  const int kNumTotalSegments = 4;
  const int kNumOpsPerSegment = 5;
  int num_gced_segments;
  OpId op_id = MakeOpId(1, 1);

  ASSERT_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, &anchors));

  // We should get 4 anchors, each pointing at the beginning of a new segment
  ASSERT_EQ(anchors.size(), 4);

  // Anchors should prevent GC.
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(4, segments.size()) << DumpSegmentsToString(segments);
  RetentionIndexes retention;
  ASSERT_OK(log_anchor_registry_->GetEarliestRegisteredLogIndex(&retention.for_durability));
  ASSERT_OK(log_->GC(retention, &num_gced_segments));
  ASSERT_EQ(0, num_gced_segments);
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(4, segments.size()) << DumpSegmentsToString(segments);

  // Logs should be retained for durability even if this puts it above the
  // maximum configured number of segments.
  {
    google::FlagSaver saver;
    FLAGS_log_min_segments_to_retain = 1;
    FLAGS_log_max_segments_to_retain = 1;
    ASSERT_OK(log_->GC(retention, &num_gced_segments));
    ASSERT_EQ(0, num_gced_segments);
  }

  // Freeing the first 2 anchors should allow GC of them.
  ASSERT_OK(log_anchor_registry_->Unregister(anchors[0]));
  ASSERT_OK(log_anchor_registry_->Unregister(anchors[1]));
  ASSERT_OK(log_anchor_registry_->GetEarliestRegisteredLogIndex(&retention.for_durability));
  // We should now be anchored on op 0.11, i.e. on the 3rd segment
  ASSERT_EQ(anchors[2]->log_index, retention.for_durability);

  // However, first, we'll try bumping the min retention threshold and
  // verify that we don't GC any.
  {
    google::FlagSaver saver;
    FLAGS_log_min_segments_to_retain = 10;
    ASSERT_OK(log_->GC(retention, &num_gced_segments));
    ASSERT_EQ(0, num_gced_segments);
  }

  // Try again without the modified flag.
  ASSERT_OK(log_->GC(retention, &num_gced_segments));
  ASSERT_EQ(2, num_gced_segments) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);

  // Release the remaining "rolled segment" anchor. GC will not delete the
  // last rolled segment.
  ASSERT_OK(log_anchor_registry_->Unregister(anchors[2]));
  ASSERT_OK(log_anchor_registry_->GetEarliestRegisteredLogIndex(&retention.for_durability));
  ASSERT_OK(log_->GC(retention, &num_gced_segments));
  ASSERT_EQ(0, num_gced_segments) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);

  // Check that we get a NotFound if we try to read before the GCed point.
  {
    vector<ReplicateMsg*> repls;
    ElementDeleter d(&repls);
    Status s = log_->reader()->ReadReplicatesInRange(
      1, 2, LogReader::kNoSizeLimit, &repls);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  ASSERT_OK(log_->Close());
  CheckRightNumberOfSegmentFiles(2);

  // We skip the first three, since we unregistered them above.
  for (int i = 3; i < kNumTotalSegments; i++) {
    ASSERT_OK(log_anchor_registry_->Unregister(anchors[i]));
  }
}

// Test that, when we are set to retain a given number of log segments,
// we also retain any relevant log index chunks, even if those operations
// are not necessary for recovery.
TEST_P(LogTestOptionalCompression, TestGCOfIndexChunks) {
  FLAGS_log_min_segments_to_retain = 4;
  ASSERT_OK(BuildLog());

  // Append some segments which cross from one index chunk into another.
  // 999990-999994        \___ the first index
  // 999995-999999        /    chunk points to these
  // 1000000-100004       \_
  // 1000005-100009        _|- the second index chunk points to these
  // 1000010-<still open> /
  const int kNumTotalSegments = 5;
  const int kNumOpsPerSegment = 5;
  OpId op_id = MakeOpId(1, 999990);
  ASSERT_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, nullptr));

  // Run a GC on an op in the second index chunk. We should remove only the
  // earliest segment, because we are set to retain 4.
  int num_gced_segments = 0;
  ASSERT_OK(log_->GC(RetentionIndexes(1000006, 1000006), &num_gced_segments));
  ASSERT_EQ(1, num_gced_segments);

  // And we should still be able to read ops in the retained segment, even though
  // the GC index was higher.
  OpId loaded_op;
  ASSERT_OK(log_->reader()->LookupOpId(999995, &loaded_op));
  ASSERT_EQ("1.999995", OpIdToString(loaded_op));

  // If we drop the retention count down to 1, we can now GC, and the log index
  // chunk should also be GCed.
  FLAGS_log_min_segments_to_retain = 1;
  ASSERT_OK(log_->GC(RetentionIndexes(1000003, 1000003), &num_gced_segments));
  ASSERT_EQ(1, num_gced_segments);

  Status s = log_->reader()->LookupOpId(999995, &loaded_op);
  ASSERT_TRUE(s.IsNotFound()) << "unexpected status: " << s.ToString();
}

// Tests that we can append FLUSH_MARKER messages to the log queue to make sure
// all messages up to a certain point were fsync()ed without actually
// writing them to the log.
TEST_P(LogTestOptionalCompression, TestWaitUntilAllFlushed) {
  ASSERT_OK(BuildLog());
  // Append 2 replicate/commit pairs asynchronously
  AppendReplicateBatchAndCommitEntryPairsToLog(2, APPEND_ASYNC);

  ASSERT_OK(log_->WaitUntilAllFlushed());

  // Make sure we only get 4 entries back and that no FLUSH_MARKER commit is found.
  vector<scoped_refptr<ReadableLogSegment> > segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));

  ASSERT_OK(segments[0]->ReadEntries(&entries_));
  ASSERT_EQ(entries_.size(), 4);
  for (int i = 0; i < 4 ; i++) {
    if (i % 2 == 0) {
      ASSERT_TRUE(entries_[i]->has_replicate());
    } else {
      ASSERT_TRUE(entries_[i]->has_commit());
      ASSERT_EQ(WRITE_OP, entries_[i]->commit().op_type());
    }
  }
}

// Tests log reopening and that GC'ing the old log's segments works.
TEST_P(LogTestOptionalCompression, TestLogReopenAndGC) {
  ASSERT_OK(BuildLog());

  SegmentSequence segments;

  vector<LogAnchor*> anchors;
  ElementDeleter deleter(&anchors);

  const int kNumTotalSegments = 3;
  const int kNumOpsPerSegment = 5;
  int num_gced_segments;
  OpId op_id = MakeOpId(1, 1);
  ASSERT_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, &anchors));
  // Anchors should prevent GC.
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(3, segments.size());
  RetentionIndexes retention;
  ASSERT_OK(log_anchor_registry_->GetEarliestRegisteredLogIndex(&retention.for_durability));
  ASSERT_OK(log_->GC(retention, &num_gced_segments));
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(3, segments.size());

  ASSERT_OK(log_->Close());

  // Now reopen the log as if we had replayed the state into the stores.
  // that were in memory and do GC.
  ASSERT_OK(BuildLog());

  // The "old" data consists of 3 segments. We still hold anchors.
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(4, segments.size());

  // Write to a new log segment, as if we had taken new requests and the
  // mem stores are holding anchors, but don't roll it.
  CreateAndRegisterNewAnchor(op_id.index(), &anchors);
  ASSERT_OK(AppendNoOps(&op_id, kNumOpsPerSegment));

  // Now release the "old" anchors and GC them.
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(log_anchor_registry_->Unregister(anchors[i]));
  }
  ASSERT_OK(log_anchor_registry_->GetEarliestRegisteredLogIndex(&retention.for_durability));

  // If we set the 'for_peers' index to indicate that these log
  // segments are needed for catchup, that will prevent GC,
  // even though they're no longer necessary for durability.
  retention.for_peers = 0;
  ASSERT_OK(log_->GC(retention, &num_gced_segments));
  ASSERT_EQ(0, num_gced_segments);
  CheckRightNumberOfSegmentFiles(4);

  // Set the max segments to retain so that, even though we have peers who need
  // the segments, we'll GC them.
  FLAGS_log_max_segments_to_retain = 2;
  ASSERT_OK(log_->GC(retention, &num_gced_segments));
  ASSERT_EQ(2, num_gced_segments);

  // After GC there should be only one left, besides the one currently being
  // written to. That is because min_segments_to_retain defaults to 2.
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->Close());

  CheckRightNumberOfSegmentFiles(2);

  // Unregister the final anchor.
  ASSERT_OK(log_anchor_registry_->Unregister(anchors[3]));
}

// Helper to measure the performance of the log.
TEST_P(LogTestOptionalCompression, TestWriteManyBatches) {
  uint64_t num_batches = 10;
  if (AllowSlowTests()) {
    num_batches = FLAGS_num_batches;
  }
  ASSERT_OK(BuildLog());

  LOG(INFO)<< "Starting to write " << num_batches << " to log";
  LOG_TIMING(INFO, "Wrote all batches to log") {
    AppendReplicateBatchAndCommitEntryPairsToLog(num_batches);
  }
  ASSERT_OK(log_->Close());
  LOG(INFO) << "Done writing";

  LOG_TIMING(INFO, "Read all entries from Log") {
    LOG(INFO) << "Starting to read log";
    uint32_t num_entries = 0;

    vector<scoped_refptr<ReadableLogSegment> > segments;

    shared_ptr<LogReader> reader;
    ASSERT_OK(LogReader::Open(fs_manager_.get(), NULL, kTestTablet, NULL, &reader));
    ASSERT_OK(reader->GetSegmentsSnapshot(&segments));

    for (const scoped_refptr<ReadableLogSegment>& entry : segments) {
      STLDeleteElements(&entries_);
      ASSERT_OK(entry->ReadEntries(&entries_));
      num_entries += entries_.size();
    }
    ASSERT_EQ(num_entries, num_batches * 2);
    LOG(INFO) << "End readfile";
  }
}

// This tests that querying LogReader works.
// This sets up a reader with some segments to query which amount to the
// following:
// seg002: 0.10 through 0.19
// seg003: 0.20 through 0.29
// seg004: 0.30 through 0.39
TEST_P(LogTestOptionalCompression, TestLogReader) {
  LogReader reader(env_,
                   scoped_refptr<LogIndex>(),
                   kTestTablet,
                   nullptr);
  reader.InitEmptyReaderForTests();
  ASSERT_OK(AppendNewEmptySegmentToReader(2, 10, &reader));
  ASSERT_OK(AppendNewEmptySegmentToReader(3, 20, &reader));
  ASSERT_OK(AppendNewEmptySegmentToReader(4, 30, &reader));

  OpId op;
  op.set_term(0);
  SegmentSequence segments;

  // Queries for specific segment sequence numbers.
  scoped_refptr<ReadableLogSegment> segment = reader.GetSegmentBySequenceNumber(2);
  ASSERT_EQ(2, segment->header().sequence_number());
  segment = reader.GetSegmentBySequenceNumber(3);
  ASSERT_EQ(3, segment->header().sequence_number());

  segment = reader.GetSegmentBySequenceNumber(4);
  ASSERT_EQ(4, segment->header().sequence_number());

  segment = reader.GetSegmentBySequenceNumber(5);
  ASSERT_TRUE(segment.get() == nullptr);
}

// Test that, even if the LogReader's index is empty because no segments
// have been properly closed, we can still read the entries as the reader
// returns the current segment.
TEST_P(LogTestOptionalCompression, TestLogReaderReturnsLatestSegmentIfIndexEmpty) {
  ASSERT_OK(BuildLog());

  OpId opid = MakeOpId(1, 1);
  AppendCommit(opid, APPEND_ASYNC);
  AppendReplicateBatch(opid, APPEND_SYNC);

  SegmentSequence segments;
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(segments.size(), 1);

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  ASSERT_OK(segments[0]->ReadEntries(&entries));
  ASSERT_EQ(2, entries.size());
}

TEST_F(LogTest, TestOpIdUtils) {
  OpId id = MakeOpId(1, 2);
  ASSERT_EQ("1.2", consensus::OpIdToString(id));
  ASSERT_EQ(1, id.term());
  ASSERT_EQ(2, id.index());
}

std::ostream& operator<<(std::ostream& os, const TestLogSequenceElem& elem) {
  switch (elem.type) {
    case TestLogSequenceElem::ROLL:
      os << "ROLL";
      break;
    case TestLogSequenceElem::REPLICATE:
      os << "R" << elem.id;
      break;
    case TestLogSequenceElem::COMMIT:
      os << "C" << elem.id;
      break;
  }
  return os;
}

// Generates a plausible sequence of items in the log, including term changes, moving the
// index backwards, log rolls, etc.
//
// NOTE: this log sequence may contain some aberrations which would not occur in a real
// consensus log, but our API supports them. In the future we may want to add assertions
// to the Log implementation that prevent such aberrations, in which case we'd need to
// modify this.
void LogTest::GenerateTestSequence(Random* rng, int seq_len,
                                   vector<TestLogSequenceElem>* ops,
                                   vector<int64_t>* terms_by_index) {
  terms_by_index->assign(seq_len + 1L, -1L);
  int64_t committed_index = 0;
  int64_t max_repl_index = 0;

  OpId id = MakeOpId(1, 0);
  for (int i = 0; i < seq_len; i++) {
    if (rng->OneIn(5)) {
      // Reset term - it may stay the same, or go up/down
      id.set_term(std::max(static_cast<int64_t>(1), id.term() + rng->Uniform(5) - 2));
    }

    // Advance index by exactly one
    id.set_index(id.index() + 1);

    if (rng->OneIn(5)) {
      // Move index backward a bit, but not past the committed index
      id.set_index(std::max(committed_index + 1, id.index() - rng->Uniform(5)));
    }

    // Roll the log sometimes
    if (i != 0 && rng->OneIn(15)) {
      TestLogSequenceElem op;
      op.type = TestLogSequenceElem::ROLL;
      ops->push_back(op);
    }

    TestLogSequenceElem op;
    op.type = TestLogSequenceElem::REPLICATE;
    op.id = id;
    ops->push_back(op);
    (*terms_by_index)[id.index()] = id.term();
    max_repl_index = std::max(max_repl_index, id.index());

    // Advance the commit index sometimes
    if (rng->OneIn(5)) {
      while (committed_index < id.index()) {
        committed_index++;
        TestLogSequenceElem op;
        op.type = TestLogSequenceElem::COMMIT;
        op.id = MakeOpId((*terms_by_index)[committed_index], committed_index);
        ops->push_back(op);
      }
    }
  }
  terms_by_index->resize(max_repl_index + 1);
}

void LogTest::AppendTestSequence(const vector<TestLogSequenceElem>& seq) {
  for (const TestLogSequenceElem& e : seq) {
    VLOG(1) << "Appending: " << e;
    switch (e.type) {
      case TestLogSequenceElem::REPLICATE:
      {
        OpId id(e.id);
        ASSERT_OK(AppendNoOp(&id));
        break;
      }
      case TestLogSequenceElem::COMMIT:
      {
        gscoped_ptr<CommitMsg> commit(new CommitMsg);
        commit->set_op_type(NO_OP);
        commit->mutable_commited_op_id()->CopyFrom(e.id);
        Synchronizer s;
        ASSERT_OK(log_->AsyncAppendCommit(std::move(commit), s.AsStatusCallback()));
        ASSERT_OK(s.Wait());
      }
      case TestLogSequenceElem::ROLL:
      {
        ASSERT_OK(RollLog());
      }
    }
  }
}

static int RandInRange(Random* r, int min_inclusive, int max_inclusive) {
  int width = max_inclusive - min_inclusive + 1;
  return min_inclusive + r->Uniform(width);
}

// Test that if multiple REPLICATE entries are written for the same index,
// that we read the latest one.
//
// This is a randomized test: we generate a plausible sequence of log messages,
// write it out, and then read random ranges of log indexes, making sure we
// always see the correct term for each REPLICATE message (i.e whichever term
// was the last to append it).
TEST_P(LogTestOptionalCompression, TestReadLogWithReplacedReplicates) {
  const int kSequenceLength = AllowSlowTests() ? 1000 : 50;

  Random rng(SeedRandom());
  vector<int64_t> terms_by_index;
  vector<TestLogSequenceElem> seq;
  GenerateTestSequence(&rng, kSequenceLength, &seq, &terms_by_index);
  LOG(INFO) << "test sequence: " << seq;
  const int64_t max_repl_index = terms_by_index.size() - 1;
  LOG(INFO) << "max_repl_index: " << max_repl_index;

  // Write the test sequence to the log.
  // TODO: should consider adding batching here of multiple replicates
  ASSERT_OK(BuildLog());
  AppendTestSequence(seq);

  const int kNumRandomReads = 100;

  // We'll advance 'gc_index' randomly through the log until we've gotten to
  // the end. This ensures that, when we GC, we don't ever remove the latest
  // version of a replicate message unintentionally.
  shared_ptr<LogReader> reader = log_->reader();
  for (int gc_index = 1; gc_index < max_repl_index;) {
    SCOPED_TRACE(Substitute("after GCing $0", gc_index));

    // Test reading random ranges of indexes and verifying that we get back the
    // REPLICATE messages with the correct terms
    for (int random_read = 0; random_read < kNumRandomReads; random_read++) {
      int start_index = RandInRange(&rng, gc_index, max_repl_index - 1);
      int end_index = RandInRange(&rng, start_index, max_repl_index);
      {
        SCOPED_TRACE(Substitute("Reading $0-$1", start_index, end_index));
        vector<ReplicateMsg*> repls;
        ElementDeleter d(&repls);
        ASSERT_OK(log_->reader()->ReadReplicatesInRange(
                    start_index, end_index, LogReader::kNoSizeLimit, &repls));
        ASSERT_EQ(end_index - start_index + 1, repls.size());
        int expected_index = start_index;
        for (const ReplicateMsg* repl : repls) {
          ASSERT_EQ(expected_index, repl->id().index());
          ASSERT_EQ(terms_by_index[expected_index], repl->id().term());
          expected_index++;
        }
      }

      int64_t bytes_read = reader->bytes_read_->value();
      int64_t entries_read = reader->entries_read_->value();
      int64_t read_batch_count = reader->read_batch_latency_->TotalCount();
      EXPECT_GT(reader->bytes_read_->value(), 0);
      EXPECT_GT(reader->entries_read_->value(), 0);
      EXPECT_GT(reader->read_batch_latency_->TotalCount(), 0);

      // Test a size-limited read.
      int size_limit = RandInRange(&rng, 1, 1000);
      {
        SCOPED_TRACE(Substitute("Reading $0-$1 with size limit $2",
                                start_index, end_index, size_limit));
        vector<ReplicateMsg*> repls;
        ElementDeleter d(&repls);
        ASSERT_OK(reader->ReadReplicatesInRange(start_index, end_index, size_limit, &repls));
        ASSERT_LE(repls.size(), end_index - start_index + 1);
        int total_size = 0;
        int expected_index = start_index;
        for (const ReplicateMsg* repl : repls) {
          ASSERT_EQ(expected_index, repl->id().index());
          ASSERT_EQ(terms_by_index[expected_index], repl->id().term());
          expected_index++;
          total_size += repl->SpaceUsed();
        }
        if (total_size > size_limit) {
          ASSERT_EQ(1, repls.size());
        } else {
          ASSERT_LE(total_size, size_limit);
        }
      }

      EXPECT_GT(reader->bytes_read_->value(), bytes_read);
      EXPECT_GT(reader->entries_read_->value(), entries_read);
      EXPECT_GT(reader->read_batch_latency_->TotalCount(), read_batch_count);
    }

    int num_gced = 0;
    ASSERT_OK(log_->GC(RetentionIndexes(gc_index), &num_gced));
    gc_index += rng.Uniform(10);
  }
}

// Ensure that we can read replicate messages from the LogReader with a very
// high (> 32 bit) log index and term. Regression test for KUDU-1933.
TEST_P(LogTestOptionalCompression, TestReadReplicatesHighIndex) {
  const int64_t first_log_index = std::numeric_limits<int32_t>::max() - 3;
  const int kSequenceLength = 10;

  ASSERT_OK(BuildLog());
  OpId op_id;
  op_id.set_term(first_log_index);
  op_id.set_index(first_log_index);
  ASSERT_OK(AppendNoOps(&op_id, kSequenceLength));

  shared_ptr<LogReader> reader = log_->reader();
  vector<ReplicateMsg*> replicates;
  ElementDeleter deleter(&replicates);
  ASSERT_OK(reader->ReadReplicatesInRange(first_log_index, first_log_index + kSequenceLength - 1,
                                          LogReader::kNoSizeLimit, &replicates));
  ASSERT_EQ(kSequenceLength, replicates.size());
  ASSERT_GT(op_id.index(), std::numeric_limits<int32_t>::max());
}

// Test various situations where we expect different segments depending on what the
// min log index is.
TEST_F(LogTest, TestGetGCableDataSize) {
  FLAGS_log_compression_codec = "none";
  FLAGS_log_min_segments_to_retain = 2;
  ASSERT_OK(BuildLog());

  const int kNumTotalSegments = 5;
  const int kNumOpsPerSegment = 5;
  const int kSegmentSizeBytes = 331;
  OpId op_id = MakeOpId(1, 10);
  // Create 5 segments, starting from log index 10, with 5 ops per segment.
  // [10-14], [15-19], [20-24], [25-29], [30-34]
  ASSERT_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, nullptr));

  // GCing through the first op should not be able to remove any logs.
  EXPECT_EQ(0, log_->GetGCableDataSize(RetentionIndexes(10)));

  // Check that even when the min index is the last index from the oldest segment,
  // we still return the same result.
  EXPECT_EQ(0, log_->GetGCableDataSize(RetentionIndexes(14)));

  // Check that if the first segment is GCable, we return its size.
  EXPECT_EQ(kSegmentSizeBytes, log_->GetGCableDataSize(RetentionIndexes(15)));

  // GCable index at the end of the third segment. Should only be able to GC the first
  // two.
  EXPECT_EQ(kSegmentSizeBytes * 2, log_->GetGCableDataSize(RetentionIndexes(24)));

  // GCing through the first op in the fourth segment should be able to remove
  // the first three.
  EXPECT_EQ(kSegmentSizeBytes * 3, log_->GetGCableDataSize(RetentionIndexes(25)));

  // Even if we could GC all of the ops written, we should respect the 'log_min_segments_to_retain'
  // setting and not GC the last two.
  EXPECT_EQ(kSegmentSizeBytes * 3, log_->GetGCableDataSize(RetentionIndexes(35)));

  // If we change the configuration, we should be able to GC all of the closed segments.
  // The last segment is not GCable because it is still open.
  FLAGS_log_min_segments_to_retain = 0;
  EXPECT_EQ(kSegmentSizeBytes * 4, log_->GetGCableDataSize(RetentionIndexes(35)));
}

// Regression test. Check that failed preallocation returns an error instead of
// hanging.
TEST_F(LogTest, TestFailedLogPreAllocation) {
  options_.async_preallocate_segments = false;
  ASSERT_OK(BuildLog());

  log_->SetMaxSegmentSizeForTests(1);
  FLAGS_log_inject_io_error_on_preallocate_fraction = 1.0;
  OpId opid = MakeOpId(1, 1);
  Status s = AppendNoOp(&opid);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Injected IOError");
}

// Test the enforcement of reserving disk space for the log.
TEST_F(LogTest, TestDiskSpaceCheck) {
  FLAGS_fs_wal_dir_reserved_bytes = 1; // Keep at least 1 byte reserved in the FS.
  FLAGS_disk_reserved_bytes_free_for_testing = 0;
  options_.segment_size_mb = 1;
  Status s = BuildLog();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(ENOSPC, s.posix_code());
  ASSERT_STR_CONTAINS(s.ToString(), "Insufficient disk space");

  FLAGS_disk_reserved_bytes_free_for_testing = 2 * 1024 * 1024;
  ASSERT_OK(BuildLog());

  // TODO: We don't currently do bookkeeping to ensure that we check if the
  // disk is past its quota if we write beyond the preallocation limit for a
  // single segment. If we did that, we could ensure that we check once we
  // detect that we are past the preallocation limit.
}

// Test that the append thread shuts itself down after it's idle.
TEST_F(LogTest, TestAutoStopIdleAppendThread) {
  ASSERT_OK(BuildLog());
  OpId opid = MakeOpId(1, 1);

  // Append something to the queue and ensure that the thread starts itself.
  // We loop here in case for some reason this thread gets de-scheduled just
  // after the append long enough for the append thread to shut itself down
  // again.
  ASSERT_EVENTUALLY([&]() {
      AppendNoOpsToLogSync(clock_, log_.get(), &opid, 2);
      ASSERT_TRUE(log_->append_thread_active_for_tests());
      debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
      ASSERT_GT(log_->active_segment_->compress_buf_.capacity(), faststring::kInitialCapacity);
    });
  // After some time, the append thread should shut itself down.
  ASSERT_EVENTUALLY([&]() {
      ASSERT_FALSE(log_->append_thread_active_for_tests());
    });

  // The log should free its buffer once it is idle.
  {
    debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
    ASSERT_EQ(faststring::kInitialCapacity, log_->active_segment_->compress_buf_.capacity());
  }
}

// Test that Log::TotalSize() captures creation, addition, and deletion of log segments.
TEST_P(LogTestOptionalCompression, TestTotalSize) {
  // Build a log. There is an active segment, so on-disk size should be positive.
  ASSERT_OK(BuildLog());
  int64_t one_segment_size = log_->OnDiskSize();
  ASSERT_GT(one_segment_size, 0);

  // Append entries and roll over to new segments.
  vector<LogAnchor*> anchors;
  ElementDeleter deleter(&anchors);
  SegmentSequence segments;
  const int kNumTotalSegments = 3;
  const int kNumOpsPerSegment = 2;

  OpId op_id = MakeOpId(1, 1);
  ASSERT_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                       &op_id, &anchors));
  ASSERT_EQ(3, anchors.size());

  // Now that there's multiple segments, the total size should be larger than
  // the one-segment size.
  int64_t three_segment_size = log_->OnDiskSize();
  ASSERT_GT(three_segment_size, one_segment_size);

  // Free an anchor so we can GC the segment it points to.
  RetentionIndexes retention;
  ASSERT_OK(log_anchor_registry_->Unregister(anchors[0]));
  ASSERT_OK(log_anchor_registry_->GetEarliestRegisteredLogIndex(&retention.for_durability));
  int num_gced_segments;
  ASSERT_OK(log_->GC(retention, &num_gced_segments));
  ASSERT_EQ(1, num_gced_segments) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->reader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);

  // Now we've added two segments and GC'd one, so the total size should be
  // between the one-segment size and the three-segment size.
  int64_t two_segment_size = log_->OnDiskSize();
  ASSERT_LT(two_segment_size, three_segment_size);
  ASSERT_GT(two_segment_size, one_segment_size);

  // Cleanup: close the log and unregister the remaining registered anchors.
  ASSERT_OK(log_->Close());
  for (int i = 1; i < kNumTotalSegments; i++) {
    ASSERT_OK(log_anchor_registry_->Unregister(anchors[i]));
  }
}

} // namespace log
} // namespace kudu
