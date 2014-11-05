// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <glog/stl_logging.h>
#include <vector>

#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/random.h"

DEFINE_int32(num_batches, 10000,
             "Number of batches to write to/read from the Log in TestWriteManyBatches");

DECLARE_int32(log_min_segments_to_retain);

namespace kudu {
namespace log {

using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using consensus::MakeOpId;
using consensus::MinimumOpId;
using strings::Substitute;

extern const char* kTestTable;
extern const char* kTestTablet;

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
  void CreateAndRegisterNewAnchor(const OpId& op_id, vector<OpIdAnchor*>* anchors) {
    anchors->push_back(new OpIdAnchor());
    opid_anchor_registry_->Register(op_id, CURRENT_TEST_NAME(), anchors->back());
  }

  // Create a series of NO_OP entries in the log.
  // Anchor each segment on the first OpId of each log segment,
  // and update op_id to point to the next valid OpId.
  Status AppendMultiSegmentSequence(int num_total_segments, int num_ops_per_segment,
                                    OpId* op_id, vector<OpIdAnchor*>* anchors) {
    CHECK(op_id->IsInitialized());
    for (int i = 0; i < num_total_segments - 1; i++) {
      CreateAndRegisterNewAnchor(*op_id, anchors);
      RETURN_NOT_OK(AppendNoOps(op_id, num_ops_per_segment));
      RETURN_NOT_OK(RollLog());
    }

    CreateAndRegisterNewAnchor(*op_id, anchors);
    RETURN_NOT_OK(AppendNoOps(op_id, num_ops_per_segment));
    return Status::OK();
  }

  Status AppendNewEmptySegmentToReader(int sequence_number,
                                       int first_repl_index,
                                       LogReader* reader) {
    string fqp = GetTestPath(strings::Substitute("wal-00000000$0", sequence_number));
    gscoped_ptr<WritableFile> w_log_seg;
    RETURN_NOT_OK(fs_manager_->env()->NewWritableFile(fqp, &w_log_seg));
    gscoped_ptr<RandomAccessFile> r_log_seg;
    RETURN_NOT_OK(fs_manager_->env()->NewRandomAccessFile(fqp, &r_log_seg));

    scoped_refptr<ReadableLogSegment> readable_segment(
        new ReadableLogSegment(fqp, shared_ptr<RandomAccessFile>(r_log_seg.release())));

    LogSegmentHeaderPB header;
    header.set_sequence_number(sequence_number);
    header.set_major_version(0);
    header.set_minor_version(0);
    header.set_tablet_id(kTestTablet);
    SchemaToPB(GetSimpleTestSchema(), header.mutable_schema());

    LogSegmentFooterPB footer;
    footer.set_num_entries(10);
    footer.set_min_replicate_index(first_repl_index);
    footer.set_max_replicate_index(first_repl_index + 9);

    RETURN_NOT_OK(readable_segment->Init(header, footer, 0));
    RETURN_NOT_OK(reader->AppendSegment(readable_segment));
    return Status::OK();
  }

  void GenerateTestSequence(Random* rng, int seq_len,
                            vector<TestLogSequenceElem>* ops,
                            vector<int64_t>* terms_by_index);
  void AppendTestSequence(const vector<TestLogSequenceElem>& seq);
};

// If we write more than one entry in a batch, we should be able to
// read all of those entries back.
TEST_F(LogTest, TestMultipleEntriesInABatch) {
  BuildLog();

  OpId opid;
  opid.set_term(0);
  opid.set_index(1);

  AppendNoOps(&opid, 2);

  // RollOver() the batch so that we have a properly formed footer.
  ASSERT_STATUS_OK(log_->AllocateSegmentAndRollOver());

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  SegmentSequence segments;
  ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));

  ASSERT_STATUS_OK(segments[0]->ReadEntries(&entries));

  ASSERT_EQ(2, entries.size());

  ASSERT_STATUS_OK(log_->Close());
}

// Tests that everything works properly with fsync enabled:
// This also tests SyncDir() (see KUDU-261), which is called whenever
// a new log segment is initialized.
TEST_F(LogTest, TestFsync) {
  options_.force_fsync_all = true;
  BuildLog();

  OpId opid;
  opid.set_term(0);
  opid.set_index(1);

  AppendNoOp(&opid);

  ASSERT_STATUS_OK(log_->Close());
}

// Test that the reader can read from the log even if it hasn't been
// properly closed.
TEST_F(LogTest, TestLogNotTrimmed) {
  BuildLog();

  OpId opid;
  opid.set_term(0);
  opid.set_index(1);

  AppendNoOp(&opid);

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  SegmentSequence segments;
  ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));

  ASSERT_STATUS_OK(segments[0]->ReadEntries(&entries));
  // Close after testing to ensure correct shutdown
  // TODO : put this in TearDown() with a test on log state?
  ASSERT_STATUS_OK(log_->Close());
}

// Test that the reader will not fail if a log file is completely blank.
// This happens when it's opened but nothing has been written.
// The reader should gracefully handle this situation, but somehow expose that
// the segment is uninitialized. See KUDU-140.
TEST_F(LogTest, TestBlankLogFile) {
  BuildLog();

  // The log's reader will have a segment...
  ASSERT_EQ(log_->GetLogReader()->num_segments(), 1);

  // ...and we're able to read from it.
  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  SegmentSequence segments;
  ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));

  ASSERT_STATUS_OK(segments[0]->ReadEntries(&entries));

  // ...It's just that it's empty.
  ASSERT_EQ(entries.size(), 0);
}

// Tests that the log reader reads up until some corrupt entry is found.
// TODO: Test partially written/corrupt headers.
TEST_F(LogTest, TestCorruptLog) {
  const int kNumEntries = 4;
  BuildLog();
  OpId op_id(MinimumOpId());
  ASSERT_OK(AppendNoOps(&op_id, kNumEntries));
  ASSERT_STATUS_OK(log_->Close());

  ASSERT_STATUS_OK(CorruptLogFile(env_.get(), log_.get(), 30));

  ASSERT_EQ(1, log_->GetLogReader()->num_segments());

  SegmentSequence segments;
  ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));

  Status status = segments[0]->ReadEntries(&entries_);
  ASSERT_TRUE(status.IsCorruption());

  // Last entry is corrupted but we should still see the previous ones.
  ASSERT_EQ(kNumEntries - 1, entries_.size());
}

// Tests that segments roll over when max segment size is reached
// and that the player plays all entries in the correct order.
TEST_F(LogTest, TestSegmentRollover) {
  BuildLog();
  // Set a small segment size so that we have roll overs.
  log_->SetMaxSegmentSizeForTests(990);
  const int kNumEntriesPerBatch = 100;

  OpId op_id(MinimumOpId());
  int num_entries = 0;

  SegmentSequence segments;
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));

  while (segments.size() < 3) {
    ASSERT_OK(AppendNoOps(&op_id, kNumEntriesPerBatch));
    num_entries += kNumEntriesPerBatch;
    // Update the segments
    ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
  }

  ASSERT_FALSE(segments.back()->HasFooter());
  ASSERT_OK(log_->Close());

  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_TRUE(segments.back()->HasFooter());

  BOOST_FOREACH(const scoped_refptr<ReadableLogSegment>& entry, segments) {
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
  const int kNumEntries = 4;
  BuildLog();

  SegmentSequence segments;
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
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
  OpId op_id(MinimumOpId());
  LogEntryPB* log_entry = batch.add_entry();
  log_entry->set_type(REPLICATE);
  ReplicateMsg* repl = log_entry->mutable_replicate();
  repl->mutable_id()->CopyFrom(op_id);
  repl->set_op_type(NO_OP);

  // Entries are length-prefix encoded so add an additional 4
  // bytes.
  int single_entry_size = batch.ByteSize() + 4;

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
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
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
TEST_F(LogTest, TestGCWithLogRunning) {
  BuildLog();

  vector<OpIdAnchor*> anchors;
  ElementDeleter deleter(&anchors);

  SegmentSequence segments;

  const int kNumTotalSegments = 4;
  const int kNumOpsPerSegment = 5;
  int num_gced_segments;
  OpId op_id(MinimumOpId());
  OpId anchored_opid;

  ASSERT_STATUS_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, &anchors));

  // We should get 4 anchors, each pointing at the beginning of a new segment
  ASSERT_EQ(anchors.size(), 4);

  // Anchors should prevent GC.
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(4, segments.size()) << DumpSegmentsToString(segments);
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid.index(), &num_gced_segments));
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(4, segments.size()) << DumpSegmentsToString(segments);

  // Freeing the first 2 anchors should allow GC of them.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[0]));
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[1]));
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  // We should now be anchored on op 0.10, i.e. on the 3rd segment
  ASSERT_TRUE(consensus::OpIdEquals(anchors[2]->op_id, anchored_opid));

  // However, first, we'll try bumping the min retention threshold and
  // verify that we don't GC any.
  {
    google::FlagSaver saver;
    FLAGS_log_min_segments_to_retain = 10;
    ASSERT_STATUS_OK(log_->GC(anchored_opid.index(), &num_gced_segments));
    ASSERT_EQ(0, num_gced_segments);
  }

  // Try again without the modified flag.
  ASSERT_STATUS_OK(log_->GC(anchored_opid.index(), &num_gced_segments));
  ASSERT_EQ(2, num_gced_segments) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);

  // Release the remaining "rolled segment" anchor. GC will not delete the
  // last rolled segment.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[2]));
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid.index(), &num_gced_segments));
  ASSERT_EQ(0, num_gced_segments) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);

  // Check that we get a NotFound if we try to read before the GCed point.
  {
    vector<ReplicateMsg*> repls;
    ElementDeleter d(&repls);
    Status s = log_->GetLogReader()->ReadAllReplicateEntries(1, 2, &repls);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  ASSERT_STATUS_OK(log_->Close());
  CheckRightNumberOfSegmentFiles(2);

  // We skip the first three, since we unregistered them above.
  for (int i = 3; i < kNumTotalSegments; i++) {
    ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[i]));
  }
}

// Tests that we can append FLUSH_MARKER messages to the log queue to make sure
// all messages up to a certain point were fsync()ed without actually
// writing them to the log.
TEST_F(LogTest, TestWaitUntilAllFlushed) {
  BuildLog();
  // Append 4 replicate/commit pairs asynchronously
  AppendReplicateBatchAndCommitEntryPairsToLog(2, APPEND_ASYNC);

  ASSERT_STATUS_OK(log_->WaitUntilAllFlushed());

  // Make sure we only get 4 entries back and that no FLUSH_MARKER commit is found.
  vector<scoped_refptr<ReadableLogSegment> > segments;
  ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));

  ASSERT_STATUS_OK(segments[0]->ReadEntries(&entries_));
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
TEST_F(LogTest, TestLogReopenAndGC) {
  BuildLog();

  SegmentSequence segments;

  vector<OpIdAnchor*> anchors;
  ElementDeleter deleter(&anchors);

  const int kNumTotalSegments = 3;
  const int kNumOpsPerSegment = 5;
  int num_gced_segments;
  OpId op_id(MinimumOpId());
  OpId anchored_opid;

  ASSERT_STATUS_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, &anchors));
  // Anchors should prevent GC.
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(3, segments.size());
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid.index(), &num_gced_segments));
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(3, segments.size());

  ASSERT_STATUS_OK(log_->Close());

  // Now reopen the log as if we had replayed the state into the stores.
  // that were in memory and do GC.
  BuildLog();

  // The "old" data consists of 3 segments. We still hold anchors.
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(4, segments.size());

  // Write to a new log segment, as if we had taken new requests and the
  // mem stores are holding anchors, but don't roll it.
  CreateAndRegisterNewAnchor(op_id, &anchors);
  ASSERT_STATUS_OK(AppendNoOps(&op_id, kNumOpsPerSegment));

  // Now release the "old" anchors and GC them.
  for (int i = 0; i < 3; i++) {
    ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[i]));
  }
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid.index(), &num_gced_segments));

  // After GC there should be only one left, besides the one currently being
  // written to. That is because min_segments_to_retain defaults to 2.
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);
  ASSERT_STATUS_OK(log_->Close());

  CheckRightNumberOfSegmentFiles(2);

  // Unregister the final anchor.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[3]));
}

// Helper to measure the performance of the log.
TEST_F(LogTest, TestWriteManyBatches) {
  uint64_t num_batches = 10;
  if (AllowSlowTests()) {
    num_batches = FLAGS_num_batches;
  }
  BuildLog();

  LOG(INFO)<< "Starting to write " << num_batches << " to log";
  LOG_TIMING(INFO, "Wrote all batches to log") {
    AppendReplicateBatchAndCommitEntryPairsToLog(num_batches);
  }
  ASSERT_STATUS_OK(log_->Close());
  LOG(INFO) << "Done writing";

  LOG_TIMING(INFO, "Read all entries from Log") {
    LOG(INFO) << "Starting to read log";
    uint32_t num_entries = 0;

    vector<scoped_refptr<ReadableLogSegment> > segments;
    ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));

    BOOST_FOREACH(const scoped_refptr<ReadableLogSegment> entry, segments) {
      STLDeleteElements(&entries_);
      ASSERT_STATUS_OK(entry->ReadEntries(&entries_));
      num_entries += entries_.size();
    }
    ASSERT_EQ(num_entries, num_batches * 2);
    LOG(INFO) << "End readfile";
  }
}

// Test that, if we log a COMMIT before its REPLICATE, it will be buffered
// and only written out once the REPLICATE has been written.
TEST_F(LogTest, TestCommitsAreBufferedUntilReplicates) {
  BuildLog();

  AppendCommit(1, APPEND_ASYNC);
  AppendReplicateBatch(1, APPEND_SYNC);
  log_->WaitUntilAllFlushed();

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  SegmentSequence segments;
  ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_STATUS_OK(segments[0]->ReadEntries(&entries));

  // We enqueued the Commit before the Replicate, but it should have
  // buffered it until the Replicate arrived.
  ASSERT_EQ(entries.size(), 2);
  EXPECT_EQ(REPLICATE, entries[0]->type()) << entries[0]->ShortDebugString();
  EXPECT_EQ(COMMIT, entries[1]->type())  << entries[1]->ShortDebugString();

  log_->Close();
}

// This tests that querying LogReader works.
// This sets up a reader with some segments to query which amount to the
// following:
// seg002: 0.10 through 0.19
// seg003: 0.20 through 0.29
// seg004: 0.30 through 0.39
TEST_F(LogTest, TestLogReader) {
  LogReader reader(fs_manager_.get(), kTestTablet);
  reader.InitEmptyReaderForTests();
  ASSERT_STATUS_OK(AppendNewEmptySegmentToReader(2, 10, &reader));
  ASSERT_STATUS_OK(AppendNewEmptySegmentToReader(3, 20, &reader));
  ASSERT_STATUS_OK(AppendNewEmptySegmentToReader(4, 30, &reader));

  OpId op;
  op.set_term(0);
  SegmentSequence segments;

  // Queries for segment prefixes (used for GC)

  // Asking the reader the prefix of segments that does not include op 1
  // should return the empty set.
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(1, &segments));
  ASSERT_TRUE(segments.empty());

  // .. same for op 10
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(10, &segments));
  ASSERT_TRUE(segments.empty());

  // Asking for the prefix of segments not including op 20 should return
  // the first segment, since 20 is the first operation in segment 3.
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(20, &segments));
  ASSERT_EQ(segments.size(), 1);
  ASSERT_EQ(segments[0]->header().sequence_number(), 2);

  // Asking for 30 should include the first two.
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(30, &segments));
  ASSERT_EQ(segments.size(), 2);
  ASSERT_EQ(segments[0]->header().sequence_number(), 2);
  ASSERT_EQ(segments[1]->header().sequence_number(), 3);

  // Asking for anything higher should return all segments.
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(1000, &segments));
  ASSERT_EQ(segments.size(), 3);
  ASSERT_EQ(segments[0]->header().sequence_number(), 2);
  ASSERT_EQ(segments[1]->header().sequence_number(), 3);

  // Queries for segment suffixes (useful to seek to segments, e.g. when
  // when serving ops from the log)

  // Asking the reader for the suffix of segments sure to include 1 should
  // include all the segments.
  ASSERT_OK(reader.GetSegmentSuffixIncluding(1, &segments));
  ASSERT_EQ(segments.size(), 3);

  // ... asking for 10 should return all three segments as well.
  ASSERT_OK(reader.GetSegmentSuffixIncluding(10, &segments));
  ASSERT_EQ(segments.size(), 3);

  // ... asking for 15 should return all three segments
  ASSERT_OK(reader.GetSegmentSuffixIncluding(15, &segments));
  ASSERT_EQ(segments.size(), 3);

  // ... asking for 20 should return segments 3, 4
  ASSERT_OK(reader.GetSegmentSuffixIncluding(20, &segments));
  ASSERT_EQ(segments.size(), 2);
  ASSERT_EQ(segments[0]->header().sequence_number(), 3);
  ASSERT_EQ(segments[1]->header().sequence_number(), 4);

  // ... asking for 40 should return no segments
  ASSERT_OK(reader.GetSegmentSuffixIncluding(40, &segments));
  ASSERT_EQ(segments.size(), 0);

  // ... asking for anything higher should return no segments
  ASSERT_OK(reader.GetSegmentSuffixIncluding(1000, &segments));
  ASSERT_EQ(segments.size(), 0);
}

// Test that, even if the LogReader's index is empty because no segments
// have been properly closed, we can still read the entries as the reader
// returns the current segment.
TEST_F(LogTest, TestLogReaderReturnsLatestSegmentIfIndexEmpty) {
  BuildLog();
  AppendCommit(1, APPEND_ASYNC);
  AppendReplicateBatch(1, APPEND_SYNC);

  // The reader has nothing in the index, since we've only appended
  // a small batch and have not rolled over.
  SegmentSequence segments;
  ASSERT_STATUS_OK(log_->GetLogReader()->GetSegmentSuffixIncluding(1, &segments));
  ASSERT_EQ(segments.size(), 1);

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  ASSERT_STATUS_OK(segments[0]->ReadEntries(&entries));
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
  terms_by_index->assign(seq_len + 1, -1);
  int64_t committed_index = 0;
  int64_t max_repl_index = 0;

  OpId id = MakeOpId(1, 0);
  for (int i = 0; i < seq_len; i++) {
    if (rng->OneIn(5)) {
      // Reset term - it may stay the same, or go up/down
      id.set_term(std::max(1L, id.term() + rng->Uniform(5) - 2));
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
  BOOST_FOREACH(const TestLogSequenceElem& e, seq) {
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
        ASSERT_STATUS_OK(log_->AsyncAppendCommit(commit.Pass(), s.AsStatusCallback()));
        ASSERT_STATUS_OK(s.Wait());
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
TEST_F(LogTest, TestReadLogWithReplacedReplicates) {
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
  BuildLog();
  AppendTestSequence(seq);

  const int kNumRandomReads = 100;

  // We'll advance 'gc_index' randomly through the log until we've gotten to
  // the end. This ensures that, when we GC, we don't ever remove the latest
  // version of a replicate message unintentionally.
  LogReader* reader = log_->GetLogReader();
  for (int gc_index = 1; gc_index < max_repl_index;) {
    SCOPED_TRACE(Substitute("after GCing $0", gc_index));

    // Test reading random ranges of indexes and verifying that we get back the
    // REPLICATE messages with the correct terms
    for (int random_read = 0; random_read < kNumRandomReads; random_read++) {
      int start_index = RandInRange(&rng, gc_index, max_repl_index - 1);
      int end_index = RandInRange(&rng, start_index, max_repl_index);
      SCOPED_TRACE(Substitute("Reading $0-$1", start_index, end_index));
      vector<ReplicateMsg*> repls;
      ElementDeleter d(&repls);
      ASSERT_OK(reader->ReadAllReplicateEntries(start_index, end_index, &repls));
      ASSERT_EQ(end_index - start_index + 1, repls.size());
      int expected_index = start_index;
      BOOST_FOREACH(const ReplicateMsg* repl, repls) {
        ASSERT_EQ(expected_index, repl->id().index());
        ASSERT_EQ(terms_by_index[expected_index], repl->id().term());
        expected_index++;
      }
    }

    int num_gced = 0;
    ASSERT_OK(log_->GC(gc_index, &num_gced));
    gc_index += rng.Uniform(10);
  }
}
} // namespace log
} // namespace kudu
