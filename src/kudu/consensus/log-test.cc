// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <vector>

#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/tablet/mvcc.h"

DEFINE_int32(num_batches, 10000,
             "Number of batches to write to/read from the Log in TestWriteManyBatches");

namespace kudu {
namespace log {

using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using consensus::MinimumOpId;

extern const char* kTestTable;
extern const char* kTestTablet;

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
                                       int first_op_index,
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
    SegmentIdxPosPB* pb = footer.add_idx_entry();
    OpId* opid = pb->mutable_id();
    opid->set_term(0);
    opid->set_index(first_op_index);

    RETURN_NOT_OK(readable_segment->Init(header, footer, 0));
    RETURN_NOT_OK(reader->AppendSegment(readable_segment));
    return Status::OK();
  }
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
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(4, segments.size()) << DumpSegmentsToString(segments);

  // Freeing the first 2 anchors should allow GC of them.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[0]));
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[1]));
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  // We should now be anchored on op 0.10, i.e. on the 3rd segment
  ASSERT_TRUE(consensus::OpIdEquals(anchors[2]->op_id, anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
  ASSERT_EQ(2, num_gced_segments) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);

  // Release the remaining "rolled segment" anchor. GC will not delete the
  // last rolled segment.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[2]));
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
  ASSERT_EQ(0, num_gced_segments) << DumpSegmentsToString(segments);
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size()) << DumpSegmentsToString(segments);

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
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
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
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));

  // After GC there should be only one left, besides the one currently being
  // written to, because it's the first segment (counting in reverse order)
  // that has an earlier initial OpId than the earliest one we are anchored
  // on (which is in our active "new" segment).
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments))
  ASSERT_EQ(2, segments.size());
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

// This tests that querying LogReader works.
// This sets up a reader with some segments to query which amount to the
// following index:
// Index entries (first op in the segment, segment number):
// - {0.40, seg004}
// - {0.20, seg003}
// - {0.10, seg002}
TEST_F(LogTest, TestLogReader) {
  LogReader reader(fs_manager_.get(), kTestTablet);
  reader.InitEmptyReaderForTests();
  ASSERT_STATUS_OK(AppendNewEmptySegmentToReader(2, 10, &reader));
  ASSERT_STATUS_OK(AppendNewEmptySegmentToReader(3, 20, &reader));
  ASSERT_STATUS_OK(AppendNewEmptySegmentToReader(4, 40, &reader));

  OpId op;
  op.set_term(0);
  SegmentSequence segments;

  // Queries for segment prefixes (used for GC)

  // Asking the reader the prefix of segments that does not include op 0.1
  // should return the empty set.
  op.set_index(1);
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(op, &segments));
  ASSERT_TRUE(segments.empty());

  // .. same for op 0.10
  op.set_index(10);
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(op, &segments));
  ASSERT_TRUE(segments.empty());

  // Asking for the prefix of segments not including op 0.20 should return
  // the first segment, since 0.20 is the first operation in segment 3.
  op.set_index(20);
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 1);
  ASSERT_EQ(segments[0]->header().sequence_number(), 2);

  // Asking for 0.40 should include the first two.
  op.set_index(40);
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 2);
  ASSERT_EQ(segments[0]->header().sequence_number(), 2);
  ASSERT_EQ(segments[1]->header().sequence_number(), 3);

  // Asking for anything higher should still return the first two.
  op.set_index(1000);
  ASSERT_OK(reader.GetSegmentPrefixNotIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 2);
  ASSERT_EQ(segments[0]->header().sequence_number(), 2);
  ASSERT_EQ(segments[1]->header().sequence_number(), 3);

  // Queries for segment suffixes (useful to seek to segments, e.g. when
  // when serving ops from the log)

  // Asking the reader for the suffix of segments sure to include 0.1 should
  // return Status::NotFound();
  op.set_index(1);
  ASSERT_TRUE(reader.GetSegmentSuffixIncluding(op, &segments).IsNotFound());

  // ... asking for 0.10 should return all three segments
  op.set_index(10);
  ASSERT_OK(reader.GetSegmentSuffixIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 3);

  // ... asking for 0.15 should return all three segments
  op.set_index(15);
  ASSERT_OK(reader.GetSegmentSuffixIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 3);

  // ... asking for 0.20 should return segments 3, 4
  op.set_index(20);
  ASSERT_OK(reader.GetSegmentSuffixIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 2);
  ASSERT_EQ(segments[0]->header().sequence_number(), 3);
  ASSERT_EQ(segments[1]->header().sequence_number(), 4);

  // ... asking for 0.40 should return segments 4
  op.set_index(40);
  ASSERT_OK(reader.GetSegmentSuffixIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 1);
  ASSERT_EQ(segments[0]->header().sequence_number(), 4);

  // ... asking for anything higher should return segment 4
  op.set_index(1000);
  ASSERT_OK(reader.GetSegmentSuffixIncluding(op, &segments));
  ASSERT_EQ(segments.size(), 1);
  ASSERT_EQ(segments[0]->header().sequence_number(), 4);
}

} // namespace log
} // namespace kudu
