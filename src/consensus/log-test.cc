// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <vector>

#include "consensus/log-test-base.h"
#include "gutil/stl_util.h"
#include "tablet/mvcc.h"

DEFINE_int32(num_batches, 10000,
             "Number of batches to write to/read from the Log in TestWriteManyBatches");

namespace kudu {
namespace log {

using std::tr1::shared_ptr;
using std::tr1::unordered_map;

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

};

// If we write more than one entry in a batch, we should be able to
// read all of those entries back.
TEST_F(LogTest, TestMultipleEntriesInABatch) {
  BuildLog();

  OperationPB op1;
  op1.mutable_replicate()->set_op_type(WRITE_OP);
  OpId* op_id = op1.mutable_id();
  op_id->set_term(0);
  op_id->set_index(1);

  OperationPB op2;
  op2.mutable_replicate()->set_op_type(WRITE_OP);
  op_id = op2.mutable_id();
  op_id->set_term(0);
  op_id->set_index(1);

  vector<const consensus::OperationPB*> ops;
  ops.push_back(&op1);
  ops.push_back(&op2);

  LogEntryBatch* reserved_entry;
  ASSERT_STATUS_OK(log_->Reserve(&ops[0], 2, &reserved_entry));
  Synchronizer sync;
  ASSERT_STATUS_OK(log_->AsyncAppend(reserved_entry, sync.AsStatusCallback()));
  ASSERT_STATUS_OK(sync.Wait());

  BuildLogReader();
  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  ASSERT_STATUS_OK(log_reader_->segments().begin()->second->ReadEntries(&entries));

  ASSERT_EQ(2, entries.size());

  ASSERT_STATUS_OK(log_->Close());
}

// Tests that everything works properly with fsync enabled:
// This also tests SyncDir() (see KUDU-261), which is called whenever
// a new log segment is initialized.
TEST_F(LogTest, TestFsync) {
  options_.force_fsync_all = true;
  BuildLog();
  ASSERT_STATUS_OK(log_->WriteHeaderForTests());
  ASSERT_STATUS_OK(log_->Close());
}

// Test that the reader can read from the log even if it hasn't been
// properly closed.
TEST_F(LogTest, TestLogNotTrimmed) {
  BuildLog();
  ASSERT_STATUS_OK(log_->WriteHeaderForTests());
  BuildLogReader();
  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  const scoped_refptr<ReadableLogSegment>& first_segment = log_reader_->segments().begin()->second;
  ASSERT_STATUS_OK(first_segment->ReadEntries(&entries));
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
  Status s = LogReader::Open(fs_manager_.get(), kTestTablet, &log_reader_);

  // The reader needs to be able to open the directory, and we need to
  // skip the segment while reading.
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(log_reader_->size(), 0);
  ASSERT_TRUE(log_reader_->segments().empty());
}

// Tests that the log reader reads up until some corrupt entry is found.
// TODO: Test partially written/corrupt headers.
TEST_F(LogTest, TestCorruptLog) {
  const int kNumEntries = 4;
  BuildLog();
  OpId op_id(MinimumOpId());
  AppendNoOps(&op_id, kNumEntries);
  ASSERT_STATUS_OK(log_->Close());

  // Rewrite the file but truncate the last entry partially.
  shared_ptr<RandomAccessFile> source;
  const string log_path = log_->ActiveSegmentPathForTests();
  ASSERT_STATUS_OK(env_util::OpenFileForRandom(env_.get(), log_path, &source));
  uint64_t file_size;
  ASSERT_STATUS_OK(env_.get()->GetFileSize(log_path, &file_size));

  uint8_t entry_space[file_size];
  Slice log_slice;

  // Truncate by 10 bytes.
  ASSERT_STATUS_OK(source->Read(0, file_size - 10, &log_slice, entry_space));

  // We need to actually copy the slice or we run into trouble
  // because we're reading and writing to the same file.
  faststring copied;
  copied.append(log_slice.data(), log_slice.size());

  // Rewrite the file with the corrupt log.
  shared_ptr<WritableFile> sink;
  ASSERT_STATUS_OK(env_util::OpenFileForWrite(env_.get(), log_path, &sink));

  ASSERT_STATUS_OK(sink->Append(Slice(copied)));
  ASSERT_STATUS_OK(sink->Sync());
  ASSERT_STATUS_OK(sink->Close());

  BuildLogReader();
  ASSERT_EQ(1, log_reader_->size());
  const scoped_refptr<ReadableLogSegment>& first_segment = log_reader_->segments().begin()->second;
  Status status = first_segment->ReadEntries(&entries_);
  ASSERT_TRUE(status.IsCorruption());

  // Last entry is corrupted but we should still see the previous ones.
  ASSERT_EQ(kNumEntries - 1, entries_.size());
}

// Tests that segments roll over when max segment size is reached
// and that the player plays all entries in the correct order.
TEST_F(LogTest, TestSegmentRollover) {
  BuildLog();
  // Set a small segment size so that we have roll overs.
  log_->SetMaxSegmentSizeForTests(1024);
  const int kNumEntriesPerBatch = 100;

  OpId op_id(MinimumOpId());
  int num_entries = 0;
  do {
    AppendNoOps(&op_id, kNumEntriesPerBatch);
    num_entries += kNumEntriesPerBatch;
  } while (log_->GetNumReadableLogSegmentsForTests() < 3);

  ASSERT_STATUS_OK(log_->Close());
  BuildLogReader();
  BOOST_FOREACH(const ReadableLogSegmentMap::value_type& entry, log_reader_->segments()) {
    ASSERT_STATUS_OK(entry.second->ReadEntries(&entries_));
  }

  ASSERT_EQ(num_entries, entries_.size());
}

// Tests that segments can be GC'd while the log is running.
TEST_F(LogTest, TestGCWithLogRunning) {
  BuildLog();

  vector<OpIdAnchor*> anchors;
  ElementDeleter deleter(&anchors);

  const int kNumTotalSegments = 4;
  const int kNumOpsPerSegment = 5;
  int num_gced_segments;
  OpId op_id(MinimumOpId());
  OpId anchored_opid;

  ASSERT_STATUS_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, &anchors));
  // Anchors should prevent GC.
  ASSERT_EQ(3, log_->GetNumReadableLogSegmentsForTests());
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
  ASSERT_EQ(3, log_->GetNumReadableLogSegmentsForTests());

  // Freeing the first 2 anchors should allow GC of them.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[0]));
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[1]));
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
  ASSERT_EQ(1, log_->GetNumReadableLogSegmentsForTests());

  // Release the remaining "rolled segment" anchor. GC will not delete the
  // last log.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[2]));
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
  ASSERT_EQ(1, log_->GetNumReadableLogSegmentsForTests());

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
  BuildLogReader();
  ASSERT_STATUS_OK(log_reader_->segments().begin()->second->ReadEntries(&entries_));
  ASSERT_EQ(entries_.size(), 4);
  for (int i = 0; i < 4 ; i++) {
    ASSERT_TRUE(entries_[i]->has_operation());
    if (i % 2 == 0) {
      ASSERT_TRUE(entries_[i]->operation().has_replicate());
    } else {
      ASSERT_TRUE(entries_[i]->operation().has_commit());
      ASSERT_EQ(WRITE_OP, entries_[i]->operation().commit().op_type());
    }
  }
}

// Tests log reopening and that GC'ing the old log's segments works.
TEST_F(LogTest, TestLogReopenAndGC) {
  BuildLog();

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
  ASSERT_EQ(2, log_->GetNumReadableLogSegmentsForTests());
  ASSERT_STATUS_OK(opid_anchor_registry_->GetEarliestRegisteredOpId(&anchored_opid));
  ASSERT_STATUS_OK(log_->GC(anchored_opid, &num_gced_segments));
  ASSERT_EQ(2, log_->GetNumReadableLogSegmentsForTests());

  ASSERT_STATUS_OK(log_->Close());

  // Now reopen the log as if we had replayed the state into the stores.
  // that were in memory and do GC.
  BuildLog();

  // The "old" data consists of 3 segments. We still hold anchors.
  ASSERT_EQ(3, log_->GetNumReadableLogSegmentsForTests());

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

  // After GC there should be only one left, because it's the first segment
  // (counting in reverse order) that has an earlier initial OpId than the
  // earliest one we are anchored on (which is in our active "new" segment).
  ASSERT_EQ(1, log_->GetNumReadableLogSegmentsForTests());
  ASSERT_STATUS_OK(log_->Close());

  CheckRightNumberOfSegmentFiles(2);

  // Unregister the final anchor.
  ASSERT_STATUS_OK(opid_anchor_registry_->Unregister(anchors[3]));
}

// Helper to measure the performance of the log.
TEST_F(LogTest, TestWriteManyBatches) {
  uint64_t num_batches = 10;
  if (this->AllowSlowTests()) {
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
    BuildLogReader();
    uint32_t num_entries = 0;
    BOOST_FOREACH(const ReadableLogSegmentMap::value_type& entry, log_reader_->segments()) {
      STLDeleteElements(&entries_);
      ASSERT_STATUS_OK(entry.second->ReadEntries(&entries_));
      num_entries += entries_.size();
    }
    ASSERT_EQ(num_entries, num_batches * 2);
    LOG(INFO) << "End readfile";
  }
}

} // namespace log
} // namespace kudu
