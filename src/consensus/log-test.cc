// Copyright (c) 2013, Cloudera, inc.

#include <vector>
#include "consensus/log-test-base.h"
#include "gutil/stl_util.h"
#include "tablet/mvcc.h"

DEFINE_int32(num_batches, 10000,
             "Number of batches to write to/read from the Log in TestWriteManyBatches");

namespace kudu {
namespace log {

using consensus::NO_OP;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using tserver::WriteRequestPB;
using tablet::TxResultPB;
using tablet::TxOperationPB;
using tablet::MutationResultPB;
using tablet::MutationTargetPB;

extern const char* kTestTablet;

class LogTest : public LogTestBase {
 public:

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  // Note that this test does not insert into tablet so the data contained in
  // the ReplicateMsgs doesn't necessarily need to make sense.
  void AppendReplicateBatch(int index) {
    LogEntryPB log_entry;
    log_entry.set_type(OPERATION);
    OperationPB* operation = log_entry.mutable_operation();

    ReplicateMsg* replicate = operation->mutable_replicate();
    replicate->set_op_type(WRITE_OP);

    OpId* op_id = operation->mutable_id();
    op_id->set_term(0);
    op_id->set_index(index);

    WriteRequestPB* batch_request = replicate->mutable_write_request();
    AddTestRowToPB(schema_,
                   index,
                   0,
                   "this is a test insert",
                   batch_request->mutable_to_insert_rows());

    faststring mutations;
    AddTestMutationToRowBlockAndBuffer(
        schema_,
        index + 1,
        0,
        "this is a test mutate",
        batch_request->mutable_to_mutate_row_keys(),
        &mutations);

    batch_request->set_encoded_mutations(mutations.data(), mutations.size());
    batch_request->set_tablet_id(kTestTablet);

    ASSERT_STATUS_OK(log_->Append(&log_entry));
  }

  // Append a commit log entry containing one entry for the insert and one
  // for the mutate.
  void AppendCommit(int index, int original_op_index) {

    // The mrs id for the insert.
    const int kTargetMrsId = 1;

    // The rs and delta ids for the mutate.
    const int kTargetRsId = 0;
    const int kTargetDeltaId = 0;

    LogEntryPB log_entry;
    log_entry.set_type(OPERATION);
    OperationPB* operation = log_entry.mutable_operation();

    CommitMsg* commit = operation->mutable_commit();
    commit->set_op_type(WRITE_OP);
    Timestamp(original_op_index).EncodeToString(commit->mutable_timestamp());

    OpId* original_op_id = commit->mutable_commited_op_id();
    original_op_id->set_term(0);
    original_op_id->set_index(original_op_index);

    OpId* commit_id = operation->mutable_id();
    commit_id->set_term(0);
    commit_id->set_index(index);

    TxResultPB* result = commit->mutable_result();

    TxOperationPB* insert = result->add_inserts();
    insert->set_type(TxOperationPB::INSERT);
    insert->set_mrs_id(kTargetMrsId);

    TxOperationPB* mutate = result->add_mutations();
    mutate->set_type(TxOperationPB::MUTATE);

    MutationResultPB* mutation_result = mutate->mutable_mutation_result();

    MutationTargetPB* target = mutation_result->add_mutations();
    target->set_delta_id(kTargetDeltaId);
    target->set_rs_id(kTargetRsId);

    mutation_result->set_type(MutationType(mutation_result));

    ASSERT_STATUS_OK(log_->Append(&log_entry));
  }

  // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  void AppendReplicateBatchAndCommitEntryPairsToLog(int count) {
    for (int i = 0; i < count; i++) {
      AppendReplicateBatch(current_id_);
      AppendCommit(current_id_ + 1, current_id_);
      current_id_ += 2;
    }
  }

  // Append a single NO_OP entry. Increments op_id by one.
  Status AppendNoOp(OpId* op_id) {
    LogEntryPB log_entry;
    log_entry.set_type(OPERATION);
    OperationPB* operation = log_entry.mutable_operation();
    operation->mutable_id()->CopyFrom(*op_id);
    operation->mutable_replicate()->set_op_type(NO_OP);
    RETURN_NOT_OK(log_->Append(&log_entry));

    // Increment op_id.
    op_id->set_index(op_id->index() + 1);
    return Status::OK();
  }

  // Append a number of no-op entries to the log.
  // Increments op_id's index by the number of records written.
  Status AppendNoOps(OpId* op_id, int num) {
    for (int i = 0; i < num; i++) {
      RETURN_NOT_OK(AppendNoOp(op_id));
    }
    return Status::OK();
  }

  Status RollLog() {
    RETURN_NOT_OK(log_->AsyncAllocateSegment());
    return log_->RollOver();
  }

  void CreateAndRegisterNewAnchor(const OpId& op_id, vector<OpIdAnchor*>* anchors) {
    anchors->push_back(new OpIdAnchor());
    opid_anchor_registry_.Register(op_id, CURRENT_TEST_NAME(), anchors->back());
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

// Test that the reader can read from the log even if it hasn't been
// properly closed.
TEST_F(LogTest, TestLogNotTrimmed) {
  BuildLog();
  ASSERT_STATUS_OK(log_->WriteHeaderForTests());
  BuildLogReader();
  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  ASSERT_STATUS_OK(log_reader_->ReadEntries(log_reader_->segments()[0], &entries));
  // Close after testing to ensure correct shutdown
  // TODO : put this in TearDown() with a test on log state?
  ASSERT_STATUS_OK(log_->Close());
}

// Test that the reader will not fail if a log file is completely blank.
// This happens when it's opened but nothing has been written.
// The reader should gracefully handle this situation, but somehow expose that
// the segment is uninitialized. See KUDU-140.
TEST_F(LogTest, DISABLED_TestBlankLogFile) {
  BuildLog();
  Status s = LogReader::Open(fs_manager_.get(), kTestTablet, &log_reader_);
  // The reader needs to be able to open the file, and we need to skip the
  // segment somehow while reading.
  ASSERT_TRUE(s.ok()) << s.ToString();
  // TODO: Test that we handle the empty log segments properly so that bootstrap
  // can move them aside or something like that.
  FAIL() << "Ensure that we test when the ReadableLogSement is uninitialized";
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
  Status status = LogReader::ReadEntries(log_reader_->segments()[0], &entries_);
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
  } while (log_->PreviousSegmentsForTests().size() < 3);

  ASSERT_STATUS_OK(log_->Close());
  BuildLogReader();
  for (int i = 0; i < log_reader_->size(); i++) {
    ASSERT_STATUS_OK(LogReader::ReadEntries(log_reader_->segments()[i], &entries_));
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
  OpId op_id(MinimumOpId());

  ASSERT_STATUS_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, &anchors));
  // Anchors should prevent GC.
  ASSERT_EQ(3, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->GC());
  ASSERT_EQ(3, log_->PreviousSegmentsForTests().size());

  // Freeing the first 2 anchors should allow GC of the first.
  // This is because we are anchoring on the earliest OpId in each log, and
  // GC() preserved the first file it finds (searching backwards) with initial
  // OpId strictly earlier than the earliest anchored OpId, plus all following
  // log segments (by sequence number, ascending).
  ASSERT_STATUS_OK(opid_anchor_registry_.Unregister(anchors[0]));
  ASSERT_STATUS_OK(opid_anchor_registry_.Unregister(anchors[1]));
  ASSERT_STATUS_OK(log_->GC());
  ASSERT_EQ(2, log_->PreviousSegmentsForTests().size());

  // Release and GC another segment.
  ASSERT_STATUS_OK(opid_anchor_registry_.Unregister(anchors[2]));
  ASSERT_STATUS_OK(log_->GC());
  ASSERT_EQ(1, log_->PreviousSegmentsForTests().size());

  ASSERT_STATUS_OK(log_->Close());
  CheckRightNumberOfSegmentFiles(2);

  // We skip the first three, since we unregistered them above.
  for (int i = 3; i < kNumTotalSegments; i++) {
    ASSERT_STATUS_OK(opid_anchor_registry_.Unregister(anchors[i]));
  }
}

// Tests log reopening and that GC'ing the old log's segments works.
TEST_F(LogTest, TestLogReopenAndGC) {
  BuildLog();

  vector<OpIdAnchor*> anchors;
  ElementDeleter deleter(&anchors);

  const int kNumTotalSegments = 3;
  const int kNumOpsPerSegment = 5;
  OpId op_id(MinimumOpId());

  ASSERT_STATUS_OK(AppendMultiSegmentSequence(kNumTotalSegments, kNumOpsPerSegment,
                                              &op_id, &anchors));
  // Anchors should prevent GC.
  ASSERT_EQ(2, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->GC());
  ASSERT_EQ(2, log_->PreviousSegmentsForTests().size());

  ASSERT_STATUS_OK(log_->Close());

  // Now reopen the log as if we had replayed the state into the stores.
  // that were in memory and do GC.
  BuildLog();

  // The "old" data consists of 3 segments. We still hold anchors.
  ASSERT_EQ(3, log_->PreviousSegmentsForTests().size());

  // Write to a new log segment, as if we had taken new requests and the
  // mem stores are holding anchors, but don't roll it.
  CreateAndRegisterNewAnchor(op_id, &anchors);
  ASSERT_STATUS_OK(AppendNoOps(&op_id, kNumOpsPerSegment));

  // Now release the "old" anchors and GC them.
  for (int i = 0; i < 3; i++) {
    ASSERT_STATUS_OK(opid_anchor_registry_.Unregister(anchors[i]));
  }
  ASSERT_STATUS_OK(log_->GC());

  // After GC there should be only one left, because it's the first segment
  // (counting in reverse order) that has an earlier initial OpId than the
  // earliest one we are anchored on (which is in our active "new" segment).
  ASSERT_EQ(1, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->Close());

  CheckRightNumberOfSegmentFiles(2);

  // Unregister the final anchor.
  ASSERT_STATUS_OK(opid_anchor_registry_.Unregister(anchors[3]));
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
    for (int i = 0; i < log_reader_->size(); i++) {
      STLDeleteElements(&entries_);
      ASSERT_STATUS_OK(LogReader::ReadEntries(log_reader_->segments()[i], &entries_));
      num_entries += entries_.size();
    }
    ASSERT_EQ(num_entries, num_batches * 2);
    LOG(INFO) << "End readfile";
  }
}

} // namespace log
} // namespace kudu
