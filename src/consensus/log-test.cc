// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log-test-base.h"
#include "tablet/mvcc.h"

DEFINE_int32(num_batches, 10000,
             "Number of batches to write to/read from the Log in TestWriteManyBatches");

namespace kudu {
namespace log {

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

  typedef pair<int, int> DeltaId;

  virtual void SetUp() {
    KuduTest::SetUp();
    current_id_ = 0;
    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
    CreateTestSchema(&schema_);
  }

  void BuildLog(int term = 0,
                int index = 0,
                TabletSuperBlockPB* meta = NULL) {

    OpId id;
    id.set_term(term);
    id.set_index(index);

    if (meta == NULL) {
      TabletSuperBlockPB default_meta;
      CreateTabletMetaForRowSets(&default_meta);
      ASSERT_STATUS_OK(Log::Open(options_,
                                 fs_manager_.get(),
                                 default_meta,
                                 id,
                                 kTestTablet,
                                 &log_));
    } else {
      ASSERT_STATUS_OK(Log::Open(options_,
                                 fs_manager_.get(),
                                 *meta,
                                 id,
                                 kTestTablet,
                                 &log_));
    }
  }

  // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  void AppendBatchAndCommitEntryPairsToLog(int count) {
    for (int i = 0; i < count; i++) {
      AppendBatch(current_id_);
      AppendCommit(current_id_ + 1, current_id_);
      current_id_ += 2;
    }
  }

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  // Note that this test does not insert into tablet so the data contained in
  // the ReplicateMsgs doesn't necessarily need to make sense.
  void AppendBatch(int index) {
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
  void AppendCommit(int index, int original_op_index,
                    // the mrs id for the insert
                    int target_mrs_id = 1,
                    // the rs and delta ids for the mutate
                    int target_rs_id = 0,
                    int target_delta_id = 0) {
    LogEntryPB log_entry;
    log_entry.set_type(OPERATION);
    OperationPB* operation = log_entry.mutable_operation();

    CommitMsg* commit = operation->mutable_commit();
    commit->set_op_type(WRITE_OP);

    OpId* original_op_id = commit->mutable_commited_op_id();
    original_op_id->set_term(0);
    original_op_id->set_index(original_op_index);

    OpId* commit_id = operation->mutable_id();
    commit_id->set_term(0);
    commit_id->set_index(index);

    TxResultPB* result = commit->mutable_result();
    tablet::txid_t(original_op_index).EncodeToString(result->mutable_txid());

    TxOperationPB* insert = result->add_inserts();
    insert->set_type(TxOperationPB::INSERT);
    insert->set_mrs_id(target_mrs_id);

    TxOperationPB* mutate = result->add_mutations();
    mutate->set_type(TxOperationPB::MUTATE);

    MutationResultPB* mutation_result = mutate->mutable_mutation_result();

    MutationTargetPB* target = mutation_result->add_mutations();
    target->set_delta_id(target_delta_id);
    target->set_rs_id(target_rs_id);

    mutation_result->set_type(MutationType(mutation_result));

    ASSERT_STATUS_OK(log_->Append(&log_entry));
  }

  // This is used to test log GC. It inserts data into three different segments
  // where the first has only flushed entries, the second has flushed and
  // unflushed entries and the last has only unflushed entries. The point being
  // that the first segment is GC'able and the second and third aren't even if the
  // second contains flushed entries.
  void AppendMultiSegmentSequence() {
    // append a batch/commit pair
    AppendBatch(0);
    AppendCommit(1, 0);

    // rollover the log (the current MRS set will be in the new segment header)
    ASSERT_STATUS_OK(log_->RollOver());

    // append another batch to the initial memstores
    AppendBatch(2);
    AppendCommit(3, 2);

    // flush the MemRowSet, keeping the DeltaMemStore
    LogEntryPB meta_entry;
    TabletSuperBlockPB* meta = meta_entry.mutable_tablet_meta();
    CreateTabletMetaForRowSets(meta, 1);
    meta_entry.set_type(TABLET_METADATA);
    ASSERT_STATUS_OK(log_->Append(&meta_entry));

    // append a batch where the insert goes into the new MemRowStore
    // and the mutate goes into the old DeltaMemStore
    AppendBatch(5);
    AppendCommit(6, 5, 2);

    // flush the remaining delta store
    meta_entry.Clear();
    meta = meta_entry.mutable_tablet_meta();
    vector<DeltaId> new_deltas = boost::assign::list_of(DeltaId(0, 0));
    CreateTabletMetaForRowSets(meta, 1, &new_deltas);
    meta_entry.set_type(TABLET_METADATA);
    ASSERT_STATUS_OK(log_->Append(&meta_entry));

    // Roll over the log
    ASSERT_STATUS_OK(log_->RollOver());

    // append a new batch of updates/inserts to the last memstores.
    AppendBatch(8);
    AppendCommit(9, 8, 2, 0, 1);
  }
};

// Test that the reader can read from the log even if it hasn't been
// properly closed.
TEST_F(LogTest, TestLogNotTrimmed) {
  BuildLog();
  BuildLogReader();
  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  ASSERT_STATUS_OK(log_reader_->ReadEntries(log_reader_->segments()[0], &entries));
  // Close after testing to ensure correct shutdown
  // TODO : put this in TearDown() with a test on log state?
  ASSERT_STATUS_OK(log_->Close());
}

// Tests that the log reader reads up until some corrupt entry is found.
// TODO test partially written/corrupt headers
TEST_F(LogTest, TestCorruptLog) {
  BuildLog();
  AppendBatchAndCommitEntryPairsToLog(2);
  ASSERT_STATUS_OK(log_->Close());

  // rewrite the file but truncate the last entry partially
  shared_ptr<RandomAccessFile> source;
  const string log_path = log_->ActiveSegmentPathForTests();
  ASSERT_STATUS_OK(env_util::OpenFileForRandom(env_.get(), log_path, &source));
  uint64_t file_size;
  ASSERT_STATUS_OK(env_.get()->GetFileSize(log_path, &file_size));

  uint8_t entry_space[file_size];
  Slice log_slice;

  ASSERT_STATUS_OK(source->Read(0, file_size - 10, &log_slice, entry_space));

  // we need to actually copy the slice or we run into trouble
  // because we're reading and writing to the same file.
  faststring copied;
  copied.append(log_slice.data(), log_slice.size());

  // rewrite the file with the corrupt log
  shared_ptr<WritableFile> sink;
  ASSERT_STATUS_OK(env_util::OpenFileForWrite(env_.get(), log_path, &sink));

  ASSERT_STATUS_OK(sink->Append(Slice(copied)));
  ASSERT_STATUS_OK(sink->Sync());
  ASSERT_STATUS_OK(sink->Close());

  BuildLogReader();
  ASSERT_EQ(1, log_reader_->size());
  Status status = LogReader::ReadEntries(log_reader_->segments()[0], &entries_);
  ASSERT_TRUE(status.IsCorruption());
  // last entry is corrupted but we should still get 3
  ASSERT_EQ(3, entries_.size());
}

// Tests that segments roll over when max segment size is reached
// and that the player plays all entries in the correct order
TEST_F(LogTest, TestSegmentRollover) {
  // set a small segment size so that we have roll overs
  BuildLog();
  log_->SetMaxSegmentSizeForTests(1024);

  // Write 100 replicate/commit pairs to the log.
  AppendBatchAndCommitEntryPairsToLog(100);

  // At the time of writing, we can fit 4 pairs per log segment.
  // Therefore, we should have 24 "rolled" segments and one "full" but
  // currently active one, for a total of 25 * 4 = 100 records.
  ASSERT_EQ(24, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->Close());

  BuildLogReader();
  for (int i = 0; i < log_reader_->size(); i++) {
    ASSERT_STATUS_OK(LogReader::ReadEntries(log_reader_->segments()[i], &entries_));
  }
  // expect 100 <op,commit> entries, i.e. 200 LogEntryPB's
  ASSERT_EQ(200, entries_.size());

}

// Tests that segments that can be are GC'd, while the log is running.
TEST_F(LogTest, TestGCWithLogRunning) {
  BuildLog();
  AppendMultiSegmentSequence();

  ASSERT_EQ(2, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->GC());

  ASSERT_EQ(1, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->Close());

  CheckRightNumberOfSegmentFiles(2);
}

// Tests log reopening and that GC'ing the old log's segments works.
TEST_F(LogTest, TestLogReopenAndGC) {
  BuildLog();
  AppendMultiSegmentSequence();
  ASSERT_EQ(2, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->Close());

  LogEntryPB meta_entry;
  TabletSuperBlockPB* meta = meta_entry.mutable_tablet_meta();
  vector<DeltaId> new_deltas = boost::assign::list_of(DeltaId(0, 0));
  CreateTabletMetaForRowSets(meta, 1, &new_deltas);
  meta_entry.set_type(TABLET_METADATA);

  // now reopen the log as if we had replayed the state into the stores
  // that were in memory and do GC
  BuildLog(0, 10, meta);
  // log starts with 3 segments
  ASSERT_EQ(3, log_->PreviousSegmentsForTests().size());

  ASSERT_STATUS_OK(log_->GC());
  // after GC there should be only two
  ASSERT_EQ(2, log_->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(log_->Close());

  CheckRightNumberOfSegmentFiles(3);
}

// Helper to measure the performance of the log, disabled by default.
TEST_F(LogTest, TestWriteManyBatches) {
  uint64_t num_batches = 10;
  if (this->AllowSlowTests()) {
    num_batches = FLAGS_num_batches;
  }
  BuildLog();

  LOG(INFO)<< "Starting to write " << num_batches << " to log";
  LOG_TIMING(INFO, "Wrote all batches to log") {
    AppendBatchAndCommitEntryPairsToLog(num_batches);
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
