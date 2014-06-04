// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CONSENSUS_LOG_TEST_BASE_H
#define KUDU_CONSENSUS_LOG_TEST_BASE_H

#include "consensus/log.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <utility>
#include <vector>
#include <string>

#include "common/timestamp.h"
#include "common/wire_protocol-test-util.h"
#include "consensus/log_reader.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/stl_util.h"
#include "gutil/stringprintf.h"
#include "server/fsmanager.h"
#include "server/metadata.h"
#include "tablet/transactions/write_util.h"
#include "tserver/tserver.pb.h"
#include "util/env_util.h"
#include "util/path_util.h"
#include "util/test_macros.h"
#include "util/test_util.h"
#include "util/stopwatch.h"

namespace kudu {
namespace log {

using consensus::OpId;
using consensus::CommitMsg;
using consensus::OperationPB;
using consensus::ReplicateMsg;
using consensus::WRITE_OP;
using consensus::NO_OP;

using metadata::TabletSuperBlockPB;
using metadata::TabletMasterBlockPB;
using metadata::RowSetDataPB;
using metadata::DeltaDataPB;
using metadata::BlockIdPB;
using metadata::kNoDurableMemStore;

using tserver::WriteRequestPB;

using tablet::TxResultPB;
using tablet::OperationResultPB;
using tablet::MemStoreTargetPB;

const char* kTestTable = "test-log-table";
const char* kTestTablet = "test-log-tablet";
const bool APPEND_SYNC = true;
const bool APPEND_ASYNC = false;

class LogTestBase : public KuduTest {
 public:

  typedef pair<int, int> DeltaId;

  LogTestBase()
    : opid_anchor_registry_(new OpIdAnchorRegistry()) {
  }

  virtual void SetUp() {
    KuduTest::SetUp();
    current_id_ = 0;
    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
    CreateTestSchema(&schema_);
  }

  virtual void TearDown() {
    KuduTest::TearDown();
    STLDeleteElements(&entries_);
  }

  void BuildLog() {
    CHECK_OK(Log::Open(options_,
                       fs_manager_.get(),
                       kTestTablet,
                       NULL,
                       &log_));
  }

  void BuildLogReader() {
    CHECK_OK(LogReader::Open(fs_manager_.get(), kTestTablet, &log_reader_));
  }

  void CheckRightNumberOfSegmentFiles(int expected) {
    // Test that we actually have the expected number of files in the fs.
    // We should have n segments plus '.' and '..'
    vector<string> segments;
    ASSERT_STATUS_OK(env_->GetChildren(
                       JoinPathSegments(fs_manager_->GetWalsRootDir(),
                                        kTestTablet),
                       &segments));
    ASSERT_EQ(expected + 2, segments.size());
  }

  void EntriesToIdList(vector<uint32_t>* ids) {
    BOOST_FOREACH(const LogEntryPB* entry, entries_) {
      VLOG(2) << "Entry contents: " << entry->DebugString();
      if (entry->type() == OPERATION) {
        if (PREDICT_TRUE(entry->operation().has_id())) {
          ids->push_back(entry->operation().id().index());
        }
      }
    }
  }

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  void AppendReplicateBatch(int index, bool sync = APPEND_SYNC) {
    LogEntryPB log_entry;
    log_entry.set_type(OPERATION);
    OperationPB* operation = log_entry.mutable_operation();

    ReplicateMsg* replicate = operation->mutable_replicate();
    replicate->set_op_type(WRITE_OP);

    OpId* op_id = operation->mutable_id();
    op_id->set_term(0);
    op_id->set_index(index);

    WriteRequestPB* batch_request = replicate->mutable_write_request();
    ASSERT_STATUS_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
    AddTestRowToPB(RowOperationsPB::INSERT, schema_,
                   index,
                   0,
                   "this is a test insert",
                   batch_request->mutable_row_operations());
    AddTestRowToPB(RowOperationsPB::UPDATE, schema_,
                   index + 1,
                   0,
                   "this is a test mutate",
                   batch_request->mutable_row_operations());
    batch_request->set_tablet_id(kTestTablet);

    if (sync) {
      AppendSync(&log_entry);
    } else {
      AppendAsync(operation);
    }
  }

  // Append a commit log entry containing one entry for the insert and one
  // for the mutate.
  void AppendCommit(int index, int original_op_index, bool sync = APPEND_SYNC) {
    // The mrs id for the insert.
    const int kTargetMrsId = 1;

    // The rs and delta ids for the mutate.
    const int kTargetRsId = 0;
    const int kTargetDeltaId = 0;

    AppendCommit(index, original_op_index, kTargetMrsId, kTargetRsId, kTargetDeltaId, sync);
  }

  void AppendCommit(int index, int original_op_index, int mrs_id, int rs_id, int delta_id,
                    bool sync = APPEND_SYNC) {
    LogEntryPB log_entry;
    log_entry.set_type(OPERATION);
    OperationPB* operation = log_entry.mutable_operation();

    CommitMsg* commit = operation->mutable_commit();
    commit->set_op_type(WRITE_OP);
    commit->set_timestamp(Timestamp(original_op_index).ToUint64());

    OpId* original_op_id = commit->mutable_commited_op_id();
    original_op_id->set_term(0);
    original_op_id->set_index(original_op_index);

    OpId* commit_id = operation->mutable_id();
    commit_id->set_term(0);
    commit_id->set_index(index);

    TxResultPB* result = commit->mutable_result();

    OperationResultPB* insert = result->add_ops();
    insert->add_mutated_stores()->set_mrs_id(mrs_id);

    OperationResultPB* mutate = result->add_ops();
    MemStoreTargetPB* target = mutate->add_mutated_stores();
    target->set_delta_id(delta_id);
    target->set_rs_id(rs_id);

    if (sync) {
      AppendSync(&log_entry);
    } else {
      AppendAsync(operation);
    }
  }

  void AppendSync(LogEntryPB* log_entry) {
    ASSERT_STATUS_OK(log_->Append(log_entry));
  }

  void AppendAsync(OperationPB* operation) {
    LogEntryBatch* reserved_entry_batch;
    ASSERT_STATUS_OK(log_->Reserve(boost::assign::list_of(operation),
                                   &reserved_entry_batch));
    ASSERT_STATUS_OK(log_->AsyncAppend(reserved_entry_batch));
  }

    // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  void AppendReplicateBatchAndCommitEntryPairsToLog(int count, bool sync = true) {
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

 protected:
  Schema schema_;
  gscoped_ptr<Log> log_;
  gscoped_ptr<LogReader> log_reader_;
  gscoped_ptr<FsManager> fs_manager_;
  uint32_t current_id_;
  LogOptions options_;
  // Reusable entries vector that deletes the entries on destruction.
  vector<LogEntryPB* > entries_;
  scoped_refptr<OpIdAnchorRegistry> opid_anchor_registry_;
};


} // namespace log
} // namespace kudu

#endif
