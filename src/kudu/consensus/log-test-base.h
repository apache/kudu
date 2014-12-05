// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CONSENSUS_LOG_TEST_BASE_H
#define KUDU_CONSENSUS_LOG_TEST_BASE_H

#include "kudu/consensus/log.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <utility>
#include <vector>
#include <string>

#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/stopwatch.h"

namespace kudu {
namespace log {

using consensus::OpId;
using consensus::CommitMsg;
using consensus::ReplicateMsg;
using consensus::WRITE_OP;
using consensus::NO_OP;

using tserver::WriteRequestPB;

using tablet::TxResultPB;
using tablet::OperationResultPB;
using tablet::MemStoreTargetPB;

const char* kTestTable = "test-log-table";
const char* kTestTablet = "test-log-tablet";
const bool APPEND_SYNC = true;
const bool APPEND_ASYNC = false;

static Status AppendNoOpToLogSync(Log* log, OpId* op_id, int* size = NULL) {
  LogEntryPB log_entry;
  log_entry.set_type(REPLICATE);
  ReplicateMsg* repl = log_entry.mutable_replicate();
  repl->mutable_id()->CopyFrom(*op_id);
  repl->set_op_type(NO_OP);
  RETURN_NOT_OK(log->Append(&log_entry));
  if (size) {
    // If we're tracking the sizes we need to account for the fact
    // that the Log wraps the log entry in an LogEntryBatchPB and
    // that entries are length-prefix encoded (+ 4 bytes).
    LogEntryBatchPB batch;
    batch.add_entry()->CopyFrom(log_entry);
    *size += batch.ByteSize() + log::kEntryHeaderSize;
  }
  // Increment op_id.
  op_id->set_index(op_id->index() + 1);
  return Status::OK();
}

class LogTestBase : public KuduTest {
 public:

  typedef pair<int, int> DeltaId;

  LogTestBase()
    : schema_(GetSimpleTestSchema()),
      log_anchor_registry_(new LogAnchorRegistry()) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    current_id_ = 0;
    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
  }

  virtual void TearDown() OVERRIDE {
    KuduTest::TearDown();
    STLDeleteElements(&entries_);
  }

  void BuildLog() {
    CHECK_OK(Log::Open(options_,
                       fs_manager_.get(),
                       kTestTablet,
                       schema_,
                       NULL,
                       &log_));
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
      if (entry->type() == REPLICATE) {
        ids->push_back(entry->replicate().id().index());
      }
    }
  }

  static void CheckReplicateResult(const consensus::ReplicateRefPtr& msg, const Status& s) {
    CHECK_OK(s);
  }

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  void AppendReplicateBatch(int index, bool sync = APPEND_SYNC) {
    consensus::ReplicateRefPtr replicate = make_scoped_refptr_replicate(new ReplicateMsg());
    replicate->get()->set_op_type(WRITE_OP);

    OpId* op_id = replicate->get()->mutable_id();
    op_id->set_term(0);
    op_id->set_index(index);

    WriteRequestPB* batch_request = replicate->get()->mutable_write_request();
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
      Synchronizer s;
      ASSERT_STATUS_OK(log_->AsyncAppendReplicates(boost::assign::list_of(replicate),
                                                   s.AsStatusCallback()));
      ASSERT_STATUS_OK(s.Wait());
    } else {
      // AsyncAppendReplicates does not free the ReplicateMsg on completion, so we
      // need to pass it through to our callback.
      ASSERT_STATUS_OK(log_->AsyncAppendReplicates(boost::assign::list_of(replicate),
                                                   Bind(&LogTestBase::CheckReplicateResult,
                                                        replicate)));
    }
  }

  static void CheckCommitResult(const Status& s) {
    CHECK_OK(s);
  }

  // Append a commit log entry containing one entry for the insert and one
  // for the mutate.
  void AppendCommit(int original_op_index, bool sync = APPEND_SYNC) {
    // The mrs id for the insert.
    const int kTargetMrsId = 1;

    // The rs and delta ids for the mutate.
    const int kTargetRsId = 0;
    const int kTargetDeltaId = 0;

    AppendCommit(original_op_index, kTargetMrsId, kTargetRsId, kTargetDeltaId, sync);
  }

  void AppendCommit(int original_op_index, int mrs_id, int rs_id, int dms_id,
                    bool sync = APPEND_SYNC) {
    gscoped_ptr<CommitMsg> commit(new CommitMsg);
    commit->set_op_type(WRITE_OP);
    commit->set_timestamp(Timestamp(original_op_index).ToUint64());

    OpId* original_op_id = commit->mutable_commited_op_id();
    original_op_id->set_term(0);
    original_op_id->set_index(original_op_index);

    TxResultPB* result = commit->mutable_result();

    OperationResultPB* insert = result->add_ops();
    insert->add_mutated_stores()->set_mrs_id(mrs_id);

    OperationResultPB* mutate = result->add_ops();
    MemStoreTargetPB* target = mutate->add_mutated_stores();
    target->set_dms_id(dms_id);
    target->set_rs_id(rs_id);

    if (sync) {
      Synchronizer s;
      ASSERT_STATUS_OK(log_->AsyncAppendCommit(commit.Pass(), s.AsStatusCallback()));
      ASSERT_STATUS_OK(s.Wait());
    } else {
      ASSERT_STATUS_OK(log_->AsyncAppendCommit(commit.Pass(),
                                               Bind(&LogTestBase::CheckCommitResult)));
    }
  }

    // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  void AppendReplicateBatchAndCommitEntryPairsToLog(int count, bool sync = true) {
    for (int i = 0; i < count; i++) {
      AppendReplicateBatch(current_id_, sync);
      AppendCommit(current_id_, sync);
      current_id_ += 1;
    }
  }

  // Append a single NO_OP entry. Increments op_id by one.
  // If non-NULL, and if the write is successful, 'size' is incremented
  // by the size of the written operation.
  Status AppendNoOp(OpId* op_id, int* size = NULL) {
    return AppendNoOpToLogSync(log_.get(), op_id, size);
  }

  // Append a number of no-op entries to the log.
  // Increments op_id's index by the number of records written.
  // If non-NULL, 'size' keeps track of the size of the operations
  // successfully written.
  Status AppendNoOps(OpId* op_id, int num, int* size = NULL) {
    for (int i = 0; i < num; i++) {
      RETURN_NOT_OK(AppendNoOp(op_id, size));
    }
    return Status::OK();
  }

  Status RollLog() {
    RETURN_NOT_OK(log_->AsyncAllocateSegment());
    return log_->RollOver();
  }

  string DumpSegmentsToString(const SegmentSequence& segments) {
    string dump;
    BOOST_FOREACH(const scoped_refptr<ReadableLogSegment>& segment, segments) {
      dump.append("------------\n");
      strings::SubstituteAndAppend(&dump, "Segment: $0, Path: $1\n",
                                   segment->header().sequence_number(), segment->path());
      strings::SubstituteAndAppend(&dump, "Header: $0\n",
                                   segment->header().ShortDebugString());
      if (segment->HasFooter()) {
        strings::SubstituteAndAppend(&dump, "Footer: $0\n", segment->footer().ShortDebugString());
      } else {
        dump.append("Footer: None or corrupt.");
      }
    }
    return dump;
  }

 protected:
  const Schema schema_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<Log> log_;
  uint32_t current_id_;
  LogOptions options_;
  // Reusable entries vector that deletes the entries on destruction.
  vector<LogEntryPB* > entries_;
  scoped_refptr<LogAnchorRegistry> log_anchor_registry_;
};

// Corrupts the last segment of the provided log by either truncating it
// or modifying a byte at the given offset.
enum CorruptionType {
  TRUNCATE_FILE,
  FLIP_BYTE
};

Status CorruptLogFile(Env* env, Log* log, CorruptionType type, int corruption_offset) {
  const string log_path = log->ActiveSegmentPathForTests();
  faststring buf;
  RETURN_NOT_OK_PREPEND(ReadFileToString(env, log_path, &buf),
                        "Couldn't read log");

  switch (type) {
    case TRUNCATE_FILE:
      buf.resize(corruption_offset);
      break;
    case FLIP_BYTE:
      CHECK_LT(corruption_offset, buf.size());
      buf[corruption_offset] ^= 0xff;
      break;
  }

  // Rewrite the file with the corrupt log.
  RETURN_NOT_OK_PREPEND(WriteStringToFile(env, Slice(buf), log_path),
                        "Couldn't rewrite corrupt log file");

  return Status::OK();
}

} // namespace log
} // namespace kudu

#endif
