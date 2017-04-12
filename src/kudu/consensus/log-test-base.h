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
#ifndef KUDU_CONSENSUS_LOG_TEST_BASE_H
#define KUDU_CONSENSUS_LOG_TEST_BASE_H

#include "kudu/consensus/log.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/server/clock.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/env_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(tablet);

namespace kudu {
namespace log {

using consensus::OpId;
using consensus::CommitMsg;
using consensus::ReplicateMsg;
using consensus::WRITE_OP;
using consensus::NO_OP;

using server::Clock;

using tserver::WriteRequestPB;

using tablet::TxResultPB;
using tablet::OperationResultPB;
using tablet::MemStoreTargetPB;

constexpr char kTestTable[] = "test-log-table";
constexpr char kTestTableId[] = "test-log-table-id";
constexpr char kTestTablet[] = "test-log-tablet";
constexpr bool APPEND_SYNC = true;
constexpr bool APPEND_ASYNC = false;

// Append a single batch of 'count' NoOps to the log.
// If 'size' is not NULL, increments it by the expected increase in log size.
// Increments 'op_id''s index once for each operation logged.
inline Status AppendNoOpsToLogSync(const scoped_refptr<Clock>& clock,
                            Log* log,
                            OpId* op_id,
                            int count,
                            int* size = NULL) {

  vector<consensus::ReplicateRefPtr> replicates;
  for (int i = 0; i < count; i++) {
    consensus::ReplicateRefPtr replicate = make_scoped_refptr_replicate(new ReplicateMsg());
    ReplicateMsg* repl = replicate->get();

    repl->mutable_id()->CopyFrom(*op_id);
    repl->set_op_type(NO_OP);
    repl->set_timestamp(clock->Now().ToUint64());

    // Increment op_id.
    op_id->set_index(op_id->index() + 1);

    if (size) {
      // If we're tracking the sizes we need to account for the fact that the Log wraps the
      // log entry in an LogEntryBatchPB, and each actual entry will have a one-byte tag.
      *size += repl->ByteSize() + 1;
    }
    replicates.push_back(replicate);
  }

  // Account for the entry batch header and wrapper PB.
  if (size) {
    *size += log::kEntryHeaderSizeV2 + 5;
  }

  Synchronizer s;
  RETURN_NOT_OK(log->AsyncAppendReplicates(replicates,
                                           s.AsStatusCallback()));
  return s.Wait();
}

inline Status AppendNoOpToLogSync(const scoped_refptr<Clock>& clock,
                           Log* log,
                           OpId* op_id,
                           int* size = NULL) {
  return AppendNoOpsToLogSync(clock, log, op_id, 1, size);
}


// Corrupts the last segment of the provided log by either truncating it
// or modifying a byte at the given offset.
enum CorruptionType {
  TRUNCATE_FILE,
  FLIP_BYTE
};

inline Status CorruptLogFile(Env* env, const string& log_path,
                             CorruptionType type, int corruption_offset) {
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

class LogTestBase : public KuduTest {
 public:
  typedef pair<int, int> DeltaId;

  LogTestBase()
    : schema_(GetSimpleTestSchema()),
      log_anchor_registry_(new LogAnchorRegistry()) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    current_index_ = kStartIndex;
    fs_manager_.reset(new FsManager(env_, GetTestPath("fs_root")));
    metric_registry_.reset(new MetricRegistry());
    metric_entity_ = METRIC_ENTITY_tablet.Instantiate(metric_registry_.get(), "log-test-base");
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());

    clock_.reset(new server::HybridClock());
    ASSERT_OK(clock_->Init());
  }

  virtual void TearDown() OVERRIDE {
    KuduTest::TearDown();
    STLDeleteElements(&entries_);
  }

  Status BuildLog() {
    Schema schema_with_ids = SchemaBuilder(schema_).Build();
    return Log::Open(options_,
                     fs_manager_.get(),
                     kTestTablet,
                     schema_with_ids,
                     0, // schema_version
                     metric_entity_.get(),
                     &log_);
  }

  void CheckRightNumberOfSegmentFiles(int expected) {
    // Test that we actually have the expected number of files in the fs.
    // We should have n segments plus '.' and '..'
    vector<string> files;
    ASSERT_OK(env_->GetChildren(
                       JoinPathSegments(fs_manager_->GetWalsRootDir(),
                                        kTestTablet),
                       &files));
    int count = 0;
    for (const string& s : files) {
      if (HasPrefixString(s, FsManager::kWalFileNamePrefix)) {
        count++;
      }
    }
    ASSERT_EQ(expected, count);
  }

  void EntriesToIdList(vector<uint32_t>* ids) {
    for (const LogEntryPB* entry : entries_) {
      VLOG(2) << "Entry contents: " << SecureDebugString(*entry);
      if (entry->type() == REPLICATE) {
        ids->push_back(entry->replicate().id().index());
      }
    }
  }

  static void CheckReplicateResult(const consensus::ReplicateRefPtr& msg, const Status& s) {
    CHECK_OK(s);
  }

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  void AppendReplicateBatch(const OpId& opid, bool sync = APPEND_SYNC) {
    consensus::ReplicateRefPtr replicate = make_scoped_refptr_replicate(new ReplicateMsg());
    replicate->get()->set_op_type(WRITE_OP);
    replicate->get()->mutable_id()->CopyFrom(opid);
    replicate->get()->set_timestamp(clock_->Now().ToUint64());
    WriteRequestPB* batch_request = replicate->get()->mutable_write_request();
    ASSERT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
    AddTestRowToPB(RowOperationsPB::INSERT, schema_,
                   opid.index(),
                   0,
                   "this is a test insert",
                   batch_request->mutable_row_operations());
    AddTestRowToPB(RowOperationsPB::UPDATE, schema_,
                   opid.index() + 1,
                   0,
                   "this is a test mutate",
                   batch_request->mutable_row_operations());
    batch_request->set_tablet_id(kTestTablet);
    AppendReplicateBatch(replicate, sync);
  }

  // Appends the provided batch to the log.
  void AppendReplicateBatch(const consensus::ReplicateRefPtr& replicate,
                            bool sync = APPEND_SYNC) {
    if (sync) {
      Synchronizer s;
      ASSERT_OK(log_->AsyncAppendReplicates({ replicate }, s.AsStatusCallback()));
      ASSERT_OK(s.Wait());
    } else {
      // AsyncAppendReplicates does not free the ReplicateMsg on completion, so we
      // need to pass it through to our callback.
      ASSERT_OK(log_->AsyncAppendReplicates({ replicate },
                                            Bind(&LogTestBase::CheckReplicateResult, replicate)));
    }
  }

  static void CheckCommitResult(const Status& s) {
    CHECK_OK(s);
  }

  // Append a commit log entry containing one entry for the insert and one
  // for the mutate.
  void AppendCommit(const OpId& original_opid,
                    bool sync = APPEND_SYNC) {
    // The mrs id for the insert.
    const int kTargetMrsId = 1;

    // The rs and delta ids for the mutate.
    const int kTargetRsId = 0;
    const int kTargetDeltaId = 0;

    AppendCommit(original_opid, kTargetMrsId, kTargetRsId, kTargetDeltaId, sync);
  }

  void AppendCommit(const OpId& original_opid,
                    int mrs_id, int rs_id, int dms_id,
                    bool sync = APPEND_SYNC) {
    gscoped_ptr<CommitMsg> commit(new CommitMsg);
    commit->set_op_type(WRITE_OP);

    commit->mutable_commited_op_id()->CopyFrom(original_opid);

    TxResultPB* result = commit->mutable_result();

    OperationResultPB* insert = result->add_ops();
    insert->add_mutated_stores()->set_mrs_id(mrs_id);

    OperationResultPB* mutate = result->add_ops();
    MemStoreTargetPB* target = mutate->add_mutated_stores();
    target->set_dms_id(dms_id);
    target->set_rs_id(rs_id);
    AppendCommit(std::move(commit), sync);
  }

  // Append a COMMIT message for 'original_opid', but with results
  // indicating that the associated writes failed due to
  // "NotFound" errors.
  void AppendCommitWithNotFoundOpResults(const OpId& original_opid) {
    gscoped_ptr<CommitMsg> commit(new CommitMsg);
    commit->set_op_type(WRITE_OP);

    commit->mutable_commited_op_id()->CopyFrom(original_opid);

    TxResultPB* result = commit->mutable_result();

    OperationResultPB* insert = result->add_ops();
    StatusToPB(Status::NotFound("fake failed write"), insert->mutable_failed_status());
    OperationResultPB* mutate = result->add_ops();
    StatusToPB(Status::NotFound("fake failed write"), mutate->mutable_failed_status());

    AppendCommit(std::move(commit));
  }

  void AppendCommit(gscoped_ptr<CommitMsg> commit, bool sync = APPEND_SYNC) {
    if (sync) {
      Synchronizer s;
      ASSERT_OK(log_->AsyncAppendCommit(std::move(commit), s.AsStatusCallback()));
      ASSERT_OK(s.Wait());
    } else {
      ASSERT_OK(log_->AsyncAppendCommit(std::move(commit),
                                               Bind(&LogTestBase::CheckCommitResult)));
    }
  }

    // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  void AppendReplicateBatchAndCommitEntryPairsToLog(int count, bool sync = true) {
    for (int i = 0; i < count; i++) {
      OpId opid = consensus::MakeOpId(1, current_index_);
      AppendReplicateBatch(opid);
      AppendCommit(opid, sync);
      current_index_ += 1;
    }
  }

  // Append a single NO_OP entry. Increments op_id by one.
  // If non-NULL, and if the write is successful, 'size' is incremented
  // by the size of the written operation.
  Status AppendNoOp(OpId* op_id, int* size = NULL) {
    return AppendNoOpToLogSync(clock_, log_.get(), op_id, size);
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
    for (const scoped_refptr<ReadableLogSegment>& segment : segments) {
      dump.append("------------\n");
      strings::SubstituteAndAppend(&dump, "Segment: $0, Path: $1\n",
                                   segment->header().sequence_number(), segment->path());
      strings::SubstituteAndAppend(&dump, "Header: $0\n",
                                   SecureShortDebugString(segment->header()));
      if (segment->HasFooter()) {
        strings::SubstituteAndAppend(&dump, "Footer: $0\n",
                                     SecureShortDebugString(segment->footer()));
      } else {
        dump.append("Footer: None or corrupt.");
      }
    }
    return dump;
  }

 protected:
  enum {
    kStartIndex = 1
  };

  const Schema schema_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<Log> log_;
  int64_t current_index_;
  LogOptions options_;
  // Reusable entries vector that deletes the entries on destruction.
  vector<LogEntryPB* > entries_;
  scoped_refptr<LogAnchorRegistry> log_anchor_registry_;
  scoped_refptr<Clock> clock_;
};

} // namespace log
} // namespace kudu

#endif
