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

#include "kudu/consensus/log.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env_util.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_entity(tablet);

namespace kudu {
namespace log {

constexpr char kTestTable[] = "test-log-table";
constexpr char kTestTableId[] = "test-log-table-id";
constexpr char kTestTablet[] = "test-log-tablet";
constexpr bool APPEND_SYNC = true;
constexpr bool APPEND_ASYNC = false;

// Append a single batch of 'count' NoOps to the log.
// If 'size' is not NULL, increments it by the expected increase in log size.
// Increments 'op_id''s index once for each operation logged.
inline Status AppendNoOpsToLogSync(clock::Clock* clock,
                                   Log* log,
                                   consensus::OpId* op_id,
                                   int count,
                                   size_t* size = nullptr) {

  std::vector<consensus::ReplicateRefPtr> replicates;
  for (int i = 0; i < count; i++) {
    consensus::ReplicateRefPtr replicate =
        make_scoped_refptr_replicate(new consensus::ReplicateMsg());
    consensus::ReplicateMsg* repl = replicate->get();

    repl->mutable_id()->CopyFrom(*op_id);
    repl->set_op_type(consensus::NO_OP);
    repl->set_timestamp(clock->Now().ToUint64());

    // Increment op_id.
    op_id->set_index(op_id->index() + 1);

    if (size) {
      // If we're tracking the sizes we need to account for the fact that the Log wraps the
      // log entry in an LogEntryBatchPB, and each actual entry will have a one-byte tag.
      *size += repl->ByteSizeLong() + 1;
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

inline Status AppendNoOpToLogSync(clock::Clock* clock,
                                  Log* log,
                                  consensus::OpId* op_id,
                                  size_t* size = nullptr) {
  return AppendNoOpsToLogSync(clock, log, op_id, 1, size);
}


// Corrupts the last segment of the provided log by either truncating it
// or modifying a byte at the given offset.
enum CorruptionType {
  TRUNCATE_FILE,
  FLIP_BYTE
};

inline Status CorruptLogFile(Env* env, const std::string& log_path,
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
  typedef std::pair<int, int> DeltaId;

  LogTestBase()
      : schema_(GetSimpleTestSchema()),
        log_anchor_registry_(new LogAnchorRegistry) {
  }

  void SetUp() override {
    KuduTest::SetUp();
    current_index_ = kStartIndex;
    fs_manager_.reset(new FsManager(env_, FsManagerOpts(GetTestPath("fs_root"))));
    metric_registry_.reset(new MetricRegistry);
    metric_entity_tablet_ = METRIC_ENTITY_tablet.Instantiate(
        metric_registry_.get(), "tablet");
    metric_entity_server_ = METRIC_ENTITY_server.Instantiate(
        metric_registry_.get(), "server");
    // Capacity was chosen arbitrarily: high enough to cache multiple files, but
    // low enough to see some eviction.
    file_cache_.reset(new FileCache("log-test-base", env_, 5, metric_entity_server_));
    ASSERT_OK(file_cache_->Init());
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());

    clock_.reset(new clock::HybridClock(metric_entity_server_));
    ASSERT_OK(clock_->Init());
  }

  void TearDown() override {
    KuduTest::TearDown();
  }

  Status BuildLog() {
    SchemaPtr schema_with_ids_ptr = std::make_shared<Schema>(SchemaBuilder(schema_).Build());
    return Log::Open(options_,
                     fs_manager_.get(),
                     file_cache_.get(),
                     kTestTablet,
                     schema_with_ids_ptr,
                     0, // schema_version
                     metric_entity_tablet_.get(),
                     &log_);
  }

  void CheckRightNumberOfSegmentFiles(int expected) {
    // Test that we actually have the expected number of files in the fs.
    // We should have n segments plus '.' and '..'
    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(
                       JoinPathSegments(fs_manager_->GetWalsRootDir(),
                                        kTestTablet),
                       &files));
    int count = 0;
    for (const std::string& s : files) {
      if (HasPrefixString(s, FsManager::kWalFileNamePrefix)) {
        count++;
      }
    }
    ASSERT_EQ(expected, count);
  }

  void EntriesToIdList(std::vector<uint32_t>* ids) {
    for (const auto& entry : entries_) {
      VLOG(2) << "Entry contents: " << pb_util::SecureDebugString(*entry);
      if (entry->type() == REPLICATE) {
        ids->push_back(entry->replicate().id().index());
      }
    }
  }

  static void CheckReplicateResult(const consensus::ReplicateRefPtr& msg, const Status& s) {
    CHECK_OK(s);
  }

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  Status AppendReplicateBatch(const consensus::OpId& opid,
                              bool sync = APPEND_SYNC) {
    consensus::ReplicateRefPtr replicate =
        make_scoped_refptr_replicate(new consensus::ReplicateMsg());
    replicate->get()->set_op_type(consensus::WRITE_OP);
    replicate->get()->mutable_id()->CopyFrom(opid);
    replicate->get()->set_timestamp(clock_->Now().ToUint64());
    tserver::WriteRequestPB* batch_request = replicate->get()->mutable_write_request();
    RETURN_NOT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
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
    return AppendReplicateBatch(replicate, sync);
  }

  // Appends the provided batch to the log.
  Status AppendReplicateBatch(const consensus::ReplicateRefPtr& replicate,
                              bool sync = APPEND_SYNC) {
    if (sync) {
      Synchronizer s;
      RETURN_NOT_OK(log_->AsyncAppendReplicates({ replicate }, s.AsStatusCallback()));
      return s.Wait();
    }
    // AsyncAppendReplicates does not free the ReplicateMsg on completion, so we
    // need to pass it through to our callback.
    return log_->AsyncAppendReplicates(
        { replicate }, [replicate](const Status& s) { CheckReplicateResult(replicate, s); });
  }

  static void CheckCommitResult(const Status& s) {
    CHECK_OK(s);
  }

  // Append a commit log entry containing one entry for the insert and one
  // for the mutate.
  Status AppendCommit(const consensus::OpId& original_opid,
                      bool sync = APPEND_SYNC) {
    // The mrs id for the insert.
    constexpr int kTargetMrsId = 1;

    // The rs and delta ids for the mutate.
    constexpr int kTargetRsId = 0;
    constexpr int kTargetDeltaId = 0;

    return AppendCommit(original_opid, kTargetMrsId, kTargetRsId, kTargetDeltaId, sync);
  }

  Status AppendCommit(const consensus::OpId& original_opid,
                      int mrs_id,
                      int rs_id,
                      int dms_id,
                      bool sync = APPEND_SYNC) {
    consensus::CommitMsg commit;
    commit.set_op_type(consensus::WRITE_OP);

    commit.mutable_commited_op_id()->CopyFrom(original_opid);

    tablet::TxResultPB* result = commit.mutable_result();

    tablet::OperationResultPB* insert = result->add_ops();
    insert->add_mutated_stores()->set_mrs_id(mrs_id);

    tablet::OperationResultPB* mutate = result->add_ops();
    tablet::MemStoreTargetPB* target = mutate->add_mutated_stores();
    target->set_dms_id(dms_id);
    target->set_rs_id(rs_id);
    return AppendCommit(commit, sync);
  }

  // Append a COMMIT message for 'original_opid', but with results
  // indicating that the associated writes failed due to
  // "NotFound" errors.
  Status AppendCommitWithNotFoundOpResults(const consensus::OpId& original_opid) {
    consensus::CommitMsg commit;
    commit.set_op_type(consensus::WRITE_OP);
    commit.mutable_commited_op_id()->CopyFrom(original_opid);

    tablet::TxResultPB* result = commit.mutable_result();

    tablet::OperationResultPB* insert = result->add_ops();
    StatusToPB(Status::NotFound("fake failed write"), insert->mutable_failed_status());
    tablet::OperationResultPB* mutate = result->add_ops();
    StatusToPB(Status::NotFound("fake failed write"), mutate->mutable_failed_status());

    return AppendCommit(commit);
  }

  Status AppendCommit(const consensus::CommitMsg& commit,
                      bool sync = APPEND_SYNC) {
    if (sync) {
      Synchronizer s;
      RETURN_NOT_OK(log_->AsyncAppendCommit(commit, s.AsStatusCallback()));
      return s.Wait();
    }
    return log_->AsyncAppendCommit(commit,
                                   [](const Status& s) { CheckCommitResult(s); });
  }

    // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  Status AppendReplicateBatchAndCommitEntryPairsToLog(int count,
                                                      bool sync = APPEND_SYNC) {
    for (int i = 0; i < count; i++) {
      consensus::OpId opid = consensus::MakeOpId(1, current_index_);
      RETURN_NOT_OK(AppendReplicateBatch(opid));
      RETURN_NOT_OK(AppendCommit(opid, sync));
      current_index_ += 1;
    }
    return Status::OK();
  }

  // Append a single NO_OP entry. Increments op_id by one.
  // If non-NULL, and if the write is successful, 'size' is incremented
  // by the size of the written operation.
  Status AppendNoOp(consensus::OpId* op_id, size_t* size = nullptr) {
    return AppendNoOpToLogSync(clock_.get(), log_.get(), op_id, size);
  }

  // Append a number of no-op entries to the log.
  // Increments op_id's index by the number of records written.
  // If non-NULL, 'size' keeps track of the size of the operations
  // successfully written.
  Status AppendNoOps(consensus::OpId* op_id, int num, size_t* size = nullptr) {
    for (int i = 0; i < num; i++) {
      RETURN_NOT_OK(AppendNoOp(op_id, size));
    }
    return Status::OK();
  }

  Status RollLog() {
    return log_->AllocateSegmentAndRollOverForTests();
  }

  static std::string DumpSegmentsToString(const SegmentSequence& segments) {
    std::string dump;
    for (const scoped_refptr<ReadableLogSegment>& segment : segments) {
      dump.append("------------\n");
      strings::SubstituteAndAppend(&dump, "Segment: $0, Path: $1\n",
                                   segment->header().sequence_number(), segment->path());
      strings::SubstituteAndAppend(&dump, "Header: $0\n",
                                   pb_util::SecureShortDebugString(segment->header()));
      if (segment->HasFooter()) {
        strings::SubstituteAndAppend(&dump, "Footer: $0\n",
                                     pb_util::SecureShortDebugString(segment->footer()));
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
  std::unique_ptr<FsManager> fs_manager_;
  std::unique_ptr<MetricRegistry> metric_registry_;
  std::unique_ptr<FileCache> file_cache_;
  scoped_refptr<MetricEntity> metric_entity_tablet_;
  scoped_refptr<MetricEntity> metric_entity_server_;
  scoped_refptr<Log> log_;
  int64_t current_index_;
  LogOptions options_;
  // Reusable entries vector that deletes the entries on destruction.
  LogEntries entries_;
  scoped_refptr<LogAnchorRegistry> log_anchor_registry_;
  std::unique_ptr<clock::Clock> clock_;
};

} // namespace log
} // namespace kudu
