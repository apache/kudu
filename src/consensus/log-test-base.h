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

using metadata::TabletSuperBlockPB;
using metadata::TabletMasterBlockPB;
using metadata::RowSetDataPB;
using metadata::DeltaDataPB;
using metadata::BlockIdPB;
using metadata::kNoDurableMemStore;

const char* kTestTablet = "test-log-tablet";

class LogTestBase : public KuduTest {
 public:

  typedef pair<int, int> DeltaId;

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
                       &opid_anchor_registry_,
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

 protected:
  Schema schema_;
  gscoped_ptr<Log> log_;
  gscoped_ptr<LogReader> log_reader_;
  gscoped_ptr<FsManager> fs_manager_;
  uint32_t current_id_;
  LogOptions options_;
  // Reusable entries vector that deletes the entries on destruction.
  vector<LogEntryPB* > entries_;
  OpIdAnchorRegistry opid_anchor_registry_;
};


} // namespace log
} // namespace kudu

#endif
