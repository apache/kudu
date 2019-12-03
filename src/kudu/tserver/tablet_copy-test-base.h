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
#ifndef KUDU_TSERVER_TABLET_COPY_TEST_BASE_H_
#define KUDU_TSERVER_TABLET_COPY_TEST_BASE_H_

#include "kudu/tserver/tablet_server-test-base.h"

#include <string>
#include <vector>

#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_manager.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_copy.pb.h"
#include "kudu/util/crc.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tserver {

// Number of times to roll the log.
static const int kNumLogRolls = 2;

class TabletCopyTest : public TabletServerTestBase {
 public:
  virtual void SetUp() OVERRIDE {
    NO_FATALS(TabletServerTestBase::SetUp());
    // Create a tablet server with multiple data dirs. In most cases, this is
    // unimportant, but in some cases can be helpful to test multi-disk
    // behavior and disk failures.
    NO_FATALS(StartTabletServer(kNumDataDirs));
    // Prevent logs from being deleted out from under us until / unless we want
    // to test that we are anchoring correctly. Since GenerateTestData() does a
    // Flush(), Log GC is allowed to eat the logs before we get around to
    // starting a tablet copy session.
    tablet_replica_->log_anchor_registry()->Register(
        consensus::MinimumOpId().index(), CURRENT_TEST_NAME(), &anchor_);
    NO_FATALS(GenerateTestData());
  }

  virtual void TearDown() OVERRIDE {
    if (tablet_replica_) {
      ASSERT_OK(tablet_replica_->log_anchor_registry()->Unregister(&anchor_));
    }
    NO_FATALS(TabletServerTestBase::TearDown());
  }

 protected:
  // Number of data directories on the copying server.
  const int kNumDataDirs = 3;

  // Grab the first column block we find in the SuperBlock.
  static BlockId FirstColumnBlockId(const tablet::TabletSuperBlockPB& superblock) {
    DCHECK_GT(superblock.rowsets_size(), 0);
    const tablet::RowSetDataPB& rowset = superblock.rowsets(0);
    DCHECK_GT(rowset.columns_size(), 0);
    const tablet::ColumnDataPB& column = rowset.columns(0);
    return BlockId::FromPB(column.block());
  }

  // Return a vector of the blocks contained in the specified superblock (not
  // including orphaned blocks).
  static std::vector<BlockId> ListBlocks(const tablet::TabletSuperBlockPB& superblock) {
    std::vector<BlockId> block_ids;
    for (const auto& rowset : superblock.rowsets()) {
      for (const auto& col : rowset.columns()) {
        block_ids.emplace_back(col.block().id());
      }
      for (const auto& redos : rowset.redo_deltas()) {
        block_ids.emplace_back(redos.block().id());
      }
      for (const auto& undos : rowset.undo_deltas()) {
        block_ids.emplace_back(undos.block().id());
      }
      if (rowset.has_bloom_block()) {
        block_ids.emplace_back(rowset.bloom_block().id());
      }
      if (rowset.has_adhoc_index_block()) {
        block_ids.emplace_back(rowset.adhoc_index_block().id());
      }
    }
    return block_ids;
  }

  // Check that the contents and CRC32C of a DataChunkPB are equal to a local buffer.
  static void AssertDataEqual(const uint8_t* local, int64_t size, const DataChunkPB& remote) {
    ASSERT_EQ(size, remote.data().size());
    ASSERT_TRUE(strings::memeq(local, remote.data().data(), size));
    uint32_t crc32 = crc::Crc32c(local, size);
    ASSERT_EQ(crc32, remote.crc32());
  }

  // Generate the test data for the tablet and do the flushing we assume will be
  // done in the unit tests for tablet copy.
  virtual void GenerateTestData() {
    const int kIncr = 50;
    LOG_TIMING(INFO, "Loading test data") {
      for (int row_id = 0; row_id < kNumLogRolls * kIncr; row_id += kIncr) {
        InsertTestRowsRemote(row_id, kIncr);
        ASSERT_OK(tablet_replica_->tablet()->Flush());
        ASSERT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());
      }
    }
  }

  // Return the permananent_uuid of the local service.
  const std::string GetLocalUUID() const {
    return tablet_replica_->permanent_uuid();
  }

  const std::string& GetTabletId() const {
    return tablet_replica_->tablet()->tablet_id();
  }

  // Read a block file from the file system fully into memory and return a
  // Slice pointing to it.
  Status ReadLocalBlockFile(FsManager* fs_manager, const BlockId& block_id,
                            faststring* scratch, Slice* slice) {
    std::unique_ptr<fs::ReadableBlock> block;
    RETURN_NOT_OK(fs_manager->OpenBlock(block_id, &block));

    uint64_t size = 0;
    RETURN_NOT_OK(block->Size(&size));
    scratch->resize(size);
    *slice = Slice(scratch->data(), size);
    RETURN_NOT_OK(block->Read(0, *slice));
    return Status::OK();
  }

  log::LogAnchor anchor_;
};

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_TABLET_COPY_TEST_BASE_H_
