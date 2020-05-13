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

#include "kudu/tserver/tablet_copy_source_session.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_copy.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/crc.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/threadpool.h"

METRIC_DECLARE_entity(tablet);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

class BlockIdPB;

namespace tserver {

using consensus::ConsensusMetadataManager;
using consensus::RaftConfigPB;
using consensus::RaftPeerPB;
using consensus::kMinimumTerm;
using fs::BlockDeletionTransaction;
using fs::ReadableBlock;
using log::Log;
using log::LogOptions;
using log::LogAnchorRegistry;
using rpc::Messenger;
using rpc::MessengerBuilder;
using strings::Substitute;
using tablet::ColumnDataPB;
using tablet::DeltaDataPB;
using tablet::KuduTabletTest;
using tablet::RowSetDataPB;
using tablet::TabletReplica;
using tablet::TabletSuperBlockPB;
using tablet::WriteOpState;

class TabletCopyTest : public KuduTabletTest {
 public:
  TabletCopyTest()
      : KuduTabletTest(Schema({ ColumnSchema("key", STRING),
                                ColumnSchema("val", INT32) }, 1)),
        dns_resolver_(new DnsResolver) {
    CHECK_OK(ThreadPoolBuilder("prepare").Build(&prepare_pool_));
    CHECK_OK(ThreadPoolBuilder("apply").Build(&apply_pool_));
    CHECK_OK(ThreadPoolBuilder("raft").Build(&raft_pool_));
  }

  void SetUp() override {
    NO_FATALS(KuduTabletTest::SetUp());
    NO_FATALS(SetUpTabletReplica());
    NO_FATALS(PopulateTablet());
    NO_FATALS(InitSession());
  }

  void TearDown() override {
    session_.reset();
    tablet_replica_->Shutdown();
    KuduTabletTest::TearDown();
  }

 protected:
  void SetUpTabletReplica() {
    scoped_refptr<Log> log;
    ASSERT_OK(Log::Open(LogOptions(),
                        fs_manager(),
                        /*file_cache=*/ nullptr,
                        tablet()->tablet_id(),
                        *tablet()->schema(),
                        /*schema_version=*/ 0,
                        /*metric_entity=*/ nullptr,
                        &log));

    scoped_refptr<MetricEntity> metric_entity =
      METRIC_ENTITY_tablet.Instantiate(&metric_registry_, CURRENT_TEST_NAME());

    // TODO(mpercy): Similar to code in tablet_replica-test, consider refactor.
    RaftConfigPB config;
    config.set_opid_index(consensus::kInvalidOpIdIndex);
    RaftPeerPB* config_peer = config.add_peers();
    config_peer->set_permanent_uuid(fs_manager()->uuid());
    config_peer->mutable_last_known_addr()->set_host("0.0.0.0");
    config_peer->mutable_last_known_addr()->set_port(0);
    config_peer->set_member_type(RaftPeerPB::VOTER);

    scoped_refptr<ConsensusMetadataManager> cmeta_manager(
        new ConsensusMetadataManager(fs_manager()));
    ASSERT_OK(cmeta_manager->Create(tablet()->tablet_id(), config, kMinimumTerm));

    const auto& tablet_id = tablet()->tablet_id();
    tablet_replica_.reset(
        new TabletReplica(tablet()->metadata(),
                          cmeta_manager,
                          *config_peer,
                          apply_pool_.get(),
                          [this, tablet_id](const string& reason) {
                            this->TabletReplicaStateChangedCallback(tablet_id, reason);
                          }));
    ASSERT_OK(tablet_replica_->Init({ /*quiescing*/nullptr,
                                      /*num_leaders*/nullptr,
                                      raft_pool_.get() }));

    shared_ptr<Messenger> messenger;
    MessengerBuilder mbuilder(CURRENT_TEST_NAME());
    mbuilder.Build(&messenger);

    log_anchor_registry_.reset(new LogAnchorRegistry());
    tablet_replica_->SetBootstrapping();
    consensus::ConsensusBootstrapInfo boot_info;
    ASSERT_OK(tablet_replica_->Start(boot_info,
                                     tablet(),
                                     clock(),
                                     messenger,
                                     scoped_refptr<rpc::ResultTracker>(),
                                     log,
                                     prepare_pool_.get(),
                                     dns_resolver_.get()));
    ASSERT_OK(tablet_replica_->WaitUntilConsensusRunning(MonoDelta::FromSeconds(10)));
    ASSERT_OK(tablet_replica_->consensus()->WaitUntilLeaderForTests(MonoDelta::FromSeconds(10)));
  }

  void TabletReplicaStateChangedCallback(const string& tablet_id, const string& reason) {
    LOG(INFO) << "Tablet replica state changed for tablet " << tablet_id << ". Reason: " << reason;
  }

  void PopulateTablet() {
    for (int32_t i = 0; i < 1000; i++) {
      WriteRequestPB req;
      req.set_tablet_id(tablet_replica_->tablet_id());
      ASSERT_OK(SchemaToPB(client_schema_, req.mutable_schema()));
      RowOperationsPB* data = req.mutable_row_operations();
      RowOperationsPBEncoder enc(data);
      KuduPartialRow row(&client_schema_);

      string key = Substitute("key$0", i);
      ASSERT_OK(row.SetStringNoCopy(0, key));
      ASSERT_OK(row.SetInt32(1, i));
      enc.Add(RowOperationsPB::INSERT, row);

      WriteResponsePB resp;
      CountDownLatch latch(1);

      unique_ptr<tablet::WriteOpState> state(
          new tablet::WriteOpState(tablet_replica_.get(),
                                   &req,
                                   nullptr, // No RequestIdPB
                                   &resp));
      state->set_completion_callback(unique_ptr<tablet::OpCompletionCallback>(
          new tablet::LatchOpCompletionCallback<WriteResponsePB>(&latch, &resp)));
      ASSERT_OK(tablet_replica_->SubmitWrite(std::move(state)));
      latch.Wait();
      ASSERT_FALSE(resp.has_error())
          << "Request failed: " << pb_util::SecureShortDebugString(resp.error());
      ASSERT_EQ(0, resp.per_row_errors_size())
          << "Insert error: " << pb_util::SecureShortDebugString(resp);
    }
    ASSERT_OK(tablet()->Flush());
  }

  void InitSession() {
    session_.reset(new TabletCopySourceSession(tablet_replica_.get(), "TestSession", "FakeUUID",
                   fs_manager(), nullptr /* no metrics */));
    ASSERT_OK(session_->Init());
  }

  // Read the specified BlockId, via the TabletCopySourceSession, into a file.
  // 'path' will be populated with the name of the file used.
  // 'file' will be set to point to the SequentialFile containing the data.
  void FetchBlockToFile(const BlockId& block_id,
                        string* path,
                        unique_ptr<SequentialFile>* file) {
    string data;
    int64_t block_file_size = 0;
    TabletCopyErrorPB::Code error_code;
    CHECK_OK(session_->GetBlockPiece(block_id, 0, 0, &data, &block_file_size, &error_code));
    if (block_file_size > 0) {
      CHECK_GT(data.size(), 0);
    }

    // Write the file to a temporary location.
    WritableFileOptions opts;
    string path_template = GetTestPath(Substitute("test_block_$0$1.XXXXXX",
                                                  block_id.ToString(),
                                                  kTmpInfix));
    unique_ptr<WritableFile> writable_file;
    CHECK_OK(Env::Default()->NewTempWritableFile(opts, path_template, path, &writable_file));
    CHECK_OK(writable_file->Append(Slice(data.data(), data.size())));
    CHECK_OK(writable_file->Close());

    CHECK_OK(Env::Default()->NewSequentialFile(*path, file));
  }

  MetricRegistry metric_registry_;
  scoped_refptr<LogAnchorRegistry> log_anchor_registry_;
  unique_ptr<ThreadPool> prepare_pool_;
  unique_ptr<ThreadPool> apply_pool_;
  unique_ptr<ThreadPool> raft_pool_;
  unique_ptr<DnsResolver> dns_resolver_;
  scoped_refptr<TabletReplica> tablet_replica_;
  scoped_refptr<TabletCopySourceSession> session_;
};

// Ensure that the serialized SuperBlock included in the TabletCopySourceSession is
// equal to the serialized live superblock (on a quiesced tablet).
TEST_F(TabletCopyTest, TestSuperBlocksEqual) {
  // Compare content of superblocks.
  faststring session_buf;
  faststring tablet_buf;

  {
    const TabletSuperBlockPB& session_superblock = session_->tablet_superblock();
    int size = session_superblock.ByteSize();
    session_buf.resize(size);
    uint8_t* session_dst = session_buf.data();
    session_superblock.SerializeWithCachedSizesToArray(session_dst);
  }

  {
    TabletSuperBlockPB tablet_superblock;
    ASSERT_OK(tablet()->metadata()->ToSuperBlock(&tablet_superblock));
    int size = tablet_superblock.ByteSize();
    tablet_buf.resize(size);
    uint8_t* tablet_dst = tablet_buf.data();
    tablet_superblock.SerializeWithCachedSizesToArray(tablet_dst);
  }

  ASSERT_EQ(session_buf.size(), tablet_buf.size());
  int size = tablet_buf.size();
  ASSERT_EQ(0, strings::fastmemcmp_inlined(session_buf.data(), tablet_buf.data(), size));
}

// Test fetching all files from tablet server, ensure the checksums for each
// chunk and the total file sizes match.
TEST_F(TabletCopyTest, TestBlocksEqual) {
  TabletSuperBlockPB tablet_superblock;
  ASSERT_OK(tablet()->metadata()->ToSuperBlock(&tablet_superblock));
  for (int i = 0; i < tablet_superblock.rowsets_size(); i++) {
    const RowSetDataPB& rowset = tablet_superblock.rowsets(i);
    for (int j = 0; j < rowset.columns_size(); j++) {
      const ColumnDataPB& column = rowset.columns(j);
      const BlockIdPB& block_id_pb = column.block();
      BlockId block_id = BlockId::FromPB(block_id_pb);

      string path;
      unique_ptr<SequentialFile> file;
      FetchBlockToFile(block_id, &path, &file);
      uint64_t session_block_size = 0;
      ASSERT_OK(Env::Default()->GetFileSize(path, &session_block_size));
      faststring buf;
      buf.resize(session_block_size);
      Slice data(buf.data(), session_block_size);
      ASSERT_OK(file->Read(&data));
      uint32_t session_crc = crc::Crc32c(data.data(), data.size());
      LOG(INFO) << "session block file has size of " << session_block_size
                << " and CRC32C of " << session_crc << ": " << path;

      unique_ptr<ReadableBlock> tablet_block;
      ASSERT_OK(fs_manager()->OpenBlock(block_id, &tablet_block));
      uint64_t tablet_block_size = 0;
      ASSERT_OK(tablet_block->Size(&tablet_block_size));
      buf.resize(tablet_block_size);
      Slice data2(buf.data(), tablet_block_size);
      ASSERT_OK(tablet_block->Read(0, data2));
      uint32_t tablet_crc = crc::Crc32c(data.data(), data.size());
      LOG(INFO) << "tablet block file has size of " << tablet_block_size
                << " and CRC32C of " << tablet_crc
                << ": " << block_id;

      // Compare the blocks.
      ASSERT_EQ(tablet_block_size, session_block_size);
      ASSERT_EQ(tablet_crc, session_crc);
    }
  }
}

// Ensure that blocks are still readable through the open session even
// after they've been deleted.
TEST_F(TabletCopyTest, TestBlocksAreFetchableAfterBeingDeleted) {
  TabletSuperBlockPB tablet_superblock;
  ASSERT_OK(tablet()->metadata()->ToSuperBlock(&tablet_superblock));

  // Gather all the blocks.
  vector<BlockId> data_blocks;
  for (const RowSetDataPB& rowset : tablet_superblock.rowsets()) {
    for (const DeltaDataPB& redo : rowset.redo_deltas()) {
      data_blocks.push_back(BlockId::FromPB(redo.block()));
    }
    for (const DeltaDataPB& undo : rowset.undo_deltas()) {
      data_blocks.push_back(BlockId::FromPB(undo.block()));
    }
    for (const ColumnDataPB& column : rowset.columns()) {
      data_blocks.push_back(BlockId::FromPB(column.block()));
    }
    if (rowset.has_bloom_block()) {
      data_blocks.push_back(BlockId::FromPB(rowset.bloom_block()));
    }
    if (rowset.has_adhoc_index_block()) {
      data_blocks.push_back(BlockId::FromPB(rowset.adhoc_index_block()));
    }
  }

  // Delete them.
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      fs_manager()->block_manager()->NewDeletionTransaction();
  for (const BlockId& block_id : data_blocks) {
    deletion_transaction->AddDeletedBlock(block_id);
  }
  vector<BlockId> deleted;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
  ASSERT_EQ(data_blocks.size(), deleted.size());

  // Read them back.
  for (const BlockId& block_id : data_blocks) {
    ASSERT_TRUE(session_->IsBlockOpenForTests(block_id));
    string data;
    TabletCopyErrorPB::Code error_code;
    int64_t piece_size;
    ASSERT_OK(session_->GetBlockPiece(block_id, 0, 0,
                                      &data, &piece_size, &error_code));
  }
}

}  // namespace tserver
}  // namespace kudu
