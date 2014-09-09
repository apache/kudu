// Copyright (c) 2014, Cloudera, inc.
#include "kudu/tserver/remote_bootstrap-test-base.h"

#include "kudu/gutil/strings/fastmem.h"
#include "kudu/tserver/remote_bootstrap_client.h"

namespace kudu {
namespace tserver {

class RemoteBootstrapClientTest : public RemoteBootstrapTest {
 public:
  virtual void SetUp() OVERRIDE {
    RemoteBootstrapTest::SetUp();

    fs_manager_.reset(new FsManager(Env::Default(), GetTestPath("client_tablet")));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());

    tablet_peer_->WaitUntilRunning(MonoDelta::FromSeconds(10.0));
    rpc::MessengerBuilder(CURRENT_TEST_NAME()).Build(&messenger_);
    client_.reset(new RemoteBootstrapClient(fs_manager_.get(),
                                            messenger_,
                                            GetLocalUUID()));
  }

 protected:
  Status CompareFileContents(const string& path1, const string& path2);

  gscoped_ptr<FsManager> fs_manager_;
  shared_ptr<rpc::Messenger> messenger_;
  gscoped_ptr<RemoteBootstrapClient> client_;
};

Status RemoteBootstrapClientTest::CompareFileContents(const string& path1, const string& path2) {
  shared_ptr<RandomAccessFile> file1, file2;
  RETURN_NOT_OK(env_util::OpenFileForRandom(fs_manager_->env(), path1, &file1));
  RETURN_NOT_OK(env_util::OpenFileForRandom(fs_manager_->env(), path2, &file2));

  uint64_t size1, size2;
  RETURN_NOT_OK(file1->Size(&size1));
  RETURN_NOT_OK(file2->Size(&size2));
  if (size1 != size2) {
    return Status::Corruption("Sizes of files don't match",
                              strings::Substitute("$0 vs $1 bytes", size1, size2));
  }

  Slice slice1, slice2;
  faststring scratch1, scratch2;
  RETURN_NOT_OK(env_util::ReadFully(file1.get(), 0, size1, &slice1, scratch1.data()));
  RETURN_NOT_OK(env_util::ReadFully(file2.get(), 0, size2, &slice2, scratch2.data()));
  int result = strings::fastmemcmp_inlined(slice1.data(), slice2.data(), size1);
  if (result != 0) {
    return Status::Corruption("Files do not match");
  }
  return Status::OK();
}

// Basic begin / end remote bootstrap session.
TEST_F(RemoteBootstrapClientTest, TestBeginEndSession) {
  ASSERT_OK(client_->BeginRemoteBootstrapSession(GetTabletId(),
                                                 tablet_peer_->Quorum(), NULL));
  ASSERT_OK(client_->EndRemoteBootstrapSession());
}

// Basic data block download unit test.
TEST_F(RemoteBootstrapClientTest, TestDownloadBlock) {
  ASSERT_OK(client_->BeginRemoteBootstrapSession(GetTabletId(),
                                                 tablet_peer_->Quorum(), NULL));
  BlockId block_id = FirstColumnBlockId(*client_->superblock_);
  Slice slice;
  faststring scratch;

  // Ensure the block wasn't there before (it shouldn't be, we use our own FsManager dir).
  Status s;
  s = ReadLocalBlockFile(fs_manager_.get(), block_id, &scratch, &slice);
  ASSERT_TRUE(s.IsNotFound()) << "Expected block not found: " << s.ToString();

  // Check that the client downloaded the block and verification passed.
  ASSERT_OK(client_->DownloadBlock(block_id));

  // Ensure it placed the block where we expected it to.
  ASSERT_OK(ReadLocalBlockFile(fs_manager_.get(), block_id, &scratch, &slice));
  ASSERT_OK(client_->EndRemoteBootstrapSession());
}

// Basic WAL segment download unit test.
TEST_F(RemoteBootstrapClientTest, TestDownloadWalSegment) {
  const int kSeqNo = 0;
  ASSERT_OK(fs_manager_->CreateDirIfMissing(fs_manager_->GetTabletWalDir(GetTabletId())));

  ASSERT_OK(client_->BeginRemoteBootstrapSession(GetTabletId(),
                                                 tablet_peer_->Quorum(), NULL));
  const consensus::OpId& op_id = *client_->wal_initial_opids_.begin();
  string path = fs_manager_->GetWalSegmentFileName(GetTabletId(), kSeqNo);

  ASSERT_FALSE(fs_manager_->Exists(path));
  ASSERT_OK(client_->DownloadWAL(op_id, kSeqNo));
  ASSERT_TRUE(fs_manager_->Exists(path));

  log::ReadableLogSegmentMap local_segments;
  tablet_peer_->log()->GetReadableLogSegments(&local_segments);
  const scoped_refptr<log::ReadableLogSegment>& segment = local_segments.begin()->second;
  string server_path = segment->path();

  // Compare the downloaded file with the source file.
  ASSERT_OK(CompareFileContents(path, server_path));

  ASSERT_OK(client_->EndRemoteBootstrapSession());
}

// Ensure that we detect data corruption at the per-transfer level.
TEST_F(RemoteBootstrapClientTest, TestVerifyData) {
  string good = "This is a known good string";
  string bad = "This is a known bad! string";
  const int kGoodOffset = 0;
  const int kBadOffset = 1;
  const int kDataTotalLen = std::numeric_limits<uint64_t>::max(); // Ignored.

  // Create a known-good PB.
  DataChunkPB valid_chunk;
  valid_chunk.set_offset(0);
  valid_chunk.set_data(good);
  valid_chunk.set_crc32(crc::Crc32c(good.data(), good.length()));
  valid_chunk.set_total_data_length(kDataTotalLen);

  // Make sure we work on the happy case.
  ASSERT_OK(client_->VerifyData(kGoodOffset, valid_chunk));

  // Test unexpected offset.
  DataChunkPB bad_offset = valid_chunk;
  bad_offset.set_offset(kBadOffset);
  Status s;
  s = client_->VerifyData(kGoodOffset, bad_offset);
  ASSERT_TRUE(s.IsInvalidArgument()) << "Bad offset expected: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Offset did not match");
  LOG(INFO) << "Expected error returned: " << s.ToString();

  // Test bad checksum.
  DataChunkPB bad_checksum = valid_chunk;
  bad_checksum.set_data(bad);
  s = client_->VerifyData(kGoodOffset, bad_checksum);
  ASSERT_TRUE(s.IsCorruption()) << "Invalid checksum expected: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "CRC32 does not match");
  LOG(INFO) << "Expected error returned: " << s.ToString();
}

} // namespace tserver
} // namespace kudu
