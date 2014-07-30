// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "kudu/fs/block_id.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/env_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::tr1::shared_ptr;

namespace kudu {

class FsManagerTestBase : public KuduTest {
 public:
  void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Initialize File-System Layout
    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
    ASSERT_STATUS_OK(fs_manager_->CreateInitialFileSystemLayout());
  }

  void TestReadWriteDataFile(const Slice& data) {
    uint8_t buffer[64];
    DCHECK_LT(data.size(), sizeof(buffer));

    // Test Write
    BlockId block_id;
    shared_ptr<WritableFile> writer;
    ASSERT_STATUS_OK(fs_manager()->CreateNewBlock(&writer, &block_id));
    ASSERT_STATUS_OK(writer->Append(data));
    ASSERT_STATUS_OK(writer->Close());

    // Test Read
    Slice result;
    shared_ptr<RandomAccessFile> reader;
    ASSERT_STATUS_OK(fs_manager()->OpenBlock(block_id, &reader));
    ASSERT_STATUS_OK(env_util::ReadFully(reader.get(), 0, data.size(), &result, buffer));
    ASSERT_EQ(data.size(), result.size());
    ASSERT_EQ(0, result.compare(data));
  }

  FsManager *fs_manager() const { return fs_manager_.get(); }

 private:
  gscoped_ptr<FsManager> fs_manager_;
};

TEST_F(FsManagerTestBase, TestBaseOperations) {
  fs_manager()->DumpFileSystemTree(std::cout);

  TestReadWriteDataFile(Slice("test0"));
  TestReadWriteDataFile(Slice("test1"));

  fs_manager()->DumpFileSystemTree(std::cout);
}

} // namespace kudu
