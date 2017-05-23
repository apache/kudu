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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace tablet {

class TestTabletMetadata : public KuduTabletTest {
 public:
  TestTabletMetadata()
      : KuduTabletTest(GetSimpleTestSchema()) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
    writer_.reset(new LocalTabletWriter(harness_->tablet().get(),
                                        &client_schema_));
  }

  void BuildPartialRow(int key, int intval, const char* strval,
                       gscoped_ptr<KuduPartialRow>* row);

 protected:
  gscoped_ptr<LocalTabletWriter> writer_;
};

void TestTabletMetadata::BuildPartialRow(int key, int intval, const char* strval,
                                         gscoped_ptr<KuduPartialRow>* row) {
  row->reset(new KuduPartialRow(&client_schema_));
  CHECK_OK((*row)->SetInt32(0, key));
  CHECK_OK((*row)->SetInt32(1, intval));
  CHECK_OK((*row)->SetStringCopy(2, strval));
}

// Test that loading & storing the superblock results in an equivalent file.
TEST_F(TestTabletMetadata, TestLoadFromSuperBlock) {
  // Write some data to the tablet and flush.
  gscoped_ptr<KuduPartialRow> row;
  BuildPartialRow(0, 0, "foo", &row);
  writer_->Insert(*row);
  ASSERT_OK(harness_->tablet()->Flush());

  // Create one more rowset. Write and flush.
  BuildPartialRow(1, 1, "bar", &row);
  writer_->Insert(*row);
  ASSERT_OK(harness_->tablet()->Flush());

  // Shut down the tablet.
  harness_->tablet()->Shutdown();

  TabletMetadata* meta = harness_->tablet()->metadata();

  // Dump the superblock to a PB. Save the PB to the side.
  TabletSuperBlockPB superblock_pb_1;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb_1));

  // Load the superblock PB back into the TabletMetadata.
  ASSERT_OK(meta->ReplaceSuperBlock(superblock_pb_1));

  // Dump the tablet metadata to a superblock PB again, and save it.
  TabletSuperBlockPB superblock_pb_2;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb_2));

  // Compare the 2 dumped superblock PBs.
  ASSERT_EQ(superblock_pb_1.SerializeAsString(),
            superblock_pb_2.SerializeAsString())
    << pb_util::SecureDebugString(superblock_pb_1)
    << pb_util::SecureDebugString(superblock_pb_2);

  LOG(INFO) << "Superblocks match:\n"
            << pb_util::SecureDebugString(superblock_pb_1);
}

TEST_F(TestTabletMetadata, TestOnDiskSize) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  // The tablet metadata was flushed on creation.
  int64_t initial_size = meta->on_disk_size();
  ASSERT_GT(initial_size, 0);

  // Write some data to the tablet and flush.
  gscoped_ptr<KuduPartialRow> row;
  BuildPartialRow(0, 0, "foo", &row);
  writer_->Insert(*row);
  ASSERT_OK(harness_->tablet()->Flush());

  // The tablet metadata grows after flushing a new rowset.
  int64_t middle_size = meta->on_disk_size();
  ASSERT_GT(middle_size, initial_size);

  // Create another rowset.
  // The on-disk size shouldn't change until after flush.
  BuildPartialRow(1, 1, "bar", &row);
  writer_->Insert(*row);
  ASSERT_EQ(middle_size, meta->on_disk_size());
  ASSERT_OK(harness_->tablet()->Flush());
  int64_t final_size = meta->on_disk_size();
  ASSERT_GT(final_size, middle_size);

  // Shut down the tablet.
  harness_->tablet()->Shutdown();

  // The tablet metadata is a container file holding the superblock PB,
  // so the on_disk_size should be at least as big.
  TabletSuperBlockPB superblock_pb;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb));
  ASSERT_GE(final_size, superblock_pb.ByteSize());
}


} // namespace tablet
} // namespace kudu
