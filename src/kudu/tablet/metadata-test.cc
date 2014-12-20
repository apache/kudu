// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

using boost::assign::list_of;
using std::vector;
using std::string;

namespace kudu {
namespace tablet {

class MetadataTest : public KuduTest {
 public:
  MetadataTest() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", STRING));
    CHECK_OK(builder.AddColumn("val", UINT32));
    schema_ = builder.Build();

    CHECK_OK(RowSetMetadata::CreateNew(NULL, 0, schema_, &meta_));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(1, BlockId("delta_001")));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(2, BlockId("delta_002")));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(3, BlockId("delta_003")));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(4, BlockId("delta_004")));
    CHECK_EQ(4, meta_->redo_delta_blocks().size());
  }

 protected:
  gscoped_ptr<RowSetMetadata> meta_;
  Schema schema_;
};

// Swap out some deltas from the middle of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_1) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_002"));
  to_replace.push_back(BlockId("delta_003"));

  vector<BlockId> removed;
  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block"))),
              &removed));
  EXPECT_EQ("delta_001,new_block,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
  EXPECT_EQ("delta_002,delta_003",
            BlockId::JoinStrings(removed));
}

// Swap out some deltas from the beginning of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_2) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_001"));
  to_replace.push_back(BlockId("delta_002"));

  vector<BlockId> removed;
  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block"))),
              &removed));
  EXPECT_EQ("new_block,delta_003,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
  EXPECT_EQ("delta_001,delta_002",
            BlockId::JoinStrings(removed));
}

// Swap out some deltas from the end of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_3) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_003"));
  to_replace.push_back(BlockId("delta_004"));

  vector<BlockId> removed;
  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block"))),
              &removed));
  ASSERT_EQ("delta_001,delta_002,new_block",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
  EXPECT_EQ("delta_003,delta_004",
            BlockId::JoinStrings(removed));
}

// Swap out a non-contiguous list, check error.
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_Bad_NonContiguous) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_002"));
  to_replace.push_back(BlockId("delta_004"));

  vector<BlockId> removed;
  Status s = meta_->CommitUpdate(
    RowSetMetadataUpdate()
    .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block"))),
    &removed);
  EXPECT_EQ("Invalid argument: Cannot find subsequence <delta_002,delta_004> "
            "in <delta_001,delta_002,delta_003,delta_004>",
            s.ToString());

  // Should be unchanged
  EXPECT_EQ("delta_001,delta_002,delta_003,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
  EXPECT_TRUE(removed.empty());
}

// Swap out a list which contains an invalid element, check error.
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_Bad_DoesntExist) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_noexist"));

  vector<BlockId> removed;
  Status s = meta_->CommitUpdate(
    RowSetMetadataUpdate()
    .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block"))),
    &removed);
  EXPECT_EQ("Invalid argument: Cannot find subsequence <delta_noexist> "
            "in <delta_001,delta_002,delta_003,delta_004>",
            s.ToString());

  // Should be unchanged
  EXPECT_EQ("delta_001,delta_002,delta_003,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
  EXPECT_TRUE(removed.empty());
}

} // namespace tablet
} // namespace kudu
