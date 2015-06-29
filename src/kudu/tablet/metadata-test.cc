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
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

using boost::assign::list_of;
using std::vector;
using std::string;
using strings::Substitute;

namespace kudu {
namespace tablet {

class MetadataTest : public KuduTest {
 public:
  MetadataTest() {
    all_blocks_ = list_of(BlockId(1))(BlockId(2))(BlockId(3))(BlockId(4));

    tablet_meta_ = new TabletMetadata(NULL, "fake-tablet");
    CHECK_OK(RowSetMetadata::CreateNew(tablet_meta_.get(), 0, &meta_));
    for (int i = 0; i < all_blocks_.size(); i++) {
      CHECK_OK(meta_->CommitRedoDeltaDataBlock(i, all_blocks_[i]));
    }
    CHECK_EQ(4, meta_->redo_delta_blocks().size());
  }

 protected:
  vector<BlockId> all_blocks_;
  scoped_refptr<TabletMetadata> tablet_meta_;
  gscoped_ptr<RowSetMetadata> meta_;
};

// Swap out some deltas from the middle of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_1) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId(2));
  to_replace.push_back(BlockId(3));

  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId(123)))));
  ASSERT_EQ(list_of(BlockId(1))(BlockId(123))(BlockId(4)),
            meta_->redo_delta_blocks());
}

// Swap out some deltas from the beginning of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_2) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId(1));
  to_replace.push_back(BlockId(2));

  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId(123)))));
  ASSERT_EQ(list_of(BlockId(123))(BlockId(3))(BlockId(4)),
            meta_->redo_delta_blocks());
}

// Swap out some deltas from the end of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_3) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId(3));
  to_replace.push_back(BlockId(4));

  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId(123)))));
  ASSERT_EQ(list_of(BlockId(1))(BlockId(2))(BlockId(123)),
            meta_->redo_delta_blocks());
}

// Swap out a non-contiguous list, check error.
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_Bad_NonContiguous) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId(2));
  to_replace.push_back(BlockId(4));

  Status s = meta_->CommitUpdate(
    RowSetMetadataUpdate()
    .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId(123))));
  EXPECT_EQ(Substitute("Invalid argument: Cannot find subsequence <$0> in <$1>",
                       BlockId::JoinStrings(to_replace),
                       BlockId::JoinStrings(all_blocks_)),
            s.ToString());

  // Should be unchanged
  EXPECT_EQ(all_blocks_, meta_->redo_delta_blocks());
}

// Swap out a list which contains an invalid element, check error.
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_Bad_DoesntExist) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId(555));

  Status s = meta_->CommitUpdate(
    RowSetMetadataUpdate()
    .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId(123))));
  EXPECT_EQ(Substitute("Invalid argument: Cannot find subsequence <$0> in <$1>",
                       BlockId::JoinStrings(to_replace),
                       BlockId::JoinStrings(all_blocks_)),
            s.ToString());

  // Should be unchanged
  EXPECT_EQ(all_blocks_, meta_->redo_delta_blocks());
}

} // namespace tablet
} // namespace kudu
