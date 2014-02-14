// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log_util.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <utility>

#include "server/metadata.h"
#include "util/test_macros.h"

namespace kudu {
namespace log {

using metadata::DeltaDataPB;
using metadata::RowSetDataPB;
using metadata::TabletSuperBlockPB;

void AddDeltasToRowSet(RowSetDataPB* new_rs, int64_t rs_id,
                       vector<int64_t> deltas) {
  new_rs->set_id(rs_id);
  BOOST_FOREACH(const int64_t delta_id, deltas) {
    DeltaDataPB* delta_data = new_rs->add_redo_deltas();
    delta_data->set_id(delta_id);
  }
}

// case where we flushed a new mrs but there are common dms's
TEST(TestLogUtil, TestHasCommonStoresMemStoresMrsFlush) {
  // the older superblock has the current mrs set to 1 and has a delta (0,0)
  TabletSuperBlockPB older;
  older.set_last_durable_mrs_id(0);
  AddDeltasToRowSet(older.add_rowsets(), 0, boost::assign::list_of(0));
  // the newer superblock has the current mrs set to 2 and has deltas (0,0) and (1,0)
  TabletSuperBlockPB newer;
  newer.set_last_durable_mrs_id(1);
  AddDeltasToRowSet(newer.add_rowsets(), 0, boost::assign::list_of(0));
  AddDeltasToRowSet(newer.add_rowsets(), 1, boost::assign::list_of(0));
  ASSERT_FALSE(HasNoCommonMemStores(older, newer));

}

// Case where we flushed the deltas but there is a common mrs
TEST(TestLogUtil, TestHasCommonStoresMemStoresDmsFlush) {
  TabletSuperBlockPB older;
  older.set_last_durable_mrs_id(0);
  AddDeltasToRowSet(older.add_rowsets(), 0, boost::assign::list_of(0));
  TabletSuperBlockPB newer;
  newer.set_last_durable_mrs_id(0);
  AddDeltasToRowSet(newer.add_rowsets(), 0, boost::assign::list_of(1));
  ASSERT_FALSE(HasNoCommonMemStores(older, newer));
}

// case where we compacted some row sets but the mrs is the same
TEST(TestLogUtil, TestHasCommonStoresMemStoresRowSetCompaction) {
  TabletSuperBlockPB older;
  older.set_last_durable_mrs_id(1);
  AddDeltasToRowSet(older.add_rowsets(), 0, boost::assign::list_of(0));
  AddDeltasToRowSet(older.add_rowsets(), 1, boost::assign::list_of(0));
  TabletSuperBlockPB newer;
  newer.set_last_durable_mrs_id(1);
  // TODO do we increase the delta id of the compacted rs?
  AddDeltasToRowSet(newer.add_rowsets(), 1, boost::assign::list_of(0));
  ASSERT_FALSE(HasNoCommonMemStores(older, newer));
}

// case where we compacted the deltas of one of the row sets but everything
// else is the same
TEST(TestLogUtil, TestHasCommonStoresMemStoresDeltaCompaction) {
  TabletSuperBlockPB older;
  older.set_last_durable_mrs_id(1);
  AddDeltasToRowSet(older.add_rowsets(), 0, boost::assign::list_of(0)(1)(2));
  AddDeltasToRowSet(older.add_rowsets(), 1, boost::assign::list_of(0));
  TabletSuperBlockPB newer;
  newer.set_last_durable_mrs_id(1);
  AddDeltasToRowSet(newer.add_rowsets(), 0, boost::assign::list_of(2));
  AddDeltasToRowSet(newer.add_rowsets(), 1, boost::assign::list_of(0));
  ASSERT_FALSE(HasNoCommonMemStores(older, newer));
}

// Case where we flushed the mrs but there are no deltas
TEST(TestLogUtil, TestHasNoCommonMemStoresAndNoDeltas) {
  TabletSuperBlockPB older;
  older.set_last_durable_mrs_id(0);
  AddDeltasToRowSet(older.add_rowsets(), 0, boost::assign::list_of(0));
  TabletSuperBlockPB newer;
  newer.set_last_durable_mrs_id(1);
  RowSetDataPB* row_set  = newer.add_rowsets();
  row_set->set_id(0);
  ASSERT_TRUE(HasNoCommonMemStores(older, newer));
}

// case where we flushed the mrs and flushed each delta (no common stores)
TEST(TestLogUtil, TestHasNoCommonStoresMemStoresDeltaCompaction) {
  TabletSuperBlockPB older;
  older.set_last_durable_mrs_id(1);
  AddDeltasToRowSet(older.add_rowsets(), 0, boost::assign::list_of(0));
  AddDeltasToRowSet(older.add_rowsets(), 1, boost::assign::list_of(0));
  TabletSuperBlockPB newer;
  newer.set_last_durable_mrs_id(2);
  AddDeltasToRowSet(newer.add_rowsets(), 0, boost::assign::list_of(1));
  AddDeltasToRowSet(newer.add_rowsets(), 1, boost::assign::list_of(1));
  AddDeltasToRowSet(newer.add_rowsets(), 2, boost::assign::list_of(0));
  ASSERT_TRUE(HasNoCommonMemStores(older, newer));
}

TEST(TestLogUtil, TestFourStaleSegmentsWithNoCommonStoresWithCurrent) {
  TabletSuperBlockPB current;
  current.set_last_durable_mrs_id(3);
  AddDeltasToRowSet(current.add_rowsets(), 0, boost::assign::list_of(1));
  AddDeltasToRowSet(current.add_rowsets(), 1, boost::assign::list_of(1));
  AddDeltasToRowSet(current.add_rowsets(), 2, boost::assign::list_of(1));
  AddDeltasToRowSet(current.add_rowsets(), 3, boost::assign::list_of(0));

  LogSegmentHeaderPB seg1_header;
  TabletSuperBlockPB* seg1_sb = seg1_header.mutable_tablet_meta();
  seg1_sb->set_last_durable_mrs_id(metadata::kNoDurableMemStore);

  shared_ptr<ReadableLogSegment> seg1(new ReadableLogSegment(seg1_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  LogSegmentHeaderPB seg2_header;
  TabletSuperBlockPB* seg2_sb = seg2_header.mutable_tablet_meta();
  seg2_sb->set_last_durable_mrs_id(0);
  AddDeltasToRowSet(seg2_sb->add_rowsets(), 0, boost::assign::list_of(0));

  shared_ptr<ReadableLogSegment> seg2(new ReadableLogSegment(seg2_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  LogSegmentHeaderPB seg3_header;
  TabletSuperBlockPB* seg3_sb = seg3_header.mutable_tablet_meta();
  seg3_sb->set_last_durable_mrs_id(1);
  AddDeltasToRowSet(seg3_sb->add_rowsets(), 0, boost::assign::list_of(0));
  AddDeltasToRowSet(seg3_sb->add_rowsets(), 1, boost::assign::list_of(0));

  shared_ptr<ReadableLogSegment> seg3(new ReadableLogSegment(seg3_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  LogSegmentHeaderPB seg4_header;
  TabletSuperBlockPB* seg4_sb = seg4_header.mutable_tablet_meta();
  seg2_sb->set_last_durable_mrs_id(2);
  AddDeltasToRowSet(seg4_sb->add_rowsets(), 0, boost::assign::list_of(0));
  AddDeltasToRowSet(seg4_sb->add_rowsets(), 1, boost::assign::list_of(0));
  AddDeltasToRowSet(seg4_sb->add_rowsets(), 2, boost::assign::list_of(0));

  shared_ptr<ReadableLogSegment> seg4(new ReadableLogSegment(seg4_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  uint32_t prefix;
  ASSERT_STATUS_OK(FindStaleSegmentsPrefixSize(boost::assign::list_of
                                               (seg1)
                                               (seg2)
                                               (seg3)
                                               (seg4),
                                               current,
                                               &prefix));
  // We expect 3 (as even if the last segment has no common stores with 'current' it
  // is expected to be the one where 'current' was written)
  ASSERT_EQ(3, prefix);
}

TEST(TestLogUtil, TestThreeSegmentsTwoStaleOneCurrent) {
  TabletSuperBlockPB current;
  current.set_last_durable_mrs_id(2);
  AddDeltasToRowSet(current.add_rowsets(), 0, boost::assign::list_of(1));
  AddDeltasToRowSet(current.add_rowsets(), 1, boost::assign::list_of(1));
  AddDeltasToRowSet(current.add_rowsets(), 2, boost::assign::list_of(0));

  LogSegmentHeaderPB seg1_header;
  TabletSuperBlockPB* seg1_sb = seg1_header.mutable_tablet_meta();
  seg1_sb->set_last_durable_mrs_id(metadata::kNoDurableMemStore);

  shared_ptr<ReadableLogSegment> seg1(new ReadableLogSegment(seg1_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  LogSegmentHeaderPB seg2_header;
  TabletSuperBlockPB* seg2_sb = seg2_header.mutable_tablet_meta();
  seg2_sb->set_last_durable_mrs_id(0);
  AddDeltasToRowSet(seg2_sb->add_rowsets(), 0, boost::assign::list_of(0));

  shared_ptr<ReadableLogSegment> seg2(new ReadableLogSegment(seg2_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  LogSegmentHeaderPB seg3_header;
  seg3_header.mutable_tablet_meta()->CopyFrom(current);

  shared_ptr<ReadableLogSegment> seg3(new ReadableLogSegment(seg3_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  uint32_t prefix;
  ASSERT_STATUS_OK(FindStaleSegmentsPrefixSize(boost::assign::list_of
                                               (seg1)
                                               (seg2)
                                               (seg3),
                                               current,
                                               &prefix));
  ASSERT_EQ(1, prefix);
}

TEST(TestLogUtil, TestNoStaleSegments) {
  TabletSuperBlockPB current;
  current.set_last_durable_mrs_id(2);
  AddDeltasToRowSet(current.add_rowsets(), 0, boost::assign::list_of(1));
  AddDeltasToRowSet(current.add_rowsets(), 1, boost::assign::list_of(1));
  AddDeltasToRowSet(current.add_rowsets(), 2, boost::assign::list_of(0));

  LogSegmentHeaderPB seg1_header;
  seg1_header.mutable_tablet_meta()->CopyFrom(current);

  shared_ptr<ReadableLogSegment> seg1(new ReadableLogSegment(seg1_header, "", 0, 0,
                                                             shared_ptr<RandomAccessFile>()));

  uint32_t prefix;
  ASSERT_STATUS_OK(FindStaleSegmentsPrefixSize(boost::assign::list_of
                                               (seg1),
                                               current,
                                               &prefix));
  ASSERT_EQ(0, prefix);
}

}  // namespace log
}  // namespace kudu

