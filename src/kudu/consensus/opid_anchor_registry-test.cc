// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/opid_anchor_registry.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_util.h"

using strings::Substitute;
using kudu::consensus::OpId;

namespace kudu {
namespace log {

// Tests for the current incarnation of the OpIdAnchorRegistry.
// Since OpId is currently just typedef'ed to OpId, and since these
// tests generate OpIds, we are just using OpId directly here instead of the
// typedef to be transparent about the assumptions being made in these tests.
class OpIdAnchorRegistryTest : public KuduTest {
};

TEST_F(OpIdAnchorRegistryTest, TestUpdateRegistration) {
  const string test_name = CURRENT_TEST_NAME();
  scoped_refptr<OpIdAnchorRegistry> reg(new OpIdAnchorRegistry());
  OpId op_id(consensus::MinimumOpId());
  OpIdAnchor anchor;

  reg->Register(op_id, test_name, &anchor);
  op_id.set_index(1);
  ASSERT_STATUS_OK(reg->UpdateRegistration(op_id, test_name, &anchor));
  ASSERT_STATUS_OK(reg->Unregister(&anchor));
}

TEST_F(OpIdAnchorRegistryTest, TestDuplicateInserts) {
  const string test_name = CURRENT_TEST_NAME();
  scoped_refptr<OpIdAnchorRegistry> reg(new OpIdAnchorRegistry());
  OpId op_id;
  op_id.set_term(0);
  op_id.set_index(1);

  const int num_anchors = 10;
  OpIdAnchor anchors[num_anchors];
  for (int i = 0; i < num_anchors; i++) {
    reg->Register(op_id, test_name, &anchors[i]);
  }

  OpId first_op_id;
  ASSERT_STATUS_OK(reg->GetEarliestRegisteredOpId(&first_op_id));
  ASSERT_EQ(op_id.term(), first_op_id.term());
  ASSERT_EQ(op_id.index(), first_op_id.index());

  for (int i = 0; i < num_anchors; i++) {
    ASSERT_STATUS_OK(reg->Unregister(&anchors[i]));
  }

  Status s = reg->GetEarliestRegisteredOpId(&first_op_id);
  ASSERT_TRUE(s.IsNotFound())
      << Substitute("Should have empty OpId registry. Status: $0, OpId: $1, Num anchors: $2",
                    s.ToString(), first_op_id.ShortDebugString(), reg->GetAnchorCountForTests());

  ASSERT_EQ(0, reg->GetAnchorCountForTests());
}

// Ensure that the OpId ordering is correct.
TEST_F(OpIdAnchorRegistryTest, TestOrderedEarliestOpId) {
  scoped_refptr<OpIdAnchorRegistry> reg(new OpIdAnchorRegistry());
  const int kNumAnchors = 4;
  const string test_name = CURRENT_TEST_NAME();
  OpId op_id;

  OpIdAnchor anchors[kNumAnchors];

  op_id.set_term(0);
  op_id.set_index(2);
  reg->Register(op_id, test_name, &anchors[0]);

  op_id.set_term(1);
  op_id.set_index(2);
  reg->Register(op_id, test_name, &anchors[1]);

  op_id.set_term(0);
  op_id.set_index(1);
  reg->Register(op_id, test_name, &anchors[2]);

  op_id.set_term(3);
  op_id.set_index(3);
  reg->Register(op_id, test_name, &anchors[3]);

  ASSERT_STATUS_OK(reg->GetEarliestRegisteredOpId(&op_id));
  ASSERT_EQ(0, op_id.term());
  ASSERT_EQ(1, op_id.index());

  for (int i = 0; i < kNumAnchors; i++) {
    ASSERT_STATUS_OK(reg->Unregister(&anchors[i]));
  }
}

} // namespace log
} // namespace kudu
