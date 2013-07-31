// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "common/schema.h"
#include "common/predicate_encoder.h"
#include "util/test_macros.h"

namespace kudu {

// Tests an edge condition in
// RangePredicateEncoder::EncodeRangePredicates
TEST(TestRangePredicateEncoder, TestEncodeRangePredicates) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("a", UINT8))
                (ColumnSchema("b", UINT8))
                (ColumnSchema("c", UINT8)),
                3);
  RangePredicateEncoder enc(schema);
  uint8_t l = 3;
  uint8_t u = 255;
  ColumnRangePredicate pred_a(schema.column(0), &u, &u);
  ColumnRangePredicate pred_b(schema.column(1), &l, &u);
  ScanSpec spec;
  spec.AddPredicate(pred_a);
  spec.AddPredicate(pred_b);
  ASSERT_NO_FATAL_FAILURE(enc.EncodeRangePredicates(&spec));
  ASSERT_TRUE(spec.predicates().empty()) << "Should have pushed down all predicates";
  ASSERT_EQ(spec.encoded_ranges().size(), 1);
  ASSERT_FALSE(spec.encoded_ranges().front()->has_upper_bound());
}

} // namespace kudu
