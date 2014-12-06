// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/common/predicate_encoder.h"
#include "kudu/util/test_macros.h"

namespace kudu {

// Tests an edge condition in
// RangePredicateEncoder::EncodeRangePredicates
TEST(TestRangePredicateEncoder, TestEncodeRangePredicates) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("a", UINT8))
                (ColumnSchema("b", UINT8))
                (ColumnSchema("c", UINT8)),
                3);
  RangePredicateEncoder enc(&schema);
  uint8_t l = 3;
  uint8_t u = 255;
  {
    // Apply predicate: a == 255
    ColumnRangePredicate pred_a(schema.column(0), &u, &u);
    // Apply predicate b BETWEEN 3 AND 255
    ColumnRangePredicate pred_b(schema.column(1), &l, &u);
    ScanSpec spec;
    spec.AddPredicate(pred_a);
    spec.AddPredicate(pred_b);
    ASSERT_NO_FATAL_FAILURE(enc.EncodeRangePredicates(&spec, true));
    SCOPED_TRACE(spec.ToString());
    ASSERT_TRUE(spec.predicates().empty()) << "Should have pushed down all predicates";
    // Expect: key >= (255, 3)
    ASSERT_EQ("encoded key >= \\xff\\x03", EncodedKey::RangeToString(spec.lower_bound_key(),
                                                                     spec.upper_bound_key()));
  }

  u = 254;
  {
    ScanSpec spec;
    ColumnRangePredicate pred_a(schema.column(0), NULL, &u);
    spec.AddPredicate(pred_a);
    ASSERT_NO_FATAL_FAILURE(enc.EncodeRangePredicates(&spec, true));
    ASSERT_EQ("encoded key <= \\xff", EncodedKey::RangeToString(spec.lower_bound_key(),
                                                                     spec.upper_bound_key()));
  }

  // Test that, if so desired, pushed predicates are not erased.
  {
    ScanSpec spec;
    ColumnRangePredicate pred_a(schema.column(0), &u, &u);
    spec.AddPredicate(pred_a);
    ASSERT_NO_FATAL_FAILURE(enc.EncodeRangePredicates(&spec, false));
    ASSERT_TRUE(spec.lower_bound_key());
    ASSERT_TRUE(spec.upper_bound_key());
  }

  // Test that, if pushed predicates are erased, that we don't
  // erase non-pushed predicates.
  {
    ScanSpec spec;

    // Add predicates on column A and C. They're not contiguous so
    // we can't turn it into a single range predicate.
    ColumnRangePredicate pred_a(schema.column(0), &u, &u);
    spec.AddPredicate(pred_a);

    ColumnRangePredicate pred_c(schema.column(2), &u, &u);
    spec.AddPredicate(pred_c);

    ASSERT_NO_FATAL_FAILURE(enc.EncodeRangePredicates(&spec, true));

    // We should have one predicate remaining for column C
    ASSERT_EQ(1, spec.predicates().size());

    // The predicate on column A should be pushed
    ASSERT_TRUE(spec.lower_bound_key());
    ASSERT_TRUE(spec.upper_bound_key());
  }

  // Test that predicates added out of key order are OK.
  {
    ScanSpec spec;
    ColumnRangePredicate pred_b(schema.column(1), &u, &u);
    spec.AddPredicate(pred_b);
    ColumnRangePredicate pred_a(schema.column(0), &u, &u);
    spec.AddPredicate(pred_a);
    ASSERT_NO_FATAL_FAILURE(enc.EncodeRangePredicates(&spec, true));
    ASSERT_TRUE(spec.lower_bound_key());
    ASSERT_TRUE(spec.upper_bound_key());
  }
}

} // namespace kudu
