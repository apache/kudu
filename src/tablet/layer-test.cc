// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <time.h>

#include "tablet/layer.h"
#include "tablet/row.h"
#include "tablet/schema.h"
#include "util/env.h"
#include "util/status.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

Schema CreateTestSchema() {
  ColumnSchema col1("key", kudu::cfile::STRING);
  ColumnSchema col2("val", kudu::cfile::UINT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2);
  return Schema(cols, 1);
}

// Test round-trip writing and reading back a layer with
// multiple columns. Does not test any modifications.
TEST(TestLayer, TestLayerRoundTrip) {
  Env *env = Env::Default();

  Schema schema = CreateTestSchema();
  string test_dir;
  ASSERT_STATUS_OK(env->GetTestDirectory(&test_dir));
  test_dir += "/TestLayer.TestLayerRoundTrip." +
    boost::lexical_cast<string>(time(NULL));

  // Write 1000 rows into a new Layer.
  LayerWriter lw(env, schema, test_dir);

  ASSERT_STATUS_OK(lw.Open());

  char buf[256];
  RowBuilder rb(schema);
  for (int i = 0; i < 1000; i++) {
    rb.Reset();
    snprintf(buf, sizeof(buf), "hello %d", i);
    rb.AddString(Slice(buf));
    rb.AddUint32(i);
    ASSERT_STATUS_OK_FAST(lw.WriteRow(rb.data()));
  }
  ASSERT_STATUS_OK(lw.Finish());

  // Now open the Layer for read
  LayerReader lr(env, schema, test_dir);
  ASSERT_STATUS_OK(lr.Open());
}

} // namespace tablet
} // namespace kudu
