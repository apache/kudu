// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <time.h>

#include "common/row.h"
#include "common/schema.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/status.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_int32(roundtrip_num_rows, 100000,
             "Number of rows to use for the round-trip test");

namespace kudu {
namespace tablet {

Schema CreateTestSchema() {
  ColumnSchema col1("key", STRING);
  ColumnSchema col2("val", UINT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2);
  return Schema(cols, 1);
}

// Iterate over a Layer, dumping occasional rows to the console,
// using the given schema as a projection.
static void IterateProjection(const Layer &l, const Schema &schema,
                              int expected_rows) {
  scoped_ptr<Layer::RowIterator> row_iter(l.NewRowIterator(schema));
  ASSERT_STATUS_OK(row_iter->Init());
  ASSERT_STATUS_OK(row_iter->SeekToOrdinal(0));

  int batch_size = 100;
  Arena arena(1024, 1024*1024);
  char dst[schema.byte_size() * batch_size];

  int i = 0;
  int log_interval = expected_rows/20 / batch_size;
  while (row_iter->HasNext()) {
    arena.Reset();
    size_t n = batch_size;
    ASSERT_STATUS_OK_FAST(row_iter->CopyNextRows(&n, dst, &arena));
    i += n;

    LOG_EVERY_N(INFO, log_interval) << "Got row: " <<
      schema.DebugRow(dst);
  }

  EXPECT_EQ(expected_rows, i);

}


// TODO: add test which calls CopyNextRows on an iterator with no more
// rows - i think it segfaults!

// Test round-trip writing and reading back a layer with
// multiple columns. Does not test any modifications.
TEST(TestLayer, TestLayerRoundTrip) {
  Env *env = Env::Default();

  Schema schema = CreateTestSchema();
  string test_dir;
  test_dir="/tmp/kudutest-1000/TestLayer.TestLayerRoundTrip.1352878579";

  int n_rows = FLAGS_roundtrip_num_rows;
  CHECK(n_rows > 0);

  ASSERT_STATUS_OK(env->GetTestDirectory(&test_dir));
  test_dir += "/TestLayer.TestLayerRoundTrip." +
    boost::lexical_cast<string>(time(NULL));

  // Write 1000 rows into a new Layer.
  LOG_TIMING(INFO, "Writing layer") {
    LayerWriter lw(env, schema, test_dir);

    ASSERT_STATUS_OK(lw.Open());


    char buf[256];
    RowBuilder rb(schema);
    for (int i = 0; i < n_rows; i++) {
      rb.Reset();
      snprintf(buf, sizeof(buf), "hello %d", i);
      rb.AddString(Slice(buf));
      rb.AddUint32(i);
      ASSERT_STATUS_OK_FAST(lw.WriteRow(rb.data()));
    }
    ASSERT_STATUS_OK(lw.Finish());
  }

  // Now open the Layer for read
  Layer l(env, schema, test_dir);
  ASSERT_STATUS_OK(l.Open());

  // First iterate over all columns
  LOG_TIMING(INFO, "Iterating over all columns") {
    IterateProjection(l, schema, n_rows);
  }

  // Now iterate only over the key column
  Schema proj_key(boost::assign::list_of
                  (ColumnSchema("key", STRING)),
                  1);

  LOG_TIMING(INFO, "Iterating over only key column") {
    IterateProjection(l, proj_key, n_rows);
  }


  // Now iterate only over the non-key column
  Schema proj_val(boost::assign::list_of
                  (ColumnSchema("val", UINT32)),
                  1);
  LOG_TIMING(INFO, "Iterating over only val column") {
    IterateProjection(l, proj_val, n_rows);
  }

}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
