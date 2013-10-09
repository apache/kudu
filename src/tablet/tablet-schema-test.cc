// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "common/schema.h"
#include "gutil/strings/substitute.h"
#include "tablet/tablet.h"
#include "tablet/tablet-test-base.h"
#include "util/test_macros.h"
#include "util/test_util.h"

using strings::Substitute;

namespace kudu {
namespace tablet {

class TestTabletSchema : public KuduTabletTest {
 public:
  TestTabletSchema()
    : KuduTabletTest(CreateBaseSchema()) {
  }

  void InsertBaseData(size_t nrows) {
    for (size_t i = 0; i < nrows; ++i) {
      RowBuilder rb(schema_);
      rb.AddUint32(i);
      rb.AddUint32(i);
      TransactionContext tx_ctx;
      ASSERT_STATUS_OK(tablet_->Insert(&tx_ctx, rb.row()));
      // Half of the rows will be on disk
      // and the other half in the MemRowSet
      if (i == (nrows / 2)) {
        ASSERT_STATUS_OK(tablet_->Flush());
      }
    }
  }

  void VerifyTabletRows(const Schema& projection,
                        const std::vector<std::pair<string, string> >& keys) {
    typedef std::pair<string, string> StringPair;
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet_->NewRowIterator(projection, &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    std::vector<string> rows;
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), &rows));
    BOOST_FOREACH(const string& row, rows) {
      bool found = false;
      BOOST_FOREACH(const StringPair& k, keys) {
        if (row.find(k.first) != string::npos) {
          ASSERT_STR_CONTAINS(row, k.second);
          found = true;
          break;
        }
      }
      ASSERT_TRUE(found);
    }
  }

 private:
  Schema CreateBaseSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key", UINT32))
                  (ColumnSchema("c1", UINT32)),
                  1);
  }
};

// Read from a tablet using a projection schema with columns not present in
// the original schema. Vertify that the new columns are added and have the
// default value.
TEST_F(TestTabletSchema, TestRead) {
  const size_t kNumRows = 10;
  const uint64_t c2_default = 25;
  const Slice c3_default("Hello World");
  Schema projection(boost::assign::list_of
                    (ColumnSchema("key", UINT32))
                    (ColumnSchema("c2", UINT64, false, &c2_default))
                    (ColumnSchema("c3", STRING, false, &c3_default)),
                    1);

  InsertBaseData(kNumRows);

  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet_->NewRowIterator(projection, &iter));
  ASSERT_STATUS_OK(iter->Init(NULL));

  size_t nrows = 0;
  Arena arena(32*1024, 256*1024);
  RowBlock block(projection, 20, &arena);
  while (iter->HasNext()) {
    ASSERT_STATUS_OK(RowwiseIterator::CopyBlock(iter.get(), &block));

    for (size_t i = 0; i < block.nrows(); ++i) {
      RowBlockRow row = block.row(i);
      uint32_t key = *projection.ExtractColumnFromRow<UINT32>(row, 0);
      ASSERT_TRUE(key >= 0 && key <= kNumRows);
      ASSERT_EQ(c2_default, *projection.ExtractColumnFromRow<UINT64>(row, 1));
      ASSERT_EQ(c3_default, *projection.ExtractColumnFromRow<STRING>(row, 2));
      nrows++;
    }
  }
  ASSERT_EQ(kNumRows, nrows);
}

// Write to the tablet using different schemas,
// and verifies that the read and write defauls are respected.
TEST_F(TestTabletSchema, TestWrite) {
  const size_t kNumBaseRows = 10;

  // Insert some rows with the base schema
  InsertBaseData(kNumBaseRows);

  // Add one column with a default value
  const uint32_t c2_write_default = 5;
  const uint32_t c2_read_default = 7;
  Schema s2(boost::assign::list_of
            (ColumnSchema("key", UINT32))
            (ColumnSchema("c1", UINT32))
            (ColumnSchema("c2", UINT32, false, &c2_read_default, &c2_write_default)),
            1);
  ASSERT_STATUS_OK(tablet_->AlterSchema(s2));

  // Insert with base/old schema
  size_t s2Key = kNumBaseRows + 1;
  {
    RowBuilder rb(schema_);
    rb.AddUint32(s2Key);
    rb.AddUint32(s2Key);
    TransactionContext tx_ctx;
    ASSERT_STATUS_OK(tablet_->Insert(&tx_ctx, rb.row()));
  }

  // Verify the default value
  std::vector<std::pair<string, string> > keys;
  keys.push_back(std::pair<string, string>(Substitute("key=$0", s2Key), Substitute("c2=$0", c2_write_default)));
  keys.push_back(std::pair<string, string>("", Substitute("c2=$0", c2_read_default)));
  VerifyTabletRows(s2, keys);

  // Delete the row
  {
    RowBuilder rb(schema_.CreateKeyProjection());
    rb.AddUint32(s2Key);
    faststring buf;
    RowChangeListEncoder mutation(s2, &buf);
    mutation.SetToDelete();
    TransactionContext tx_ctx;
    ASSERT_STATUS_OK(tablet_->MutateRow(&tx_ctx, rb.row(), s2, mutation.as_changelist()));
  }
  // Verify the default value
  VerifyTabletRows(s2, keys);

  // Re-Insert with base/old schema
  {
    RowBuilder rb(schema_);
    rb.AddUint32(s2Key);
    rb.AddUint32(s2Key);
    TransactionContext tx_ctx;
    ASSERT_STATUS_OK(tablet_->Insert(&tx_ctx, rb.row()));
  }
  VerifyTabletRows(s2, keys);

  // Try compact all (different schemas)
  ASSERT_STATUS_OK(tablet_->Compact(Tablet::FORCE_COMPACT_ALL));
  VerifyTabletRows(s2, keys);
}

} // namespace tablet
} // namespace kudu
