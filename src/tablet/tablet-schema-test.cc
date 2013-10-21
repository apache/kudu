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

  void InsertRows(const Schema& schema, size_t first_key, size_t nrows) {
    for (size_t i = first_key; i < nrows; ++i) {
      InsertRow(schema, i);

      // Half of the rows will be on disk
      // and the other half in the MemRowSet
      if (i == (nrows / 2)) {
        ASSERT_STATUS_OK(tablet_->Flush());
      }
    }
  }

  void InsertRow(const Schema& schema, size_t key) {
    RowBuilder rb(schema);
    rb.AddUint32(key);
    rb.AddUint32(key);
    TransactionContext tx_ctx;
    ASSERT_STATUS_OK(tablet_->Insert(&tx_ctx, rb.row()));
  }

  void DeleteRow(const Schema& schema, size_t key) {
    RowBuilder rb(schema.CreateKeyProjection());
    rb.AddUint32(key);
    faststring buf;
    RowChangeListEncoder mutation(schema, &buf);
    mutation.SetToDelete();
    TransactionContext tx_ctx;
    ASSERT_STATUS_OK(tablet_->MutateRow(&tx_ctx, rb.row(), schema, mutation.as_changelist()));
  }

  void MutateRow(const Schema& schema, size_t key, size_t col_idx, size_t new_val) {
    RowBuilder rb(schema.CreateKeyProjection());
    rb.AddUint32(key);
    faststring buf;
    RowChangeListEncoder mutation(schema, &buf);
    mutation.AddColumnUpdate(col_idx, &new_val);
    TransactionContext tx_ctx;
    ASSERT_STATUS_OK(tablet_->MutateRow(&tx_ctx, rb.row(), schema, mutation.as_changelist()));
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
    SchemaBuilder builder;
    CHECK(builder.AddKeyColumn("key", UINT32).ok());
    CHECK(builder.AddColumn("c1", UINT32).ok());
    return builder.Build();
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

  InsertRows(schema_, 0, kNumRows);

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
  InsertRows(schema_, 0, kNumBaseRows);

  // Add one column with a default value
  const uint32_t c2_write_default = 5;
  const uint32_t c2_read_default = 7;

  SchemaBuilder builder(schema_);
  ASSERT_STATUS_OK(builder.AddColumn("c2", UINT32, false, &c2_read_default, &c2_write_default));
  Schema s2 = builder.Build();

  ASSERT_STATUS_OK(tablet_->AlterSchema(s2));

  // Insert with base/old schema
  size_t s2Key = kNumBaseRows + 1;
  InsertRow(schema_, s2Key);

  // Verify the default value
  std::vector<std::pair<string, string> > keys;
  keys.push_back(std::pair<string, string>(Substitute("key=$0", s2Key), Substitute("c2=$0", c2_write_default)));
  keys.push_back(std::pair<string, string>("", Substitute("c2=$0", c2_read_default)));
  VerifyTabletRows(s2, keys);

  // Delete the row
  DeleteRow(s2, s2Key);

  // Verify the default value
  VerifyTabletRows(s2, keys);

  // Re-Insert with base/old schema
  InsertRow(schema_, s2Key);
  VerifyTabletRows(s2, keys);

  // Try compact all (different schemas)
  ASSERT_STATUS_OK(tablet_->Compact(Tablet::FORCE_COMPACT_ALL));
  VerifyTabletRows(s2, keys);
}

// Write to the table using a projection schema with a renamed field.
TEST_F(TestTabletSchema, TestRenameProjection) {
  std::vector<std::pair<string, string> > keys;

  SchemaBuilder builder(schema_);
  ASSERT_STATUS_OK(builder.RenameColumn("c1", "c1_renamed"));
  Schema s2 = builder.Build();

  // Insert with the base schema
  InsertRow(schema_, 1);

  // Insert with the s2 schema
  InsertRow(s2, 2);

  // Switch schema to s2
  ASSERT_STATUS_OK(tablet_->AlterSchema(s2));

  // Insert with the base schema after AlterSchema(s2)
  InsertRow(schema_, 3);

  // Insert with the s2 schema after AlterSchema(s2)
  InsertRow(s2, 4);

  // Read and verify using the s2 schema
  keys.clear();
  for (int i = 1; i <= 4; ++i) {
    keys.push_back(std::pair<string, string>(Substitute("key=$0", i), Substitute("c1_renamed=$0", i)));
  }
  VerifyTabletRows(s2, keys);

  // Read and verify using the base schema
  keys.clear();
  for (int i = 1; i <= 4; ++i) {
    keys.push_back(std::pair<string, string>(Substitute("key=$0", i), Substitute("c1=$0", i)));
  }
  VerifyTabletRows(schema_, keys);

  // Delete the first two rows
  DeleteRow(s2, /* key= */ 1);
  DeleteRow(schema_, /* key= */ 2);

  // Alter the remaining two rows
  MutateRow(s2,      /* key= */ 3, /* col_idx= */ 1, /* new_val= */ 6);
  MutateRow(schema_, /* key= */ 4, /* col_idx= */ 1, /* new_val= */ 8);

  // Read and verify using the s2 schema
  keys.clear();
  keys.push_back(std::pair<string, string>("key=3", "c1_renamed=6"));
  keys.push_back(std::pair<string, string>("key=4", "c1_renamed=8"));
  VerifyTabletRows(s2, keys);

  // Read and verify using the base schema
  keys.clear();
  keys.push_back(std::pair<string, string>("key=3", "c1=6"));
  keys.push_back(std::pair<string, string>("key=4", "c1=8"));
  VerifyTabletRows(schema_, keys);
}

// Verify that removing a column and re-adding it will not result in making old data visible
TEST_F(TestTabletSchema, TestDeleteAndReAddColumn) {
  std::vector<std::pair<string, string> > keys;

  // Insert and Mutate with the base schema
  InsertRow(schema_, 1);
  MutateRow(schema_, /* key= */ 1, /* col_idx= */ 1, /* new_val= */ 2);

  keys.clear();
  keys.push_back(std::pair<string, string>("key=1", "c1=2"));
  VerifyTabletRows(schema_, keys);

  SchemaBuilder builder(schema_);
  ASSERT_STATUS_OK(builder.RemoveColumn("c1"));
  // NOTE this new 'c1' will have a different id from the previous one
  //      so the data added to the previous 'c1' will not be visible.
  ASSERT_STATUS_OK(builder.AddNullableColumn("c1", UINT32));
  Schema s2 = builder.Build();

  // Switch schema to s2
  ASSERT_STATUS_OK(tablet_->AlterSchema(s2));

  // Verify that the new 'c1' have the default value
  keys.clear();
  keys.push_back(std::pair<string, string>("key=1", "c1=NULL"));
  VerifyTabletRows(s2, keys);
}

} // namespace tablet
} // namespace kudu
