// Copyright (c) 2014, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>
#include <string>

#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/util/test_util.h"

namespace kudu {

class RowOperationsTest : public KuduTest {
 public:
  RowOperationsTest()
    : arena_(1024, 128 * 1024) {
    SeedRandom();

    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", UINT32));
    CHECK_OK(builder.AddColumn("int_val", UINT32));
    CHECK_OK(builder.AddNullableColumn("string_val", STRING));
    schema_ = builder.Build();
    schema_without_ids_ = builder.BuildWithoutIds();
  }
 protected:
  void CheckDecodeDoesntCrash(const RowOperationsPB& pb);
  void DoFuzzTest(const KuduPartialRow& row);

  Schema schema_;
  Schema schema_without_ids_;
  Arena arena_;
};

// Perform some random mutation to a random byte in the provided string.
static void DoRandomMutation(string* s) {
  int target_idx = random() % s->size();
  char* target_byte = &(*s)[target_idx];
  switch (random() % 3) {
    case 0:
      // increment a random byte by 1
      (*target_byte)++;
      break;
    case 1:
      // decrement a random byte by 1
      (*target_byte)--;
      break;
    case 2:
      // replace byte with random value
      (*target_byte) = random();
      break;
  }
}

void RowOperationsTest::CheckDecodeDoesntCrash(const RowOperationsPB& pb) {
  arena_.Reset();
  RowOperationsPBDecoder decoder(&pb, &schema_without_ids_, &schema_, &arena_);
  vector<DecodedRowOperation> ops;
  Status s = decoder.DecodeOperations(&ops);
  if (s.ok() && !ops.empty()) {
    // If we got an OK result, then we should be able to stringify without
    // crashing. This ensures that any indirect data (eg strings) gets
    // set correctly.
    ignore_result(ops[0].ToString(schema_));
  }
  // Bad Status is OK -- we expect corruptions here.
}

void RowOperationsTest::DoFuzzTest(const KuduPartialRow& row) {
  RowOperationsPB pb;
  RowOperationsPBEncoder enc(&pb);
  enc.Add(RowOperationsPB::INSERT, row);

  RowOperationsPB mutated;

  // Check all possible truncations of the protobuf 'rows' field.
  for (int i = 0; i < pb.rows().size(); i++) {
    mutated.CopyFrom(pb);
    mutated.mutable_rows()->resize(i);
    CheckDecodeDoesntCrash(mutated);
  }

  // Check random byte changes in the 'rows' field.
  const int n_iters = AllowSlowTests() ? 10000 : 1000;
  for (int i = 0; i < n_iters; i++) {
    mutated.CopyFrom(pb);
    DoRandomMutation(mutated.mutable_rows());
    CheckDecodeDoesntCrash(mutated);
  }
}


// Test that, even if the protobuf is corrupt in some way, we do not
// crash. These protobufs are provided by clients, so we want to make sure
// a malicious client can't crash the server.
TEST_F(RowOperationsTest, FuzzTest) {
  KuduPartialRow row(&schema_);
  EXPECT_OK(row.SetUInt32("int_val", 54321));
  EXPECT_OK(row.SetStringCopy("string_val", "hello world"));
  DoFuzzTest(row);
  EXPECT_OK(row.SetNull("string_val"));
  DoFuzzTest(row);
}

namespace {

// Project client_row into server_schema, and stringify the result.
// If an error occurs, the result string is "error: <stringified Status>"
string TestProjection(RowOperationsPB::Type type,
                      const KuduPartialRow& client_row,
                      const Schema& server_schema) {
  RowOperationsPB pb;
  RowOperationsPBEncoder enc(&pb);
  enc.Add(type, client_row);

  // Decode it
  Arena arena(1024, 1024*1024);
  vector<DecodedRowOperation> ops;
  RowOperationsPBDecoder dec(&pb, client_row.schema(), &server_schema, &arena);
  Status s = dec.DecodeOperations(&ops);

  if (!s.ok()) {
    return "error: " + s.ToString();
  }
  CHECK_EQ(1, ops.size());
  return ops[0].ToString(server_schema);
}

} // anonymous namespace

// Test decoding partial rows from a client who has a schema which matches
// the table schema.
TEST_F(RowOperationsTest, ProjectionTestWholeSchemaSpecified) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       1);

  // Test a row missing 'int_val', which is required.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("error: Invalid argument: No value provided for required column: "
              "int_val[uint32 NOT NULL]",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));
  }

  // Test a row missing 'string_val', which is nullable
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
             CHECK_OK(client_row.SetUInt32("int_val", 54321));
    // The NULL should get filled in.
    EXPECT_EQ("INSERT (uint32 key=12345, uint32 int_val=54321, string string_val=NULL)",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));
  }

  // Test a row with all of the fields specified, both with the nullable field
  // specified to be NULL and non-NULL.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetUInt32("int_val", 54321));
    CHECK_OK(client_row.SetStringCopy("string_val", "hello world"));
    EXPECT_EQ("INSERT (uint32 key=12345, uint32 int_val=54321, string string_val=hello world)",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));

    // The first result should have the field specified.
    // The second result should have the field NULL, since it was explicitly set.
    CHECK_OK(client_row.SetNull("string_val"));
    EXPECT_EQ("INSERT (uint32 key=12345, uint32 int_val=54321, string string_val=NULL)",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));

  }
}

TEST_F(RowOperationsTest, ProjectionTestWithDefaults) {
  uint32_t nullable_default = 123;
  uint32_t non_null_default = 456;
  SchemaBuilder b;
  CHECK_OK(b.AddKeyColumn("key", UINT32));
  CHECK_OK(b.AddColumn("nullable_with_default", UINT32, true,
                       &nullable_default, &nullable_default));
  CHECK_OK(b.AddColumn("non_null_with_default", UINT32, false,
                       &non_null_default, &non_null_default));
  Schema server_schema = b.Build();

  // Clients may not have the defaults specified.
  // TODO: evaluate whether this should be true - how "dumb" should clients be?
  Schema client_schema(
    boost::assign::list_of
    (ColumnSchema("key", UINT32))
    (ColumnSchema("nullable_with_default", UINT32, true))
    (ColumnSchema("non_null_with_default", UINT32, false)),
    1);

  // Specify just the key. The other two columns have defaults, so they'll get filled in.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("INSERT (uint32 key=12345, uint32 nullable_with_default=123,"
              " uint32 non_null_with_default=456)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }

  // Specify the key and override both defaults
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetUInt32("nullable_with_default", 12345));
    CHECK_OK(client_row.SetUInt32("non_null_with_default", 54321));
    EXPECT_EQ("INSERT (uint32 key=12345, uint32 nullable_with_default=12345,"
              " uint32 non_null_with_default=54321)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }

  // Specify the key and override both defaults, overriding the nullable
  // one to NULL.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetNull("nullable_with_default"));
    CHECK_OK(client_row.SetUInt32("non_null_with_default", 54321));
    EXPECT_EQ("INSERT (uint32 key=12345, uint32 nullable_with_default=NULL,"
              " uint32 non_null_with_default=54321)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }
}

// Test cases where the client only has a subset of the fields
// of the table, but where the missing columns have defaults
// or are NULLable.
TEST_F(RowOperationsTest, ProjectionTestWithClientHavingValidSubset) {
  uint32_t nullable_default = 123;
  SchemaBuilder b;
  CHECK_OK(b.AddKeyColumn("key", UINT32));
  CHECK_OK(b.AddColumn("int_val", UINT32));
  CHECK_OK(b.AddColumn("new_int_with_default", UINT32, false,
                       &nullable_default, &nullable_default));
  CHECK_OK(b.AddNullableColumn("new_nullable_int", UINT32));
  Schema server_schema = b.Build();

  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32)),
                       1);

  // Specify just the key. This is an error because we're missing int_val.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("error: Invalid argument: No value provided for required column:"
              " int_val[uint32 NOT NULL]",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }

  // Specify both of the columns that the client is aware of.
  // Defaults should be filled for the other two.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetUInt32("int_val", 12345));
    EXPECT_EQ("INSERT (uint32 key=12345, uint32 int_val=12345,"
              " uint32 new_int_with_default=123, uint32 new_nullable_int=NULL)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }
}

// Test cases where the client is missing a column which is non-null
// and has no default. This is an incompatible client.
TEST_F(RowOperationsTest, ProjectionTestWithClientHavingInvalidSubset) {
  SchemaBuilder b;
  CHECK_OK(b.AddKeyColumn("key", UINT32));
  CHECK_OK(b.AddColumn("int_val", UINT32));
  Schema server_schema = b.Build();

  CHECK_OK(b.RemoveColumn("int_val"));
  Schema client_schema = b.BuildWithoutIds();

  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("error: Invalid argument: Client missing required column:"
              " int_val[uint32 NOT NULL]",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }
}

// Simple Update case where the client and server schemas match.
TEST_F(RowOperationsTest, TestProjectUpdates) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       1);
  Schema server_schema = SchemaBuilder(client_schema).Build();

  // Check without specifying any columns
  KuduPartialRow client_row(&client_schema);
  EXPECT_EQ("error: Invalid argument: No value provided for key column: key[uint32 NOT NULL]",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Specify the key and no columns to update
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  EXPECT_EQ("error: Invalid argument: No fields updated, key is: (uint32 key=12345)",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));


  // Specify the key and update one column.
  ASSERT_STATUS_OK(client_row.SetUInt32("int_val", 12345));
  EXPECT_EQ("MUTATE (uint32 key=12345) SET int_val=12345",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Specify the key and update both columns
  ASSERT_STATUS_OK(client_row.SetString("string_val", "foo"));
  EXPECT_EQ("MUTATE (uint32 key=12345) SET int_val=12345, string_val=foo",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Update the nullable column to null.
  ASSERT_STATUS_OK(client_row.SetNull("string_val"));
  EXPECT_EQ("MUTATE (uint32 key=12345) SET int_val=12345, string_val=NULL",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
}

// Client schema has the columns in a different order. Makes
// sure the name-based projection is functioning.
TEST_F(RowOperationsTest, TestProjectUpdatesReorderedColumns) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("string_val", STRING, true))
                       (ColumnSchema("int_val", UINT32)),
                       1);
  Schema server_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       1);
  server_schema = SchemaBuilder(server_schema).Build();

  KuduPartialRow client_row(&client_schema);
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  ASSERT_STATUS_OK(client_row.SetUInt32("int_val", 54321));
  EXPECT_EQ("MUTATE (uint32 key=12345) SET int_val=54321",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
}

// Client schema is missing one of the columns in the server schema.
// This is OK on an update.
TEST_F(RowOperationsTest, DISABLED_TestProjectUpdatesSubsetOfColumns) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       1);
  Schema server_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       1);
  server_schema = SchemaBuilder(server_schema).Build();

  KuduPartialRow client_row(&client_schema);
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  ASSERT_STATUS_OK(client_row.SetString("string_val", "foo"));
  EXPECT_EQ("MUTATE (uint32 key=12345) SET string_val=foo",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
}

TEST_F(RowOperationsTest, TestClientMismatchedType) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT8)),
                       1);
  Schema server_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32)),
                       1);
  server_schema = SchemaBuilder(server_schema).Build();

  KuduPartialRow client_row(&client_schema);
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  ASSERT_STATUS_OK(client_row.SetUInt8("int_val", 1));
  EXPECT_EQ("error: Invalid argument: The column 'int_val' must have type "
            "uint32 NOT NULL found uint8 NOT NULL",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
}

TEST_F(RowOperationsTest, TestProjectDeletes) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("key_2", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       2);
  Schema server_schema = SchemaBuilder(client_schema).Build();

  KuduPartialRow client_row(&client_schema);
  // No columns set
  EXPECT_EQ("error: Invalid argument: No value provided for key column: key[uint32 NOT NULL]",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Only half the key set
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  EXPECT_EQ("error: Invalid argument: No value provided for key column: key_2[uint32 NOT NULL]",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Whole key set (correct)
  ASSERT_STATUS_OK(client_row.SetUInt32("key_2", 54321));
  EXPECT_EQ("MUTATE (uint32 key=12345, uint32 key_2=54321) DELETE",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Extra column set (incorrect)
  ASSERT_STATUS_OK(client_row.SetString("string_val", "hello"));
  EXPECT_EQ("error: Invalid argument: DELETE should not have a value for column: "
            "string_val[string NULLABLE]",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));
}


} // namespace kudu
