// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <boost/assign/list_of.hpp>

#include "common/partial_row.h"
#include "common/row.h"
#include "common/row_changelist.h"
#include "common/schema.h"
#include "common/wire_protocol.pb.h"
#include "util/memory/arena.h"
#include "util/test_util.h"

namespace kudu {

class PartialRowTest : public KuduTest {
 public:
  PartialRowTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING, true)),
              1) {
    SeedRandom();
  }
 protected:
  void CheckPBRoundTrip(const PartialRow& row);
  void DoFuzzTest(const PartialRow& row);

  Schema schema_;
};

TEST_F(PartialRowTest, UnitTest) {
  PartialRow row(&schema_);

  // Initially all columns are unset.
  EXPECT_FALSE(row.IsColumnSet(0));
  EXPECT_FALSE(row.IsColumnSet(1));
  EXPECT_FALSE(row.IsColumnSet(2));
  EXPECT_FALSE(row.IsKeySet());
  EXPECT_EQ("", row.ToString());

  // Set just the key.
  EXPECT_STATUS_OK(row.SetUInt32("key", 12345));
  EXPECT_TRUE(row.IsKeySet());
  EXPECT_FALSE(row.IsColumnSet(1));
  EXPECT_FALSE(row.IsColumnSet(2));
  EXPECT_EQ("uint32 key=12345", row.ToString());
  uint32_t x;
  EXPECT_STATUS_OK(row.GetUInt32("key", &x));
  EXPECT_EQ(12345, x);
  EXPECT_FALSE(row.IsNull("key"));

  // Fill in the other columns.
  EXPECT_STATUS_OK(row.SetUInt32("int_val", 54321));
  EXPECT_STATUS_OK(row.SetStringCopy("string_val", "hello world"));
  EXPECT_TRUE(row.IsColumnSet(1));
  EXPECT_TRUE(row.IsColumnSet(2));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=hello world",
            row.ToString());
  Slice slice;
  EXPECT_STATUS_OK(row.GetString("string_val", &slice));
  EXPECT_EQ("hello world", slice.ToString());
  EXPECT_FALSE(row.IsNull("key"));

  // Set a nullable entry to NULL
  EXPECT_STATUS_OK(row.SetNull("string_val"));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=NULL",
            row.ToString());
  EXPECT_TRUE(row.IsNull("string_val"));

  // Try to set an entry with the wrong type
  Status s = row.SetStringCopy("int_val", "foo");
  EXPECT_EQ("Invalid argument: invalid type string provided for column 'int_val' (expected uint32)",
            s.ToString());

  // Try to get an entry with the wrong type
  s = row.GetString("int_val", &slice);
  EXPECT_EQ("Invalid argument: invalid type string provided for column 'int_val' (expected uint32)",
            s.ToString());

  // Try to set a non-nullable entry to NULL
  s = row.SetNull("key");
  EXPECT_EQ("Invalid argument: column not nullable: key[uint32 NOT NULL]", s.ToString());

  // Set the NULL string back to non-NULL
  EXPECT_STATUS_OK(row.SetStringCopy("string_val", "goodbye world"));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=goodbye world",
            row.ToString());

  // Unset some columns.
  EXPECT_STATUS_OK(row.Unset("string_val"));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321", row.ToString());

  EXPECT_STATUS_OK(row.Unset("key"));
  EXPECT_EQ("uint32 int_val=54321", row.ToString());

  // Set the column by index
  EXPECT_STATUS_OK(row.SetUInt32(1, 99999));
  EXPECT_EQ("uint32 int_val=99999", row.ToString());
}

void PartialRowTest::CheckPBRoundTrip(const PartialRow& row) {
  RowOperationsPB pb;
  row.AppendToPB(RowOperationsPB::INSERT, &pb);

  RowOperationsPB::Type type;
  PartialRow decoded(&schema_);
  ASSERT_STATUS_OK(decoded.CopyFromPB(pb, 0, &type));
  ASSERT_EQ(row.ToString(), decoded.ToString());
  ASSERT_EQ(RowOperationsPB::INSERT, type);
}

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

static void CheckDecodeDoesntCrash(const RowOperationsPB& pb, PartialRow* decoded) {
  RowOperationsPB::Type type;
  Status s = decoded->CopyFromPB(pb, 0, &type);
  if (s.ok()) {
    // If we got an OK result, then we should be able to stringify without
    // crashing. This ensures that any indirect data (eg strings) gets
    // set correctly.
    ignore_result(decoded->ToString());
  }
  // Bad Status is OK -- we expect corruptions here.
}

void PartialRowTest::DoFuzzTest(const PartialRow& row) {
  RowOperationsPB pb;
  row.AppendToPB(RowOperationsPB::INSERT, &pb);

  PartialRow decoded(&schema_);
  RowOperationsPB mutated;

  // Check all possible truncations of the protobuf 'rows' field.
  for (int i = 0; i < pb.rows().size(); i++) {
    mutated.CopyFrom(pb);
    mutated.mutable_rows()->resize(i);
    CheckDecodeDoesntCrash(mutated, &decoded);
  }

  // Check random byte changes in the 'rows' field.
  const int n_iters = AllowSlowTests() ? 10000 : 1000;
  for (int i = 0; i < n_iters; i++) {
    mutated.CopyFrom(pb);
    DoRandomMutation(mutated.mutable_rows());
    CheckDecodeDoesntCrash(mutated, &decoded);
  }
}

// Check that partial rows convert to/from PB format correctly.
TEST_F(PartialRowTest, TestToFromPB) {
  PartialRow row(&schema_);
  CheckPBRoundTrip(row);
  EXPECT_STATUS_OK(row.SetUInt32("int_val", 54321));
  CheckPBRoundTrip(row);
  EXPECT_STATUS_OK(row.SetStringCopy("string_val", "hello world"));
  CheckPBRoundTrip(row);
  EXPECT_STATUS_OK(row.SetNull("string_val"));
  CheckPBRoundTrip(row);
}

// Test that, even if the protobuf is corrupt in some way, we do not
// crash. These protobufs are provided by clients, so we want to make sure
// a malicious client can't crash the server.
TEST_F(PartialRowTest, FuzzTest) {
  PartialRow row(&schema_);
  EXPECT_STATUS_OK(row.SetUInt32("int_val", 54321));
  EXPECT_STATUS_OK(row.SetStringCopy("string_val", "hello world"));
  DoFuzzTest(row);
  EXPECT_STATUS_OK(row.SetNull("string_val"));
  DoFuzzTest(row);
}

namespace {

// Project client_row into server_schema, and stringify the result.
// If an error occurs, the result string is "error: <stringified Status>"
string TestProjection(const PartialRow& client_row,
                      const Schema& server_schema) {
  Arena arena(1024, 1024*1024);
  vector<uint8_t*> rows;
  RowOperationsPB pb;
  client_row.AppendToPB(RowOperationsPB::INSERT, &pb);

  Status s = PartialRow::DecodeAndProject(pb, *client_row.schema(),
                                          server_schema, &rows, &arena);
  if (!s.ok()) {
    return "error: " + s.ToString();
  }
  CHECK_EQ(1, rows.size());
  return server_schema.DebugRow(ContiguousRow(server_schema, rows[0]));;
}

string TestMutateProjection(const PartialRow& client_row,
                            const Schema& server_schema,
                            RowOperationsPB::Type type) {
  Arena arena(1024, 1024*1024);
  vector<RowChangeList> rcls;
  vector<uint8_t*> row_keys;
  RowOperationsPB pb;
  client_row.AppendToPB(type, &pb);

  Status s;
  if (type == RowOperationsPB::UPDATE) {
    s = PartialRow::DecodeAndProjectUpdates(pb, *client_row.schema(),
                                            server_schema, &row_keys, &rcls,
                                            &arena);
  } else if (type == RowOperationsPB::DELETE) {
    s = PartialRow::DecodeAndProjectDeletes(pb, *client_row.schema(),
                                            server_schema, &row_keys, &rcls,
                                            &arena);
  } else {
    LOG(FATAL) << type;
  }

  if (!s.ok()) {
    return "error: " + s.ToString();
  }
  CHECK_EQ(1, row_keys.size());
  CHECK_EQ(1, rcls.size());
  Schema key_schema = server_schema.CreateKeyProjection();
  string key = key_schema.DebugRow(ContiguousRow(key_schema, row_keys[0]));
  string update = rcls[0].ToString(server_schema);
  return "key: " + key + " update: " + update;
}

} // anonymous namespace

// Test decoding partial rows from a client who has a schema which matches
// the table schema.
TEST_F(PartialRowTest, ProjectionTestWholeSchemaSpecified) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       1);

  // Test a row missing 'int_val', which is required.
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("error: Invalid argument: No value provided for required column: "
              "int_val[uint32 NOT NULL]",
              TestProjection(client_row, schema_));
  }

  // Test a row missing 'string_val', which is nullable
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
             CHECK_OK(client_row.SetUInt32("int_val", 54321));
    // The NULL should get filled in.
    EXPECT_EQ("(uint32 key=12345, uint32 int_val=54321, string string_val=NULL)",
              TestProjection(client_row, schema_));
  }

  // Test a row with all of the fields specified, both with the nullable field
  // specified to be NULL and non-NULL.
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetUInt32("int_val", 54321));
    CHECK_OK(client_row.SetStringCopy("string_val", "hello world"));
    EXPECT_EQ("(uint32 key=12345, uint32 int_val=54321, string string_val=hello world)",
              TestProjection(client_row, schema_));

    // The first result should have the field specified.
    // The second result should have the field NULL, since it was explicitly set.
    CHECK_OK(client_row.SetNull("string_val"));
    EXPECT_EQ("(uint32 key=12345, uint32 int_val=54321, string string_val=NULL)",
              TestProjection(client_row, schema_));

  }
}

TEST_F(PartialRowTest, ProjectionTestWithDefaults) {
  uint32_t nullable_default = 123;
  uint32_t non_null_default = 456;
  Schema server_schema(
    boost::assign::list_of
    (ColumnSchema("key", UINT32))
    (ColumnSchema("nullable_with_default", UINT32, true,
                  &nullable_default, &nullable_default))
    (ColumnSchema("non_null_with_default", UINT32, false,
                  &non_null_default, &non_null_default)),
    1);

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
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("(uint32 key=12345, uint32 nullable_with_default=123,"
              " uint32 non_null_with_default=456)",
              TestProjection(client_row, server_schema));
  }

  // Specify the key and override both defaults
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetUInt32("nullable_with_default", 12345));
    CHECK_OK(client_row.SetUInt32("non_null_with_default", 54321));
    EXPECT_EQ("(uint32 key=12345, uint32 nullable_with_default=12345,"
              " uint32 non_null_with_default=54321)",
              TestProjection(client_row, server_schema));
  }

  // Specify the key and override both defaults, overriding the nullable
  // one to NULL.
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetNull("nullable_with_default"));
    CHECK_OK(client_row.SetUInt32("non_null_with_default", 54321));
    EXPECT_EQ("(uint32 key=12345, uint32 nullable_with_default=NULL,"
              " uint32 non_null_with_default=54321)",
              TestProjection(client_row, server_schema));
  }
}

// Test cases where the client only has a subset of the fields
// of the table, but where the missing columns have defaults
// or are NULLable.
TEST_F(PartialRowTest, ProjectionTestWithClientHavingValidSubset) {
  uint32_t nullable_default = 123;
  Schema server_schema(
    boost::assign::list_of
    (ColumnSchema("key", UINT32))
    (ColumnSchema("int_val", UINT32))
    (ColumnSchema("new_int_with_default", UINT32, false,
                  &nullable_default, &nullable_default))
    (ColumnSchema("new_nullable_int", UINT32, true)),
    1);
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32)),
                       1);

  // Specify just the key. This is an error because we're missing int_val.
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("error: Invalid argument: No value provided for required column:"
              " int_val[uint32 NOT NULL]",
              TestProjection(client_row, server_schema));
  }

  // Specify both of the columns that the client is aware of.
  // Defaults should be filled for the other two.
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    CHECK_OK(client_row.SetUInt32("int_val", 12345));
    EXPECT_EQ("(uint32 key=12345, uint32 int_val=12345,"
              " uint32 new_int_with_default=123, uint32 new_nullable_int=NULL)",
              TestProjection(client_row, server_schema));
  }
}

// Test cases where the client is missing a column which is non-null
// and has no default. This is an incompatible client.
TEST_F(PartialRowTest, ProjectionTestWithClientHavingInvalidSubset) {
  Schema server_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32)),
                       1);
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32)),
                       1);
  {
    PartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetUInt32("key", 12345));
    EXPECT_EQ("error: Invalid argument: Client missing required column:"
              " int_val[uint32 NOT NULL]",
              TestProjection(client_row, server_schema));
  }
}

// Simple Update case where the client and server schemas match.
TEST_F(PartialRowTest, TestProjectUpdates) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       1);
  Schema server_schema = SchemaBuilder(client_schema).Build();

  // Check without specifying any columns
  PartialRow client_row(&client_schema);
  EXPECT_EQ("error: Invalid argument: No value provided for key column: key[uint32 NOT NULL]",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));

  // Specify the key and no columns to update
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  EXPECT_EQ("error: Invalid argument: No fields updated, key is: (uint32 key=12345)",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));


  // Specify the key and update one column.
  ASSERT_STATUS_OK(client_row.SetUInt32("int_val", 12345));
  EXPECT_EQ("key: (uint32 key=12345) update: SET int_val=12345",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));

  // Specify the key and update both columns
  ASSERT_STATUS_OK(client_row.SetString("string_val", "foo"));
  EXPECT_EQ("key: (uint32 key=12345) update: SET int_val=12345, string_val=foo",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));

  // Update the nullable column to null.
  ASSERT_STATUS_OK(client_row.SetNull("string_val"));
  EXPECT_EQ("key: (uint32 key=12345) update: SET int_val=12345, string_val=NULL",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));
}

// Client schema has the columns in a different order. Makes
// sure the name-based projection is functioning.
TEST_F(PartialRowTest, TestProjectUpdatesReorderedColumns) {
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

  PartialRow client_row(&client_schema);
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  ASSERT_STATUS_OK(client_row.SetUInt32("int_val", 54321));
  EXPECT_EQ("key: (uint32 key=12345) update: SET int_val=54321",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));
}

// Client schema is missing one of the columns in the server schema.
// This is OK on an update.
TEST_F(PartialRowTest, TestProjectUpdatesSubsetOfColumns) {
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

  PartialRow client_row(&client_schema);
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  ASSERT_STATUS_OK(client_row.SetString("string_val", "foo"));
  EXPECT_EQ("key: (uint32 key=12345) update: SET string_val=foo",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));
}

TEST_F(PartialRowTest, TestClientMismatchedType) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT8)),
                       1);
  Schema server_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("int_val", UINT32)),
                       1);
  server_schema = SchemaBuilder(server_schema).Build();

  PartialRow client_row(&client_schema);
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  ASSERT_STATUS_OK(client_row.SetUInt8("int_val", 1));
  EXPECT_EQ("error: Invalid argument: The column 'int_val' must have type "
            "uint32 NOT NULL found uint8 NOT NULL",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::UPDATE));
}

TEST_F(PartialRowTest, TestProjectDeletes) {
  Schema client_schema(boost::assign::list_of
                       (ColumnSchema("key", UINT32))
                       (ColumnSchema("key_2", UINT32))
                       (ColumnSchema("string_val", STRING, true)),
                       2);
  Schema server_schema = SchemaBuilder(client_schema).Build();

  PartialRow client_row(&client_schema);
  // No columns set
  EXPECT_EQ("error: Invalid argument: No value provided for key column: key[uint32 NOT NULL]",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::DELETE));

  // Only half the key set
  ASSERT_STATUS_OK(client_row.SetUInt32("key", 12345));
  EXPECT_EQ("error: Invalid argument: No value provided for key column: key_2[uint32 NOT NULL]",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::DELETE));

  // Whole key set (correct)
  ASSERT_STATUS_OK(client_row.SetUInt32("key_2", 54321));
  EXPECT_EQ("key: (uint32 key=12345, uint32 key_2=54321) update: DELETE",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::DELETE));

  // Extra column set (incorrect)
  ASSERT_STATUS_OK(client_row.SetString("string_val", "hello"));
  EXPECT_EQ("error: Invalid argument: DELETE should not have a value for column: "
            "string_val[string NULLABLE]",
            TestMutateProjection(client_row, server_schema, RowOperationsPB::DELETE));
}

} // namespace kudu
