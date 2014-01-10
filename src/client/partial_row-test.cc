// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <boost/assign/list_of.hpp>

#include "client/partial_row.h"
#include "common/row.h"
#include "common/schema.h"
#include "common/wire_protocol.pb.h"
#include "util/memory/arena.h"
#include "util/test_util.h"

namespace kudu {
namespace client {

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

  // Fill in the other columns.
  EXPECT_STATUS_OK(row.SetUInt32("int_val", 54321));
  EXPECT_STATUS_OK(row.SetStringCopy("string_val", "hello world"));
  EXPECT_TRUE(row.IsColumnSet(1));
  EXPECT_TRUE(row.IsColumnSet(2));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=hello world",
            row.ToString());

  // Set a nullable entry to NULL
  EXPECT_STATUS_OK(row.SetNull("string_val"));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=NULL",
            row.ToString());

  // Try to set an entry with the wrong type
  Status s = row.SetStringCopy("int_val", "foo");
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
}

void PartialRowTest::CheckPBRoundTrip(const PartialRow& row) {
  PartialRowsPB pb;
  row.AppendToPB(&pb);

  PartialRow decoded(&schema_);
  ASSERT_STATUS_OK(decoded.CopyFromPB(pb, 0));
  ASSERT_EQ(row.ToString(), decoded.ToString());
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

static void CheckDecodeDoesntCrash(const PartialRowsPB& pb, PartialRow* decoded) {
  Status s = decoded->CopyFromPB(pb, 0);
  if (s.ok()) {
    // If we got an OK result, then we should be able to stringify without
    // crashing. This ensures that any indirect data (eg strings) gets
    // set correctly.
    ignore_result(decoded->ToString());
  }
  // Bad Status is OK -- we expect corruptions here.
}

void PartialRowTest::DoFuzzTest(const PartialRow& row) {
  PartialRowsPB pb;
  row.AppendToPB(&pb);

  PartialRow decoded(&schema_);
  PartialRowsPB mutated;

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
  PartialRowsPB pb;
  client_row.AppendToPB(&pb);

  Status s = PartialRow::DecodeAndProject(pb, *client_row.schema(),
                                          server_schema, &rows, &arena);
  if (!s.ok()) {
    return "error: " + s.ToString();
  }
  CHECK_EQ(1, rows.size());
  return server_schema.DebugRow(ContiguousRow(server_schema, rows[0]));;
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

} // namespace client
} // namespace kudu
