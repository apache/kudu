// Copyright (c) 2014, Cloudera, inc.

#include "kudu/common/encoded_key.h"

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>

#include "kudu/util/slice.h"
#include "kudu/util/test_macros.h"

namespace kudu {

#define EXPECT_ROWKEY_EQ(schema, expected, enc_key)  \
  EXPECT_NO_FATAL_FAILURE(ExpectRowKeyEq((schema), (expected), (enc_key)))

#define EXPECT_DECODED_KEY_EQ(type, expected, val) \
  EXPECT_NO_FATAL_FAILURE(ExpectDecodedKeyEq<(type)>((expected), (val)))

class EncodedKeyTest : public ::testing::Test {
 public:
  EncodedKeyTest()
  : schema_(CreateSchema()),
    key_builder_(&schema_) {
  }

  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key", UINT32)), 1);
  }

  EncodedKey* BuildEncodedKey(EncodedKeyBuilder& key_builder, int val) {
    key_builder.Reset();
    key_builder.AddColumnKey(&val);
    return key_builder.BuildEncodedKey();
  }

  // Test whether target lies within the numerical key ranges given by
  // start and end. If -1, an empty slice is used instead.
  bool InRange(int start, int end, int target) {
    gscoped_ptr<EncodedKey> start_key(BuildEncodedKey(key_builder_, start));
    gscoped_ptr<EncodedKey> end_key(BuildEncodedKey(key_builder_, end));
    gscoped_ptr<EncodedKey> target_key(BuildEncodedKey(key_builder_, target));
    return target_key->InRange(start != -1 ? start_key->encoded_key() : Slice(),
                               end != -1 ? end_key->encoded_key() : Slice());
  }

  void ExpectRowKeyEq(const Schema& schema,
                      const string& exp_str,
                      const EncodedKey& key) {
    EXPECT_EQ(exp_str, schema.DebugEncodedRowKey(key.encoded_key()));
  }

  template<DataType Type>
  void ExpectDecodedKeyEq(const string& expected,
                          void* val) {
    Schema schema(boost::assign::list_of
                  (ColumnSchema("key", Type)), 1);
    EncodedKeyBuilder builder(&schema);
    builder.AddColumnKey(val);
    gscoped_ptr<EncodedKey> key(builder.BuildEncodedKey());
    EXPECT_ROWKEY_EQ(schema, expected, *key);
  }

 private:
  Schema schema_;
  EncodedKeyBuilder key_builder_;
};

TEST_F(EncodedKeyTest, TestKeyInRange) {
  ASSERT_TRUE(InRange(-1, -1, 0));
  ASSERT_TRUE(InRange(-1, -1, 50));

  ASSERT_TRUE(InRange(-1, 30, 0));
  ASSERT_TRUE(InRange(-1, 30, 29));
  ASSERT_FALSE(InRange(-1, 30, 30));
  ASSERT_FALSE(InRange(-1, 30, 31));

  ASSERT_FALSE(InRange(10, -1, 9));
  ASSERT_TRUE(InRange(10, -1, 10));
  ASSERT_TRUE(InRange(10, -1, 11));
  ASSERT_TRUE(InRange(10, -1, 31));

  ASSERT_FALSE(InRange(10, 20, 9));
  ASSERT_TRUE(InRange(10, 20, 10));
  ASSERT_TRUE(InRange(10, 20, 19));
  ASSERT_FALSE(InRange(10, 20, 20));
  ASSERT_FALSE(InRange(10, 20, 21));
}

TEST_F(EncodedKeyTest, TestDecodeSimpleKeys) {
  {
    uint8_t val = 123;
    EXPECT_DECODED_KEY_EQ(UINT8, "(uint8 key=123)", &val);
  }

  {
    int8_t val = -123;
    EXPECT_DECODED_KEY_EQ(INT8, "(int8 key=-123)", &val);
  }

  {
    uint16_t val = 12345;
    EXPECT_DECODED_KEY_EQ(UINT16, "(uint16 key=12345)", &val);
  }

  {
    int16_t val = -12345;
    EXPECT_DECODED_KEY_EQ(INT16, "(int16 key=-12345)", &val);
  }

  {
    uint32_t val = 123456;
    EXPECT_DECODED_KEY_EQ(UINT32, "(uint32 key=123456)", &val);
  }

  {
    int32_t val = -123456;
    EXPECT_DECODED_KEY_EQ(INT32, "(int32 key=-123456)", &val);
  }

  {
    uint64_t val = 1234567891011121314;
    EXPECT_DECODED_KEY_EQ(UINT64, "(uint64 key=1234567891011121314)", &val);
  }

  {
    int64_t val = -1234567891011121314;
    EXPECT_DECODED_KEY_EQ(INT64, "(int64 key=-1234567891011121314)", &val);
  }

  {
    Slice val("aKey");
    EXPECT_DECODED_KEY_EQ(STRING, "(string key=aKey)", &val);
  }
}

TEST_F(EncodedKeyTest, TestDecodeCompoundKeys) {
  gscoped_ptr<EncodedKey> key;
  {
    // Integer type compound key.
    Schema schema(boost::assign::list_of
                  (ColumnSchema("key0", UINT16))
                  (ColumnSchema("key1", UINT32))
                  (ColumnSchema("key2", UINT64)), 3);

    EncodedKeyBuilder builder(&schema);
    uint16_t key0 = 12345;
    uint32_t key1 = 123456;
    uint64_t key2 = 1234567891011121314;
    builder.AddColumnKey(&key0);
    builder.AddColumnKey(&key1);
    builder.AddColumnKey(&key2);
    key.reset(builder.BuildEncodedKey());

    EXPECT_ROWKEY_EQ(schema,
                     "(uint16 key0=12345, uint32 key1=123456, uint64 key2=1234567891011121314)",
                     *key);
  }

  {
    // Mixed type compound key with STRING last.
    Schema schema(boost::assign::list_of
                  (ColumnSchema("key0", UINT16))
                  (ColumnSchema("key1", STRING)), 2);
    EncodedKeyBuilder builder(&schema);
    uint16_t key0 = 12345;
    Slice key1("aKey");
    builder.AddColumnKey(&key0);
    builder.AddColumnKey(&key1);
    key.reset(builder.BuildEncodedKey());

    EXPECT_ROWKEY_EQ(schema, "(uint16 key0=12345, string key1=aKey)", *key);
  }

  {
    // Mixed type compound key with STRING in the middle
    Schema schema(boost::assign::list_of
                  (ColumnSchema("key0", UINT16))
                  (ColumnSchema("key1", STRING))
                  (ColumnSchema("key2", UINT8)), 3);
    EncodedKeyBuilder builder(&schema);
    uint16_t key0 = 12345;
    Slice key1("aKey");
    uint8_t key2 = 123;
    builder.AddColumnKey(&key0);
    builder.AddColumnKey(&key1);
    builder.AddColumnKey(&key2);
    key.reset(builder.BuildEncodedKey());

    EXPECT_ROWKEY_EQ(schema, "(uint16 key0=12345, string key1=aKey, uint8 key2=123)", *key);
  }
}

} // namespace kudu
