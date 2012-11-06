// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/noncopyable.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "tablet/memstore.h"
#include "util/memory/arena.h"
#include "util/slice.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

// TODO: this might move to non-test code soon
class RowBuilder : boost::noncopyable {
public:
  RowBuilder(const Schema &schema) :
    schema_(schema),
    arena_(1024, 1024*1024)
  {
    Reset();
  }

  void Reset() {
    arena_.Reset();
    buf_ = reinterpret_cast<uint8_t *>(
      arena_.AllocateBytes(schema_.byte_size()));
    CHECK(buf_) <<
      "could not allocate " << schema_.byte_size() << " bytes for row builder";
    col_idx_ = 0;
    byte_idx_ = 0;
  }

  void AddString(const string &str) {
    CheckNextType(cfile::STRING);
    Slice *ptr = reinterpret_cast<Slice *>(buf_ + byte_idx_);

    char *in_arena = arena_.AddStringPieceContent(str);
    CHECK(in_arena) << "could not allocate space in arena";
    *ptr = Slice(in_arena, str.size());
    Advance();
  }

  void AddUint32(uint32_t val) {
    CheckNextType(cfile::UINT32);
    *reinterpret_cast<uint32_t *>(&buf_[byte_idx_]) = val;
    Advance();
  }

  const Slice data() const {
    CHECK_EQ(byte_idx_, schema_.byte_size());
    return Slice(reinterpret_cast<const char *>(buf_), byte_idx_);
  }

private:
  void CheckNextType(DataType type) {
    CHECK_EQ(schema_.column(col_idx_).type_info().type(),
             type);
  }

  void Advance() {
    int size = schema_.column(col_idx_).type_info().size();
    byte_idx_ += size;
    col_idx_++;
  }

  const Schema schema_;
  Arena arena_;
  uint8_t *buf_;

  size_t col_idx_;
  size_t byte_idx_;
};

TEST(TestTablet, TestMemStore) {
  ColumnSchema col1(kudu::cfile::STRING);
  ColumnSchema col2(kudu::cfile::UINT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2);
  Schema schema(cols, 1);

  MemStore ms(schema);

  RowBuilder rb(schema);
  rb.AddString("hello world");
  rb.AddUint32(12345);
  ASSERT_STATUS_OK(ms.Insert(rb.data()));

  rb.Reset();
  rb.AddString("goodbye world");
  rb.AddUint32(54321);
  ASSERT_STATUS_OK(ms.Insert(rb.data()));

  ASSERT_EQ(2, ms.entry_count());

  scoped_ptr<MemStore::Iterator> iter(ms.NewIterator());

  // The first row returned from the iterator should
  // be "goodbye" because 'g' sorts before 'h'
  ASSERT_TRUE(iter->IsValid());
  Slice s = iter->GetCurrentRow();
  ASSERT_EQ(schema.byte_size(), s.size());
  ASSERT_EQ(Slice("goodbye world"),
            *schema.ExtractColumnFromRow<cfile::STRING>(s, 0));
  ASSERT_EQ(54321,
            *schema.ExtractColumnFromRow<cfile::UINT32>(s, 1));

  // Next row should be 'hello world'
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->IsValid());
  s = iter->GetCurrentRow();
  ASSERT_EQ(schema.byte_size(), s.size());
  ASSERT_EQ(Slice("hello world"),
            *schema.ExtractColumnFromRow<cfile::STRING>(s, 0));
  ASSERT_EQ(12345,
            *schema.ExtractColumnFromRow<cfile::UINT32>(s, 1));

  ASSERT_FALSE(iter->Next());
  ASSERT_FALSE(iter->IsValid());
}

}
}
