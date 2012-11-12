// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_SCHEMA_H
#define KUDU_TABLET_SCHEMA_H

#include "cfile/cfile.pb.h"
#include "cfile/types.h"
#include "gutil/stringprintf.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>

namespace kudu { namespace tablet {

using std::vector;
using kudu::cfile::DataType;
using kudu::cfile::TypeInfo;

// The schema for a given column.
//
// Currently a simple wrapper around a data type, but in the future
// will probably hold other information like nullability, column name,
// annotations, etc.
class ColumnSchema {
public:
  ColumnSchema(DataType type) :
    type_info_(kudu::cfile::GetTypeInfo(type))
  {}

  const TypeInfo &type_info() const {
    return type_info_;
  }

  string ToString() const {
    return StringPrintf("[type='%s']", type_info_.name().c_str());
  }

private:
  const TypeInfo &type_info_;
};


// The schema for a set of rows.
//
// A Schema is simply a set of columns, along with information about
// which prefix of columns makes up the primary key.
class Schema {
public:
  Schema(const vector<ColumnSchema> &cols,
         int key_columns) :
    cols_(cols),
    num_key_columns_(key_columns)
  {
    CHECK_GT(cols_.size(), 0);
    CHECK_LE(key_columns, cols_.size());

    CHECK_EQ(1, key_columns) <<
      "TODO: Currently only support a single key-column.";

    // Calculate the offset of each column in the row format.
    col_offsets_.reserve(cols_.size());
    size_t off = 0;
    BOOST_FOREACH(ColumnSchema col, cols) {
      col_offsets_.push_back(off);
      off += col.type_info().size();
    }
    // Add an extra element on the end for the total
    // byte size
    col_offsets_.push_back(off);
  }

  // Return the number of bytes needed to represent a single row of this schema.
  //
  // This size does not include any indirected (variable length) data (eg strings)
  size_t byte_size() const {
    return col_offsets_[num_columns()];
  }

  // Return the number of bytes needed to represent
  // only the key portion of this schema.
  size_t key_byte_size() const {
    return col_offsets_[num_key_columns_];
  }

  // Return the number of columns in this schema
  size_t num_columns() const {
    return cols_.size();
  }

  // Return the byte offset within the row for the given column index.
  size_t column_offset(size_t col_idx) const {
    CHECK_LT(col_idx, cols_.size());
    return col_offsets_[col_idx];
  }

  // Return the ColumnSchema corresponding to the given column index.
  const ColumnSchema &column(size_t idx) const {
    CHECK_LT(idx, cols_.size());
    return cols_[idx];
  }

  // Extract a given column from a row where the type is
  // known at compile-time. The type is checked with a debug
  // assertion -- but if the wrong type is used and these assertions
  // are off, incorrect data may result.
  //
  // This is mostly useful for tests at this point.
  // TODO: consider removing it.
  template<DataType Type>
  const typename cfile::DataTypeTraits<Type>::cpp_type *
  ExtractColumnFromRow(const Slice &row, size_t idx) {
    DCHECK_LT(idx, cols_.size());
    DCHECK_EQ(cols_[idx].type_info().type(), Type);

    return reinterpret_cast<const typename cfile::DataTypeTraits<Type>::cpp_type *>(
      row.data() + col_offsets_[idx]);
  }

  // Compare two rows of this schema.
  int Compare(const void *lhs, const void *rhs) const {
    DCHECK(lhs && rhs) << "may not pass null";

    for (size_t col = 0; col < num_key_columns_; col++) {
      const TypeInfo &ti = cols_[col].type_info();

      // TODO: move this into types.cc or something
      switch (ti.type()) {
        case cfile::UINT32:
        {
          uint32_t lhs_int = *reinterpret_cast<const uint32_t *>(lhs);
          uint32_t rhs_int = *reinterpret_cast<const uint32_t *>(rhs);
          if (lhs_int < rhs_int) {
            return -1;
          } else if (lhs_int > rhs_int) {
            return 1;
          }
          break;
        }
        case cfile::STRING:
        {
          const Slice *lhs_slice = reinterpret_cast<const Slice *>(lhs);
          const Slice *rhs_slice = reinterpret_cast<const Slice *>(rhs);
          int cmp = lhs_slice->compare(*rhs_slice);
          if (cmp != 0) {
            return cmp;
          }
          break;
        }
        default:
        {
          CHECK(0) << "unable to compare type: " << ti.name();
        }
      }
      lhs = reinterpret_cast<const uint8_t *>(lhs) + ti.size();
      rhs = reinterpret_cast<const uint8_t *>(rhs) + ti.size();
    }
    return 0;
  }

private:
  const vector<ColumnSchema> cols_;
  const size_t num_key_columns_;
  vector<size_t> col_offsets_;
};

} // namespace tablet
} // namespace kudu

#endif
