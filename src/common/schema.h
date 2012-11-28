// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_SCHEMA_H
#define KUDU_COMMON_SCHEMA_H

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include <tr1/unordered_map>
#include <vector>

#include "common/types.h"
#include "common/common.pb.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/join.h"
#include "gutil/strings/strcat.h"
#include "util/memory/arena.h"
#include "util/status.h"

namespace kudu {

using std::vector;
using std::tr1::unordered_map;

// The schema for a given column.
//
// Currently a simple wrapper around a data type, but in the future
// will probably hold other information like nullability, column name,
// annotations, etc.
class ColumnSchema {
public:
  ColumnSchema(const string &name,
               DataType type) :
    name_(name),
    type_info_(GetTypeInfo(type))
  {}

  const TypeInfo &type_info() const {
    return type_info_;
  }

  const string &name() const {
    return name_;
  }

  string ToString() const {
    return StringPrintf("%s[type='%s']",
                        name_.c_str(),
                        type_info_.name().c_str());
  }

  bool EqualsType(const ColumnSchema &other) const {
    return type_info_.type() == other.type_info().type();
  }

  Status CopyCell(void *dst, const void *src, Arena *dst_arena) const {
    if (type_info().type() == STRING) {
      // If it's a Slice column, need to relocate the referred-to data
      // as well as the slice itself.
      // TODO: potential optimization here: if the new value is smaller than
      // the old value, we could potentially just overwrite in some cases.
      const Slice *src_slice = reinterpret_cast<const Slice *>(src);
      Slice *dst_slice = reinterpret_cast<Slice *>(dst);
      if (PREDICT_FALSE(!dst_arena->RelocateSlice(*src_slice, dst_slice))) {
        return Status::IOError("out of memory copying slice", src_slice->ToString());
      }
    } else {
      size_t size = type_info().size();
      memcpy(dst, src, size); // TODO: inline?
    }
    return Status::OK();
  }

private:
  const string name_;
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
    size_t i = 0;
    BOOST_FOREACH(ColumnSchema col, cols) {
      name_to_index_[col.name()] = i++;
      col_offsets_.push_back(off);
      off += col.type_info().size();
    }

    CHECK_EQ(cols.size(), name_to_index_.size())
      << "Duplicate name present in schema!";

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

  // Return the length of the key prefix in this schema.
  // TODO: this is currently always 1
  size_t num_key_columns() const {
    return num_key_columns_;
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
  const typename DataTypeTraits<Type>::cpp_type *
  ExtractColumnFromRow(const Slice &row, size_t idx) const {
    DCHECK_LT(idx, cols_.size());
    DCHECK_EQ(cols_[idx].type_info().type(), Type);

    return reinterpret_cast<const typename DataTypeTraits<Type>::cpp_type *>(
      row.data() + col_offsets_[idx]);
  }

  // Stringify the given row, which conforms to this schema,
  // in a way suitable for debugging. This isn't currently optimized
  // so should be avoided in hot paths.
  string DebugRow(const void *row_v) const {
    const uint8_t *row = reinterpret_cast<const uint8_t *>(row_v);
    string ret;
    ret.append("(");

    for (size_t col = 0; col < num_columns(); col++) {
      const TypeInfo &ti = cols_[col].type_info();

      if (col > 0) {
        ret.append(", ");
      }
      ret.append(ti.name());
      ret.append(" ");
      ret.append(cols_[col].name());
      ret.append("=");
      ti.AppendDebugStringForValue(&row[col_offsets_[col]], &ret);
    }
    ret.append(")");
    return ret;
  }

  // Compare two rows of this schema.
  int Compare(const void *lhs, const void *rhs) const {
    DCHECK(lhs && rhs) << "may not pass null";

    for (size_t col = 0; col < num_key_columns_; col++) {
      const TypeInfo &ti = cols_[col].type_info();

      // TODO: move this into types.cc or something
      switch (ti.type()) {
        case UINT32:
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
        case STRING:
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

  // Determine the mapping to project from from_schema into
  // this schema. This schema's fields must be a subset of from_schema's
  // fields.
  // 'indexes' is mutated such that its length is equal to this schema's
  // length, and each index stores the source schema's column index which
  // corresponds to the same projected column.
  //
  // For example:
  // this:  [foo, bar]
  // from_schema: [bar, baz, foo]
  // resulting indexes: [2, 0]
  Status GetProjectionFrom(const Schema &from_schema,
                           vector<size_t> *indexes) const {
    indexes->clear();
    indexes->reserve(num_columns());
    BOOST_FOREACH(const ColumnSchema &col, cols_) {
      NameToIndexMap::const_iterator iter =
        from_schema.name_to_index_.find(col.name());
      if (iter == from_schema.name_to_index_.end()) {
        return Status::InvalidArgument(
          string("Cannot map from schema ") +
          from_schema.ToString() + " to " + ToString() +
          ": column '" + col.name() + "' not present in source");
      }

      size_t idx = (*iter).second;
      const ColumnSchema &from_col = from_schema.column(idx);
      if (!from_col.EqualsType(col)) {
        return Status::InvalidArgument(
          string("Cannot map from schema ") +
          from_schema.ToString() + " to " + ToString() +
          ": type mismatch for column '" + col.name() + "'");
      }

      indexes->push_back((*iter).second);
    }
    return Status::OK();
  }

  // Stringify this Schema. This is not particularly efficient,
  // so should only be used when necessary for output.
  string ToString() const {
    vector<string> col_strs;
    BOOST_FOREACH(const ColumnSchema &col, cols_) {
      col_strs.push_back(col.ToString());
    }

    return StrCat("Schema [",
                  JoinStrings(col_strs, ", "),
                  "]");
  }

private:
  const vector<ColumnSchema> cols_;
  const size_t num_key_columns_;
  vector<size_t> col_offsets_;

  typedef unordered_map<string, size_t> NameToIndexMap;
  NameToIndexMap name_to_index_;
};

} // namespace kudu

#endif
