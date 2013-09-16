// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_SCHEMA_H
#define KUDU_COMMON_SCHEMA_H

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <string>
#include <utility>
#include <vector>

#include "common/types.h"
#include "common/common.pb.h"
#include "common/key_encoder.h"
#include "util/status.h"

// Check that two schemas are equal, yielding a useful error message in the case that
// they are not.
#define DCHECK_SCHEMA_EQ(s1, s2) \
  do { \
    DCHECK((s1).Equals((s2))) << "Schema " << (s1).ToString() << " does not match " << (s2).ToString(); \
  } while (0);

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
  // name: column name
  // type: column type (e.g. UINT8, INT32, STRING, ...)
  // is_nullable: true if a row value can be null
  // read_default: default value used on read if the column was not present before alter.
  //    The value will be copied and released on ColumnSchema destruction.
  // write_default: default value added to the row if the column value was
  //    not specified on insert.
  //    The value will be copied and released on ColumnSchema destruction.
  //
  // Example:
  //   ColumnSchema col_a("a", UINT32)
  //   ColumnSchema col_b("b", STRING, true);
  //   uint32_t default_i32 = -15;
  //   ColumnSchema col_c("c", INT32, false, &default_i32);
  //   Slice default_str("Hello");
  //   ColumnSchema col_d("d", STRING, false, &default_str);
  ColumnSchema(const string &name,
               DataType type,
               bool is_nullable = false,
               const void *read_default = NULL,
               const void *write_default = NULL) :
      name_(name),
      type_info_(&GetTypeInfo(type)),
      is_nullable_(is_nullable),
      read_default_(read_default ? new Variant(type, read_default) : NULL) {
    if (write_default == read_default) {
      write_default_ = read_default_;
    } else if (write_default != NULL) {
      DCHECK(read_default != NULL) << "Must have a read default";
      write_default_.reset(new Variant(type, write_default));
    }
  }

  const TypeInfo &type_info() const {
    return *type_info_;
  }

  bool is_nullable() const {
    return is_nullable_;
  }

  const string &name() const {
    return name_;
  }

  string ToString() const;

  // Returns true if the column has a read default value
  bool has_read_default() const {
    return read_default_ != NULL;
  }

  // Returns a pointer the default value associated with the column
  // or NULL if there is no default value. You may check has_read_default() first.
  // The returned value will be valid until the ColumnSchema will be destroyed.
  //
  // Example:
  //    const uint32_t *vu32 = static_cast<const uint32_t *>(col_schema.read_default_value());
  //    const Slice *vstr = static_cast<const Slice *>(col_schema.read_default_value());
  const void *read_default_value() const {
    if (read_default_ != NULL) {
      return read_default_->value();
    }
    return NULL;
  }

  // Returns true if the column has a write default value
  bool has_write_default() const {
    return write_default_ != NULL;
  }

  // Returns a pointer the default value associated with the column
  // or NULL if there is no default value. You may check has_write_default() first.
  // The returned value will be valid until the ColumnSchema will be destroyed.
  //
  // Example:
  //    const uint32_t *vu32 = static_cast<const uint32_t *>(col_schema.write_default_value());
  //    const Slice *vstr = static_cast<const Slice *>(col_schema.write_default_value());
  const void *write_default_value() const {
    if (write_default_ != NULL) {
      return write_default_->value();
    }
    return NULL;
  }

  bool EqualsType(const ColumnSchema &other) const {
    return type_info().type() == other.type_info().type();
  }

  bool Equals(const ColumnSchema &other) const {
    return EqualsType(other) && this->name_ == other.name_;
  }

  int Compare(const void *lhs, const void *rhs) const {
    return type_info_->Compare(lhs, rhs);
  }

  // Stringify the given cell.
  string Stringify(const void *cell) const {
    string ret;
    type_info_->AppendDebugStringForValue(cell, &ret);
    return ret;
  }

 private:
  string name_;
  const TypeInfo *type_info_;
  bool is_nullable_;
  // use shared_ptr since the ColumnSchema is always copied around.
  std::tr1::shared_ptr<Variant> read_default_;
  std::tr1::shared_ptr<Variant> write_default_;
};


// The schema for a set of rows.
//
// A Schema is simply a set of columns, along with information about
// which prefix of columns makes up the primary key.
//
// Note that, while Schema is copyable and assignable, it is a complex
// object that is not inexpensive to copy. You should generally prefer
// passing by pointer or reference, and functions that create new
// Schemas should generally prefer taking a Schema pointer and using
// Schema::swap() or Schema::Reset() rather than returning by value.
class Schema {
 public:
  Schema()
    : num_key_columns_(0) {
  }

  void swap(Schema& other) { // NOLINT(build/include_what_you_use)
    int tmp = other.num_key_columns_;
    other.num_key_columns_ = num_key_columns_;
    num_key_columns_ = tmp;
    cols_.swap(other.cols_);
    col_offsets_.swap(other.col_offsets_);
    name_to_index_.swap(other.name_to_index_);
  }

  // Construct a schema with the given information.
  //
  // NOTE: if the schema is user-provided, it's better to construct an
  // empty schema and then use Reset(...)  so that errors can be
  // caught. If an invalid schema is passed to this constructor, an
  // assertion will be fired!
  Schema(const vector<ColumnSchema> &cols,
         int key_columns) {
    CHECK_OK(Reset(cols, key_columns));
  }

  // Reset this Schema object to the given schema.
  // If this fails, the Schema object is left in an inconsistent
  // state and may not be used.
  Status Reset(const vector<ColumnSchema> &cols,
               int key_columns);

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
    DCHECK_LT(col_idx, cols_.size());
    return col_offsets_[col_idx];
  }

  // Return the ColumnSchema corresponding to the given column index.
  inline const ColumnSchema &column(size_t idx) const {
    DCHECK_LT(idx, cols_.size());
    return cols_[idx];
  }

  const std::vector<ColumnSchema>& columns() const {
    return cols_;
  }

  // Return the column index corresponding to the given column,
  // or -1 if the column is not in this schema.
  int find_column(const string &col_name) const {
    NameToIndexMap::const_iterator iter = name_to_index_.find(col_name);
    if (PREDICT_FALSE(iter == name_to_index_.end())) {
      return -1;
    } else {
      return (*iter).second;
    }
  }

  // Returns true if the schema contains nullable columns
  bool has_nullables() const {
    BOOST_FOREACH(const ColumnSchema& col, cols_) {
      if (col.is_nullable()) {
        return true;
      }
    }
    return false;
  }

  // Return true if this Schema is initialized and valid.
  bool initialized() const {
    return !col_offsets_.empty();
  }

  // Extract a given column from a row where the type is
  // known at compile-time. The type is checked with a debug
  // assertion -- but if the wrong type is used and these assertions
  // are off, incorrect data may result.
  //
  // This is mostly useful for tests at this point.
  // TODO: consider removing it.
  template<DataType Type, class RowType>
  const typename DataTypeTraits<Type>::cpp_type *
  ExtractColumnFromRow(const RowType& row, size_t idx) const {
    DCHECK_SCHEMA_EQ(*this, row.schema());
    const ColumnSchema& col_schema = cols_[idx];
    DCHECK_LT(idx, cols_.size());
    DCHECK_EQ(col_schema.type_info().type(), Type);

    const void *val;
    if (col_schema.is_nullable()) {
      val = row.nullable_cell_ptr(idx);
    } else {
      val = row.cell_ptr(idx);
    }

    return reinterpret_cast<const typename DataTypeTraits<Type>::cpp_type *>(val);
  }

  // Stringify the given row, which conforms to this schema,
  // in a way suitable for debugging. This isn't currently optimized
  // so should be avoided in hot paths.
  template<class RowType>
  string DebugRow(const RowType& row) const {
    DCHECK_SCHEMA_EQ(*this, row.schema());

    string ret;
    ret.append("(");

    for (size_t col = 0; col < num_columns(); col++) {
      const ColumnSchema& col_schema = cols_[col];
      const TypeInfo &ti = col_schema.type_info();

      if (col > 0) {
        ret.append(", ");
      }
      ret.append(ti.name());
      ret.append(" ");
      ret.append(cols_[col].name());
      ret.append("=");
      if (col_schema.is_nullable() && row.is_null(col)) {
        ret.append("NULL");
      } else {
        ti.AppendDebugStringForValue(row.cell_ptr(col), &ret);
      }
    }
    ret.append(")");
    return ret;
  }

  // Compare two rows of this schema.
  template<class RowTypeA, class RowTypeB>
  int Compare(const RowTypeA& lhs, const RowTypeB& rhs) const {
    // TODO: Handle rows with different schema?
    DCHECK(Equals(lhs.schema()) && Equals(rhs.schema()));

    for (size_t col = 0; col < num_key_columns_; col++) {
      int col_compare = column(col).Compare(lhs.cell_ptr(col), rhs.cell_ptr(col));
      if (col_compare != 0) {
        return col_compare;
      }
    }
    return 0;
  }

  // Return the projection of this schema which contains only
  // the key columns.
  // TODO: this should take a Schema* out-parameter to avoid an
  // extra copy of the ColumnSchemas.
  // TODO this should probably be cached since the key projection
  // is not supposed to change, for a single schema.
  Schema CreateKeyProjection() const {
    vector<ColumnSchema> key_cols(cols_.begin(),
                                  cols_.begin() + num_key_columns_);
    return Schema(key_cols, num_key_columns_);
  }

  // Encode the key portion of the given row into a buffer
  // such that the buffer's lexicographic comparison represents
  // the proper comparison order of the underlying types.
  //
  // The key is encoded into the given buffer, replacing its current
  // contents.
  // Returns the encoded key.
  template <class RowType>
  Slice EncodeComparableKey(const RowType& row, faststring *dst) const {
    DCHECK_SCHEMA_EQ(*this, row.schema());
    dst->clear();
    for (size_t i = 0; i < num_key_columns_; i++) {
      DCHECK(!cols_[i].is_nullable());
      const TypeInfo &ti = cols_[i].type_info();
      bool is_last = i == num_key_columns_ - 1;
      GetKeyEncoder(ti.type()).Encode(row.cell_ptr(i), is_last, dst);
    }
    return Slice(*dst);
  }

  // Stringify this Schema. This is not particularly efficient,
  // so should only be used when necessary for output.
  string ToString() const;

  // Return true if the schemas have exactly the same set of columns
  // and respective types.
  bool Equals(const Schema &other) const {
    if (this == &other) return true;
    if (this->num_key_columns_ != other.num_key_columns_) return false;
    if (this->cols_.size() != other.cols_.size()) return false;

    for (size_t i = 0; i < other.cols_.size(); i++) {
      if (!this->cols_[i].Equals(other.cols_[i])) return false;
    }

    return true;
  }

  // Loops through the projection schema and calls the projector methods:
  // - Status ProjectBaseColumn(size_t proj_col_idx, size_t base_col_idx)
  //   called if the column already exists in the base schema.
  // - Status ProjectDefaultColumn(size_t proj_idx)
  //   called if the column does not exists in the base schema. In this case
  //   the (projection) column must have a default or be nullable, otherwise
  //   Status::InvalidArgument will be returned.
  //
  // TODO(MAYBE): Pass the ColumnSchema and not only the column index?
  template <class Projector>
  Status GetProjectionMapping(const Schema& base_schema, Projector *projector) const {
    int proj_idx = 0;
    BOOST_FOREACH(const ColumnSchema& col_schema, cols_) {
      int base_idx = base_schema.find_column(col_schema.name());
      if (base_idx >= 0) {
        const ColumnSchema& base_col_schema = base_schema.column(base_idx);
        // Column present in the Base Schema...
        if (!col_schema.EqualsType(base_col_schema)) {
          // ...but with a different type, (TODO: try with an adaptor)
          return Status::InvalidArgument("The column '" + col_schema.name() + "' must have type " +
                                         DataType_Name(base_col_schema.type_info().type()));
        } else {
          RETURN_NOT_OK(projector->ProjectBaseColumn(proj_idx, base_idx));
        }
      } else {
        bool has_default = col_schema.has_read_default() || col_schema.has_write_default();
        if (!has_default && !col_schema.is_nullable()) {
          return Status::InvalidArgument("The column '" + col_schema.name() +
                                         "' must have a default value or be nullable");
        }

        // Column missing from the Base Schema, use the default value of the projection
        RETURN_NOT_OK(projector->ProjectDefaultColumn(proj_idx));
      }
      proj_idx++;
    }
    return Status::OK();
  }

 private:
  vector<ColumnSchema> cols_;
  size_t num_key_columns_;
  vector<size_t> col_offsets_;

  typedef unordered_map<string, size_t> NameToIndexMap;
  NameToIndexMap name_to_index_;

  // NOTE: if you add more members, make sure to add the appropriate
  // code to swap() as well to prevent subtle bugs.
};

} // namespace kudu

#endif
