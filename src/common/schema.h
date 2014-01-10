// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_COMMON_SCHEMA_H
#define KUDU_COMMON_SCHEMA_H

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <string>
#include <utility>
#include <vector>

#include "common/types.h"
#include "common/common.pb.h"
#include "common/key_encoder.h"
#include "gutil/map-util.h"
#include "gutil/strings/substitute.h"
#include "util/status.h"

// Check that two schemas are equal, yielding a useful error message in the case that
// they are not.
#define DCHECK_SCHEMA_EQ(s1, s2) \
  do { \
    DCHECK((s1).Equals((s2))) << "Schema " << (s1).ToString() \
                              << " does not match " << (s2).ToString(); \
  } while (0);

#define DCHECK_KEY_PROJECTION_SCHEMA_EQ(s1, s2) \
  do { \
    DCHECK((s1).KeyEquals((s2))) << "Key-Projection Schema " \
                                 << (s1).ToString() << " does not match " \
                                 << (s2).ToString(); \
  } while (0);

namespace kudu {

using std::vector;
using std::tr1::unordered_map;
using std::tr1::unordered_set;

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

  // Return a string identifying this column, including its
  // name.
  string ToString() const;

  // Same as above, but only including the type information.
  // For example, "STRING NOT NULL".
  string TypeToString() const;

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
    return is_nullable_ == other.is_nullable_ &&
           type_info().type() == other.type_info().type();
  }

  bool Equals(const ColumnSchema &other, bool check_defaults) const {
    if (!EqualsType(other) || this->name_ != other.name_)
      return false;

    // For Key comparison checking the defauls doesn't make sense,
    // since we don't support them, for server vs user schema this comparison
    // will always fail, since the user does not specify the defaults.
    if (check_defaults) {
      if (read_default_ == NULL && other.read_default_ != NULL)
        return false;

      if (write_default_ == NULL && other.write_default_ != NULL)
        return false;

      if (read_default_ != NULL && !read_default_->Equals(other.read_default_.get()))
        return false;

      if (write_default_ != NULL && !write_default_->Equals(other.write_default_.get()))
        return false;
    }
    return true;
  }

  int Compare(const void *lhs, const void *rhs) const {
    return type_info_->Compare(lhs, rhs);
  }

  // Stringify the given cell. This just stringifies the cell contents,
  // and doesn't include the column name or type.
  string Stringify(const void *cell) const {
    string ret;
    type_info_->AppendDebugStringForValue(cell, &ret);
    return ret;
  }

  // Append a debug string for this cell. This differs from Stringify above
  // in that it also includes the column info, for example 'STRING foo=bar'.
  template<class CellType>
  void DebugCellAppend(const CellType& cell, std::string* ret) const {
    ret->append(type_info_->name());
    ret->append(" ");
    ret->append(name_);
    ret->append("=");
    if (is_nullable_ && cell.is_null()) {
      ret->append("NULL");
    } else {
      type_info_->AppendDebugStringForValue(cell.ptr(), ret);
    }
  }

 private:
  friend class SchemaBuilder;

  void set_name(const string& name) {
    name_ = name;
  }

  size_t id_;
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

  Schema(const Schema& other);
  Schema& operator=(const Schema& other);

  void swap(Schema& other); // NOLINT(build/include_what_you_use)

  void CopyFrom(const Schema& other);

  // Construct a schema with the given information.
  //
  // NOTE: if the schema is user-provided, it's better to construct an
  // empty schema and then use Reset(...)  so that errors can be
  // caught. If an invalid schema is passed to this constructor, an
  // assertion will be fired!
  Schema(const vector<ColumnSchema>& cols,
         int key_columns) {
    CHECK_OK(Reset(cols, key_columns));
  }

  // Construct a schema with the given information.
  //
  // NOTE: if the schema is user-provided, it's better to construct an
  // empty schema and then use Reset(...)  so that errors can be
  // caught. If an invalid schema is passed to this constructor, an
  // assertion will be fired!
  Schema(const vector<ColumnSchema>& cols,
         const vector<size_t>& ids,
         int key_columns) {
    CHECK_OK(Reset(cols, ids, key_columns));
  }

  // Reset this Schema object to the given schema.
  // If this fails, the Schema object is left in an inconsistent
  // state and may not be used.
  Status Reset(const vector<ColumnSchema>& cols, int key_columns) {
    std::vector<size_t> ids;
    return Reset(cols, ids, key_columns);
  }

  // Reset this Schema object to the given schema.
  // If this fails, the Schema object is left in an inconsistent
  // state and may not be used.
  Status Reset(const vector<ColumnSchema>& cols,
               const vector<size_t>& ids,
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

  // Return the ColumnSchema corresponding to the given column ID.
  inline const ColumnSchema& column_by_id(size_t id) const {
    DCHECK_LT(id, cols_.size());
    return cols_[find_column_by_id(id)];
  }

  // Return the column ID corresponding to the given column index
  size_t column_id(size_t idx) const {
    DCHECK(has_column_ids());
    DCHECK_LT(idx, cols_.size());
    return col_ids_[idx];
  }

  // Return true if the schema has the IDs mapping
  bool has_column_ids() const {
    return !col_ids_.empty();
  }

  const std::vector<ColumnSchema>& columns() const {
    return cols_;
  }

  // Return the column index corresponding to the given column,
  // or -1 if the column is not in this schema.
  int find_column(const StringPiece col_name) const {
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

    for (size_t col_idx = 0; col_idx < num_columns(); col_idx++) {
      if (col_idx > 0) {
        ret.append(", ");
      }
      const ColumnSchema& col = cols_[col_idx];
      col.DebugCellAppend(row.cell(col_idx), &ret);
    }
    ret.append(")");
    return ret;
  }

  // Compare two rows of this schema.
  template<class RowTypeA, class RowTypeB>
  int Compare(const RowTypeA& lhs, const RowTypeB& rhs) const {
    DCHECK(KeyEquals(lhs.schema()) && KeyEquals(rhs.schema()));

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

  // Create a new Schema which only includes columns specified by 'col_indexes'
  // (which must be sorted, but do not need to be contiguous) and write it into
  // 'out'; mapping between old and new column indexes is written to 'old_to_new'.
  //
  // E.g., if the schema is {("a", UINT32), ("b", UINT32), ("c", UINT32)}
  // and 'col_indexes' is [0, 2] then 'out' will be set to
  // {("a", UINT32), ("c", UINT32)} and 'old_to_new' will be
  // { 0 => 0, 2 => 1}
  // See cfile_set-test.cc (TestSparseProjection) and
  // major_delta_compaction-test.cc (TestRowSetColumnUpdater) for example use.
  Status CreatePartialSchema(const std::vector<size_t>& col_indexes,
                             std::tr1::unordered_map<size_t, size_t>* old_to_new,
                             Schema* out) const;

  // Encode the key portion of the given row into a buffer
  // such that the buffer's lexicographic comparison represents
  // the proper comparison order of the underlying types.
  //
  // The key is encoded into the given buffer, replacing its current
  // contents.
  // Returns the encoded key.
  template <class RowType>
  Slice EncodeComparableKey(const RowType& row, faststring *dst) const {
    DCHECK_KEY_PROJECTION_SCHEMA_EQ(*this, row.schema());

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

    const bool have_column_ids = other.has_column_ids() && has_column_ids();
    for (size_t i = 0; i < other.cols_.size(); i++) {
      if (!this->cols_[i].Equals(other.cols_[i], have_column_ids)) return false;
    }

    return true;
  }

  // Return true if the key projection schemas have exactly the same set of
  // columns and respective types.
  bool KeyEquals(const Schema& other) const {
    if (this->num_key_columns_ != other.num_key_columns_) return false;
    for (size_t i = 0; i < this->num_key_columns_; i++) {
      if (!this->cols_[i].Equals(other.cols_[i], false)) return false;
    }
    return true;
  }

  // Return a non-OK status if the project is not compatible with the current schema
  // - User columns non present in the tablet are considered errors
  // - Matching columns with different types, at the moment, are considered errors
  Status VerifyProjectionCompatibility(const Schema& projection) const;

  // Returns the projection schema mapped on the current one
  // If the project is invalid, return a non-OK status.
  Status GetMappedReadProjection(const Schema& projection,
                                 Schema *mapped_projection) const;

  // Loops through this schema (the projection) and calls the projector methods once for
  // each column.
  //
  // - Status ProjectBaseColumn(size_t proj_col_idx, size_t base_col_idx)
  //
  //     Called if the column in this schema matches one of the columns in 'base_schema'.
  //     The column type must match exactly.
  //
  // - Status ProjectDefaultColumn(size_t proj_idx)
  //
  //     Called if the column in this schema does not match any column in 'base_schema',
  //     but has a default or is nullable. If the column in this schema has no default or is
  //     not nullable, InvalidArgument will be returned.
  //
  // If both schemas have column IDs, then the matching is done by ID. Otherwise, it is
  // done by name.
  //
  // TODO(MAYBE): Pass the ColumnSchema and not only the column index?
  template <class Projector>
  Status GetProjectionMapping(const Schema& base_schema, Projector *projector) const {
    const bool use_column_ids = base_schema.has_column_ids() && has_column_ids();

    int proj_idx = 0;
    for (int i = 0; i < cols_.size(); ++i) {
      const ColumnSchema& col_schema = cols_[i];

      // try to lookup the column by ID if present or just by name.
      // Unit tests and Iter-Projections are probably always using the
      // lookup by name. The IDs are generally set by the server on AlterTable().
      int base_idx;
      if (use_column_ids) {
        base_idx = base_schema.find_column_by_id(col_ids_[i]);
      } else {
        base_idx = base_schema.find_column(col_schema.name());
      }

      if (base_idx >= 0) {
        const ColumnSchema& base_col_schema = base_schema.column(base_idx);
        // Column present in the Base Schema...
        if (!col_schema.EqualsType(base_col_schema)) {
          // ...but with a different type, (TODO: try with an adaptor)
          return Status::InvalidArgument("The column '" + col_schema.name() +
                                         "' must have type " +
                                         base_col_schema.TypeToString() +
                                         " found " + col_schema.TypeToString());
        } else {
          RETURN_NOT_OK(projector->ProjectBaseColumn(proj_idx, base_idx));
        }
      } else {
        bool has_default = col_schema.has_read_default() || col_schema.has_write_default();
        if (!has_default && !col_schema.is_nullable()) {
          return Status::InvalidArgument("The column '" + col_schema.name() +
                "' does not exist in the projection, and it does not have a "
                "default value or a nullable type");
        }

        // Column missing from the Base Schema, use the default value of the projection
        RETURN_NOT_OK(projector->ProjectDefaultColumn(proj_idx));
      }
      proj_idx++;
    }
    return Status::OK();
  }

  // Returns the column index given the column ID
  int find_column_by_id(size_t id) const {
    if (has_column_ids()) {
      IdToIndexMap::const_iterator iter = id_to_index_.find(id);
      if (PREDICT_FALSE(iter == id_to_index_.end())) {
        return -1;
      }
      return iter->second;
    }
    return id;
  }

 private:
  friend class SchemaBuilder;

  vector<ColumnSchema> cols_;
  size_t num_key_columns_;
  vector<size_t> col_ids_;
  vector<size_t> col_offsets_;

  // The keys of this map are StringPiece references to the actual name members of the
  // ColumnSchema objects inside cols_. This avoids an extra copy of those strings,
  // and also allows us to do lookups on the map using StringPiece keys, sometimes
  // avoiding copies.
  typedef unordered_map<StringPiece, size_t, __gnu_cxx::hash<StringPiece> > NameToIndexMap;
  NameToIndexMap name_to_index_;

  typedef unordered_map<size_t, size_t> IdToIndexMap;
  IdToIndexMap id_to_index_;

  // NOTE: if you add more members, make sure to add the appropriate
  // code to swap() and CopyFrom() as well to prevent subtle bugs.
};

// Helper used for schema creation/editing.
//
// Example:
//   Status s;
//   SchemaBuilder builder(base_schema);
//   s = builder.RemoveColumn("value");
//   s = builder.AddKeyColumn("key2", STRING);
//   s = builder.AddColumn("new_c1", UINT32);
//   ...
//   Schema new_schema = builder.Build();
class SchemaBuilder {
 public:
  explicit SchemaBuilder() { Reset(); }
  explicit SchemaBuilder(const Schema& schema) { Reset(schema); }

  void Reset();
  void Reset(const Schema& schema);

  bool is_valid() const { return cols_.size() > 0; }

  Schema Build() const { return Schema(cols_, col_ids_, num_key_columns_); }
  Schema BuildWithoutIds() const { return Schema(cols_, num_key_columns_); }

  Status AddKeyColumn(const string& name, DataType type);

  Status AddColumn(const string& name, DataType type) {
    return AddColumn(name, type, false, NULL, NULL);
  }

  Status AddNullableColumn(const string& name, DataType type) {
    return AddColumn(name, type, true, NULL, NULL);
  }

  Status AddColumn(const string& name,
                   DataType type,
                   bool is_nullable,
                   const void *read_default,
                   const void *write_default);

  Status RemoveColumn(const string& name);
  Status RenameColumn(const string& old_name, const string& new_name);

 private:
  Status AddColumn(const ColumnSchema& column, bool is_key);

 private:
  DISALLOW_COPY_AND_ASSIGN(SchemaBuilder);

  uint64_t next_id_;
  vector<size_t> col_ids_;
  vector<ColumnSchema> cols_;
  unordered_set<string> col_names_;
  size_t num_key_columns_;
};

} // namespace kudu

#endif
