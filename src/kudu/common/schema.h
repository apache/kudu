// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <sparsehash/dense_hash_map>

#include "kudu/common/common.pb.h"
#include "kudu/common/id_mapping.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;
}  // namespace kudu

// Check that two schemas are equal, yielding a useful error message in the case that
// they are not.
#define DCHECK_SCHEMA_EQ(s1, s2) \
  do { \
    DCHECK((s1).Equals((s2))) << "Schema " << (s1).ToString() \
                              << " does not match " << (s2).ToString(); \
  } while (0)

#define DCHECK_KEY_PROJECTION_SCHEMA_EQ(s1, s2) \
  do { \
    DCHECK((s1).KeyEquals((s2))) << "Key-Projection Schema " \
                                 << (s1).ToString() << " does not match " \
                                 << (s2).ToString(); \
  } while (0)

template <class X> struct GoodFastHash;

namespace kudu {

class Arena;

// The ID of a column. Each column in a table has a unique ID.
struct ColumnId {
  explicit ColumnId(int32_t t) : t_(t) {}
  ColumnId() = default;
  ColumnId(const ColumnId& o) = default;

  ColumnId& operator=(const ColumnId& rhs) { t_ = rhs.t_; return *this; }
  ColumnId& operator=(const int32_t& rhs) { t_ = rhs; return *this; }
  operator const int32_t() const { return t_; } // NOLINT
  operator const strings::internal::SubstituteArg() const { return t_; } // NOLINT
  operator const AlphaNum() const { return t_; } // NOLINT
  bool operator==(const ColumnId& rhs) const { return t_ == rhs.t_; }
  bool operator<(const ColumnId& rhs) const { return t_ < rhs.t_; }
  friend std::ostream& operator<<(std::ostream& os, ColumnId column_id) {
    return os << column_id.t_;
  }
 private:
  int32_t t_;
};

// Class for storing column attributes such as precision and scale
// for decimal types. Column attributes describe logical features of
// the column; these features are usually relative to the column's type.
struct ColumnTypeAttributes {
 public:
  ColumnTypeAttributes()
      : precision(0),
        scale(0),
        length(0) {
  }

  ColumnTypeAttributes(int8_t precision, int8_t scale)
      : precision(precision),
        scale(scale),
        length(0) {
  }

  explicit ColumnTypeAttributes(uint16_t length)
      : precision(0),
        scale(0),
        length(length) {
  }

  // Does `other` represent equivalent attributes for `type`?
  // For example, if type == DECIMAL64 then attributes are equivalent iff
  // they have the same precision and scale.
  bool EqualsForType(ColumnTypeAttributes other, DataType type) const;

  // Return a string representation appropriate for `type`
  // This is meant to be postfixed to the name of a primitive type to describe
  // the full type, e.g. decimal(10, 4)
  std::string ToStringForType(DataType type) const;

  int8_t precision;
  int8_t scale;

  // Maximum value of the length is 65,535 for compatibility reasons as it's
  // used by VARCHAR type which can be set to a maximum of 65,535 in case of
  // MySQL and less for other major RDBMS implementations. The length refers to
  // the number of characters/symbols (not bytes).
  uint16_t length;
};

// Class for storing column attributes such as compression and
// encoding.  Column attributes describe the physical storage and
// representation of bytes, as opposed to a purely logical description
// normally associated with the term Schema.
//
// Column attributes are presently specified in the ColumnSchema
// protobuf message, but this should ideally be separate.
struct ColumnStorageAttributes {
 public:
  ColumnStorageAttributes()
    : encoding(AUTO_ENCODING),
      compression(DEFAULT_COMPRESSION),
      cfile_block_size(0) {
  }

  ColumnStorageAttributes(EncodingType enc, CompressionType cmp)
    : encoding(enc),
      compression(cmp),
      cfile_block_size(0) {
  }

  std::string ToString() const;

  EncodingType encoding;
  CompressionType compression;

  // The preferred block size for cfile blocks. If 0, uses the
  // server-wide default.
  int32_t cfile_block_size;
};

// A struct representing changes to a ColumnSchema.
//
// In the future, as more complex alter operations need to be supported,
// this may evolve into a class, but for now it's just POD.
struct ColumnSchemaDelta {
public:
  explicit ColumnSchemaDelta(std::string name)
      : name(std::move(name)),
        remove_default(false) {
  }

  const std::string name;

  boost::optional<std::string> new_name;

  // NB: these properties of a column cannot be changed yet,
  // ergo type and nullable should always be empty.
  // TODO(wdberkeley) allow changing of type and nullability
  boost::optional<DataType> type;

  boost::optional<bool> nullable;

  boost::optional<Slice> default_value;

  bool remove_default;

  boost::optional<EncodingType> encoding;
  boost::optional<CompressionType> compression;
  boost::optional<int32_t> cfile_block_size;

  boost::optional<std::string> new_comment;
};

// The schema for a given column.
//
// Holds the data type as well as information about nullability & column name.
// In the future, it may hold information about annotations, etc.
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
  // comment: the comment for the column.
  //
  // Example:
  //   ColumnSchema col_a("a", UINT32)
  //   ColumnSchema col_b("b", STRING, true);
  //   uint32_t default_i32 = -15;
  //   ColumnSchema col_c("c", INT32, false, &default_i32);
  //   Slice default_str("Hello");
  //   ColumnSchema col_d("d", STRING, false, &default_str);
  ColumnSchema(std::string name,
               DataType type,
               bool is_nullable = false,
               const void* read_default = nullptr,
               const void* write_default = nullptr,
               ColumnStorageAttributes attributes = ColumnStorageAttributes(),
               ColumnTypeAttributes type_attributes = ColumnTypeAttributes(),
               std::string comment = "")
      : name_(std::move(name)),
        type_info_(GetTypeInfo(type)),
        is_nullable_(is_nullable),
        read_default_(read_default ? std::make_shared<Variant>(type, read_default) : nullptr),
        attributes_(attributes),
        type_attributes_(type_attributes),
        comment_(std::move(comment)) {
    if (write_default == read_default) {
      write_default_ = read_default_;
    } else if (write_default != nullptr) {
      write_default_ = std::make_shared<Variant>(type, write_default);
    }
  }

  const TypeInfo* type_info() const {
    return type_info_;
  }

  bool is_nullable() const {
    return is_nullable_;
  }

  const std::string& name() const {
    return name_;
  }

  // Enum to configure how a ColumnSchema is stringified.
  enum class ToStringMode {
    // Include encoding type, compression type, and default read/write value.
    WITH_ATTRIBUTES,
    // Do not include above attributes.
    WITHOUT_ATTRIBUTES,
  };

  // Return a string identifying this column, including its
  // name.
  std::string ToString(ToStringMode mode = ToStringMode::WITHOUT_ATTRIBUTES) const;

  // Same as above, but only including the type information.
  // For example, "STRING NOT NULL".
  std::string TypeToString() const;

  // Same as above, but only including the attributes information.
  // For example, "AUTO_ENCODING ZLIB 123 123".
  std::string AttrToString() const;

  const std::string& comment() const {
    return comment_;
  }

  // Returns true if the column has a read default value
  bool has_read_default() const {
    return read_default_ != nullptr;
  }

  // Returns a pointer the default value associated with the column
  // or nullptr if there is no default value. You may check has_read_default() first.
  // The returned value will be valid until the ColumnSchema will be destroyed.
  //
  // Example:
  //    const uint32_t* vu32 = static_cast<const uint32_t*>(col_schema.read_default_value());
  //    const Slice* vstr = static_cast<const Slice*>(col_schema.read_default_value());
  const void* read_default_value() const {
    if (read_default_ != nullptr) {
      return read_default_->value();
    }
    return nullptr;
  }

  // Returns true if the column has a write default value
  bool has_write_default() const {
    return write_default_ != nullptr;
  }

  // Returns a pointer the default value associated with the column
  // or nullptr if there is no default value. You may check has_write_default() first.
  // The returned value will be valid until the ColumnSchema will be destroyed.
  //
  // Example:
  //    const uint32_t* vu32 = static_cast<const uint32_t*>(col_schema.write_default_value());
  //    const Slice* vstr = static_cast<const Slice*>(col_schema.write_default_value());
  const void* write_default_value() const {
    if (write_default_ != nullptr) {
      return write_default_->value();
    }
    return nullptr;
  }

  bool EqualsPhysicalType(const ColumnSchema& other) const {
    if (this == &other) return true;
    return is_nullable_ == other.is_nullable_ &&
           type_info()->physical_type() == other.type_info()->physical_type();
  }

  bool EqualsType(const ColumnSchema& other) const {
    if (this == &other) return true;
    return is_nullable_ == other.is_nullable_ &&
           type_info()->type() == other.type_info()->type() &&
           type_attributes().EqualsForType(other.type_attributes(), type_info()->type());
  }

  // compare types in Equals function
  enum CompareFlags {
    COMPARE_NAME = 1 << 0,
    COMPARE_TYPE = 1 << 1,
    COMPARE_OTHER = 1 << 2,

    COMPARE_NAME_AND_TYPE = COMPARE_NAME | COMPARE_TYPE,
    COMPARE_ALL = COMPARE_NAME | COMPARE_TYPE | COMPARE_OTHER
  };

  bool Equals(const ColumnSchema& other,
              CompareFlags flags = COMPARE_ALL) const {
    if (this == &other) return true;

    if ((flags & COMPARE_NAME) && this->name_ != other.name_)
      return false;

    if ((flags & COMPARE_TYPE) && !EqualsType(other))
      return false;

    // For Key comparison checking the defaults doesn't make sense,
    // since we don't support them, for server vs user schema this comparison
    // will always fail, since the user does not specify the defaults.
    if (flags & COMPARE_OTHER) {
      if (read_default_ == nullptr && other.read_default_ != nullptr)
        return false;

      if (write_default_ == nullptr && other.write_default_ != nullptr)
        return false;

      if (read_default_ != nullptr && !read_default_->Equals(other.read_default_.get()))
        return false;

      if (write_default_ != nullptr && !write_default_->Equals(other.write_default_.get()))
        return false;

      if (comment_ != other.comment_) {
        return false;
      }
    }
    return true;
  }

  // Apply a ColumnSchemaDelta to this column schema, altering it according to
  // the changes in the delta.
  // The original column schema is changed only if the method returns OK.
  Status ApplyDelta(const ColumnSchemaDelta& col_delta);

  // Returns extended attributes (such as encoding, compression, etc...)
  // associated with the column schema. The reason they are kept in a separate
  // struct is so that in the future, they may be moved out to a more
  // appropriate location as opposed to parts of ColumnSchema.
  const ColumnStorageAttributes& attributes() const {
    return attributes_;
  }

  const ColumnTypeAttributes& type_attributes() const {
    return type_attributes_;
  }

  int Compare(const void* lhs, const void* rhs) const {
    return type_info_->Compare(lhs, rhs);
  }

  // Stringify the given cell. This just stringifies the cell contents,
  // and doesn't include the column name or type.
  std::string Stringify(const void* cell) const {
    std::string ret;
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

  // Returns the memory usage of this object without the object itself. Should
  // be used when embedded inside another object.
  size_t memory_footprint_excluding_this() const;

  // Returns the memory usage of this object including the object itself.
  // Should be used when allocated on the heap.
  size_t memory_footprint_including_this() const;

 private:
  friend class SchemaBuilder;

  void set_name(const std::string& name) {
    name_ = name;
  }

  std::string name_;
  const TypeInfo* type_info_;
  bool is_nullable_;
  // use shared_ptr since the ColumnSchema is always copied around.
  std::shared_ptr<Variant> read_default_;
  std::shared_ptr<Variant> write_default_;
  ColumnStorageAttributes attributes_;
  ColumnTypeAttributes type_attributes_;
  std::string comment_;
};

// The schema for a set of rows.
//
// A Schema is simply a set of columns, along with information about
// which prefix of columns makes up the primary key.
//
// Note that, while Schema is copyable and assignable, it is a complex
// object that is not inexpensive to copy. Prefer move semantics when
// possible, or pass by pointer or reference. Functions that create new
// Schemas should generally prefer taking a Schema pointer and using
// Schema::Reset() rather than returning by value.
class Schema {
 public:
  static constexpr int kColumnNotFound = -1;

  Schema()
    : num_key_columns_(0),
      // Initialize for 1 expected element since 0 gets replaced with
      // the default (32).
      name_to_index_(1),
      first_is_deleted_virtual_column_idx_(kColumnNotFound),
      has_nullables_(false) {
    name_to_index_.set_empty_key(StringPiece());
  }

  Schema(const Schema& other);
  Schema& operator=(const Schema& other);

  Schema(Schema&& other) noexcept;
  Schema& operator=(Schema&& other) noexcept;

  void CopyFrom(const Schema& other);

  // Construct a schema with the given information.
  //
  // NOTE: if the schema is user-provided, it's better to construct an
  // empty schema and then use Reset(...)  so that errors can be
  // caught. If an invalid schema is passed to this constructor, an
  // assertion will be fired!
  Schema(std::vector<ColumnSchema> cols,
         int key_columns)
      : name_to_index_(/*expected_max_items_in_table=*/cols.size()) {
    name_to_index_.set_empty_key(StringPiece());
    CHECK_OK(Reset(std::move(cols), key_columns));
  }

  // Construct a schema with the given information.
  //
  // NOTE: if the schema is user-provided, it's better to construct an
  // empty schema and then use Reset(...)  so that errors can be
  // caught. If an invalid schema is passed to this constructor, an
  // assertion will be fired!
  Schema(std::vector<ColumnSchema> cols,
         std::vector<ColumnId> ids,
         int key_columns)
      : name_to_index_(/*expected_max_items_in_table=*/cols.size()) {
    name_to_index_.set_empty_key(StringPiece());
    CHECK_OK(Reset(std::move(cols), std::move(ids), key_columns));
  }

  // Reset this Schema object to the given schema.
  // If this fails, the Schema object is left in an inconsistent
  // state and may not be used.
  Status Reset(std::vector<ColumnSchema> cols, int key_columns) {
    std::vector<ColumnId> ids;
    return Reset(std::move(cols), ids, key_columns);
  }

  // Reset this Schema object to the given schema.
  // If this fails, the Schema object is left in an inconsistent
  // state and may not be used.
  Status Reset(std::vector<ColumnSchema> cols,
               std::vector<ColumnId> ids,
               int key_columns);

  // Find the column index corresponding to the given column name,
  // return a bad Status if not found.
  Status FindColumn(Slice col_name, int* idx) const;

  // Return the number of bytes needed to represent a single row of this schema, without
  // accounting for the null bitmap if the Schema contains nullable values.
  //
  // This size does not include any indirected (variable length) data (eg strings).
  size_t byte_size() const {
    DCHECK(initialized());
    return col_offsets_.back();
  }

  // Return the number of bytes needed to represent
  // only the key portion of this schema.
  size_t key_byte_size() const {
    return col_offsets_[num_key_columns_];
  }

  // Return the number of columns in this schema
  size_t num_columns() const {
    // Use name_to_index_.size() instead of cols_.size() since the former
    // just reads a member variable of the unordered_map whereas the
    // latter involves division by sizeof(ColumnSchema). Even though
    // division-by-a-constant gets optimized into multiplication,
    // the multiplication instruction has a significantly higher latency
    // than the simple load.
    return name_to_index_.size();
  }

  // Return the length of the key prefix in this schema.
  size_t num_key_columns() const {
    return num_key_columns_;
  }

  // Return the byte offset within the row for the given column index.
  size_t column_offset(size_t col_idx) const {
    DCHECK_LT(col_idx, cols_.size());
    return col_offsets_[col_idx];
  }

  // Return the ColumnSchema corresponding to the given column index.
  const ColumnSchema& column(size_t idx) const {
    DCHECK_LT(idx, cols_.size());
    return cols_[idx];
  }

  // Return the ColumnSchema corresponding to the given column ID.
  const ColumnSchema& column_by_id(ColumnId id) const {
    int idx = find_column_by_id(id);
    DCHECK_GE(idx, 0);
    return cols_[idx];
  }

  // Return the column ID corresponding to the given column index.
  ColumnId column_id(size_t idx) const {
    DCHECK(has_column_ids());
    DCHECK_LT(idx, cols_.size());
    return col_ids_[idx];
  }

  // Return the column IDs, ordered by column index.
  const std::vector<ColumnId>& column_ids() const {
    return col_ids_;
  }

  // Return true if the schema contains an ID mapping for its columns.
  // In the case of an empty schema, this is false.
  bool has_column_ids() const {
    return !col_ids_.empty();
  }

  const std::vector<ColumnSchema>& columns() const {
    return cols_;
  }

  // Return the column index corresponding to the given column,
  // or kColumnNotFound if the column is not in this schema.
  int find_column(const StringPiece col_name) const {
    auto iter = name_to_index_.find(col_name);
    if (PREDICT_FALSE(iter == name_to_index_.end())) {
      return kColumnNotFound;
    } else {
      return (*iter).second;
    }
  }

  // Returns true if the schema contains nullable columns
  bool has_nullables() const {
    return has_nullables_;
  }

  // Returns true if the specified column (by name) is a key
  bool is_key_column(const StringPiece col_name) const {
    return is_key_column(find_column(col_name));
  }

  // Returns true if the specified column (by index) is a key
  bool is_key_column(size_t idx) const {
    return idx < num_key_columns_;
  }

  // Returns the list of primary key column IDs.
  std::vector<ColumnId> get_key_column_ids() const {
    return std::vector<ColumnId>(
        col_ids_.begin(), col_ids_.begin() + num_key_columns_);
  }

  // Return true if this Schema is initialized and valid.
  bool initialized() const {
    return !col_offsets_.empty();
  }

  // Returns the highest column id in this Schema.
  ColumnId max_col_id() const {
    return max_col_id_;
  }

  // Extract a given column from a row where the type is
  // known at compile-time. The type is checked with a debug
  // assertion -- but if the wrong type is used and these assertions
  // are off, incorrect data may result.
  //
  // This is mostly useful for tests at this point.
  template<DataType Type, class RowType>
  const typename DataTypeTraits<Type>::cpp_type*
  ExtractColumnFromRow(const RowType& row, size_t idx) const {
    DCHECK_SCHEMA_EQ(*this, *row.schema());
    const ColumnSchema& col_schema = cols_[idx];
    DCHECK_LT(idx, cols_.size());
    DCHECK_EQ(col_schema.type_info()->type(), Type);

    const void* val;
    if (col_schema.is_nullable()) {
      val = row.nullable_cell_ptr(idx);
    } else {
      val = row.cell_ptr(idx);
    }

    return reinterpret_cast<const typename DataTypeTraits<Type>::cpp_type*>(val);
  }

  // Stringify the given row, which conforms to this schema,
  // in a way suitable for debugging. This isn't currently optimized
  // so should be avoided in hot paths.
  template<class RowType>
  std::string DebugRow(const RowType& row) const {
    DCHECK_SCHEMA_EQ(*this, *row.schema());
    return DebugRowColumns(row, num_columns());
  }

  // Stringify the given row, which must have a schema which is
  // key-compatible with this one. Per above, this is not for use in
  // hot paths.
  template<class RowType>
  std::string DebugRowKey(const RowType& row) const {
    DCHECK_KEY_PROJECTION_SCHEMA_EQ(*this, *row.schema());
    return DebugRowColumns(row, num_key_columns());
  }

  // Decode the specified encoded key into the given 'row', which
  // must be at least as large as this->key_byte_size().
  //
  // 'arena' is used for allocating indirect strings, but is unused
  // for other datatypes.
  template<class RowType>
  Status DecodeRowKey(Slice encoded_key, RowType* row,
                      Arena* arena) const WARN_UNUSED_RESULT;

  // Decode and stringify the given contiguous encoded row key in
  // order to, e.g., provide print human-readable information about a
  // tablet's start and end keys.
  //
  // If the encoded key is empty then '<start of table>' or '<end of table>'
  // will be returned based on the value of 'start_or_end'.
  //
  // See also: DebugRowKey, DecodeRowKey.
  enum StartOrEnd {
    START_KEY,
    END_KEY
  };
  std::string DebugEncodedRowKey(Slice encoded_key, StartOrEnd start_or_end) const;

  // Compare two rows of this schema.
  template<class RowTypeA, class RowTypeB>
  int Compare(const RowTypeA& lhs, const RowTypeB& rhs) const {
    DCHECK(KeyEquals(*lhs.schema()) && KeyEquals(*rhs.schema()));

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
    std::vector<ColumnSchema> key_cols(cols_.begin(),
                                  cols_.begin() + num_key_columns_);
    std::vector<ColumnId> col_ids;
    if (!col_ids_.empty()) {
      col_ids.assign(col_ids_.begin(), col_ids_.begin() + num_key_columns_);
    }

    return Schema(std::move(key_cols), std::move(col_ids), num_key_columns_);
  }

  // Return a new Schema which is the same as this one, but with IDs assigned.
  // Requires that this schema has no column IDs.
  Schema CopyWithColumnIds() const;

  // Return a new Schema which is the same as this one, but without any column
  // IDs assigned.
  Schema CopyWithoutColumnIds() const;

  // Create a new schema containing only the selected columns.
  // The resulting schema will have no key columns defined.
  // If this schema has IDs, the resulting schema will as well.
  Status CreateProjectionByNames(const std::vector<StringPiece>& col_names,
                                 Schema* out) const;

  // Create a new schema containing only the selected column IDs.
  //
  // If any column IDs are invalid, then they will be ignored and the
  // result will have fewer columns than requested.
  //
  // The resulting schema will have no key columns defined.
  Status CreateProjectionByIdsIgnoreMissing(const std::vector<ColumnId>& col_ids,
                                            Schema* out) const;

  // Encode the key portion of the given row into a buffer
  // such that the buffer's lexicographic comparison represents
  // the proper comparison order of the underlying types.
  //
  // The key is encoded into the given buffer, replacing its current
  // contents.
  // Returns the encoded key.
  template <class RowType>
  Slice EncodeComparableKey(const RowType& row, faststring* dst) const {
    DCHECK_KEY_PROJECTION_SCHEMA_EQ(*this, *row.schema());

    dst->clear();
    for (size_t i = 0; i < num_key_columns_; i++) {
      DCHECK(!cols_[i].is_nullable());
      const TypeInfo* ti = cols_[i].type_info();
      bool is_last = i == num_key_columns_ - 1;
      GetKeyEncoder<faststring>(ti).Encode(row.cell_ptr(i), is_last, dst);
    }
    return Slice(*dst);
  }

  // Enum to configure how a Schema is stringified.
  enum ToStringMode {
    BASE_INFO = 0,
    // Include column ids if this instance has them.
    WITH_COLUMN_IDS = 1 << 0,
    // Include column attributes.
    WITH_COLUMN_ATTRIBUTES = 1 << 1,
  };
  // Stringify this Schema. This is not particularly efficient,
  // so should only be used when necessary for output.
  std::string ToString(ToStringMode mode = ToStringMode::WITH_COLUMN_IDS) const;

  // Compare column ids in Equals() method.
  enum SchemaComparisonType {
    COMPARE_COLUMNS = 1 << 0,
    COMPARE_COLUMN_IDS = 1 << 1,

    COMPARE_ALL = COMPARE_COLUMNS | COMPARE_COLUMN_IDS
  };

  // Return true if the schemas have exactly the same set of columns
  // and respective types.
  bool Equals(const Schema& other, SchemaComparisonType flags = COMPARE_COLUMNS) const {
    if (this == &other) return true;

    if (flags & COMPARE_COLUMNS) {
      if (this->num_key_columns_ != other.num_key_columns_) return false;
      if (this->num_columns() != other.num_columns()) return false;
      for (size_t i = 0; i < other.num_columns(); i++) {
        if (!this->cols_[i].Equals(other.cols_[i])) return false;
      }
    }

    if (flags & COMPARE_COLUMN_IDS) {
      if (this->has_column_ids() != other.has_column_ids()) return false;
      if (this->has_column_ids()) {
        if (this->col_ids_ != other.col_ids_) return false;
        if (this->max_col_id() != other.max_col_id()) return false;
      }
    }

    return true;
  }

  bool operator==(const Schema& other) const {
    return this->Equals(other);
  }

  bool operator!=(const Schema& other) const {
    return !(*this == other);
  }

  // Return true if the key projection schemas have exactly the same set of
  // columns and respective types.
  bool KeyEquals(const Schema& other,
                 ColumnSchema::CompareFlags flags = ColumnSchema::COMPARE_NAME_AND_TYPE) const {
    if (this == &other) return true;
    if (this->num_key_columns_ != other.num_key_columns_) return false;
    for (size_t i = 0; i < this->num_key_columns_; i++) {
      if (!this->cols_[i].Equals(other.cols_[i], flags)) return false;
    }
    return true;
  }

  // Return true if the key projection schemas have exactly the same set of
  // columns and respective types except name field.
  bool KeyTypeEquals(const Schema& other) const {
    return KeyEquals(other, ColumnSchema::COMPARE_TYPE);
  }

  // Return a non-OK status if the project is not compatible with the current schema
  // - User columns non present in the tablet are considered errors
  // - Matching columns with different types, at the moment, are considered errors
  Status VerifyProjectionCompatibility(const Schema& projection) const;

  // Returns the projection schema mapped on the current one
  // If the project is invalid, return a non-OK status.
  Status GetMappedReadProjection(const Schema& projection,
                                 Schema* mapped_projection) const;

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
  //     but has a default or is nullable.
  //
  // - Status ProjectExtraColumn(size_t proj_idx, const ColumnSchema& col)
  //
  //     Called if the column in this schema does not match any column in 'base_schema',
  //     and does not have a default, and is not nullable.
  //
  // If both schemas have column IDs, then the matching is done by ID. Otherwise, it is
  // done by name.
  //
  // TODO(MAYBE): Pass the ColumnSchema and not only the column index?
  template <class Projector>
  Status GetProjectionMapping(const Schema& base_schema, Projector* projector) const {
    const bool use_column_ids = base_schema.has_column_ids() && has_column_ids();

    int proj_idx = 0;
    for (int i = 0; i < num_columns(); ++i) {
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
        if (PREDICT_FALSE(!col_schema.EqualsType(base_col_schema))) {
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
          RETURN_NOT_OK(projector->ProjectExtraColumn(proj_idx));
        }

        // Column missing from the Base Schema, use the default value of the projection
        RETURN_NOT_OK(projector->ProjectDefaultColumn(proj_idx));
      }
      proj_idx++;
    }
    return Status::OK();
  }

  // Returns the column index given the column ID.
  // If no such column exists, returns kColumnNotFound.
  int find_column_by_id(ColumnId id) const {
    DCHECK(cols_.empty() || has_column_ids());
    int ret = id_to_index_[id];
    if (ret == IdMapping::kNoEntry) {
      return kColumnNotFound;
    }
    return ret;
  }

  // Returns the memory usage of this object without the object itself. Should
  // be used when embedded inside another object.
  size_t memory_footprint_excluding_this() const;

  // Returns the memory usage of this object including the object itself.
  // Should be used when allocated on the heap.
  size_t memory_footprint_including_this() const;

  // Returns the column index for the first IS_DELETED virtual column in the
  // schema, or kColumnNotFound if one cannot be found.
  int first_is_deleted_virtual_column_idx() const {
    return first_is_deleted_virtual_column_idx_;
  }

 private:
  // Return a stringified version of the first 'num_columns' columns of the
  // row.
  template<class RowType>
  std::string DebugRowColumns(const RowType& row, int num_columns) const {
    std::string ret;
    ret.append("(");

    for (size_t col_idx = 0; col_idx < num_columns; col_idx++) {
      if (col_idx > 0) {
        ret.append(", ");
      }
      const ColumnSchema& col = cols_[col_idx];
      col.DebugCellAppend(row.cell(col_idx), &ret);
    }
    ret.append(")");
    return ret;
  }

  friend class SchemaBuilder;

  std::vector<ColumnSchema> cols_;
  size_t num_key_columns_;
  std::vector<ColumnId> col_ids_;
  ColumnId max_col_id_;
  std::vector<size_t> col_offsets_;

  // The keys of this map are StringPiece references to the actual name members of the
  // ColumnSchema objects inside cols_. This avoids an extra copy of those strings,
  // and also allows us to do lookups on the map using StringPiece keys, sometimes
  // avoiding copies.
  typedef google::dense_hash_map<
      StringPiece,
      size_t,
      GoodFastHash<StringPiece>,
    std::equal_to<StringPiece>> NameToIndexMap;

  NameToIndexMap name_to_index_;

  IdMapping id_to_index_;

  // Cached index of the first IS_DELETED virtual column, or kColumnNotFound if
  // no such virtual column exists in the schema.
  int first_is_deleted_virtual_column_idx_;

  // Cached indicator whether any columns are nullable.
  bool has_nullables_;

  // NOTE: if you add more members, make sure to add the appropriate code to
  // CopyFrom() and the move constructor and assignment operator as well, to
  // prevent subtle bugs.
};

// Helper used for schema creation/editing.
//
// Example:
//   SchemaBuilder builder(base_schema);
//   RETURN_NOT_OK(builder.RemoveColumn("value"));
//   RETURN_NOT_OK(builder.AddKeyColumn("key2", STRING));
//   RETURN_NOT_OK(builder.AddColumn("new_c1", UINT32));
//   ...
//   Schema new_schema = builder.Build();
class SchemaBuilder {
 public:
  SchemaBuilder() { Reset(); }
  explicit SchemaBuilder(const Schema& schema) { Reset(schema); }

  void Reset();
  void Reset(const Schema& schema);

  bool is_valid() const { return !cols_.empty(); }

  // Set the next column ID to be assigned to columns added with
  // AddColumn.
  void set_next_column_id(ColumnId next_id) {
    DCHECK_GE(next_id, ColumnId(0));
    next_id_ = next_id;
  }

  // Return the next column ID that would be assigned with AddColumn.
  ColumnId next_column_id() const {
    return next_id_;
  }

  // Return true if the column named 'col_name' is a key column.
  // Return false if the column is not a key column, or if
  // the column is not in the builder.
  bool is_key_column(const StringPiece col_name) const {
    for (size_t i = 0; i < num_key_columns_; ++i) {
      if (cols_[i].name() == col_name) return true;
    }
    return false;
  }

  Schema Build() const { return Schema(cols_, col_ids_, num_key_columns_); }
  Schema BuildWithoutIds() const { return Schema(cols_, num_key_columns_); }

  Status AddKeyColumn(const std::string& name, DataType type);

  Status AddColumn(const ColumnSchema& column, bool is_key);

  Status AddColumn(const std::string& name, DataType type) {
    return AddColumn(name, type, false, nullptr, nullptr);
  }

  Status AddNullableColumn(const std::string& name, DataType type) {
    return AddColumn(name, type, true, nullptr, nullptr);
  }

  Status AddColumn(const std::string& name,
                   DataType type,
                   bool is_nullable,
                   const void* read_default,
                   const void* write_default);

  Status RemoveColumn(const std::string& name);

  Status RenameColumn(const std::string& old_name, const std::string& new_name);

  Status ApplyColumnSchemaDelta(const ColumnSchemaDelta& col_delta);

 private:
  DISALLOW_COPY_AND_ASSIGN(SchemaBuilder);

  ColumnId next_id_;
  std::vector<ColumnId> col_ids_;
  std::vector<ColumnSchema> cols_;
  std::unordered_set<std::string> col_names_;
  size_t num_key_columns_;
};

} // namespace kudu

// Specialize std::hash for ColumnId
namespace std {
template<>
struct hash<kudu::ColumnId> {
  int operator()(const kudu::ColumnId& col_id) const {
    return col_id;
  }
};
} // namespace std
