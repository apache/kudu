//
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
#ifndef KUDU_CLIENT_SCHEMA_H
#define KUDU_CLIENT_SCHEMA_H

#include <string>
#include <vector>

#include "kudu/client/value.h"
#include "kudu/util/kudu_export.h"

namespace kudu {

class ColumnSchema;
class KuduPartialRow;
class Schema;
class TestWorkload;

namespace tools {
class RemoteKsckMaster;
class ReplicaDumper;
}

namespace client {

namespace internal {
class GetTableSchemaRpc;
class LookupRpc;
class MetaCacheEntry;
class WriteRpc;
} // namespace internal

class KuduClient;
class KuduSchema;
class KuduSchemaBuilder;
class KuduWriteOperation;

/// @brief Representation of column storage attributes.
class KUDU_EXPORT KuduColumnStorageAttributes {
 public:
  /// @brief Column encoding types.
  enum EncodingType {
    AUTO_ENCODING = 0,
    PLAIN_ENCODING = 1,
    PREFIX_ENCODING = 2,
    RLE = 4,
    DICT_ENCODING = 5,
    BIT_SHUFFLE = 6,

    /// @deprecated GROUP_VARINT is not supported for valid types, and
    /// will fall back to another encoding on the server side.
    GROUP_VARINT = 3
  };

  /// @brief Column compression types.
  enum CompressionType {
    DEFAULT_COMPRESSION = 0,
    NO_COMPRESSION = 1,
    SNAPPY = 2,
    LZ4 = 3,
    ZLIB = 4,
  };


  /// @deprecated This constructor is deprecated for external use, and will
  ///   be made private in a future release.
  ///
  /// @todo Make this constructor private.
  ///
  /// @param [in] encoding
  ///   Encoding type for the column storage.
  /// @param [in] compression
  ///   Compression type for the column storage.
  /// @param [in] block_size
  ///   Block size (in bytes, uncompressed data) for the column storage.
  explicit KuduColumnStorageAttributes(
      EncodingType encoding = AUTO_ENCODING,
      CompressionType compression = DEFAULT_COMPRESSION,
      int32_t block_size = 0)
      ATTRIBUTE_DEPRECATED("this constructor will be private in a future release")
      : encoding_(encoding),
        compression_(compression),
        block_size_(block_size) {
  }

  /// @return Encoding type for the column storage.
  const EncodingType encoding() const {
    return encoding_;
  }

  /// @return Comporession type for the column storage.
  const CompressionType compression() const {
    return compression_;
  }

  /// @return String representation of the storage attributes.
  std::string ToString() const;

 private:
  EncodingType encoding_;
  CompressionType compression_;
  int32_t block_size_;
};

/// @brief Representation of the column schema.
class KUDU_EXPORT KuduColumnSchema {
 public:
  /// @brief Supported data types for columns.
  enum DataType {
    INT8 = 0,
    INT16 = 1,
    INT32 = 2,
    INT64 = 3,
    STRING = 4,
    BOOL = 5,
    FLOAT = 6,
    DOUBLE = 7,
    BINARY = 8,
    UNIXTIME_MICROS = 9,
    TIMESTAMP = UNIXTIME_MICROS //!< deprecated, use UNIXTIME_MICROS
  };

  /// @param [in] type
  ///   Column data type.
  /// @return String representation of the column data type.
  static std::string DataTypeToString(DataType type);

  /// @deprecated Use KuduSchemaBuilder instead.
  ///
  /// @todo KUDU-809: make this hard-to-use constructor private.
  ///   Clients should use the Builder API. Currently only the Python API
  ///   uses this old API.
  ///
  /// @param [in] name
  ///   The name of the column.
  /// @param [in] type
  ///   The type of the column.
  /// @param [in] is_nullable
  ///   Whether the column is nullable.
  /// @param [in] default_value
  ///   Default value for the column.
  /// @param [in] attributes
  ///   Column storage attributes.
  KuduColumnSchema(const std::string &name,
                   DataType type,
                   bool is_nullable = false,
                   const void* default_value = NULL,
                   KuduColumnStorageAttributes attributes = KuduColumnStorageAttributes())
      ATTRIBUTE_DEPRECATED("use KuduSchemaBuilder instead");

  /// Construct KuduColumnSchema object as a copy of another object.
  ///
  /// @param [in] other
  ///   The reference object to copy from.
  KuduColumnSchema(const KuduColumnSchema& other);
  ~KuduColumnSchema();

  /// The assignment operator.
  ///
  /// @param [in] other
  ///   The reference object to assign from.
  /// @return The updated object.
  KuduColumnSchema& operator=(const KuduColumnSchema& other);

  /// Make this object an identical copy of the other one.
  ///
  /// @param [in] other
  ///   The reference object to copy from.
  void CopyFrom(const KuduColumnSchema& other);

  /// Check whether the object is identical to the other one.
  ///
  /// @param [in] other
  ///   The reference object to compare with.
  /// @return @c true iff the object is identical to the specified one.
  bool Equals(const KuduColumnSchema& other) const;

  /// @name Getters to expose column schema information.
  ///
  /// @todo Expose default column value and attributes?
  ///
  ///@{
  /// @return Name of the column schema.
  const std::string& name() const;

  /// @return Type of the column schema.
  DataType type() const;

  /// @return @c true iff the column schema has the nullable attribute set.
  bool is_nullable() const;
  ///@}

 private:
  friend class KuduColumnSpec;
  friend class KuduSchema;
  friend class KuduSchemaBuilder;
  // KuduTableAlterer::Data needs to be a friend. Friending the parent class
  // is transitive to nested classes. See http://tiny.cloudera.com/jwtui
  friend class KuduTableAlterer;

  KuduColumnSchema();

  // Owned.
  ColumnSchema* col_;
};

/// @brief Builder API for specifying or altering a column
///   within a table schema.
///
/// An object of this type cannot be constructed directly, but rather
/// is returned from KuduSchemaBuilder::AddColumn() to specify a column
/// within a Schema.
///
/// @todo KUDU-861: this API will also be used for an improved AlterTable API.
class KUDU_EXPORT KuduColumnSpec {
 public:
  /// Set the default value for the column.
  ///
  /// When adding a new column to a table, this default value will be used to
  /// fill the new column in all existing rows. The default value
  /// will also be used when inserting a new row with no value for the column.
  ///
  /// @param [in] value
  ///   The value to use as the default. The KuduColumnSpec takes ownership
  ///   over the passed parameter.
  ///
  /// @return Pointer to the modified object.
  KuduColumnSpec* Default(KuduValue* value);

  /// Set the preferred compression type for the column.
  ///
  /// @param [in] compression
  ///   The compression type to use.
  /// @return Pointer to the modified object.
  KuduColumnSpec* Compression(KuduColumnStorageAttributes::CompressionType compression);

  /// Set the preferred encoding for the column.
  ///
  /// @note Not all encodings are supported for all column types.
  ///
  /// @param [in] encoding
  ///   The encoding to use.
  /// @return Pointer to the modified object.
  KuduColumnSpec* Encoding(KuduColumnStorageAttributes::EncodingType encoding);

  /// Set the target block size for the column.
  ///
  /// This is the number of bytes of user data packed per block on disk, and
  /// represents the unit of IO when reading the column. Larger values
  /// may improve scan performance, particularly on spinning media. Smaller
  /// values may improve random access performance, particularly for workloads
  /// that have high cache hit rates or operate on fast storage such as SSD.
  ///
  /// @note The block size specified here corresponds to uncompressed data.
  ///   The actual size of the unit read from disk may be smaller if
  ///   compression is enabled.
  ///
  /// @note It's recommended that this not be set any lower than 4096 (4KB)
  ///   or higher than 1048576 (1MB).
  /// @todo KUDU-1107: move above info to docs
  ///
  /// @param [in] block_size
  ///   Block size (in bytes) to use.
  /// @return Pointer to the modified object.
  KuduColumnSpec* BlockSize(int32_t block_size);

  /// @name Operations only relevant for Create Table
  ///
  ///@{
  /// Set the column to be the primary key of the table.
  ///
  /// This may only be used to set non-composite primary keys. If a composite
  /// key is desired, use KuduSchemaBuilder::SetPrimaryKey(). This may not be
  /// used in conjunction with KuduSchemaBuilder::SetPrimaryKey().
  ///
  /// @note Primary keys may not be changed after a table is created.
  ///
  /// @return Pointer to the modified object.
  KuduColumnSpec* PrimaryKey();

  /// Set the column to be not nullable.
  ///
  /// @note Column nullability may not be changed once a table is created.
  ///
  /// @return Pointer to the modified object.
  KuduColumnSpec* NotNull();

  /// Set the column to be nullable (the default).
  ///
  /// @note Column nullability may not be changed once a table is created.
  ///
  /// @return Pointer to the modified object.
  KuduColumnSpec* Nullable();

  /// Set the data type of the column.
  ///
  /// @note Column data types may not be changed once a table is created.
  ///
  /// @param [in] type
  ///   The data type to set.
  /// @return Pointer to the modified object.
  KuduColumnSpec* Type(KuduColumnSchema::DataType type);
  ///@}

  /// @name Operations only relevant for Alter Table
  ///
  ///@{
  /// Remove the default value for the column.
  ///
  /// Without a default, clients must always specify a value for the column
  /// when inserting data.
  ///
  /// @return Pointer to the modified object.
  KuduColumnSpec* RemoveDefault();

  /// Rename the column.
  ///
  /// @param [in] new_name
  ///   The new name for the column.
  /// @return Pointer to the modified object.
  KuduColumnSpec* RenameTo(const std::string& new_name);
  ///@}

 private:
  class KUDU_NO_EXPORT Data;
  friend class KuduSchemaBuilder;
  friend class KuduTableAlterer;

  // This class should always be owned and deleted by one of its friends,
  // not the user.
  ~KuduColumnSpec();

  explicit KuduColumnSpec(const std::string& col_name);

  Status ToColumnSchema(KuduColumnSchema* col) const;

  // Owned.
  Data* data_;
};

/// @brief Builder API for constructing a KuduSchema object.
///
/// The API here is a "fluent" style of programming, such that the resulting
/// code looks somewhat like a SQL "CREATE TABLE" statement. For example:
///
/// SQL:
/// @code
///   CREATE TABLE t (
///     my_key int not null primary key,
///     a float default 1.5
///   );
/// @endcode
///
/// is represented as:
/// @code
///   KuduSchemaBuilder t;
///   t.AddColumn("my_key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
///   t.AddColumn("a")->Type(KuduColumnSchema::FLOAT)->Default(KuduValue::FromFloat(1.5));
///   KuduSchema schema;
///   t.Build(&schema);
/// @endcode
class KUDU_EXPORT KuduSchemaBuilder {
 public:
  KuduSchemaBuilder();
  ~KuduSchemaBuilder();

  /// Add a column with the specified name to the schema.
  ///
  /// @param [in] name
  ///   Name of the column to add.
  /// @return A KuduColumnSpec object for a new column within the Schema.
  ///   The returned object is owned by the KuduSchemaBuilder.
  KuduColumnSpec* AddColumn(const std::string& name);

  /// Set the primary key of the new Schema based on the given column names.
  ///
  /// This may be used to specify a compound primary key.
  ///
  /// @param [in] key_col_names
  ///   Names of the columns to include into the compound primary key.
  /// @return Pointer to the modified object.
  KuduSchemaBuilder* SetPrimaryKey(const std::vector<std::string>& key_col_names);

  /// Build the schema based on current configuration of the builder object.
  ///
  /// @param [out] schema
  ///   The placeholder for the result schema. Upon successful completion,
  ///   the parameter is reset to the result of this builder: literally,
  ///   calling KuduSchema::Reset() on the parameter.
  /// @return Operation result status. If the resulting would-be-schema
  ///   is invalid for any reason (e.g. missing types, duplicate column names,
  ///   etc.) a bad Status is returned.
  Status Build(KuduSchema* schema);

 private:
  class KUDU_NO_EXPORT Data;
  // Owned.
  Data* data_;
};

/// @brief A representation of a table's schema.
class KUDU_EXPORT KuduSchema {
 public:
  KuduSchema();

  /// Create a KuduSchema object as a copy of the other one.
  ///
  /// @param [in] other
  ///   The other KuduSchema object to use as a reference.
  KuduSchema(const KuduSchema& other);
  ~KuduSchema();

  /// @name Assign/copy the schema
  ///
  /// @param [in] other
  ///   The source KuduSchema object to use as a reference.
  ///
  ///@{
  KuduSchema& operator=(const KuduSchema& other);
  void CopyFrom(const KuduSchema& other);
  ///@}

  /// @deprecated This method will be removed soon.
  ///
  /// @todo Remove KuduSchema::Reset().
  ///
  /// @param [in] columns
  ///   Per-column schema information.
  /// @param [in] key_columns
  ///   Number of key columns in the schema.
  /// @return Operation result status.
  Status Reset(const std::vector<KuduColumnSchema>& columns, int key_columns)
      ATTRIBUTE_DEPRECATED("this method will be removed in a future release")
      WARN_UNUSED_RESULT;

  /// Check whether the schema is identical to the other one.
  ///
  /// @param [in] other
  ///   The other KuduSchema object to compare with.
  /// @return @c true iff this KuduSchema object is identical
  ///   to the specified one.
  bool Equals(const KuduSchema& other) const;

  /// @param [in] idx
  ///   Column index.
  /// @return Schema for the specified column.
  KuduColumnSchema Column(size_t idx) const;

  /// @return The number of columns in the schema.
  size_t num_columns() const;

  /// Get the indexes of the primary key columns within this Schema.
  ///
  /// @attention In current versions of Kudu, these will always be contiguous
  ///   column indexes starting with 0. However, in future versions this
  ///   assumption may not hold, so callers should not assume it is the case.
  ///
  /// @param [out] indexes
  ///   The placeholder for the result.
  void GetPrimaryKeyColumnIndexes(std::vector<int>* indexes) const;

  /// Create a new row corresponding to this schema.
  ///
  /// @note The new row refers to this KuduSchema object, so it must be
  ///   destroyed before the KuduSchema object to avoid dangling pointers.
  ///
  /// @return A pointer to the newly created row. The caller takes ownership
  ///   of the created row.
  KuduPartialRow* NewRow() const;

 private:
  friend class KuduClient;
  friend class KuduScanner;
  friend class KuduScanToken;
  friend class KuduScanTokenBuilder;
  friend class KuduSchemaBuilder;
  friend class KuduTable;
  friend class KuduTableCreator;
  friend class KuduWriteOperation;
  friend class ScanConfiguration;
  friend class internal::GetTableSchemaRpc;
  friend class internal::LookupRpc;
  friend class internal::MetaCacheEntry;
  friend class internal::WriteRpc;
  friend class tools::RemoteKsckMaster;
  friend class tools::ReplicaDumper;

  friend KuduSchema KuduSchemaFromSchema(const Schema& schema);


  // For use by kudu tests.
  explicit KuduSchema(const Schema& schema);

  // Private since we don't want users to rely on the first N columns
  // being the keys.
  size_t num_key_columns() const;

  // Owned.
  Schema* schema_;
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCHEMA_H
