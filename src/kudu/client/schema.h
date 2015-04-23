
// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_SCHEMA_H
#define KUDU_CLIENT_SCHEMA_H

#include <string>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/kudu_export.h"

namespace kudu {

class ColumnSchema;
class Schema;

namespace client {

namespace internal {
class GetTableSchemaRpc;
class LookupRpc;
class WriteRpc;
} // namespace internal

class KuduClient;
class KuduColumnRangePredicate;
class KuduWriteOperation;

class KUDU_EXPORT KuduColumnStorageAttributes {
 public:
  enum EncodingType {
    AUTO_ENCODING = 0,
    PLAIN_ENCODING = 1,
    PREFIX_ENCODING = 2,
    GROUP_VARINT = 3,
    RLE = 4,
  };

  enum CompressionType {
    DEFAULT_COMPRESSION = 0,
    NO_COMPRESSION = 1,
    SNAPPY = 2,
    LZ4 = 3,
    ZLIB = 4,
  };

  KuduColumnStorageAttributes(EncodingType encoding = AUTO_ENCODING,
                              CompressionType compression = DEFAULT_COMPRESSION)
  : encoding_(encoding),
    compression_(compression) {}

  const EncodingType encoding() const {
    return encoding_;
  }

  const CompressionType compression() const {
    return compression_;
  }

  std::string ToString() const;

 private:
  EncodingType encoding_;
  CompressionType compression_;
};

class KUDU_EXPORT KuduColumnSchema {
 public:
  enum DataType {
    UINT8 = 0,
    INT8 = 1,
    UINT16 = 2,
    INT16 = 3,
    UINT32 = 4,
    INT32 = 5,
    UINT64 = 6,
    INT64 = 7,
    STRING = 8,
    BOOL = 9,
  };

  KuduColumnSchema(const std::string &name,
                   DataType type,
                   bool is_nullable = false,
                   const void* default_value = NULL,
                   KuduColumnStorageAttributes attributes = KuduColumnStorageAttributes());
  KuduColumnSchema(const KuduColumnSchema& other);
  ~KuduColumnSchema();

  KuduColumnSchema& operator=(const KuduColumnSchema& other);

  void CopyFrom(const KuduColumnSchema& other);

  bool Equals(const KuduColumnSchema& other) const;

  // Getters to expose column schema information.
  const std::string& name() const;
  DataType type() const;
  bool is_nullable() const;

  // TODO: Expose default column value and attributes?

 private:
  friend class KuduColumnRangePredicate;
  friend class KuduSchema;

  gscoped_ptr<ColumnSchema> col_;
};

class KUDU_EXPORT KuduSchema {
 public:
  KuduSchema();
  KuduSchema(const std::vector<KuduColumnSchema>& columns, int key_columns);
  KuduSchema(const KuduSchema& other);
  ~KuduSchema();

  KuduSchema& operator=(const KuduSchema& other);
  void CopyFrom(const KuduSchema& other);
  void Reset(const std::vector<KuduColumnSchema>& columns, int key_columns);

  bool Equals(const KuduSchema& other) const;
  KuduColumnSchema Column(size_t idx) const;
  KuduSchema CreateKeyProjection() const;
  size_t num_columns() const;
  size_t num_key_columns() const;

 private:
  friend class KuduEncodedKeyBuilder;
  friend class KuduClient;
  friend class KuduScanner;
  friend class KuduTableCreator;
  friend class KuduWriteOperation;
  friend class internal::GetTableSchemaRpc;
  friend class internal::LookupRpc;
  friend class internal::WriteRpc;

  gscoped_ptr<Schema> schema_;
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCHEMA_H
