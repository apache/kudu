// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_SCHEMA_INTERNAL_H
#define KUDU_CLIENT_SCHEMA_INTERNAL_H

#include <string>

#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"

namespace kudu {
namespace client {

// Helper functions that convert between client-facing and internal PB enums.

kudu::EncodingType ToInternalEncodingType(
    KuduColumnStorageAttributes::EncodingType type);
KuduColumnStorageAttributes::EncodingType FromInternalEncodingType(
    kudu::EncodingType type);

kudu::CompressionType ToInternalCompressionType(
    KuduColumnStorageAttributes::CompressionType type);
KuduColumnStorageAttributes::CompressionType FromInternalCompressionType(
    kudu::CompressionType type);

kudu::DataType ToInternalDataType(
    KuduColumnSchema::DataType type);
KuduColumnSchema::DataType FromInternalDataType(
    kudu::DataType type);


class KuduColumnSpec::Data {
 public:
  explicit Data(const std::string& name)
    : name(name),
      has_type(false),
      has_encoding(false),
      has_compression(false),
      has_block_size(false),
      has_nullable(false),
      primary_key(false),
      has_default(false),
      default_val(NULL),
      remove_default(false),
      has_rename_to(false) {
  }

  ~Data() {
    delete default_val;
  }

  const std::string name;

  bool has_type;
  KuduColumnSchema::DataType type;

  bool has_encoding;
  KuduColumnStorageAttributes::EncodingType encoding;

  bool has_compression;
  KuduColumnStorageAttributes::CompressionType compression;

  bool has_block_size;
  int32_t block_size;

  bool has_nullable;
  bool nullable;

  bool primary_key;

  bool has_default;
  KuduValue* default_val; // Owned.

  // For ALTER
  bool remove_default;

  // For ALTER
  bool has_rename_to;
  std::string rename_to;
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCHEMA_INTERNAL_H
