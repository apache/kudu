// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_SCHEMA_INTERNAL_H
#define KUDU_CLIENT_SCHEMA_INTERNAL_H

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

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCHEMA_INTERNAL_H
