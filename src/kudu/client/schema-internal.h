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
#ifndef KUDU_CLIENT_SCHEMA_INTERNAL_H
#define KUDU_CLIENT_SCHEMA_INTERNAL_H

#include <string>

#include <boost/optional/optional.hpp>

#include "kudu/client/schema.h"
#include "kudu/client/value.h"
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
    KuduColumnSchema::DataType type,
    const KuduColumnTypeAttributes& attributes);
KuduColumnSchema::DataType FromInternalDataType(kudu::DataType type);

class KuduColumnTypeAttributes::Data {
 public:
  Data(int8_t precision, int8_t scale, uint16_t length)
      : precision(precision),
        scale(scale),
        length(length) {
  }

  int8_t precision;
  int8_t scale;
  uint16_t length;
};

class KuduColumnSpec::Data {
 public:
  explicit Data(std::string name)
      : name(std::move(name)),
        primary_key(false),
        remove_default(false) {
  }

  ~Data() {
    if (default_val) {
      delete default_val.value();
    }
  }

  const std::string name;

  boost::optional<KuduColumnSchema::DataType> type;
  boost::optional<int8_t> precision;
  boost::optional<int8_t> scale;
  boost::optional<uint16_t> length;
  boost::optional<KuduColumnStorageAttributes::EncodingType> encoding;
  boost::optional<KuduColumnStorageAttributes::CompressionType> compression;
  boost::optional<int32_t> block_size;
  boost::optional<bool> nullable;
  bool primary_key;
  boost::optional<KuduValue*> default_val;  // Owned.
  bool remove_default;                      // For ALTER
  boost::optional<std::string> rename_to;   // For ALTER
  boost::optional<std::string> comment;
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCHEMA_INTERNAL_H
