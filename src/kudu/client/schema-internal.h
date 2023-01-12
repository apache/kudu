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

#include <optional>
#include <string>

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
        primary_key_unique(false),
        auto_incrementing(false),
        remove_default(false) {
  }

  ~Data() {
    if (default_val) {
      delete default_val.value();
    }
  }

  const std::string name;

  std::optional<KuduColumnSchema::DataType> type;
  std::optional<int8_t> precision;
  std::optional<int8_t> scale;
  std::optional<uint16_t> length;
  std::optional<KuduColumnStorageAttributes::EncodingType> encoding;
  std::optional<KuduColumnStorageAttributes::CompressionType> compression;
  std::optional<int32_t> block_size;
  std::optional<bool> nullable;
  std::optional<bool> immutable;
  bool primary_key;
  bool primary_key_unique;
  std::optional<KuduValue*> default_val;  // Owned.
  bool auto_incrementing;
  bool remove_default;                      // For ALTER
  std::optional<std::string> rename_to;   // For ALTER
  std::optional<std::string> comment;
};

} // namespace client
} // namespace kudu
