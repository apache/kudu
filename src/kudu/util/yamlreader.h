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

#include <exception>
#include <string>
#include <vector>

#include <glog/logging.h>
// IWYU pragma: no_include <yaml-cpp/node/detail/iterator.h>
// IWYU pragma: no_include <yaml-cpp/node/detail/iterator_fwd.h>
// IWYU pragma: no_include <yaml-cpp/node/impl.h>
// IWYU pragma: no_include <yaml-cpp/node/iterator.h>
// IWYU pragma: no_include <yaml-cpp/node/node.h>
// IWYU pragma: no_include <yaml-cpp/node/type.h>
#include <yaml-cpp/yaml.h>  // IWYU pragma: keep

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {

// Wraps the YAML parsing functionality of YAML::Node.
//
// This class can read yaml content from a file, extract scalar, map and array type of values.
class YamlReader {
 public:
  explicit YamlReader(std::string filename);
  ~YamlReader() = default;

  Status Init();

  // Extractor methods.
  //
  // Look for a field with the name of 'field' in the given node.
  // Return Status::OK if it can be found and extracted as the specified type,
  // or return Status::NotFound if it cannot be found, or Status::Corruption if
  // the node is not extractable or the field can not be extracted as the type.

  template <typename T>
  static Status ExtractScalar(const YAML::Node* node,
                              const std::string& field,
                              T* result);

  static Status ExtractMap(const YAML::Node* node,
                           const std::string& field,
                           YAML::Node* result);

  template <typename T>
  static Status ExtractArray(const YAML::Node* node,
                             const std::string& field,
                             std::vector<T>* result);

  const YAML::Node* node() const { return &node_; }

 private:
  static const char* TypeToString(YAML::NodeType::value t);

  static Status ExtractField(const YAML::Node* node,
                             const std::string& field,
                             YAML::Node* result);

  std::string filename_;
  YAML::Node node_;

  DISALLOW_COPY_AND_ASSIGN(YamlReader);
};

template <typename T>
Status YamlReader::ExtractScalar(const YAML::Node* node,
                                 const std::string& field,
                                 T* result) {
  CHECK(result);
  YAML::Node val;
  RETURN_NOT_OK(ExtractField(node, field, &val));
  if (PREDICT_FALSE(!val.IsScalar())) {
    return Status::Corruption(strings::Substitute(
        "wrong type during field extraction: expected scalar but got $0",
        TypeToString(val.Type())));
  }
  *result = val.as<T>();
  return Status::OK();
}

template <typename T>
Status YamlReader::ExtractArray(const YAML::Node* node,
                                const std::string& field,
                                std::vector<T>* result) {
  CHECK(result);
  YAML::Node val;
  RETURN_NOT_OK(ExtractField(node, field, &val));
  if (PREDICT_FALSE(!val.IsSequence())) {
    return Status::Corruption(strings::Substitute(
        "wrong type during field extraction: expected sequence but got $0",
        TypeToString(val.Type())));
  }
  result->reserve(val.size());
  for (YAML::const_iterator iter = val.begin(); iter != val.end(); ++iter) {
    try {
      if (PREDICT_FALSE(!iter->IsScalar())) {
        return Status::Corruption(strings::Substitute(
          "wrong type during field extraction: expected scalar but got $0",
          TypeToString(iter->Type())));
      }
      result->push_back(iter->as<T>());
    } catch (std::exception& e) {
      return Status::Corruption(strings::Substitute("parse list element error: $0", e.what()));
    }
  }
  return Status::OK();
}

} // namespace kudu
