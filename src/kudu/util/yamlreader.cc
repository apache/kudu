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

#include "kudu/util/yamlreader.h"

#include <ostream>
#include <utility>

#include <glog/logging.h>
// IWYU pragma: no_include <yaml-cpp/node/detail/impl.h>
// IWYU pragma: no_include <yaml-cpp/node/parse.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"

using std::string;
using strings::Substitute;
using YAML::Node;
using YAML::NodeType;

namespace kudu {

YamlReader::YamlReader(string filename) : filename_(std::move(filename)) {}

Status YamlReader::Init() {
  try {
    node_ = YAML::LoadFile(filename_);
  } catch (std::exception& e) {
    return Status::Corruption(Substitute("YAML::LoadFile error: $0", e.what()));
  }

  return Status::OK();
}

Status YamlReader::ExtractMap(const Node* node,
                              const string& field,
                              Node* result) {
  CHECK(result);
  Node val;
  RETURN_NOT_OK(ExtractField(node, field, &val));
  if (PREDICT_FALSE(!val.IsMap())) {
    return Status::Corruption(Substitute(
        "wrong type during field extraction: expected map but got $0",
        TypeToString(val.Type())));
  }
  *result = val;
  return Status::OK();
}

Status YamlReader::ExtractField(const Node* node,
                                const string& field,
                                Node* result) {
  if (PREDICT_FALSE(!node->IsDefined() || !node->IsMap())) {
    return Status::Corruption("node is not map type");
  }
  try {
    *result = (*node)[field];
  } catch (std::exception& e) {
    return Status::NotFound(Substitute("parse field $0 error: $1", field, e.what()));
  }
  if (PREDICT_FALSE(!result->IsDefined())) {
    return Status::Corruption("Missing field", field);
  }

  return Status::OK();
}

const char* YamlReader::TypeToString(NodeType::value t) {
  switch (t) {
    case NodeType::Undefined:
      return "undefined";
    case NodeType::Null:
      return "null";
    case NodeType::Scalar:
      return "scalar";
    case NodeType::Sequence:
      return "sequence";
    case NodeType::Map:
      return "map";
    default:
      LOG(FATAL) << "unexpected type: " << t;
  }
  return "";
}

} // namespace kudu
