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

// This file contains a schema and some utility code for a simple
// "Key-Value" table, with int32 keys and nullable int32 values. This
// is used by a few tests.
#pragma once

#include <boost/optional.hpp>
#include <iostream>
#include <string>

#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"

namespace kudu {

// Struct for keeping track of what we think the contents of a row should be after
// our random mutations.
struct ExpectedKeyValueRow {
  // Non-nullable key.
  int32_t key;

  // Nullable value.
  boost::optional<int32_t> val;

  bool operator==(const ExpectedKeyValueRow& other) const {
    return key == other.key && val == other.val;
  }

  std::string ToString() const {
    std::string ret = strings::Substitute("{$0,", key);
    if (val == boost::none) {
      ret.append("NULL}");
    } else {
      ret.append(strings::Substitute("$0}", *val));
    }
    return ret;
  }
};

inline Schema CreateKeyValueTestSchema() {
  return Schema({ColumnSchema("key", INT32),
                 ColumnSchema("val", INT32, true) }, 1);
}

inline std::ostream& operator<<(std::ostream& o, const ExpectedKeyValueRow& t) {
  return o << t.ToString();
}

} // namespace kudu
