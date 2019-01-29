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

#include "kudu/common/table_util.h"

#include <string>

#include <boost/optional/optional.hpp>

#include "kudu/gutil/strings/charset.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using boost::optional;
using std::string;

namespace kudu {

const char* const kInvalidTableError = "when the Hive Metastore integration "
    "is enabled, Kudu table names must be a period ('.') separated database and table name "
    "identifier pair, each containing only ASCII alphanumeric characters, '_', and '/'";

Status ParseHiveTableIdentifier(const string& table_name,
                                Slice* hms_database,
                                Slice* hms_table) {
  const char kSeparator = '.';
  strings::CharSet charset("abcdefghijklmnopqrstuvwxyz"
                           "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                           "0123456789"
                           "_/");

  auto separator_idx = boost::make_optional<int>(false, 0);
  for (int idx = 0; idx < table_name.size(); idx++) {
    char c = table_name[idx];
    if (!charset.Test(c)) {
      if (c == kSeparator && !separator_idx) {
        separator_idx = idx;
      } else {
        return Status::InvalidArgument(kInvalidTableError, table_name);
      }
    }
  }
  if (!separator_idx || *separator_idx == 0 || *separator_idx == table_name.size() - 1) {
    return Status::InvalidArgument(kInvalidTableError, table_name);
  }

  *hms_database = Slice(table_name.data(), *separator_idx);
  *hms_table = Slice(table_name.data() + *separator_idx + 1,
                     table_name.size() - *separator_idx - 1);
  return Status::OK();
}

} // namespace kudu
