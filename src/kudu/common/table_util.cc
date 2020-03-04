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
#include <gflags/gflags.h>

#include "kudu/gutil/strings/charset.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_string(ranger_default_database, "default",
              "Name of the default database which is used in the Ranger "
              "authorization context when the database name is not specified "
              "in the table name. Ranger makes no difference between "
              "<ranger_default_database>.<table> and <table>, so privileges "
              "granted on <table> in <ranger_default_database> are applied to"
              "both <ranger_default_database>.<table> and <table> in Kudu.");

using std::string;

namespace kudu {

const char* const kInvalidHiveTableError = "when the Hive Metastore integration "
    "is enabled, Kudu table names must be a period ('.') separated database and table name "
    "identifier pair, each containing only ASCII alphanumeric characters, '_', and '/'";

const char* const kInvalidRangerTableError = "when Ranger authorization is enabled, "
    "Kudu table names must not begin with a period ('.') and if they contain a period, there "
    "must be other characters after the first one, as the first period is treated as a separator "
    "between the database and table name. The table and the database name can't be empty.";

const char kSeparator = '.';

Status ParseHiveTableIdentifier(const string& table_name,
                                Slice* hms_database,
                                Slice* hms_table) {
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
        return Status::InvalidArgument(kInvalidHiveTableError, table_name);
      }
    }
  }
  if (!separator_idx || *separator_idx == 0 || *separator_idx == table_name.size() - 1) {
    return Status::InvalidArgument(kInvalidHiveTableError, table_name);
  }

  *hms_database = Slice(table_name.data(), *separator_idx);
  *hms_table = Slice(table_name.data() + *separator_idx + 1,
                     table_name.size() - *separator_idx - 1);
  return Status::OK();
}

Status ParseRangerTableIdentifier(const string& table_name,
                                  string* ranger_database,
                                  Slice* ranger_table) {
  auto separator_idx = boost::make_optional<int>(false, 0);
  for (int idx = 0; idx < table_name.size(); ++idx) {
    char c = table_name[idx];
    if (c == kSeparator) {
      separator_idx = idx;
      break;
    }
  }

  if (separator_idx) {
    if (*separator_idx == 0 || *separator_idx == table_name.size() - 1) {
      return Status::InvalidArgument(kInvalidRangerTableError, table_name);
    }
    *ranger_database = table_name.substr(0, *separator_idx);
    *ranger_table = Slice(table_name.data() + *separator_idx + 1,
                          table_name.size() - *separator_idx - 1);
  } else {
    *ranger_database = FLAGS_ranger_default_database;
    *ranger_table = Slice(table_name.data());
  }

  if (ranger_table->empty()) {
    return Status::InvalidArgument(kInvalidRangerTableError, table_name);
  }

  return Status::OK();
}

} // namespace kudu
