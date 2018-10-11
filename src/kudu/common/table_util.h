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

#include <string>

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

class Slice;

extern const char* const kInvalidTableError;

// Parses a Kudu table name of the form '<database>.<table>' into
// a Hive database and table name. Returns an error if the Kudu
// table name is not correctly formatted. The returned HMS database
// and table slices must not outlive 'table_name'.
Status ParseHiveTableIdentifier(const std::string& table_name,
                                Slice* hms_database,
                                Slice* hms_table) WARN_UNUSED_RESULT;

} // namespace kudu
