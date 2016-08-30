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

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

namespace log {
class ReadableLogSegment;
} // namespace log

namespace tools {

// Utility methods used by multiple actions across different modes.

// Prints the contents of a WAL segment to stdout.
//
// The following gflags affect the output:
// - print_entries: in what style entries should be printed.
// - print_meta: whether or not headers/footers are printed.
// - truncate_data: how many bytes to print for each data field.
Status PrintSegment(const scoped_refptr<log::ReadableLogSegment>& segment);

} // namespace tools
} // namespace kudu
