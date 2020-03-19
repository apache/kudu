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

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

// Log any pending errors in the given session, and then crash the current
// process.
void LogSessionErrorsAndDie(const sp::shared_ptr<KuduSession>& session,
                            const Status& s);

// Flush the given session. If any errors occur, log them and crash
// the process.
inline void FlushSessionOrDie(const sp::shared_ptr<KuduSession>& session) {
  Status s = session->Flush();
  if (PREDICT_FALSE(!s.ok())) {
    LogSessionErrorsAndDie(session, s);
  }
}

enum class ScannedRowsOrder {
  kAsIs,
  kSorted,
};

// Scans in LEADER_ONLY mode, returning stringified rows in the given vector.
Status ScanTableToStrings(KuduTable* table,
                          std::vector<std::string>* row_strings,
                          ScannedRowsOrder rows_order = ScannedRowsOrder::kAsIs);

// Count the number of rows in the table in LEADER_ONLY mode.
int64_t CountTableRows(KuduTable* table);

// Open the specified scanner and iterate through the rows, returning the row
// count. The scan operations are retried a few times in case of a timeout.
Status CountRowsWithRetries(KuduScanner* scanner, size_t* row_count);

Status ScanToStrings(KuduScanner* scanner,
                     std::vector<std::string>* row_strings) WARN_UNUSED_RESULT;

} // namespace client
} // namespace kudu
