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

#include "kudu/client/client-test-util.h"

#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/stl_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace client {

void LogSessionErrorsAndDie(const sp::shared_ptr<KuduSession>& session,
                            const Status& s) {
  CHECK(!s.ok());
  std::vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  CHECK(!overflow);

  // Log only the first 10 errors.
  LOG(INFO) << errors.size() << " failed ops. First 10 errors follow";
  int i = 0;
  for (const KuduError* e : errors) {
    if (i == 10) {
      break;
    }
    LOG(INFO) << "Op " << e->failed_op().ToString()
              << " had status " << e->status().ToString();
    i++;
  }
  CHECK_OK(s); // will fail
}

void ScanTableToStrings(KuduTable* table, vector<string>* row_strings) {
  row_strings->clear();
  KuduScanner scanner(table);
  // TODO(dralves) Change this to READ_AT_SNAPSHOT, fault tolerant scan and get rid
  // of the retry code below.
  ASSERT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
  ASSERT_OK(scanner.SetTimeoutMillis(15000));
  ScanToStrings(&scanner, row_strings);
}

int64_t CountTableRows(KuduTable* table) {
  vector<string> rows;
  client::ScanTableToStrings(table, &rows);
  return rows.size();
}

Status CountRowsWithRetries(KuduScanner* scanner, size_t* row_count) {
  if (!scanner) {
    return Status::InvalidArgument("null scanner");
  }
  // KUDU-1656: there might be timeouts, so re-try the operations
  // to avoid flakiness.
  Status row_count_status;
  size_t actual_row_count = 0;
  row_count_status = scanner->Open();
  for (size_t i = 0; i < 3; ++i) {
    if (!row_count_status.ok()) {
      if (row_count_status.IsTimedOut()) {
        // Start the row count over again.
        continue;
      }
      RETURN_NOT_OK(row_count_status);
    }
    size_t count = 0;
    while (scanner->HasMoreRows()) {
      KuduScanBatch batch;
      row_count_status = scanner->NextBatch(&batch);
      if (!row_count_status.ok()) {
        if (row_count_status.IsTimedOut()) {
          // Break the NextBatch() cycle and start row count over again.
          break;
        }
        RETURN_NOT_OK(row_count_status);
      }
      count += batch.NumRows();
    }
    if (row_count_status.ok()) {
      // Success: stop the retry cycle.
      actual_row_count = count;
      break;
    }
  }
  RETURN_NOT_OK(row_count_status);
  if (row_count) {
    *row_count = actual_row_count;
  }
  return Status::OK();
}

void ScanToStrings(KuduScanner* scanner, vector<string>* row_strings) {
  ASSERT_OK(scanner->Open());
  vector<KuduRowResult> rows;
  while (scanner->HasMoreRows()) {
    ASSERT_OK(scanner->NextBatch(&rows));
    for (const KuduRowResult& row : rows) {
      row_strings->push_back(row.ToString());
    }
  }
}

KuduSchema KuduSchemaFromSchema(const Schema& schema) {
  return KuduSchema(schema);
}

} // namespace client
} // namespace kudu
