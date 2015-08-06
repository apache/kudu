// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_CLIENT_TEST_UTIL_H
#define KUDU_CLIENT_CLIENT_TEST_UTIL_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;

namespace client {
class KuduSchema;

// Log any pending errors in the given session, and then crash the current
// process.
void LogSessionErrorsAndDie(const std::tr1::shared_ptr<KuduSession>& session,
                            const Status& s);

// Flush the given session. If any errors occur, log them and crash
// the process.
inline void FlushSessionOrDie(const std::tr1::shared_ptr<KuduSession>& session) {
  Status s = session->Flush();
  if (PREDICT_FALSE(!s.ok())) {
    LogSessionErrorsAndDie(session, s);
  }
}

void ScanTableToStrings(KuduTable* table, std::vector<std::string>* row_strings);

void ScanToStrings(KuduScanner* scanner, std::vector<std::string>* row_strings);

// Convert a kudu::Schema to a kudu::client::KuduSchema.
KuduSchema KuduSchemaFromSchema(const Schema& schema);

} // namespace client
} // namespace kudu

#endif /* KUDU_CLIENT_CLIENT_TEST_UTIL_H */
