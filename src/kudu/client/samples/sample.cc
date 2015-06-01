// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <ctime>
#include <iostream>
#include <sstream>
#include <tr1/memory>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/logging.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnRangePredicate;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Status;

using std::string;
using std::stringstream;
using std::vector;
using std::tr1::shared_ptr;

static Status CreateClient(const string& addr,
                           shared_ptr<KuduClient>* client) {
  return KuduClientBuilder()
      .add_master_server_addr(addr)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

static KuduSchema CreateSchema() {
  const int32_t kNonNullDefault = 12345;
  vector<KuduColumnSchema> columns;
  columns.push_back(KuduColumnSchema("key", KuduColumnSchema::INT32));
  columns.push_back(KuduColumnSchema("int_val", KuduColumnSchema::INT32));
  columns.push_back(KuduColumnSchema("string_val", KuduColumnSchema::STRING));
  columns.push_back(KuduColumnSchema("non_null_with_default", KuduColumnSchema::INT32, false,
                                     &kNonNullDefault));
  return KuduSchema(columns, 1);
}

static Status DoesTableExist(const shared_ptr<KuduClient>& client,
                             const string& table_name,
                             bool *exists) {
  scoped_refptr<KuduTable> table;
  Status s = client->OpenTable(table_name, &table);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

static Status CreateTable(const shared_ptr<KuduClient>& client,
                          const string& table_name,
                          const KuduSchema& schema,
                          int num_tablets) {
  // Generate the split keys for the table.
  KuduPartialRow* key = schema.NewRow();
  vector<string> splits;
  int32_t increment = 1000 / num_tablets;
  for (int32_t i = 1; i < num_tablets; i++) {
    KUDU_CHECK_OK(key->SetInt32(0, i * increment));
    splits.push_back(key->ToEncodedRowKeyOrDie());
  }
  delete key;

  // Create the table.
  KuduTableCreator* table_creator = client->NewTableCreator();
  Status s = table_creator->table_name(table_name)
      .schema(&schema)
      .split_keys(splits)
      .Create();
  delete table_creator;
  return s;
}

static Status AlterTable(const shared_ptr<KuduClient>& client,
                         const string& table_name) {
  KuduTableAlterer* table_alterer = client->NewTableAlterer();
  Status s = table_alterer->table_name(table_name)
      .rename_column("int_val", "integer_val")
      .add_nullable_column("another_val", KuduColumnSchema::BOOL)
      .drop_column("string_val")
      .Alter();
  delete table_alterer;
  return s;
}

static void StatusCB(const Status& status) {
  LOG(INFO) << "Asynchronous flush finished with status: " << status.ToString();
}

static Status InsertRows(scoped_refptr<KuduTable>& table, int num_rows) {
  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  for (int i = 0; i < num_rows; i++) {
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    KUDU_CHECK_OK(row->SetInt32("key", i));
    KUDU_CHECK_OK(row->SetInt32("integer_val", i * 2));
    KUDU_CHECK_OK(row->SetInt32("non_null_with_default", i * 5));
    KUDU_CHECK_OK(session->Apply(insert));
  }
  Status s = session->Flush();
  if (s.ok()) {
    return s;
  }

  // Test asynchronous flush.
  session->FlushAsync(Bind(&StatusCB));

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  s = overflow ? Status::IOError("Overflowed pending errors in session") :
      errors.front()->status();
  while (!errors.empty()) {
    delete errors.back();
    errors.pop_back();
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

static Status ScanRows(scoped_refptr<KuduTable>& table) {
  int32_t lower_bound = 5;
  int32_t upper_bound = 600;
  KuduColumnRangePredicate pred(table->schema().Column(0),
                                &lower_bound, &upper_bound);

  KuduScanner scanner(table.get());
  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(pred));
  KUDU_RETURN_NOT_OK(scanner.Open());
  vector<KuduRowResult> results;

  int next_row = lower_bound;
  while (scanner.HasMoreRows()) {
    KUDU_RETURN_NOT_OK(scanner.NextBatch(&results));
    for (vector<KuduRowResult>::iterator iter = results.begin();
        iter != results.end();
        iter++, next_row++) {
      const KuduRowResult& result = *iter;
      int32_t val;
      KUDU_RETURN_NOT_OK(result.GetInt32("key", &val));
      if (val != next_row) {
        stringstream out;
        out << "Scan returned the wrong results. Expected key "
            << next_row << " but got " << val;
        return Status::IOError(out.str());
      }
    }
    results.clear();
  }

  // next_row is now one past the last row we read.
  int last_row_seen = next_row - 1;

  if (last_row_seen != upper_bound) {
    stringstream out;
    out << "Scan returned the wrong results. Expected last row to be "
        << upper_bound << " rows but got " << last_row_seen;
    return Status::IOError(out.str());
  }
  return Status::OK();
}

static void LogCb(kudu::KuduLogSeverity severity,
                  const char* filename,
                  int line_number,
                  const struct ::tm* time,
                  const char* message,
                  size_t message_len) {
  LOG(INFO) << "Received log message from Kudu client library";
  LOG(INFO) << " Severity: " << severity;
  LOG(INFO) << " Filename: " << filename;
  LOG(INFO) << " Line number: " << line_number;
  char time_buf[32];
  // Example: Tue Mar 24 11:46:43 2015.
  CHECK(strftime(time_buf, sizeof(time_buf), "%a %b %d %T %Y", time));
  LOG(INFO) << " Time: " << time_buf;
  LOG(INFO) << " Message: " << string(message, message_len);
}

int main(int argc, char* argv[]) {
  kudu::client::InstallLoggingCallback(kudu::Bind(&LogCb));

  const string kTableName = "test_table";

  // Enable verbose debugging for the client library.
  kudu::client::SetVerboseLogLevel(2);

  // Create and connect a client.
  shared_ptr<KuduClient> client;
  KUDU_CHECK_OK(CreateClient("127.0.0.1", &client));
  LOG(INFO) << "Created a client connection";

  // Disable the verbose logging.
  kudu::client::SetVerboseLogLevel(0);

  // Create a schema.
  KuduSchema schema(CreateSchema());
  LOG(INFO) << "Created a schema";

  // Create a table with that schema.
  bool exists;
  KUDU_CHECK_OK(DoesTableExist(client, kTableName, &exists));
  if (exists) {
    client->DeleteTable(kTableName);
    LOG(INFO) << "Deleting old table before creating new one";
  }
  KUDU_CHECK_OK(CreateTable(client, kTableName, schema, 10));
  LOG(INFO) << "Created a table";

  // Alter the table.
  KUDU_CHECK_OK(AlterTable(client, kTableName));
  LOG(INFO) << "Altered a table";

  // Insert some rows into the table.
  scoped_refptr<KuduTable> table;
  KUDU_CHECK_OK(client->OpenTable(kTableName, &table));
  KUDU_CHECK_OK(InsertRows(table, 1000));
  LOG(INFO) << "Inserted some rows into a table";

  // Scan some rows.
  KUDU_CHECK_OK(ScanRows(table));
  LOG(INFO) << "Scanned some rows out of a table";

  // Delete the table.
  KUDU_CHECK_OK(client->DeleteTable(kTableName));
  LOG(INFO) << "Deleted a table";

  // Done!
  LOG(INFO) << "Done";
  return 0;
}
