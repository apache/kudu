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

#include <ctime>
#include <iostream>
#include <sstream>
#include <memory>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/stubs.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/util/monotime.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDelete;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduUpdate;
using kudu::client::KuduValue;
using kudu::client::KuduWriteOperation;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Status;

using std::ostringstream;
using std::string;
using std::vector;
using std::unique_ptr;

static Status CreateClient(const vector<string>& master_addrs,
                           shared_ptr<KuduClient>* client) {
  return KuduClientBuilder()
      .master_server_addrs(master_addrs)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

static KuduSchema CreateSchema() {
  KuduSchema schema;
  KuduSchemaBuilder b;
  // Columns which are not uniquely identifiable can still be used as primary keys by
  // specifying them as non-unique primary key.
  b.AddColumn("non_unique_key")->Type(KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  KUDU_CHECK_OK(b.Build(&schema));
  return schema;
}

static Status CreateTable(const shared_ptr<KuduClient>& client,
                          const string& table_name,
                          const KuduSchema& schema) {
  vector<string> column_names;
  // Use the non-unique key column for hash partitioning. The column at index 0 in the schema refers
  // to the non-unique key column specified above. The auto-incrementing column is always inserted as
  // the last key column.
  column_names.push_back(schema.Column(0).name());
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  table_creator->table_name(table_name)
      .schema(&schema)
      .add_hash_partitions(column_names, 2);
  Status s = table_creator->Create();
  return s;
}

static Status InsertStaleCounterRows(const shared_ptr<KuduTable>& table, int num_rows,
                                     int divisor) {
  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int stale_counter = 0;
  for (int i = 0; i < num_rows; i++) {
    if (i % divisor == 0) {
      stale_counter++;
    }
    unique_ptr<KuduInsert> new_insert(table->NewInsert());
    KuduPartialRow* row = new_insert->mutable_row();
    // The auto-incrementing column is populated on the server-side automatically.
    KUDU_CHECK_OK(row->SetInt32("non_unique_key", stale_counter));
    KUDU_CHECK_OK(row->SetInt32("int_val", i % divisor));
    KUDU_CHECK_OK(session->Apply(new_insert.release()));
  }

  KUDU_RETURN_NOT_OK(session->Flush());
  return session->Close();
}

static Status ScanRows(const shared_ptr<KuduTable>& table, vector<KuduPredicate*> predicates) {
  KuduScanner scanner(table.get());
  for (int i = 0; i < predicates.size(); i++) {
    KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(predicates[i]));
  }
  KUDU_RETURN_NOT_OK(scanner.Open());

  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    KUDU_RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (KuduScanBatch::const_iterator it = batch.begin(); it != batch.end(); ++it) {
      KuduScanBatch::RowPtr row(*it);
      // The row contains the auto-incrementing column. If one doesn't requre it, it can be
      // discarded through a projection.
      KUDU_LOG(INFO) << row.ToString();
    }
  }
  return Status::OK();
}

static Status UpdateRows(const shared_ptr<KuduTable>& table,
                         vector<KuduPredicate*> predicates, int new_val){
  // It's necessary to specify the entire set of key columns when updating a particular row.
  // An auto-incrementing column is auto-populated at the server side, and one way to retrieve
  // its values is scanning the table with a projection that includes the auto-incrementing column.
  KuduScanner scanner(table.get());
  for (int i = 0; i < predicates.size(); i++) {
    KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(predicates[i]));
  }
  KUDU_RETURN_NOT_OK(scanner.Open());

  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    KUDU_RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (KuduScanBatch::const_iterator it = batch.begin(); it != batch.end(); ++it) {
      KuduScanBatch::RowPtr row(*it);
      int64_t auto_incrementing_counter;
      int32_t non_unique_key, val;

      KUDU_RETURN_NOT_OK(row.GetInt32("non_unique_key", &non_unique_key));
      KUDU_RETURN_NOT_OK(row.GetInt64(KuduSchema::GetAutoIncrementingColumnName(),
          &auto_incrementing_counter));
      KUDU_RETURN_NOT_OK(row.GetInt32("int_val", &val));

      unique_ptr<KuduUpdate> new_update(table->NewUpdate());
      KuduPartialRow* update_row = new_update->mutable_row();
      KUDU_RETURN_NOT_OK(update_row->SetInt32("non_unique_key", non_unique_key));
      KUDU_RETURN_NOT_OK(update_row->SetInt64(KuduSchema::GetAutoIncrementingColumnName(),
          auto_incrementing_counter));
      KUDU_RETURN_NOT_OK(update_row->SetInt32("int_val", new_val));
      KUDU_RETURN_NOT_OK(session->Apply(new_update.release()));
    }
  }

  KUDU_RETURN_NOT_OK(session->Flush());
  return session->Close();
}

static Status DeleteRows(const shared_ptr<KuduTable>& table,
                         vector<KuduPredicate*> predicates){
  // It's necessary to specify the entire set of key columns when updating a particular row.
  // An auto-incrementing column is auto-populated at the server side, and one way to retrieve
  // its values is scanning the table with a projection that includes the auto-incrementing column.
  KuduScanner scanner(table.get());
  for (int i = 0; i < predicates.size(); i++) {
    KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(predicates[i]));
  }
  KUDU_RETURN_NOT_OK(scanner.Open());
  KuduScanBatch batch;

  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  while (scanner.HasMoreRows()) {
    KUDU_RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (KuduScanBatch::const_iterator it = batch.begin(); it != batch.end(); ++it) {
      KuduScanBatch::RowPtr row(*it);
      int64_t auto_incrementing_counter;
      int32_t non_unique_key;

      KUDU_RETURN_NOT_OK(row.GetInt64(KuduSchema::GetAutoIncrementingColumnName(),
          &auto_incrementing_counter));
      KUDU_RETURN_NOT_OK(row.GetInt32("non_unique_key", &non_unique_key));

      unique_ptr<KuduDelete> new_delete(table->NewDelete());
      KuduPartialRow* delete_row = new_delete->mutable_row();
      KUDU_RETURN_NOT_OK(delete_row->SetInt32("non_unique_key", non_unique_key));
      KUDU_RETURN_NOT_OK(delete_row->SetInt64(KuduSchema::GetAutoIncrementingColumnName(),
          auto_incrementing_counter));
      KUDU_RETURN_NOT_OK(session->Apply(new_delete.release()));
    }
  }

  KUDU_RETURN_NOT_OK(session->Flush());
  return session->Close();
}


int main(int argc, char* argv[]) {
  if (argc < 2) {
    KUDU_LOG(ERROR) << "usage: " << argv[0] << " <master host> ...";
    return -1;
  }
  vector<string> master_addrs;
  for (int i = 1; i < argc; i++) {
    master_addrs.push_back(argv[i]);
  }
  const string kTableName = "non_unique_primary_key_test_table";

  shared_ptr<KuduClient> client;
  KUDU_CHECK_OK(CreateClient(master_addrs, &client));
  KUDU_LOG(INFO) << "Created a client connection";

  // Create a schema with non-unique primary key.
  KuduSchema schema(CreateSchema());
  KUDU_LOG(INFO) << "Created the schema:";
  // The schema stringification shows the presence of the auto-incrementing column,
  // and the resulting composite primary key.
  KUDU_LOG(INFO) << schema.ToString();

  KUDU_CHECK_OK(CreateTable(client, kTableName, schema));
  KUDU_LOG(INFO) << "Created the table";

  // Insert some rows into the table.
  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(client->OpenTable(kTableName, &table));
  int divisor = 3;
  int num_rows = 10;
  KUDU_CHECK_OK(InsertStaleCounterRows(table, num_rows, divisor));
  KUDU_LOG(INFO) << "Inserted some row(s) into the table:";
  KUDU_CHECK_OK(ScanRows(table, {}));

  KUDU_LOG(INFO) << "Demonstrating scanning ...";
  {
    int non_unique_key_equals = 1;
    KUDU_LOG(INFO) << "Scanned some row(s) WHERE non_unique_key = "
        << non_unique_key_equals << ":";
    vector<KuduPredicate*> predicates;
    KuduPredicate* p = table->NewComparisonPredicate(
        "non_unique_key", KuduPredicate::EQUAL, KuduValue::FromInt(non_unique_key_equals));
    predicates.emplace_back(p);
    KUDU_CHECK_OK(ScanRows(table, predicates));
  }

  KUDU_LOG(INFO) << "Demonstrating UPDATE ...";
  // Updating based upon a predicate on a non-unique PK and on a non-PK column
  {
    int non_unique_key_equals = 1;
    int int_val_equals = 2;
    int new_val = 98;
    vector<KuduPredicate*> predicates;
    KuduPredicate* p = table->NewComparisonPredicate(
        "non_unique_key", KuduPredicate::EQUAL, KuduValue::FromInt(non_unique_key_equals));
    predicates.emplace_back(p);
    p = table->NewComparisonPredicate(
        "int_val", KuduPredicate::EQUAL, KuduValue::FromInt(int_val_equals));
    predicates.emplace_back(p);
    // Update row(s)
    KUDU_CHECK_OK(UpdateRows(table, predicates, new_val));
    KUDU_LOG(INFO) << "Updated row(s) WHERE non_unique_key = " << non_unique_key_equals
        << " AND int_val = " << int_val_equals << " to int_val = " << new_val;
    KUDU_CHECK_OK(ScanRows(table, {}));
  }

  // Updating based upon a predicate on a non-unique PK
  {
    int non_unique_key_equals = 2;
    int new_val = 99;
    vector<KuduPredicate*> predicates;
    KuduPredicate* p = table->NewComparisonPredicate(
        "non_unique_key", KuduPredicate::EQUAL, KuduValue::FromInt(non_unique_key_equals));
    predicates.emplace_back(p);
    // Update row(s)
    KUDU_CHECK_OK(UpdateRows(table, predicates, new_val));
    KUDU_LOG(INFO) << "Updated row(s) WHERE non_unique_key = " << non_unique_key_equals
        << " to int_val = " << new_val;
    KUDU_CHECK_OK(ScanRows(table, {}));
  }

  // Updating based upon a predicate on a non-unique PK and on the auto-incrementing column
  {
    int non_unique_key_equals = 2;
    int auto_incrementin_counter_val = 5;
    int new_val = 100;
    vector<KuduPredicate*> predicates;
    KuduPredicate* p = table->NewComparisonPredicate(
        "non_unique_key", KuduPredicate::EQUAL, KuduValue::FromInt(non_unique_key_equals));
    predicates.emplace_back(p);
    p = table->NewComparisonPredicate(
        KuduSchema::GetAutoIncrementingColumnName(), KuduPredicate::EQUAL,
        KuduValue::FromInt(auto_incrementin_counter_val));
    predicates.emplace_back(p);
    // Update row(s)
    KUDU_CHECK_OK(UpdateRows(table, predicates, new_val));
    KUDU_LOG(INFO) << "Updated row(s) WHERE non_unique_key = " << non_unique_key_equals <<
        " AND " << KuduSchema::GetAutoIncrementingColumnName() << " = " <<
        auto_incrementin_counter_val << " to int_val = " << new_val;
    KUDU_CHECK_OK(ScanRows(table, {}));
  }

  KUDU_LOG(INFO) << "Demonstrating DELETE ...";
  // Deleting based upon a predicate on a non-unique PK and on a non-PK column
  {
    int non_unique_key_equals = 3;
    int int_val_equals = 1;
    vector<KuduPredicate*> predicates;
    KuduPredicate* p = table->NewComparisonPredicate(
        "non_unique_key", KuduPredicate::EQUAL, KuduValue::FromInt(non_unique_key_equals));
    predicates.emplace_back(p);
    p = table->NewComparisonPredicate(
        "int_val", KuduPredicate::EQUAL, KuduValue::FromInt(int_val_equals));
    predicates.emplace_back(p);
    // Delete row(s)
    KUDU_CHECK_OK(DeleteRows(table, predicates));
    KUDU_LOG(INFO) << "Deleted row(s) WHERE non_unique_key = " << non_unique_key_equals
        << " AND int_val = " << int_val_equals;
    KUDU_CHECK_OK(ScanRows(table, {}));
  }

  // Deleting based upon a predicate on a non-unique PK
  {
    int non_unique_key_equals = 2;
    vector<KuduPredicate*> predicates;
    KuduPredicate* p = table->NewComparisonPredicate(
        "non_unique_key", KuduPredicate::EQUAL, KuduValue::FromInt(non_unique_key_equals));
    predicates.emplace_back(p);
    // Delete row(s)
    KUDU_CHECK_OK(DeleteRows(table, predicates));
    KUDU_LOG(INFO) << "Deleted row(s) WHERE non_unique_key = " << non_unique_key_equals;
    KUDU_CHECK_OK(ScanRows(table, {}));
  }

  // Deleting based upon a predicate on a non-unique PK and on the auto-incrementing column
  {
    int non_unique_key_equals = 3;
    int auto_incrementin_counter_val = 3;
    vector<KuduPredicate*> predicates;
    KuduPredicate* p = table->NewComparisonPredicate(
        "non_unique_key", KuduPredicate::EQUAL, KuduValue::FromInt(non_unique_key_equals));
    predicates.emplace_back(p);
    p = table->NewComparisonPredicate(
        KuduSchema::GetAutoIncrementingColumnName(), KuduPredicate::EQUAL,
        KuduValue::FromInt(auto_incrementin_counter_val));
    predicates.emplace_back(p);
    // Delete row(s)
    KUDU_CHECK_OK(DeleteRows(table, predicates));
    KUDU_LOG(INFO) << "Deleted row(s) WHERE non_unique_key = " << non_unique_key_equals <<
        " AND " << KuduSchema::GetAutoIncrementingColumnName() << " = " <<
        auto_incrementin_counter_val;
    KUDU_CHECK_OK(ScanRows(table, {}));
  }

  KUDU_CHECK_OK(client->DeleteTable(kTableName));
  KUDU_LOG(INFO) << "Deleted the table";
  KUDU_LOG(INFO) << "Done";
  return 0;
}
