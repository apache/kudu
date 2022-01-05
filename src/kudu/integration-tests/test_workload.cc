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

#include "kudu/integration-tests/test_workload.h"

#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/txn_id.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/integration-tests/data_gen_util.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDelete;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTransaction;
using kudu::client::KuduUpdate;
using kudu::client::sp::shared_ptr;
using kudu::cluster::MiniCluster;
using kudu::transactions::TxnTokenPB;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

const char* const TestWorkload::kDefaultTableName = "test-workload";

TestWorkload::TestWorkload(MiniCluster* cluster,
                           PartitioningType partitioning)
  : cluster_(cluster),
    partitioning_(partitioning),
    rng_(SeedRandom()),
    num_write_threads_(4),
    num_read_threads_(0),
    // Set a high scanner timeout so that we're likely to have a chance to scan, even in
    // high-stress workloads.
    read_timeout_millis_(60000),
    write_batch_size_(50),
    write_interval_millis_(0),
    write_timeout_millis_(20000),
    txn_id_(TxnId::kInvalidTxnId),
    begin_txn_(false),
    commit_txn_(false),
    rollback_txn_(false),
    wait_for_create_(true),
    fault_tolerant_(true),
    verify_num_rows_(true),
    read_errors_allowed_(false),
    timeout_allowed_(false),
    not_found_allowed_(false),
    already_present_allowed_(false),
    network_error_allowed_(false),
    remote_error_allowed_(false),
    write_pattern_(INSERT_RANDOM_ROWS),
    selection_(client::KuduClient::CLOSEST_REPLICA),
    schema_(KuduSchema::FromSchema(GetSimpleTestSchema())),
    num_replicas_(3),
    num_tablets_(partitioning_ == PartitioningType::RANGE ? 1 : 2),
    table_name_(kDefaultTableName),
    start_latch_(0),
    should_run_(false),
    rows_inserted_(0),
    rows_deleted_(0),
    batches_completed_(0),
    sequential_key_gen_(0) {
  // Make the default write pattern random row inserts.
  set_write_pattern(INSERT_RANDOM_ROWS);
}

TestWorkload::~TestWorkload() {
  StopAndJoin();
}

void TestWorkload::set_schema(const KuduSchema& schema) {
  // Do some sanity checks on the schema. They reflect how the rest of
  // TestWorkload is going to use the schema.
  CHECK_GT(schema.num_columns(), 0) << "Schema should have at least one column";
  vector<int> key_indexes;
  schema.GetPrimaryKeyColumnIndexes(&key_indexes);
  CHECK_LE(1, key_indexes.size()) << "Schema should have at least one key column";
  CHECK_EQ(0, key_indexes[0]) << "Schema's first key column should be index 0";
  KuduColumnSchema key = schema.Column(0);
  CHECK_EQ(KuduColumnSchema::INT32, key.type())
      << "Schema's first key column should be of type INT32";
  schema_ = schema;
}

void TestWorkload::OpenTable(shared_ptr<KuduTable>* table) {
  // Loop trying to open up the table. In some tests we set up very
  // low RPC timeouts to test those behaviors, so this might fail and
  // need retrying.
  while (should_run_.Load()) {
    Status s = client_->OpenTable(table_name_, table);
    if (s.ok()) {
      break;
    }
    if (timeout_allowed_ && s.IsTimedOut()) {
      SleepFor(MonoDelta::FromMilliseconds(50));
      continue;
    }
    CHECK_OK(s);
  }

  // Wait for all of the workload threads to be ready to go. This maximizes the chance
  // that they all send a flood of requests at exactly the same time.
  //
  // This also minimizes the chance that we see failures to call OpenTable() if
  // a late-starting thread overlaps with the flood of traffic from the ones that are
  // already writing/reading data.
  start_latch_.CountDown();
  start_latch_.Wait();
}

void TestWorkload::WriteThread() {
  shared_ptr<KuduTable> table;
  OpenTable(&table);

  shared_ptr<KuduSession> session;
  if (txn_) {
    CHECK_OK(txn_->CreateSession(&session));
  } else {
    session = client_->NewSession();
  }
  session->SetTimeoutMillis(write_timeout_millis_);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  while (should_run_.Load()) {
    int inserted = 0;
    int deleted = 0;
    vector<int32_t> keys;
    // Write insert or update row to cluster.
    {
      for (int i = 0; i < write_batch_size_; i++) {
        if (write_pattern_ == UPDATE_ONE_ROW) {
          unique_ptr<KuduUpdate> update(table->NewUpdate());
          KuduPartialRow* row = update->mutable_row();
          GenerateDataForRow(schema_, 0, &rng_, row);
          CHECK_OK(session->Apply(update.release()));
        } else {
          unique_ptr<KuduInsert> insert(table->NewInsert());
          KuduPartialRow* row = insert->mutable_row();
          int32_t key;
          if (write_pattern_ == INSERT_SEQUENTIAL_ROWS ||
              write_pattern_ == INSERT_SEQUENTIAL_ROWS_WITH_DELETE) {
            key = sequential_key_gen_.Increment();
          } else {
            key = rng_.Next();
            if (write_pattern_ == INSERT_WITH_MANY_DUP_KEYS) {
              key %= kNumRowsForDuplicateKeyWorkload;
            }
          }
          keys.push_back(key);

          GenerateDataForRow(schema_, key, &rng_, row);
          if (payload_bytes_) {
            // Note: overriding payload_bytes_ requires the "simple" schema.
            std::string test_payload(payload_bytes_.get(), '0');
            CHECK_OK(row->SetStringCopy(2, test_payload));
          }
          CHECK_OK(session->Apply(insert.release()));

          inserted++;
        }
      }
      Status s = session->Flush();
      if (PREDICT_FALSE(!s.ok())) {
        inserted -= GetNumberOfErrors(session.get());
      }
      if (inserted > 0) {
        rows_inserted_.IncrementBy(inserted);
        batches_completed_.Increment();
      }
    }
    if (PREDICT_FALSE(write_interval_millis_ > 0)) {
      SleepFor(MonoDelta::FromMilliseconds(write_interval_millis_));
    }
    // Write delete row to cluster.
    if (write_pattern_ == INSERT_RANDOM_ROWS_WITH_DELETE ||
        write_pattern_ == INSERT_SEQUENTIAL_ROWS_WITH_DELETE) {
      for (auto key : keys) {
        unique_ptr<KuduDelete> op(table->NewDelete());
        KuduPartialRow* row = op->mutable_row();
        WriteValueToColumn(schema_, 0, key, row);
        CHECK_OK(session->Apply(op.release()));
        deleted++;
      }
      Status s = session->Flush();
      if (PREDICT_FALSE(!s.ok())) {
        deleted -= GetNumberOfErrors(session.get());
      }
      if (deleted > 0) {
        rows_deleted_.IncrementBy(deleted);
      }
    }
  }
}

#define CHECK_READ_OK(s) do {                               \
  const Status& __s = (s);                                  \
  if (read_errors_allowed_) {                               \
    if (PREDICT_FALSE(!__s.ok())) {                         \
      std::lock_guard<simple_spinlock> l(read_error_lock_); \
      read_errors_.emplace_back(__s);                       \
      return;                                               \
    }                                                       \
  } else {                                                  \
    CHECK_OK(__s);                                          \
  }                                                         \
} while (0)

void TestWorkload::ReadThread() {
  shared_ptr<KuduTable> table;
  OpenTable(&table);

  while (should_run_.Load()) {
    // Slow the scanners down to avoid imposing too much stress on already stressful tests.
    SleepFor(MonoDelta::FromMilliseconds(150));

    KuduScanner scanner(table.get());
    CHECK_OK(scanner.SetTimeoutMillis(read_timeout_millis_));
    CHECK_OK(scanner.SetSelection(selection_));
    if (fault_tolerant_) {
      CHECK_OK(scanner.SetFaultTolerant());
    }

    // Note: when INSERT_RANDOM_ROWS_WITH_DELETE is used, ReadThread doesn't really verify
    // anything except that a scan works.
    int64_t expected_min_rows = 0;
    if (write_pattern_ != INSERT_RANDOM_ROWS_WITH_DELETE && verify_num_rows_ &&
        !begin_txn_ && !txn_id_.IsValid()) {
      expected_min_rows = rows_inserted_.Load();
    }
    size_t row_count = 0;

    CHECK_READ_OK(scanner.Open());
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      CHECK_READ_OK(scanner.NextBatch(&batch));
      row_count += batch.NumRows();
    }

    CHECK_GE(row_count, expected_min_rows);
  }
}

#undef CHECK_READ_OK

size_t TestWorkload::GetNumberOfErrors(KuduSession* session) {
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  CHECK(!overflow);
  for (const KuduError* e : errors) {
    if ((timeout_allowed_ && e->status().IsTimedOut()) ||
        (not_found_allowed_ && e->status().IsNotFound()) ||
        (already_present_allowed_ && e->status().IsAlreadyPresent()) ||
        (network_error_allowed_ && e->status().IsNetworkError()) ||
        (remote_error_allowed_ && e->status().IsRemoteError())) {
      continue;
    }
    LOG(FATAL) << e->status().ToString();
  }
  return errors.size();
}

shared_ptr<KuduClient> TestWorkload::CreateClient() {
  CHECK_OK(cluster_->CreateClient(&client_builder_, &client_));
  return client_;
}

void TestWorkload::Setup() {
  if (begin_txn_) {
    CHECK(!txn_id_.IsValid()) << "Cannot begin txn and supply txn ID at the same time";
  }
  if (commit_txn_ || rollback_txn_) {
    CHECK(txn_id_.IsValid() || begin_txn_) << "Must participate in a txn to commit or abort";
  }
  if (!client_) {
    CHECK_OK(cluster_->CreateClient(&client_builder_, &client_));
  }

  bool table_exists;

  // Retry KuduClient::TableExists() until we make that call retry reliably.
  // See KUDU-1074.
  MonoTime deadline(MonoTime::Now() + MonoDelta::FromSeconds(10));
  Status s;
  while (true) {
    s = client_->TableExists(table_name_, &table_exists);
    if (s.ok() || MonoTime::Now() > deadline) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  CHECK_OK(s);

  if (!table_exists) {
    // Create the table.
    unique_ptr<KuduTableCreator> creator_ptr(client_->NewTableCreator());

    // Just an eye candy to use dots (.) only, not dots and arrows (->) mix.
    KuduTableCreator& table_creator = *creator_ptr;
    table_creator
        .table_name(table_name_)
        .schema(&schema_)
        .wait(wait_for_create_)
        .num_replicas(num_replicas_);

    switch (partitioning_) {
      case PartitioningType::HASH:
        table_creator.add_hash_partitions({ "key" }, num_tablets_);
        break;
      case PartitioningType::RANGE:
        {
          // Create split rows.
          vector<const KuduPartialRow*> splits;
          for (int i = 1; i < num_tablets_; i++) {
            KuduPartialRow* r = schema_.NewRow();
            CHECK_OK(r->SetInt32("key",
                                 MathLimits<int32_t>::kMax / num_tablets_ * i));
            splits.push_back(r);
          }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
          table_creator
              .set_range_partition_columns({ "key" })
              .split_rows(splits);
#pragma GCC diagnostic pop
        }
        break;
      default:
        LOG(FATAL) << "unexpected partitioning type for test table";
        return; // unreachable
    }

    const auto s = table_creator.Create();
    if (!s.ok()) {
      if (!s.IsAlreadyPresent() && !s.IsServiceUnavailable()) {
        // TODO(KUDU-1537): Should be fixed with Exactly Once semantics.
        LOG(FATAL) << s.ToString();
      }

      // If Create() failed in a non-fatal way, we still need to wait for the
      // table to finish creating.
      MonoTime deadline(MonoTime::Now() + client_->default_admin_operation_timeout());
      while (true) {
        bool still_creating;
        CHECK_OK(client_->IsCreateTableInProgress(table_name_, &still_creating));
        if (!still_creating) {
          break;
        }
        if (MonoTime::Now() > deadline) {
          LOG(FATAL) << "Timed out waiting for table to finish creating";
        }
        SleepFor(MonoDelta::FromMilliseconds(10));
      }
    }
  } else {
    KuduSchema existing_schema;
    CHECK_OK(client_->GetTableSchema(table_name_, &existing_schema));
    CHECK(schema_ == existing_schema)
        << "Existing table's schema doesn't match ours";
    LOG(INFO) << "TestWorkload: Skipping table creation because table "
              << table_name_ << " already exists";
  }


  if (write_pattern_ == UPDATE_ONE_ROW) {
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(20000);
    CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    shared_ptr<KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    std::unique_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    Random r(rng_.Next32());
    GenerateDataForRow(schema_, 0, &r, row);
    CHECK_OK(session->Apply(insert.release()));
    rows_inserted_.Store(1);
  }
}

void TestWorkload::Start() {
  CHECK(!should_run_.Load()) << "Already started";
  should_run_.Store(true);
  start_latch_.Reset(num_write_threads_ + num_read_threads_);
  if (txn_id_.IsValid()) {
    // TODO(awong): add an API to set the keepalive. For now just use an
    // arbitrary, short default value.
    CHECK(!txn_);
    TxnTokenPB txn_token_pb;
    txn_token_pb.set_txn_id(txn_id_.value());
    txn_token_pb.set_enable_keepalive(true);
    txn_token_pb.set_keepalive_millis(1000);
    string txn_token_str;
    CHECK(txn_token_pb.SerializeToString(&txn_token_str));
    CHECK_OK(KuduTransaction::Deserialize(client_, txn_token_str, &txn_));
  }
  if (begin_txn_) {
    CHECK(!txn_);
    CHECK(!txn_id_.IsValid());
    CHECK_OK(client_->NewTransaction(&txn_));
    string txn_str;
    CHECK_OK(txn_->Serialize(&txn_str));
    TxnTokenPB txn_token_pb;
    CHECK(txn_token_pb.ParseFromString(txn_str));
    CHECK(txn_token_pb.has_txn_id());
    txn_id_ = TxnId(txn_token_pb.txn_id());
  }
  for (int i = 0; i < num_write_threads_; i++) {
    threads_.emplace_back(&TestWorkload::WriteThread, this);
  }
  // Start the read threads. Order matters here, the read threads are last so that
  // we'll have a chance to do some scans after all writers are done.
  for (int i = 0; i < num_read_threads_; i++) {
    threads_.emplace_back(&TestWorkload::ReadThread, this);
  }
}

Status TestWorkload::Cleanup() {
  // Should be run only when workload is inactive.
  CHECK(!should_run_.Load() && threads_.empty());
  if (!client_) {
    CHECK_OK(cluster_->CreateClient(&client_builder_, &client_));
  }
  return client_->DeleteTable(table_name_);
}

void TestWorkload::StopAndJoin() {
  should_run_.Store(false);
  start_latch_.Reset(0);
  for (auto& t : threads_) {
    t.join();
  }
  threads_.clear();
  if (txn_) {
    if (commit_txn_) {
      CHECK_OK(txn_->Commit());
    }
    if (rollback_txn_) {
      CHECK_OK(txn_->Rollback());
    }
  }
}

} // namespace kudu
