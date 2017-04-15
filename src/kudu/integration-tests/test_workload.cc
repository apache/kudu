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

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/schema-internal.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/env.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

namespace kudu {

using client::KuduClient;
using client::KuduInsert;
using client::KuduScanBatch;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSchemaFromSchema;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableCreator;
using client::KuduUpdate;
using client::sp::shared_ptr;

const char* const TestWorkload::kDefaultTableName = "test-workload";

TestWorkload::TestWorkload(MiniClusterBase* cluster)
  : cluster_(cluster),
    rng_(SeedRandom()),
    payload_bytes_(11),
    num_write_threads_(4),
    num_read_threads_(0),
    // Set a high scanner timeout so that we're likely to have a chance to scan, even in
    // high-stress workloads.
    read_timeout_millis_(60000),
    write_batch_size_(50),
    write_timeout_millis_(20000),
    timeout_allowed_(false),
    not_found_allowed_(false),
    network_error_allowed_(false),
    num_replicas_(3),
    num_tablets_(1),
    table_name_(kDefaultTableName),
    start_latch_(0),
    should_run_(false),
    rows_inserted_(0),
    batches_completed_(0),
    sequential_key_gen_(0) {
  // Make the default write pattern random row inserts.
  set_write_pattern(INSERT_RANDOM_ROWS);
}

TestWorkload::~TestWorkload() {
  StopAndJoin();
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

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(write_timeout_millis_);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  while (should_run_.Load()) {
    int inserted = 0;
    for (int i = 0; i < write_batch_size_; i++) {
      if (write_pattern_ == UPDATE_ONE_ROW) {
        gscoped_ptr<KuduUpdate> update(table->NewUpdate());
        KuduPartialRow* row = update->mutable_row();
        CHECK_OK(row->SetInt32(0, 0));
        CHECK_OK(row->SetInt32(1, rng_.Next()));
        CHECK_OK(session->Apply(update.release()));
      } else {
        gscoped_ptr<KuduInsert> insert(table->NewInsert());
        KuduPartialRow* row = insert->mutable_row();
        int32_t key;
        if (write_pattern_ == INSERT_SEQUENTIAL_ROWS) {
          key = sequential_key_gen_.Increment();
        } else {
          key = rng_.Next();
          if (write_pattern_ == INSERT_WITH_MANY_DUP_KEYS) {
            key %= kNumRowsForDuplicateKeyWorkload;
          }
        }
        CHECK_OK(row->SetInt32(0, key));
        CHECK_OK(row->SetInt32(1, rng_.Next()));
        string test_payload("hello world");
        if (payload_bytes_ != 11) {
          // We fill with zeros if you change the default.
          test_payload.assign(payload_bytes_, '0');
        }
        CHECK_OK(row->SetStringCopy(2, test_payload));
        CHECK_OK(session->Apply(insert.release()));
        inserted++;
      }
    }

    Status s = session->Flush();

    if (PREDICT_FALSE(!s.ok())) {
      std::vector<client::KuduError*> errors;
      ElementDeleter d(&errors);
      bool overflow;
      session->GetPendingErrors(&errors, &overflow);
      CHECK(!overflow);
      for (const client::KuduError* e : errors) {
        if (timeout_allowed_ && e->status().IsTimedOut()) {
          continue;
        }

        if (not_found_allowed_ && e->status().IsNotFound()) {
          continue;
        }

        if (already_present_allowed_ && e->status().IsAlreadyPresent()) {
          continue;
        }

        if (network_error_allowed_ && e->status().IsNetworkError()) {
          continue;
        }

        LOG(FATAL) << e->status().ToString();
      }
      inserted -= errors.size();
    }

    if (inserted > 0) {
      rows_inserted_.IncrementBy(inserted);
      batches_completed_.Increment();
    }
  }
}

void TestWorkload::ReadThread() {
  shared_ptr<KuduTable> table;
  OpenTable(&table);

  while (should_run_.Load()) {
    // Slow the scanners down to avoid imposing too much stress on already stressful tests.
    SleepFor(MonoDelta::FromMilliseconds(150));

    KuduScanner scanner(table.get());
    CHECK_OK(scanner.SetTimeoutMillis(read_timeout_millis_));
    CHECK_OK(scanner.SetFaultTolerant());

    int64_t expected_row_count = rows_inserted_.Load();
    size_t row_count = 0;

    CHECK_OK(scanner.Open());
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      CHECK_OK(scanner.NextBatch(&batch));
      row_count += batch.NumRows();
    }

    CHECK_GE(row_count, expected_row_count);
  }
}

shared_ptr<KuduClient> TestWorkload::CreateClient() {
  CHECK_OK(cluster_->CreateClient(&client_builder_, &client_));
  return client_;
}

void TestWorkload::Setup() {
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
    KuduSchema client_schema(KuduSchemaFromSchema(GetSimpleTestSchema()));

    vector<const KuduPartialRow*> splits;
    for (int i = 1; i < num_tablets_; i++) {
      KuduPartialRow* r = client_schema.NewRow();
      CHECK_OK(r->SetInt32("key", MathLimits<int32_t>::kMax / num_tablets_ * i));
      splits.push_back(r);
    }

    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    Status s = table_creator->table_name(table_name_)
        .schema(&client_schema)
        .num_replicas(num_replicas_)
        .set_range_partition_columns({ "key" })
        .split_rows(splits)
        .Create();
    if (!s.ok() && !s.IsAlreadyPresent() && !s.IsServiceUnavailable()) {
      // TODO(KUDU-1537): Should be fixed with Exactly Once semantics.
      LOG(FATAL) << s.ToString();
    }
  } else {
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
    CHECK_OK(row->SetInt32(0, 0));
    CHECK_OK(row->SetInt32(1, 0));
    CHECK_OK(row->SetStringCopy(2, "hello world"));
    CHECK_OK(session->Apply(insert.release()));
    rows_inserted_.Store(1);
  }
}

void TestWorkload::Start() {
  CHECK(!should_run_.Load()) << "Already started";
  should_run_.Store(true);
  start_latch_.Reset(num_write_threads_ + num_read_threads_);
  for (int i = 0; i < num_write_threads_; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", strings::Substitute("test-writer-$0", i),
                                  &TestWorkload::WriteThread, this,
                                  &new_thread));
    threads_.push_back(new_thread);
  }
  // Start the read threads. Order matters here, the read threads are last so that
  // we'll have a chance to do some scans after all writers are done.
  for (int i = 0; i < num_read_threads_; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", strings::Substitute("test-reader-$0", i),
                                  &TestWorkload::ReadThread, this,
                                  &new_thread));
    threads_.push_back(new_thread);
  }
}

Status TestWorkload::Cleanup() {
  // Should be run only when workload is inactive.
  CHECK(!should_run_.Load() && threads_.empty());
  return client_->DeleteTable(table_name_);
}

void TestWorkload::StopAndJoin() {
  should_run_.Store(false);
  start_latch_.Reset(0);
  for (scoped_refptr<kudu::Thread> thr : threads_) {
   CHECK_OK(ThreadJoiner(thr.get()).Join());
  }
  threads_.clear();
}

} // namespace kudu
