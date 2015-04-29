// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>

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
#include "kudu/util/thread.h"

namespace kudu {

using client::FromInternalCompressionType;
using client::FromInternalDataType;
using client::FromInternalEncodingType;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduColumnStorageAttributes;
using client::KuduInsert;
using client::KuduSchema;
using client::KuduSession;
using client::KuduTable;
using std::tr1::shared_ptr;

static const char* kTableName = "test-workload";

TestWorkload::TestWorkload(ExternalMiniCluster* cluster)
  : cluster_(cluster),
    num_write_threads_(4),
    write_batch_size_(50),
    write_timeout_millis_(20000),
    timeout_allowed_(false),
    num_replicas_(3),
    start_latch_(0),
    should_run_(false),
    rows_inserted_(0) {
}

TestWorkload::~TestWorkload() {
}

void TestWorkload::WriteThread() {
  Random r(Env::Default()->gettid());

  scoped_refptr<KuduTable> table;
  // Loop trying to open up the table. In some tests we set up very
  // low RPC timeouts to test those behaviors, so this might fail and
  // need retrying.
  while (should_run_.Load()) {
    Status s = client_->OpenTable(kTableName, &table);
    if (s.ok()) {
      break;
    }
    if (timeout_allowed_ && s.IsTimedOut()) {
      SleepFor(MonoDelta::FromMilliseconds(50));
      continue;
    }
    CHECK_OK(s);
  }

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(write_timeout_millis_);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Wait for all of the workload threads to be ready to go. This maximizes the chance
  // that they all send a flood of requests at exactly the same time.
  //
  // This also minimizes the chance that we see failures to call OpenTable() if
  // a late-starting thread overlaps with the flood of outbound traffic from the
  // ones that are already writing data.
  start_latch_.CountDown();
  start_latch_.Wait();

  while (should_run_.Load()) {
    for (int i = 0; i < write_batch_size_; i++) {
      gscoped_ptr<KuduInsert> insert = table->NewInsert();
      KuduPartialRow* row = insert->mutable_row();
      CHECK_OK(row->SetInt32(0, r.Next()));
      CHECK_OK(row->SetInt32(1, r.Next()));
      CHECK_OK(row->SetStringCopy(2, "hello world"));
      CHECK_OK(session->Apply(insert.Pass()));
    }

    int inserted = write_batch_size_;

    Status s = session->Flush();

    if (PREDICT_FALSE(!s.ok())) {
      std::vector<client::KuduError*> errors;
      ElementDeleter d(&errors);
      bool overflow;
      session->GetPendingErrors(&errors, &overflow);
      CHECK(!overflow);
      BOOST_FOREACH(const client::KuduError* e, errors) {
        if (timeout_allowed_ && e->status().IsTimedOut()) {
          continue;
        }

        // We don't handle write idempotency yet. (i.e making sure that when a leader fails
        // writes to it that were eventually committed by the new leader but un-ackd to the
        // client are not retried), so some errors are expected.
        // It's OK as long as the errors are Status::AlreadyPresent();
        CHECK(e->status().IsAlreadyPresent()) << "Unexpected error: " << e->status().ToString();
      }
      inserted -= errors.size();
    }

    rows_inserted_.IncrementBy(inserted);
  }
}

namespace {

KuduSchema GetClientSchema(const Schema& server_schema) {
  std::vector<KuduColumnSchema> client_cols;
  BOOST_FOREACH(const ColumnSchema& col, server_schema.columns()) {
    CHECK_EQ(col.has_read_default(), col.has_write_default());
    if (col.has_read_default()) {
      CHECK_EQ(col.read_default_value(), col.write_default_value());
    }
    KuduColumnStorageAttributes client_attrs(
      FromInternalEncodingType(col.attributes().encoding()),
      FromInternalCompressionType(col.attributes().compression()));
    KuduColumnSchema client_col(col.name(), FromInternalDataType(col.type_info()->type()),
                                col.is_nullable(), col.read_default_value(),
                                client_attrs);
    client_cols.push_back(client_col);
  }
  return KuduSchema(client_cols, server_schema.num_key_columns());
}
} // anonymous namespace


void TestWorkload::Setup() {
  CHECK_OK(cluster_->CreateClient(client_builder_, &client_));

  KuduSchema client_schema(GetClientSchema(GetSimpleTestSchema()));
  CHECK_OK(client_->NewTableCreator()
            ->table_name(kTableName)
            .schema(&client_schema)
            .num_replicas(num_replicas_)
            // NOTE: this is quite high as a timeout, but the default (5 sec) does not
            // seem to be high enough in some cases (see KUDU-550). We should remove
            // this once that ticket is addressed.
            .timeout(MonoDelta::FromSeconds(20))
            .Create());
}

void TestWorkload::Start() {
  CHECK(!should_run_.Load()) << "Already started";
  should_run_.Store(true);
  start_latch_.Reset(num_write_threads_);
  for (int i = 0; i < num_write_threads_; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", strings::Substitute("test-writer-$0", i),
                                  &TestWorkload::WriteThread, this,
                                  &new_thread));
    threads_.push_back(new_thread);
  }
}

void TestWorkload::StopAndJoin() {
  should_run_.Store(false);
  start_latch_.Reset(0);
  BOOST_FOREACH(scoped_refptr<kudu::Thread> thr, threads_) {
   CHECK_OK(ThreadJoiner(thr.get()).Join());
  }
  threads_.clear();
}

} // namespace kudu
