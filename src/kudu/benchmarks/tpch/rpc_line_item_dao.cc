// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <vector>
#include <tr1/memory>
#include <utility>

#include "kudu/benchmarks/tpch/rpc_line_item_dao.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/write_op.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/util/coding.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

using std::tr1::shared_ptr;

namespace kudu {

using client::KuduInsert;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnRangePredicate;
using client::KuduError;
using client::KuduRowResult;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSession;
using client::KuduUpdate;
using std::vector;

namespace {

class FlushCallback : public RefCountedThreadSafe<FlushCallback> {
 public:
  FlushCallback(shared_ptr<KuduSession> session, Semaphore *sem)
    : session_(session),
      sem_(sem) {
    sem_->Acquire();
  }

  void StatusCB(const Status& s) {
    BatchFinished();
    CHECK_OK(s);
    sem_->Release();
  }

  StatusCallback AsStatusCallback() {
    return Bind(&FlushCallback::StatusCB, this);
  }

 private:
  void BatchFinished() {
    int nerrs = session_->CountPendingErrors();
    if (nerrs) {
      LOG(WARNING) << nerrs << " errors occured during last batch.";
      vector<KuduError*> errors;
      ElementDeleter d(&errors);
      bool overflow;
      session_->GetPendingErrors(&errors, &overflow);
      if (overflow) {
        LOG(WARNING) << "Error overflow occured";
      }
      BOOST_FOREACH(KuduError* error, errors) {
        LOG(WARNING) << "FAILED: " << error->failed_op().ToString();
      }
    }
  }

  shared_ptr<KuduSession> session_;
  Semaphore *sem_;
};

} // anonymous namespace

void RpcLineItemDAO::Init() {
  const KuduSchema schema = tpch::CreateLineItemSchema();

  CHECK_OK(KuduClientBuilder()
           .add_master_server_addr(master_address_)
           .default_rpc_timeout(timeout_)
           .Build(&client_));
  Status s = client_->OpenTable(table_name_, &client_table_);
  if (s.IsNotFound()) {
    CHECK_OK(client_->NewTableCreator()
             ->table_name(table_name_)
             .schema(&schema)
             .Create());
    CHECK_OK(client_->OpenTable(table_name_, &client_table_));
  } else {
    CHECK_OK(s);
  }

  session_ = client_->NewSession();
  session_->SetTimeoutMillis(timeout_.ToMilliseconds());
  CHECK_OK(session_->SetFlushMode(KuduSession::MANUAL_FLUSH));
}

void RpcLineItemDAO::WriteLine(boost::function<void(KuduPartialRow*)> f) {
  gscoped_ptr<KuduInsert> insert = client_table_->NewInsert();
  f(insert->mutable_row());
  if (!ShouldAddKey(insert->row())) return;
  CHECK_OK(session_->Apply(insert.Pass()));
  ++batch_size_;
  FlushIfBufferFull();
}

void RpcLineItemDAO::FlushIfBufferFull() {
  if (batch_size_ < batch_max_) return;

  batch_size_ = 0;
  orders_in_request_.clear();

  FlushCallback* cb = new FlushCallback(session_, &semaphore_);
  // The callback object will free 'cb' after it is invoked.
  session_->FlushAsync(cb->AsStatusCallback());
}

void RpcLineItemDAO::MutateLine(boost::function<void(KuduPartialRow*)> f) {
  gscoped_ptr<KuduUpdate> update = client_table_->NewUpdate();
  f(update->mutable_row());
  if (!ShouldAddKey(update->row())) return;
  CHECK_OK(session_->Apply(update.Pass()));
  ++batch_size_;
  FlushIfBufferFull();
}

bool RpcLineItemDAO::ShouldAddKey(const KuduPartialRow &row) {
  uint32_t l_ordernumber;
  CHECK_OK(row.GetUInt32(tpch::kOrderKeyColIdx, &l_ordernumber));
  uint32_t l_linenumber;
  CHECK_OK(row.GetUInt32(tpch::kLineNumberColIdx, &l_linenumber));
  std::pair<uint32_t, uint32_t> composite_k(l_ordernumber, l_linenumber);
  return InsertIfNotPresent(&orders_in_request_, composite_k);
}
void RpcLineItemDAO::FinishWriting() {
  semaphore_.Acquire();
  CHECK_OK(session_->Flush());
  semaphore_.Release();
}

void RpcLineItemDAO::OpenScanner(const KuduSchema& query_schema,
                                 const vector<KuduColumnRangePredicate>& preds) {
  KuduScanner *scanner = new KuduScanner(client_table_.get());

  current_scanner_.reset(scanner);
  current_scanner_projection_.reset(new KuduSchema(query_schema));
  CHECK_OK(current_scanner_->SetProjection(current_scanner_projection_.get()));
  BOOST_FOREACH(const KuduColumnRangePredicate& pred, preds) {
    CHECK_OK(current_scanner_->AddConjunctPredicate(pred));
  }
  CHECK_OK(current_scanner_->Open());
}

void RpcLineItemDAO::OpenTpch1Scanner() {
  KuduSchema schema(tpch::CreateTpch1QuerySchema());
  Slice date("1998-09-02");
  vector<KuduColumnRangePredicate> preds;
  KuduColumnRangePredicate pred1(schema.Column(0), NULL, &date);
  preds.push_back(pred1);
  OpenScanner(schema, preds);
}

bool RpcLineItemDAO::HasMore() {
  bool has_more = current_scanner_->HasMoreRows();
  if (!has_more) {
    current_scanner_->Close();
  }
  return has_more;
}


void RpcLineItemDAO::GetNext(vector<KuduRowResult> *rows) {
  rows->clear();
  CHECK_OK(current_scanner_->NextBatch(rows));
}

bool RpcLineItemDAO::IsTableEmpty() {
  KuduScanner scanner(client_table_.get());
  CHECK_OK(scanner.Open());
  return !scanner.HasMoreRows();
}

RpcLineItemDAO::~RpcLineItemDAO() {
  FinishWriting();
}

RpcLineItemDAO::RpcLineItemDAO(const string& master_address,
                               const string& table_name,
                               const int batch_size,
                               const int mstimeout)
  : master_address_(master_address), table_name_(table_name),
    timeout_(MonoDelta::FromMilliseconds(mstimeout)),
    batch_max_(batch_size),
    batch_size_(0),
    semaphore_(1) {
}

} // namespace kudu
