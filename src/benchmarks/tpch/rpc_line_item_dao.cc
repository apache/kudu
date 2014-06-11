// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <vector>
#include <tr1/memory>
#include <utility>

#include "gutil/map-util.h"
#include "gutil/gscoped_ptr.h"

#include "client/client.h"
#include "client/meta_cache.h"
#include "client/write_op.h"
#include "common/row.h"
#include "common/partial_row.h"
#include "common/row_operations.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/wire_protocol.h"
#include "tserver/tserver_service.proxy.h"
#include "util/status.h"
#include "benchmarks/tpch/rpc_line_item_dao.h"
#include "util/locks.h"
#include "util/coding.h"
#include "gutil/stl_util.h"

using std::tr1::shared_ptr;

namespace kudu {

using tserver::TabletServerServiceProxy;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;
using client::Insert;
using client::Update;

namespace {

  class CountingCallback {
  public:
    explicit CountingCallback(shared_ptr<client::KuduSession> session, Atomic32 *ctr)
      : session_(session), ctr_(ctr) {
      base::subtle::NoBarrier_AtomicIncrement(ctr_, 1);
    }

    void operator()(Status s) {
      BatchFinished();
      CHECK_OK(s);
      base::subtle::NoBarrier_AtomicIncrement(ctr_, -1);
    }

  private:
    void BatchFinished() {
      int nerrs = session_->CountPendingErrors();
      if (nerrs) {
        LOG(WARNING) << nerrs << " errors occured during last batch.";
        vector<client::Error*> errors;
        ElementDeleter d(&errors);
        bool overflow;
        session_->GetPendingErrors(&errors, &overflow);
        if (overflow) {
          LOG(WARNING) << "Error overflow occured";
        }
        BOOST_FOREACH(client::Error* error, errors) {
          LOG(WARNING) << "FAILED: " << error->failed_op().ToString();
        }
      }
    }

    shared_ptr<client::KuduSession> session_;
    Atomic32 *ctr_;
  };

} // anonymous namespace

void RpcLineItemDAO::Init() {
  const Schema schema = tpch::CreateLineItemSchema();

  client::KuduClientOptions opts;
  opts.master_server_addr = master_address_;
  CHECK_OK(client::KuduClient::Create(opts, &client_));
  Status s = client_->OpenTable(table_name_, &client_table_);
  if (s.IsNotFound()) {
    CHECK_OK(client_->CreateTable(table_name_, schema));
    CHECK_OK(client_->OpenTable(table_name_, &client_table_));
  } else {
    CHECK_OK(s);
  }

  session_ = client_->NewSession();
  session_->SetTimeoutMillis(timeout_);
  CHECK_OK(session_->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
}

void RpcLineItemDAO::WriteLine(boost::function<void(PartialRow*)> f) {
  gscoped_ptr<Insert> insert = client_table_->NewInsert();
  f(insert->mutable_row());
  if (!ShouldAddKey(insert->row())) return;
  CHECK_OK(session_->Apply(&insert));
  ++batch_size_;
  if (batch_size_ == batch_max_) {
    batch_size_ = 0;
    session_->FlushAsync(CountingCallback(session_, &semaphore_));
  }
}

void RpcLineItemDAO::MutateLine(boost::function<void(PartialRow*)> f) {
  gscoped_ptr<Update> update = client_table_->NewUpdate();
  f(update->mutable_row());
  if (!ShouldAddKey(update->row())) return;
  CHECK_OK(session_->Apply(&update));
  ++batch_size_;
  if (batch_size_ == batch_max_) {
    batch_size_ = 0;
    session_->FlushAsync(CountingCallback(session_, &semaphore_));
  }
}

bool RpcLineItemDAO::ShouldAddKey(const PartialRow &row) {
  uint32_t l_ordernumber;
  CHECK_OK(row.GetUInt32(tpch::kOrderKeyColIdx, &l_ordernumber));
  uint32_t l_linenumber;
  CHECK_OK(row.GetUInt32(tpch::kLineNumberColIdx, &l_linenumber));
  std::pair<uint32_t, uint32_t> composite_k(l_ordernumber, l_linenumber);
  return InsertIfNotPresent(&orders_in_request_, composite_k);
}

void RpcLineItemDAO::FinishWriting() {
  CHECK_OK(session_->Flush());
  while (base::subtle::NoBarrier_Load(&semaphore_)) {
    usleep(timeout_ * 10); // 1/100th of timeout
  }
}

void RpcLineItemDAO::OpenScanner(const Schema &query_schema, ScanSpec *spec) {
  client::KuduScanner *scanner = new client::KuduScanner(client_table_.get());
  current_scanner_.reset(scanner);
  CHECK_OK(current_scanner_->SetProjection(&query_schema));
  BOOST_FOREACH(const ColumnRangePredicate& pred, spec->predicates()) {
    CHECK_OK(current_scanner_->AddConjunctPredicate(pred));
  }
  CHECK_OK(current_scanner_->Open());
}

bool RpcLineItemDAO::HasMore() {
  bool has_more = current_scanner_->HasMoreRows();
  if (!has_more) {
    current_scanner_->Close();
  }
  return has_more;
}


void RpcLineItemDAO::GetNext(vector<const uint8_t*> *rows) {
  CHECK_OK(current_scanner_->NextBatch(rows));
}

bool RpcLineItemDAO::IsTableEmpty() {
  return true;
}

void RpcLineItemDAO::GetNext(RowBlock *block) {}
RpcLineItemDAO::~RpcLineItemDAO() {
  FinishWriting();
}

RpcLineItemDAO::RpcLineItemDAO(const string& master_address,
                               const string& table_name,
                               const int batch_size,
                               const int mstimeout)
  : master_address_(master_address), table_name_(table_name),
    timeout_(mstimeout), batch_max_(batch_size), batch_size_(0) {
  base::subtle::NoBarrier_Store(&semaphore_, 0);
}

} // namespace kudu
