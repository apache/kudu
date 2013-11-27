// Copyright (c) 2013, Cloudera,inc.

#include <boost/bind.hpp>
#include "common/wire_protocol.h"
#include "client/client.h"
#include "client/meta_cache.h"
#include "gutil/strings/substitute.h"
#include "master/master.h" // TODO: remove this include - just needed for default port
#include "master/master.proxy.h"
#include "rpc/messenger.h"
#include "util/async_util.h"
#include "util/countdown_latch.h"
#include "util/net/dns_resolver.h"
#include "util/net/net_util.h"
#include "util/status.h"
#include <tr1/memory>
#include <vector>

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using kudu::master::MasterServiceProxy;
using kudu::tserver::ColumnRangePredicatePB;
using kudu::tserver::NewScanRequestPB;
using kudu::tserver::ScanRequestPB;
using kudu::tserver::ScanResponsePB;
using kudu::tserver::TabletServerServiceProxy;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;

namespace kudu {
namespace client {

KuduClientOptions::KuduClientOptions() {
}

Status KuduClient::Create(const KuduClientOptions& options,
                          std::tr1::shared_ptr<KuduClient>* client) {
  shared_ptr<KuduClient> c(new KuduClient(options));
  RETURN_NOT_OK(c->Init());
  client->swap(c);
  return Status::OK();
}

KuduClient::KuduClient(const KuduClientOptions& options)
  : initted_(false),
    options_(options) {
}

Status KuduClient::Init() {
  // Init messenger.
  MessengerBuilder builder("client");
  RETURN_NOT_OK(builder.Build(&messenger_));

  // Init proxy.
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(options_.master_server_addr,
                                 master::Master::kDefaultPort, &addrs));

  if (addrs.size() > 1) {
    LOG(WARNING) << "Specified master server address '" << options_.master_server_addr << "' "
                 << "resolved to multiple IPs. Using " << addrs[0].ToString();
  }
  master_proxy_.reset(new MasterServiceProxy(messenger_, addrs[0]));

  meta_cache_.reset(new MetaCache(this));
  dns_resolver_.reset(new DnsResolver());

  initted_ = true;

  return Status::OK();
}

Status KuduClient::OpenTable(const std::string& table_name,
                             shared_ptr<KuduTable>* table) {
  CHECK(initted_) << "Must Init()";
  // In the future, probably will look up the table in some map to reuse KuduTable
  // instances.
  shared_ptr<KuduTable> ret(new KuduTable(shared_from_this(), table_name));
  RETURN_NOT_OK(ret->Open());
  table->swap(ret);

  return Status::OK();
}

Status KuduClient::GetTabletProxy(const std::string& tablet_id,
                                  shared_ptr<TabletServerServiceProxy>* proxy) {
  // TODO: write a proper async version of this for async client.
  shared_ptr<RemoteTablet> remote_tablet;
  meta_cache_->LookupTablet(tablet_id, &remote_tablet);

  CountDownLatch latch(1);
  Status s;
  remote_tablet->Refresh(this, AssignStatusAndTriggerLatch(&s, &latch), false);
  latch.Wait();
  RETURN_NOT_OK(s);

  RemoteTabletServer* ts = remote_tablet->replica_tserver(0);
  if (ts == NULL) {
    return Status::NotFound(Substitute("No replicas for tablet $0", tablet_id));
  }

  latch.Reset(1);
  ts->RefreshProxy(this, AssignStatusAndTriggerLatch(&s, &latch), false);
  latch.Wait();
  RETURN_NOT_OK(s);

  *proxy = ts->proxy();
  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduTable
////////////////////////////////////////////////////////////

KuduTable::KuduTable(const std::tr1::shared_ptr<KuduClient>& client,
                     const std::string& name)
  : client_(client),
    name_(name) {
}

Status KuduTable::Open() {
  // For now, use the table name as the tablet ID.
  RETURN_NOT_OK(client_->GetTabletProxy(name_, &proxy_));
  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduScanner
////////////////////////////////////////////////////////////

KuduScanner::KuduScanner(KuduTable* table)
  : open_(false),
    table_(DCHECK_NOTNULL(table)) {
  CHECK(table->is_open()) << "Table not open";
}

KuduScanner::~KuduScanner() {
  Close();
}

Status KuduScanner::SetProjection(const Schema& projection) {
  CHECK(!open_) << "Scanner already open";
  projection_ = projection;
  NewScanRequestPB* scan = next_req_.mutable_new_scan_request();
  RETURN_NOT_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  return Status::OK();
}

Status KuduScanner::AddConjunctPredicate(const ColumnRangePredicatePB& pb) {
  CHECK(!open_) << "Scanner already open";
  NewScanRequestPB* scan = next_req_.mutable_new_scan_request();
  scan->add_range_predicates()->CopyFrom(pb);
  return Status::OK();
}

Status KuduScanner::Open() {
  CHECK(!open_) << "Scanner already open";

  next_req_.mutable_new_scan_request()->set_tablet_id(table_->name());

  controller_.Reset();
  // TODO: make configurable through API
  const int kOpenTimeoutMs = 5000;
  controller_.set_timeout(MonoDelta::FromMilliseconds(kOpenTimeoutMs));

  RETURN_NOT_OK(table_->proxy_->Scan(next_req_, &last_response_, &controller_));
  RETURN_NOT_OK(CheckForErrors());
  CHECK(!last_response_.has_data()) << "TODO: handle data with initial response";

  next_req_.clear_new_scan_request();
  if (last_response_.has_more_results()) {
    next_req_.set_scanner_id(last_response_.scanner_id());
    VLOG(1) << "Started scanner " << last_response_.scanner_id();
  } else {
    VLOG(1) << "Scanner matched no rows, no scanner ID assigned.";
  }

  open_ = true;
  return Status::OK();
}

namespace {
// Callback for the RPC sent by Close().
// We can't use the KuduScanner response and RPC controller members for this
// call, because the scanner object may be destructed while the call is still
// being processed.
struct CloseCallback {
  RpcController controller;
  ScanResponsePB response;
  string scanner_id;
  void Callback() {
    if (!controller.status().ok()) {
      LOG(WARNING) << "Couldn't close scanner " << scanner_id << ": "
                   << controller.status().ToString();
    }
    delete this;
  }
};
} // anonymous namespace

void KuduScanner::Close() {
  if (!open_) return;

  if (next_req_.scanner_id().empty()) {
    // In the case that the scan matched no rows, and this was determined
    // in the Open() call, then we won't have been assigned a scanner ID
    // at all. So, no need to close on the server side.
    open_ = false;
    return;
  }

  CloseCallback* closer = new CloseCallback;
  closer->scanner_id = next_req_.scanner_id();
  next_req_.set_batch_size_bytes(0);
  next_req_.set_close_scanner(true);
  closer->controller.set_timeout(MonoDelta::FromMilliseconds(5000));
  table_->proxy_->ScanAsync(next_req_, &closer->response, &closer->controller,
                            boost::bind(&CloseCallback::Callback, closer));
  next_req_.Clear();
  open_ = false;
}

Status KuduScanner::CheckForErrors() {
  if (PREDICT_TRUE(!last_response_.has_error())) {
    return Status::OK();
  }

  return StatusFromPB(last_response_.error().status());
}

bool KuduScanner::HasMoreRows() const {
  CHECK(open_);
  return last_response_.has_more_results();
}

Status KuduScanner::NextBatch(std::vector<const uint8_t*>* rows) {
  // TODO: do some double-buffering here -- when we return this batch
  // we should already have fired off the RPC for the next batch, but
  // need to do some swapping of the response objects around to avoid
  // stomping on the memory the user is looking at.
  controller_.Reset();
  rows->clear();
  RETURN_NOT_OK(table_->proxy_->Scan(next_req_, &last_response_, &controller_));
  RETURN_NOT_OK(CheckForErrors());

  RETURN_NOT_OK(ExtractRowsFromRowBlockPB(projection_, last_response_.mutable_data(), rows));
  return Status::OK();
}


} // namespace client
} // namespace kudu
