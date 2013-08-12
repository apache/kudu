// Copyright (c) 2013, Cloudera,inc.

#include "common/wire_protocol.h"
#include "client/client.h"
#include "rpc/messenger.h"
#include "util/net/net_util.h"
#include "util/status.h"
#include "tserver/tablet_server.h" // TODO: remove this include - just needed for default port
#include <tr1/memory>

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using kudu::tserver::ColumnRangePredicatePB;
using kudu::tserver::NewScanRequestPB;
using kudu::tserver::ScanRequestPB;
using kudu::tserver::ScanResponsePB;
using kudu::tserver::TabletServerServiceProxy;
using kudu::tserver::TabletServer;
using kudu::rpc::MessengerBuilder;

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
  RETURN_NOT_OK(ParseAddressList(options_.tablet_server_addr, TabletServer::kDefaultPort, &addrs));

  if (addrs.size() > 1) {
    LOG(WARNING) << "Specified tablet server address '" << options_.tablet_server_addr << "' "
                 << "resolved to multiple IPs. Using " << addrs[0].ToString();
  }
  proxy_.reset(new TabletServerServiceProxy(messenger_, addrs[0]));

  return Status::OK();
}

Status KuduClient::OpenTable(const std::string& table_name,
                             shared_ptr<KuduTable>* table) {
  CHECK(proxy_) << "Must Init()";
  // In the future, probably will look up the table in some map to reuse KuduTable
  // instances.
  shared_ptr<KuduTable> ret(new KuduTable(shared_from_this(), table_name));
  RETURN_NOT_OK(ret->Open());
  table->swap(ret);

  return Status::OK();
}

Status KuduClient::GetTabletProxy(const std::string& tablet_id,
                                  shared_ptr<TabletServerServiceProxy>* proxy) {
  *proxy = proxy_;
  return Status::OK();
}


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

KuduScanner::KuduScanner(KuduTable* table)
  : open_(false),
    table_(DCHECK_NOTNULL(table)) {
  CHECK(table->is_open()) << "Table not open";
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
  const int kOpenTimeoutMs = 1000;
  controller_.set_timeout(MonoDelta::FromMilliseconds(kOpenTimeoutMs));

  RETURN_NOT_OK(table_->proxy_->Scan(next_req_, &last_response_, &controller_));
  RETURN_NOT_OK(CheckForErrors());
  CHECK(!last_response_.has_data()) << "TODO: handle data with initial response";

  next_req_.clear_new_scan_request();
  next_req_.set_scanner_id(last_response_.scanner_id());
  VLOG(1) << "Started scanner " << last_response_.scanner_id();

  open_ = true;
  return Status::OK();
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
