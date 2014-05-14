// Copyright (c) 2013, Cloudera,inc.

#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>

#include "client/batcher.h"
#include "client/client.h"
#include "client/error_collector.h"
#include "client/meta_cache.h"
#include "common/row.h"
#include "common/wire_protocol.h"
#include "gutil/casts.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "master/master.h" // TODO: remove this include - just needed for default port
#include "master/master.proxy.h"
#include "rpc/messenger.h"
#include "util/async_util.h"
#include "util/countdown_latch.h"
#include "util/net/dns_resolver.h"
#include "util/net/net_util.h"
#include "util/status.h"

#include <algorithm>
#include <tr1/memory>
#include <vector>

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using kudu::master::MasterServiceProxy;
using kudu::master::AlterTableRequestPB;
using kudu::master::AlterTableResponsePB;
using kudu::master::CreateTableRequestPB;
using kudu::master::CreateTableResponsePB;
using kudu::master::DeleteTableRequestPB;
using kudu::master::DeleteTableResponsePB;
using kudu::master::GetTableSchemaRequestPB;
using kudu::master::GetTableSchemaResponsePB;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::IsAlterTableDoneRequestPB;
using kudu::master::IsAlterTableDoneResponsePB;
using kudu::master::IsCreateTableDoneRequestPB;
using kudu::master::IsCreateTableDoneResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::TabletLocationsPB;
using kudu::tserver::ColumnRangePredicatePB;
using kudu::tserver::NewScanRequestPB;
using kudu::tserver::ScanRequestPB;
using kudu::tserver::ScanResponsePB;
using kudu::tserver::TabletServerServiceProxy;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;

MAKE_ENUM_LIMITS(kudu::client::KuduSession::FlushMode,
                 kudu::client::KuduSession::AUTO_FLUSH_SYNC,
                 kudu::client::KuduSession::MANUAL_FLUSH);

namespace kudu {
namespace client {

using internal::Batcher;
using internal::ErrorCollector;

// Retry helper, takes a function like: Status funcName(const MonoTime& deadline, bool *retry, ...)
// The function should set the retry flag (default true) if the function should
// be retried again. On retry == false the return status of the function will be
// returned to the caller, otherwise a Status::Timeout() will be returned.
// If the deadline is already expired, no attempt will be made.
static Status RetryFunc(const MonoTime& deadline,
                        const string& retry_msg,
                        const string& timeout_msg,
                        const boost::function<Status(const MonoTime&, bool*)>& func) {
  MonoTime now = MonoTime::Now(MonoTime::FINE);
  if (!now.ComesBefore(deadline)) {
    return Status::TimedOut(timeout_msg);
  }

  int64_t wait_time = 1000;
  while (1) {
    MonoTime stime = now;
    bool retry = true;
    Status s = func(deadline, &retry);
    if (!retry) {
      return s;
    }

    now = MonoTime::Now(MonoTime::FINE);
    if (!now.ComesBefore(deadline)) {
      break;
    }

    VLOG(1) << retry_msg << " status=" << s.ToString();
    int64_t timeout_usec = deadline.GetDeltaSince(now).ToNanoseconds() -
                           now.GetDeltaSince(stime).ToNanoseconds();
    if (timeout_usec > 0) {
      wait_time = std::min(wait_time * 5 / 4, timeout_usec);
      usleep(wait_time);
      now = MonoTime::Now(MonoTime::FINE);
    }
  }

  return Status::TimedOut(timeout_msg);
}

KuduClientOptions::KuduClientOptions()
  : default_admin_operation_timeout(MonoDelta::FromMilliseconds(5 * 1000)) {
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
  if (options_.messenger) {
    messenger_ = options_.messenger;
  } else {
    MessengerBuilder builder("client");
    RETURN_NOT_OK(builder.Build(&messenger_));
  }

  // Init proxy.
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(options_.master_server_addr,
                                 master::Master::kDefaultPort, &addrs));
  if (addrs.empty()) {
    return Status::InvalidArgument("No master address specified");
  }
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

Status KuduClient::CreateTable(const std::string& table_name,
                               const Schema& schema) {
  return CreateTable(table_name, schema, CreateTableOptions());
}

Status KuduClient::CreateTable(const std::string& table_name,
                               const Schema& schema,
                               const CreateTableOptions& opts) {
  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(options_.default_admin_operation_timeout);

  // Build request.
  req.set_name(table_name);
  if (opts.num_replicas_ >= 1) {
    req.set_num_replicas(opts.num_replicas_);
  }
  RETURN_NOT_OK_PREPEND(SchemaToPB(schema, req.mutable_schema()),
                        "Invalid schema");
  BOOST_FOREACH(const std::string& key, opts.split_keys_) {
    req.add_pre_split_keys(key);
  }

  // Send it.
  RETURN_NOT_OK(master_proxy_->CreateTable(req, &resp, &rpc));
  if (resp.has_error()) {
    // TODO: if already exist and in progress spin
    return StatusFromPB(resp.error().status());
  }

  // Spin until the table is fully created, if requested.
  if (opts.wait_assignment_) {
    // TODO: make the wait time configurable
    MonoTime deadline = MonoTime::Now(MonoTime::FINE);
    deadline.AddDelta(MonoDelta::FromSeconds(15));
    RETURN_NOT_OK(RetryFunc(deadline,
          "Waiting on Create Table to be completed",
          "Timeout out waiting for Table Creation",
          boost::bind(&KuduClient::IsCreateTableInProgress, this, table_name, _1, _2)));
  }

  return Status::OK();
}

Status KuduClient::IsCreateTableInProgress(const std::string& table_name,
                                           bool *create_in_progress) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(options_.default_admin_operation_timeout);
  return IsCreateTableInProgress(table_name, deadline, create_in_progress);
}

Status KuduClient::IsCreateTableInProgress(const std::string& table_name,
                                           const MonoTime& deadline,
                                           bool *create_in_progress) {
  IsCreateTableDoneRequestPB req;
  IsCreateTableDoneResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE)));
  RETURN_NOT_OK(master_proxy_->IsCreateTableDone(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *create_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::DeleteTable(const std::string& table_name) {
  DeleteTableRequestPB req;
  DeleteTableResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(options_.default_admin_operation_timeout);
  RETURN_NOT_OK(master_proxy_->DeleteTable(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  return Status::OK();
}

Status KuduClient::AlterTable(const std::string& table_name,
                              const AlterTableBuilder& alter) {
  AlterTableRequestPB req;
  AlterTableResponsePB resp;
  RpcController rpc;

  if (!alter.has_changes()) {
    return Status::InvalidArgument("No alter steps provided");
  }

  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromMilliseconds(60 * 1000));

  req.CopyFrom(*alter.alter_steps_);
  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(options_.default_admin_operation_timeout);
  RETURN_NOT_OK(master_proxy_->AlterTable(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  string alter_name = req.has_new_table_name() ? req.new_table_name() : table_name;
  RETURN_NOT_OK(RetryFunc(deadline,
        "Waiting on Alter Table to be completed",
        "Timeout out waiting for AlterTable",
        boost::bind(&KuduClient::IsAlterTableInProgress, this, alter_name, _1, _2)));

  return Status::OK();
}

Status KuduClient::IsAlterTableInProgress(const std::string& table_name,
                                          bool *alter_in_progress) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(options_.default_admin_operation_timeout);
  return IsAlterTableInProgress(table_name, deadline, alter_in_progress);
}

Status KuduClient::IsAlterTableInProgress(const std::string& table_name,
                                          const MonoTime& deadline,
                                          bool *alter_in_progress) {
  IsAlterTableDoneRequestPB req;
  IsAlterTableDoneResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE)));
  RETURN_NOT_OK(master_proxy_->IsAlterTableDone(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *alter_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::GetTableSchema(const std::string& table_name,
                                  Schema *schema) {
  GetTableSchemaRequestPB req;
  GetTableSchemaResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(options_.default_admin_operation_timeout);
  RETURN_NOT_OK(master_proxy_->GetTableSchema(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  Schema server_schema;
  RETURN_NOT_OK(SchemaFromPB(resp.schema(), &server_schema));

  // Remove the server IDs from the schema
  schema->Reset(server_schema.columns(), server_schema.num_key_columns());
  return Status::OK();
}

Status KuduClient::OpenTable(const std::string& table_name,
                             scoped_refptr<KuduTable>* table) {
  CHECK(initted_) << "Must Init()";

  Schema schema;
  RETURN_NOT_OK(GetTableSchema(table_name, &schema));

  // In the future, probably will look up the table in some map to reuse KuduTable
  // instances.
  scoped_refptr<KuduTable> ret(new KuduTable(shared_from_this(), table_name, schema));
  RETURN_NOT_OK(ret->Open());
  table->swap(ret);

  return Status::OK();
}

shared_ptr<KuduSession> KuduClient::NewSession() {
  shared_ptr<KuduSession> ret(new KuduSession(shared_from_this()));
  ret->Init();
  return ret;
}

Status KuduClient::GetTabletProxy(const std::string& tablet_id,
                                  shared_ptr<TabletServerServiceProxy>* proxy) {
  // TODO: write a proper async version of this for async client.
  scoped_refptr<RemoteTablet> remote_tablet;
  meta_cache_->LookupTabletByID(tablet_id, &remote_tablet);

  RemoteTabletServer* ts = remote_tablet->LeaderTServer();
  if (PREDICT_FALSE(ts == NULL)) {
    return Status::ServiceUnavailable(Substitute("No LEADER for tablet $0", tablet_id));
  }

  Synchronizer s;
  ts->RefreshProxy(this, s.callback(), false);
  RETURN_NOT_OK(s.Wait());

  *proxy = ts->proxy();
  return Status::OK();
}

////////////////////////////////////////////////////////////
// CreateTableOptions
////////////////////////////////////////////////////////////

CreateTableOptions::CreateTableOptions()
  : wait_assignment_(true),
    num_replicas_(0) {
}

CreateTableOptions::~CreateTableOptions() {
}

CreateTableOptions& CreateTableOptions::WithSplitKeys(
    const std::vector<std::string>& keys) {
  split_keys_ = keys;
  return *this;
}

CreateTableOptions& CreateTableOptions::WithNumReplicas(int num_replicas) {
  num_replicas_ = num_replicas;
  return *this;
}

CreateTableOptions& CreateTableOptions::WaitAssignment(bool wait_assignment) {
  wait_assignment_ = wait_assignment;
  return *this;
}

////////////////////////////////////////////////////////////
// KuduTable
////////////////////////////////////////////////////////////

KuduTable::KuduTable(const std::tr1::shared_ptr<KuduClient>& client,
                     const std::string& name,
                     const Schema& schema)
  : client_(client),
    name_(name),
    schema_(schema) {
}

KuduTable::~KuduTable() {
}

Status KuduTable::Open() {
  // TODO: fetch the schema from the master here once catalog is available.
  GetTableLocationsRequestPB req;
  GetTableLocationsResponsePB resp;

  req.mutable_table()->set_table_name(name_);
  do {
    rpc::RpcController rpc;
    rpc.set_timeout(client_->options_.default_admin_operation_timeout);
    RETURN_NOT_OK(client_->master_proxy()->GetTableLocations(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }

    if (resp.tablet_locations_size() > 0)
      break;

    /* TODO: Add a timeout or number of retries */
    usleep(100000);
  } while (1);

  VLOG(1) << "Open Table " << name_ << ", found " << resp.tablet_locations_size() << " tablets";
  return Status::OK();
}

gscoped_ptr<Insert> KuduTable::NewInsert() {
  return gscoped_ptr<Insert>(new Insert(this));
}

////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////
Error::Error(gscoped_ptr<Insert> failed_op,
             const Status& status) :
  failed_op_(failed_op.Pass()),
  status_(status) {
}

Error::~Error() {
}

////////////////////////////////////////////////////////////
// KuduSession
////////////////////////////////////////////////////////////

KuduSession::KuduSession(const std::tr1::shared_ptr<KuduClient>& client)
  : client_(client),
    error_collector_(new ErrorCollector()),
    flush_mode_(AUTO_FLUSH_SYNC),
    timeout_ms_(0) {
}

KuduSession::~KuduSession() {
  if (batcher_->HasPendingOperations()) {
    LOG(WARNING) << "Closing Session with pending operations.";
  }
  batcher_->Abort();
}

void KuduSession::Init() {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK(!batcher_);
  NewBatcher(NULL);
}

void KuduSession::NewBatcher(scoped_refptr<Batcher>* old_batcher) {
  DCHECK(lock_.is_locked());

  scoped_refptr<Batcher> batcher(
    new Batcher(client_.get(), error_collector_.get(), shared_from_this()));
  batcher->SetTimeoutMillis(timeout_ms_);
  batcher.swap(batcher_);

  if (old_batcher) {
    old_batcher->swap(batcher);
  }
}

Status KuduSession::SetFlushMode(FlushMode m) {
  if (batcher_->HasPendingOperations()) {
    // TODO: there may be a more reasonable behavior here.
    return Status::IllegalState("Cannot change flush mode when writes are buffered");
  }
  if (!tight_enum_test<FlushMode>(m)) {
    // Be paranoid in client code.
    return Status::InvalidArgument("Bad flush mode");
  }

  flush_mode_ = m;
  return Status::OK();
}

void KuduSession::SetTimeoutMillis(int millis) {
  CHECK_GE(millis, 0);
  timeout_ms_ = millis;
  batcher_->SetTimeoutMillis(millis);
}

Status KuduSession::Apply(gscoped_ptr<Insert>* insert_scoped) {
  Insert* insert = insert_scoped->get();
  if (!insert->row().IsKeySet()) {
    return Status::IllegalState("Key not specified", insert->ToString());
  }

  batcher_->Add(insert_scoped->Pass());

  if (flush_mode_ == AUTO_FLUSH_SYNC) {
    RETURN_NOT_OK(Flush());
  }

  return Status::OK();
}

Status KuduSession::Flush() {
  Synchronizer s;
  FlushAsync(s.callback());
  return s.Wait();
}

void KuduSession::FlushAsync(const StatusCallback& user_callback) {
  CHECK_EQ(flush_mode_, MANUAL_FLUSH) << "TODO: handle other flush modes";

  // Swap in a new batcher to start building the next batch.
  // Save off the old batcher.
  scoped_refptr<Batcher> old_batcher;
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    NewBatcher(&old_batcher);
    InsertOrDie(&flushed_batchers_, old_batcher.get());
  }

  // Send off any buffered data. Important to do this outside of the lock
  // since the callback may itself try to take the lock, in the case that
  // the batch fails "inline" on the same thread.
  old_batcher->FlushAsync(user_callback);
}

void KuduSession::FlushFinished(Batcher* batcher) {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(flushed_batchers_.erase(batcher), 1);
}

bool KuduSession::HasPendingOperations() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (batcher_->HasPendingOperations()) {
    return true;
  }
  BOOST_FOREACH(Batcher* b, flushed_batchers_) {
    if (b->HasPendingOperations()) {
      return true;
    }
  }
  return false;
}

int KuduSession::CountBufferedOperations() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(flush_mode_, MANUAL_FLUSH);

  return batcher_->CountBufferedOperations();
}

int KuduSession::CountPendingErrors() const {
  return error_collector_->CountErrors();
}

void KuduSession::GetPendingErrors(std::vector<Error*>* errors, bool* overflowed) {
  error_collector_->GetErrors(errors, overflowed);
}

////////////////////////////////////////////////////////////
// Mutation classes (Insert/Update/Delete)
////////////////////////////////////////////////////////////

Insert::Insert(KuduTable* table)
  : table_(table),
    row_(&table->schema()) {
}

Insert::~Insert() {}

gscoped_ptr<EncodedKey> Insert::CreateKey() {
  CHECK(row_.IsKeySet()) << "key must be set";

  ConstContiguousRow row = row_.as_contiguous_row();
  EncodedKeyBuilder kb(row.schema());
  for (int i = 0; i < row.schema().num_key_columns(); i++) {
    kb.AddColumnKey(row.cell_ptr(i));
  }
  gscoped_ptr<EncodedKey> key(kb.BuildEncodedKey());
  return key.Pass();
}

////////////////////////////////////////////////////////////
// AlterTable
////////////////////////////////////////////////////////////
AlterTableBuilder::AlterTableBuilder()
  : alter_steps_(new AlterTableRequestPB) {
}

AlterTableBuilder::~AlterTableBuilder() {
  delete alter_steps_;
}

void AlterTableBuilder::Reset() {
  alter_steps_->clear_alter_schema_steps();
}

bool AlterTableBuilder::has_changes() const {
  return alter_steps_->has_new_table_name() ||
         alter_steps_->alter_schema_steps_size() > 0;
}

Status AlterTableBuilder::RenameTable(const string& new_name) {
  alter_steps_->set_new_table_name(new_name);
  return Status::OK();
}

Status AlterTableBuilder::AddColumn(const std::string& name,
                                    DataType type,
                                    const void *default_value,
                                    ColumnStorageAttributes attributes) {
  if (default_value == NULL) {
    return Status::InvalidArgument("A new column must have a default value",
                                   "Use AddNullableColumn() to add a NULLABLE column");
  }

  AlterTableRequestPB::Step* step = alter_steps_->add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::ADD_COLUMN);
  ColumnSchemaToPB(ColumnSchema(name, type, false, default_value, default_value, attributes),
                                step->mutable_add_column()->mutable_schema());
  return Status::OK();
}

Status AlterTableBuilder::AddNullableColumn(const std::string& name,
                                            DataType type,
                                            ColumnStorageAttributes attributes) {
  AlterTableRequestPB::Step* step = alter_steps_->add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::ADD_COLUMN);
  ColumnSchemaToPB(ColumnSchema(name, type, true, NULL, NULL, attributes),
                                step->mutable_add_column()->mutable_schema());
  return Status::OK();
}

Status AlterTableBuilder::DropColumn(const std::string& name) {
  AlterTableRequestPB::Step* step = alter_steps_->add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::DROP_COLUMN);
  step->mutable_drop_column()->set_name(name);
  return Status::OK();
}

Status AlterTableBuilder::RenameColumn(const std::string& old_name,
                                       const std::string& new_name) {
  AlterTableRequestPB::Step* step = alter_steps_->add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::RENAME_COLUMN);
  step->mutable_rename_column()->set_old_name(old_name);
  step->mutable_rename_column()->set_new_name(new_name);
  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduScanner
////////////////////////////////////////////////////////////

KuduScanner::KuduScanner(KuduTable* table)
  : open_(false),
    data_in_open_(false),
    projection_(NULL),
    has_batch_size_bytes_(false),
    batch_size_bytes_(0),
    table_(DCHECK_NOTNULL(table)),
    spec_encoder_(table->schema()),
    start_key_(NULL),
    end_key_(NULL) {
}

KuduScanner::~KuduScanner() {
  Close();
}

Status KuduScanner::SetProjection(const Schema* projection) {
  CHECK(!open_) << "Scanner already open";
  projection_ = projection;
  return Status::OK();
}

Status KuduScanner::SetBatchSizeBytes(uint32_t batch_size) {
  has_batch_size_bytes_ = true;
  batch_size_bytes_ = batch_size;
  return Status::OK();
}

Status KuduScanner::AddConjunctPredicate(const ColumnRangePredicate& pred) {
  CHECK(!open_) << "Scanner already open";
  spec_.AddPredicate(pred);
  return Status::OK();
}

void KuduScanner::PrepareRequest(RequestType state) {
  if (state == KuduScanner::CLOSE) {
    next_req_.set_batch_size_bytes(0);
  } else if (has_batch_size_bytes_) {
    next_req_.set_batch_size_bytes(batch_size_bytes_);
  } else {
    next_req_.clear_batch_size_bytes();
  }

  if (state == KuduScanner::NEW) {
    next_req_.set_call_seq_id(0);
  } else {
    next_req_.set_call_seq_id(next_req_.call_seq_id() + 1);
  }
}

void KuduScanner::CopyPredicateBound(const ColumnSchema& col,
                                     const void* bound_src,
                                     string* bound_dst) {
  const void* src;
  size_t size;
  if (col.type_info()->type() == STRING) {
    // Copying a string involves an extra level of indirection through its
    // owning slice.
    const Slice* s = reinterpret_cast<const Slice*>(bound_src);
    src = s->data();
    size = s->size();
  } else {
    src = bound_src;
    size = col.type_info()->size();
  }
  bound_dst->assign(reinterpret_cast<const char*>(src), size);
}

Status KuduScanner::OpenTablet(const Slice& key) {
  // TODO: scanners don't really require a leader. For now, however,
  // we always scan from the leader.
  Synchronizer s;
  table_->client_->meta_cache_->LookupTabletByKey(table_,
                                                  key,
                                                  &remote_, s.callback());
  RETURN_NOT_OK(s.Wait());
  shared_ptr<TabletServerServiceProxy> proxy;
  RETURN_NOT_OK(table_->client_->GetTabletProxy(remote_->tablet_id(), &proxy));
  DCHECK(proxy);

  // Scan it.
  PrepareRequest(KuduScanner::NEW);
  next_req_.clear_scanner_id();
  NewScanRequestPB* scan = next_req_.mutable_new_scan_request();

  // Set up the predicates.
  scan->clear_range_predicates();
  BOOST_FOREACH(const ColumnRangePredicate& pred, spec_.predicates()) {
    const ColumnSchema& col = pred.column();
    const ValueRange& range = pred.range();
    ColumnRangePredicatePB* pb = scan->add_range_predicates();
    if (range.has_lower_bound()) {
      CopyPredicateBound(col, range.lower_bound(),
                         pb->mutable_lower_bound());
    }
    if (range.has_upper_bound()) {
      CopyPredicateBound(col, range.upper_bound(),
                         pb->mutable_upper_bound());
    }
    ColumnSchemaToPB(col, pb->mutable_column());
  }

  scan->set_tablet_id(remote_->tablet_id());
  RETURN_NOT_OK(SchemaToColumnPBs(*projection_, scan->mutable_projected_columns()));
  controller_.Reset();
  controller_.set_timeout(MonoDelta::FromMilliseconds(kRpcTimeoutMillis));
  RETURN_NOT_OK(proxy->Scan(next_req_, &last_response_, &controller_));
  RETURN_NOT_OK(CheckForErrors());

  next_req_.clear_new_scan_request();
  data_in_open_ = last_response_.has_data();
  if (last_response_.has_more_results()) {
    next_req_.set_scanner_id(last_response_.scanner_id());
    VLOG(1) << "Opened tablet " << remote_->tablet_id()
            << ", scanner ID " << last_response_.scanner_id();
  } else if (last_response_.has_data()) {
    VLOG(1) << "Opened tablet " << remote_->tablet_id() << ", no scanner ID assigned";
  } else {
    VLOG(1) << "Opened tablet " << remote_->tablet_id() << " (no rows), no scanner ID assigned";
  }

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

string KuduScanner::ToString() const {
  Slice start_key = start_key_ ? start_key_->encoded_key() : Slice("INF");
  Slice end_key = end_key_ ? end_key_->encoded_key() : Slice("INF");
  return strings::Substitute("$0: [$1,$2)", table_->name(),
                             start_key.ToString(), end_key.ToString());
}

Status KuduScanner::Open() {
  CHECK(!open_) << "Scanner already open";
  CHECK(projection_ != NULL) << "No projection provided";

  // Find the first tablet.
  spec_encoder_.EncodeRangePredicates(&spec_, false);
  CHECK(!spec_.has_encoded_ranges() ||
        spec_.encoded_ranges().size() == 1);
  if (spec_.has_encoded_ranges()) {
    const EncodedKeyRange* key_range = spec_.encoded_ranges()[0];
    if (key_range->has_lower_bound()) {
      start_key_ = &key_range->lower_bound();
    }
    if (key_range->has_upper_bound()) {
      end_key_ = &key_range->upper_bound();
    }
  }

  VLOG(1) << "Beginning scan " << ToString();

  RETURN_NOT_OK(OpenTablet(start_key_ != NULL ? start_key_->encoded_key() : Slice()));

  open_ = true;
  return Status::OK();
}

void KuduScanner::Close() {
  if (!open_) return;

  VLOG(1) << "Ending scan " << ToString();

  if (next_req_.scanner_id().empty()) {
    // In the case that the scan matched no rows, and this was determined
    // in the Open() call, then we won't have been assigned a scanner ID
    // at all. So, no need to close on the server side.
    open_ = false;
    return;
  }

  gscoped_ptr<CloseCallback> closer(new CloseCallback);
  closer->scanner_id = next_req_.scanner_id();
  PrepareRequest(KuduScanner::CLOSE);
  next_req_.set_close_scanner(true);
  closer->controller.set_timeout(MonoDelta::FromMilliseconds(kRpcTimeoutMillis));
  shared_ptr<TabletServerServiceProxy> proxy;
  Status s = table_->client_->GetTabletProxy(remote_->tablet_id(), &proxy);
  if (s.ok()) {
    proxy->ScanAsync(next_req_, &closer->response, &closer->controller,
                     boost::bind(&CloseCallback::Callback, closer.get()));
    ignore_result(closer.release());
  } else {
    LOG(WARNING) << "Unable to close scanner " << ToString() << " on server: "
                 << s.ToString();
  }
  next_req_.Clear();
  open_ = false;
}

Status KuduScanner::CheckForErrors() {
  if (PREDICT_TRUE(!last_response_.has_error())) {
    return Status::OK();
  }

  return StatusFromPB(last_response_.error().status());
}

bool KuduScanner::MoreTablets() const {
  CHECK(open_);
  return !remote_->end_key().empty() &&
      (end_key_ == NULL || end_key_->encoded_key().compare(remote_->end_key()) > 0);
}

bool KuduScanner::HasMoreRows() const {
  CHECK(open_);
  return data_in_open_ || // more data in hand
      last_response_.has_more_results() || // more data in this tablet
      MoreTablets(); // more tablets to scan, possibly with more data
}

Status KuduScanner::ExtractRows(vector<const uint8_t*>* rows) {
  size_t before = rows->size();
  Status s = ExtractRowsFromRowBlockPB(*projection_, last_response_.mutable_data(), rows);
  VLOG(1) << "Extracted " << rows->size() - before << " rows";
  return s;
}

Status KuduScanner::NextBatch(std::vector<const uint8_t*>* rows) {
  // TODO: do some double-buffering here -- when we return this batch
  // we should already have fired off the RPC for the next batch, but
  // need to do some swapping of the response objects around to avoid
  // stomping on the memory the user is looking at.
  CHECK(open_);

  if (data_in_open_) {
    // We have data from a previous scan.
    VLOG(1) << "Extracting data from scan " << ToString();
    data_in_open_ = false;
    return ExtractRows(rows);
  } else if (last_response_.has_more_results()) {
    // More data is available in this tablet.
    VLOG(1) << "Continuing scan " << ToString();

    controller_.Reset();
    shared_ptr<TabletServerServiceProxy> proxy;
    RETURN_NOT_OK(table_->client_->GetTabletProxy(remote_->tablet_id(), &proxy));
    PrepareRequest(KuduScanner::CONTINUE);
    RETURN_NOT_OK(proxy->Scan(next_req_, &last_response_, &controller_));
    RETURN_NOT_OK(CheckForErrors());
    return ExtractRows(rows);
  } else if (MoreTablets()) {
    // More data may be available in other tablets.
    //
    // No need to close the current tablet; we scanned all the data so the
    // server closed it for us.
    VLOG(1) << "Scanning next tablet " << ToString();
    RETURN_NOT_OK(OpenTablet(remote_->end_key()));

    // No rows written, the next invocation will pick them up.
    return Status::OK();
  } else {
    // No more data anywhere.
    return Status::OK();
  }
}


} // namespace client
} // namespace kudu
