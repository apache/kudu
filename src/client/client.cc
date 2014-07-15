// Copyright (c) 2013, Cloudera,inc.

#include "client/client.h"

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <vector>

#include "client/batcher.h"
#include "client/client-internal.h"
#include "client/client_builder-internal.h"
#include "client/error_collector.h"
#include "client/error-internal.h"
#include "client/meta_cache.h"
#include "client/row_result.h"
#include "client/scanner-internal.h"
#include "client/session-internal.h"
#include "client/table-internal.h"
#include "client/table_alterer-internal.h"
#include "client/table_creator-internal.h"
#include "client/write_op.h"
#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/strings/substitute.h"
#include "master/master.h" // TODO: remove this include - just needed for default port
#include "master/master.pb.h"
#include "master/master.proxy.h"
#include "rpc/messenger.h"
#include "util/net/dns_resolver.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using kudu::master::AlterTableRequestPB;
using kudu::master::AlterTableRequestPB_Step;
using kudu::master::AlterTableResponsePB;
using kudu::master::CreateTableRequestPB;
using kudu::master::CreateTableResponsePB;
using kudu::master::DeleteTableRequestPB;
using kudu::master::DeleteTableResponsePB;
using kudu::master::GetTableSchemaRequestPB;
using kudu::master::GetTableSchemaResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::TabletLocationsPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tserver::ScanResponsePB;

MAKE_ENUM_LIMITS(kudu::client::KuduSession::FlushMode,
                 kudu::client::KuduSession::AUTO_FLUSH_SYNC,
                 kudu::client::KuduSession::MANUAL_FLUSH);

MAKE_ENUM_LIMITS(kudu::client::KuduScanner::ReadMode,
                 kudu::client::KuduScanner::READ_LATEST,
                 kudu::client::KuduScanner::READ_AT_SNAPSHOT);

namespace kudu {
namespace client {

static const int kHtTimestampBitsToShift = 12;

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

KuduClientBuilder::KuduClientBuilder() {
  data_.reset(new KuduClientBuilder::Data());
}

KuduClientBuilder::~KuduClientBuilder() {
}

KuduClientBuilder& KuduClientBuilder::master_server_addr(const string& addr) {
  data_->master_server_addr_ = addr;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::default_admin_operation_timeout(const MonoDelta& timeout) {
  data_->default_admin_operation_timeout_ = timeout;
  return *this;
}

Status KuduClientBuilder::Build(shared_ptr<KuduClient>* client) {
  shared_ptr<KuduClient> c(new KuduClient());

  // Init messenger.
  MessengerBuilder builder("client");
  RETURN_NOT_OK(builder.Build(&c->data_->messenger_));

  // Init proxy.
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(data_->master_server_addr_,
                                 master::Master::kDefaultPort, &addrs));
  if (addrs.empty()) {
    return Status::InvalidArgument("No master address specified");
  }
  if (addrs.size() > 1) {
    LOG(WARNING) << "Specified master server address '" << data_->master_server_addr_ << "' "
                 << "resolved to multiple IPs. Using " << addrs[0].ToString();
  }
  c->data_->master_proxy_.reset(new MasterServiceProxy(c->data_->messenger_, addrs[0]));

  c->data_->meta_cache_.reset(new MetaCache(c.get()));
  c->data_->dns_resolver_.reset(new DnsResolver());
  c->data_->master_server_addr_ = data_->master_server_addr_;
  c->data_->default_admin_operation_timeout_ = data_->default_admin_operation_timeout_;

  // Init local host names used for locality decisions.
  RETURN_NOT_OK_PREPEND(c->data_->InitLocalHostNames(),
                        "Could not determine local host names");

  client->swap(c);
  return Status::OK();
}

KuduClient::KuduClient() {
  data_.reset(new KuduClient::Data());
}

gscoped_ptr<KuduTableCreator> KuduClient::NewTableCreator() {
  return gscoped_ptr<KuduTableCreator>(new KuduTableCreator(this));
}

Status KuduClient::IsCreateTableInProgress(const string& table_name,
                                           bool *create_in_progress) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->IsCreateTableInProgress(table_name, deadline, create_in_progress);
}

Status KuduClient::DeleteTable(const string& table_name) {
  DeleteTableRequestPB req;
  DeleteTableResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(default_admin_operation_timeout());
  RETURN_NOT_OK(data_->master_proxy_->DeleteTable(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  return Status::OK();
}

gscoped_ptr<KuduTableAlterer> KuduClient::NewTableAlterer() {
  return gscoped_ptr<KuduTableAlterer>(new KuduTableAlterer(this));
}

Status KuduClient::IsAlterTableInProgress(const string& table_name,
                                          bool *alter_in_progress) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->IsAlterTableInProgress(table_name, deadline, alter_in_progress);
}

Status KuduClient::GetTableSchema(const string& table_name,
                                  KuduSchema* schema) {
  GetTableSchemaRequestPB req;
  GetTableSchemaResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(default_admin_operation_timeout());
  RETURN_NOT_OK(data_->master_proxy_->GetTableSchema(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  Schema server_schema;
  RETURN_NOT_OK(SchemaFromPB(resp.schema(), &server_schema));

  // Remove the server IDs from the schema
  gscoped_ptr<Schema> client_schema(new Schema());
  client_schema->Reset(server_schema.columns(), server_schema.num_key_columns());
  schema->schema_.swap(client_schema);
  return Status::OK();
}

Status KuduClient::OpenTable(const string& table_name,
                             scoped_refptr<KuduTable>* table) {
  KuduSchema schema;
  RETURN_NOT_OK(GetTableSchema(table_name, &schema));

  // In the future, probably will look up the table in some map to reuse KuduTable
  // instances.
  scoped_refptr<KuduTable> ret(new KuduTable(shared_from_this(), table_name, schema));
  RETURN_NOT_OK(ret->data_->Open());
  table->swap(ret);

  return Status::OK();
}

shared_ptr<KuduSession> KuduClient::NewSession() {
  shared_ptr<KuduSession> ret(new KuduSession(shared_from_this()));
  ret->data_->Init(ret);
  return ret;
}

const std::string& KuduClient::master_server_addr() const {
  return data_->master_server_addr_;
}

const MonoDelta& KuduClient::default_admin_operation_timeout() const {
  return data_->default_admin_operation_timeout_;
}

////////////////////////////////////////////////////////////
// KuduTableCreator
////////////////////////////////////////////////////////////

KuduTableCreator::KuduTableCreator(KuduClient* client) {
  data_.reset(new KuduTableCreator::Data(client));
}

KuduTableCreator::~KuduTableCreator() {
}

KuduTableCreator& KuduTableCreator::table_name(const string& name) {
  data_->table_name_ = name;
  return *this;
}

KuduTableCreator& KuduTableCreator::schema(const KuduSchema* schema) {
  data_->schema_ = schema;
  return *this;
}

KuduTableCreator& KuduTableCreator::split_keys(const vector<string>& keys) {
  data_->split_keys_ = keys;
  return *this;
}

KuduTableCreator& KuduTableCreator::num_replicas(int num_replicas) {
  data_->num_replicas_ = num_replicas;
  return *this;
}

KuduTableCreator& KuduTableCreator::wait_for_assignment(bool wait) {
  data_->wait_for_assignment_ = wait;
  return *this;
}

Status KuduTableCreator::Create() {
  if (!data_->table_name_.length()) {
    return Status::InvalidArgument("Missing table name");
  }
  if (!data_->schema_) {
    return Status::InvalidArgument("Missing schema");
  }

  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(data_->client_->default_admin_operation_timeout());

  // Build request.
  req.set_name(data_->table_name_);
  if (data_->num_replicas_ >= 1) {
    req.set_num_replicas(data_->num_replicas_);
  }
  RETURN_NOT_OK_PREPEND(SchemaToPB(*data_->schema_->schema_, req.mutable_schema()),
                        "Invalid schema");
  BOOST_FOREACH(const string& key, data_->split_keys_) {
    req.add_pre_split_keys(key);
  }

  // Send it.
  RETURN_NOT_OK(data_->client_->data_->master_proxy_->CreateTable(req, &resp, &rpc));
  if (resp.has_error()) {
    // TODO: if already exist and in progress spin
    return StatusFromPB(resp.error().status());
  }

  // Spin until the table is fully created, if requested.
  if (data_->wait_for_assignment_) {
    // TODO: make the wait time configurable
    MonoTime deadline = MonoTime::Now(MonoTime::FINE);
    deadline.AddDelta(MonoDelta::FromSeconds(15));
    RETURN_NOT_OK(RetryFunc(deadline,
                            "Waiting on Create Table to be completed",
                            "Timeout out waiting for Table Creation",
                            boost::bind(&KuduClient::Data::IsCreateTableInProgress,
                                        data_->client_->data_.get(),
                                        data_->table_name_, _1, _2)));
  }

  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduTable
////////////////////////////////////////////////////////////

KuduTable::KuduTable(const shared_ptr<KuduClient>& client,
                     const string& name,
                     const KuduSchema& schema) {
  data_.reset(new KuduTable::Data(client, name, schema));
}

KuduTable::~KuduTable() {
}

const string& KuduTable::name() const {
  return data_->name_;
}

const KuduSchema& KuduTable::schema() const {
  return data_->schema_;
}

// Create a new write operation for this table.

gscoped_ptr<KuduInsert> KuduTable::NewInsert() {
  return gscoped_ptr<KuduInsert>(new KuduInsert(this));
}

gscoped_ptr<KuduUpdate> KuduTable::NewUpdate() {
  return gscoped_ptr<KuduUpdate>(new KuduUpdate(this));
}

gscoped_ptr<KuduDelete> KuduTable::NewDelete() {
  return gscoped_ptr<KuduDelete>(new KuduDelete(this));
}

KuduClient* KuduTable::client() const {
  return data_->client_.get();
}

////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////

const Status& KuduError::status() const {
  return data_->status_;
}

const KuduWriteOperation& KuduError::failed_op() const {
  return *data_->failed_op_;
}

gscoped_ptr<KuduWriteOperation> KuduError::release_failed_op() {
  CHECK_NOTNULL(data_->failed_op_.get());
  return data_->failed_op_.Pass();
}

bool KuduError::was_possibly_successful() const {
  // TODO: implement me - right now be conservative.
  return true;
}

KuduError::KuduError(gscoped_ptr<KuduWriteOperation> failed_op,
                     const Status& status) {
  data_.reset(new KuduError::Data(failed_op.Pass(), status));
}

KuduError::~KuduError() {
}

////////////////////////////////////////////////////////////
// KuduSession
////////////////////////////////////////////////////////////

KuduSession::KuduSession(const shared_ptr<KuduClient>& client) {
  data_.reset(new KuduSession::Data(client));
}

KuduSession::~KuduSession() {
  if (data_->batcher_->HasPendingOperations()) {
    LOG(WARNING) << "Closing Session with pending operations.";
  }
  data_->batcher_->Abort();
}

Status KuduSession::SetFlushMode(FlushMode m) {
  if (data_->batcher_->HasPendingOperations()) {
    // TODO: there may be a more reasonable behavior here.
    return Status::IllegalState("Cannot change flush mode when writes are buffered");
  }
  if (!tight_enum_test<FlushMode>(m)) {
    // Be paranoid in client code.
    return Status::InvalidArgument("Bad flush mode");
  }

  data_->flush_mode_ = m;
  return Status::OK();
}

void KuduSession::SetTimeoutMillis(int millis) {
  CHECK_GE(millis, 0);
  data_->timeout_ms_ = millis;
  data_->batcher_->SetTimeoutMillis(millis);
}

Status KuduSession::Flush() {
  Synchronizer s;
  FlushAsync(s.AsStatusCallback());
  return s.Wait();
}

void KuduSession::FlushAsync(const StatusCallback& user_callback) {
  CHECK_EQ(data_->flush_mode_, MANUAL_FLUSH) << "TODO: handle other flush modes";

  // Swap in a new batcher to start building the next batch.
  // Save off the old batcher.
  scoped_refptr<Batcher> old_batcher;
  {
    boost::lock_guard<simple_spinlock> l(data_->lock_);
    data_->NewBatcher(shared_from_this(), &old_batcher);
    InsertOrDie(&data_->flushed_batchers_, old_batcher.get());
  }

  // Send off any buffered data. Important to do this outside of the lock
  // since the callback may itself try to take the lock, in the case that
  // the batch fails "inline" on the same thread.
  old_batcher->FlushAsync(user_callback);
}

bool KuduSession::HasPendingOperations() const {
  boost::lock_guard<simple_spinlock> l(data_->lock_);
  if (data_->batcher_->HasPendingOperations()) {
    return true;
  }
  BOOST_FOREACH(Batcher* b, data_->flushed_batchers_) {
    if (b->HasPendingOperations()) {
      return true;
    }
  }
  return false;
}

Status KuduSession::Apply(gscoped_ptr<KuduInsert> write_op) {
  return Apply(write_op.PassAs<KuduWriteOperation>());
}

Status KuduSession::Apply(gscoped_ptr<KuduUpdate> write_op) {
  return Apply(write_op.PassAs<KuduWriteOperation>());
}

Status KuduSession::Apply(gscoped_ptr<KuduDelete> write_op) {
  return Apply(write_op.PassAs<KuduWriteOperation>());
}

Status KuduSession::Apply(gscoped_ptr<KuduWriteOperation> write_op) {
  if (!write_op->row().IsKeySet()) {
    Status status = Status::IllegalState("Key not specified", write_op->ToString());
    data_->error_collector_->AddError(gscoped_ptr<KuduError>(
        new KuduError(write_op.Pass(), status)));
    return status;
  }

  data_->batcher_->Add(write_op.Pass());

  if (data_->flush_mode_ == AUTO_FLUSH_SYNC) {
    return Flush();
  }

  return Status::OK();
}

int KuduSession::CountBufferedOperations() const {
  boost::lock_guard<simple_spinlock> l(data_->lock_);
  CHECK_EQ(data_->flush_mode_, MANUAL_FLUSH);

  return data_->batcher_->CountBufferedOperations();
}

int KuduSession::CountPendingErrors() const {
  return data_->error_collector_->CountErrors();
}

void KuduSession::GetPendingErrors(vector<KuduError*>* errors, bool* overflowed) {
  data_->error_collector_->GetErrors(errors, overflowed);
}

KuduClient* KuduSession::client() const {
  return data_->client_.get();
}

////////////////////////////////////////////////////////////
// KuduTableAlterer
////////////////////////////////////////////////////////////
KuduTableAlterer::KuduTableAlterer(KuduClient* client) {
  data_.reset(new KuduTableAlterer::Data(client));
}

KuduTableAlterer::~KuduTableAlterer() {
}

KuduTableAlterer& KuduTableAlterer::table_name(const string& name) {
  data_->alter_steps_.mutable_table()->set_table_name(name);
  return *this;
}

KuduTableAlterer& KuduTableAlterer::rename_table(const string& new_name) {
  data_->alter_steps_.set_new_table_name(new_name);
  return *this;
}

KuduTableAlterer& KuduTableAlterer::add_column(const string& name,
                                               DataType type,
                                               const void *default_value,
                                               KuduColumnStorageAttributes attributes) {
  if (default_value == NULL) {
    data_->status_ = Status::InvalidArgument("A new column must have a default value",
                                             "Use AddNullableColumn() to add a NULLABLE column");
  }

  AlterTableRequestPB::Step* step = data_->alter_steps_.add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::ADD_COLUMN);
  ColumnStorageAttributes attr_priv(attributes.encoding(), attributes.compression());
  ColumnSchemaToPB(ColumnSchema(name, type, false, default_value, default_value, attr_priv),
                                step->mutable_add_column()->mutable_schema());
  return *this;
}

KuduTableAlterer& KuduTableAlterer::add_nullable_column(const string& name,
                                                        DataType type,
                                                        KuduColumnStorageAttributes attributes) {
  AlterTableRequestPB::Step* step = data_->alter_steps_.add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::ADD_COLUMN);
  ColumnStorageAttributes attr_priv(attributes.encoding(), attributes.compression());
  ColumnSchemaToPB(ColumnSchema(name, type, true, NULL, NULL, attr_priv),
                                step->mutable_add_column()->mutable_schema());
  return *this;
}

KuduTableAlterer& KuduTableAlterer::drop_column(const string& name) {
  AlterTableRequestPB::Step* step = data_->alter_steps_.add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::DROP_COLUMN);
  step->mutable_drop_column()->set_name(name);
  return *this;
}

KuduTableAlterer& KuduTableAlterer::rename_column(const string& old_name,
                                                  const string& new_name) {
  AlterTableRequestPB::Step* step = data_->alter_steps_.add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::RENAME_COLUMN);
  step->mutable_rename_column()->set_old_name(old_name);
  step->mutable_rename_column()->set_new_name(new_name);
  return *this;
}

Status KuduTableAlterer::Alter() {
  if (!data_->alter_steps_.table().has_table_name()) {
    return Status::InvalidArgument("Missing table name");
  }

  if (!data_->alter_steps_.has_new_table_name() &&
      data_->alter_steps_.alter_schema_steps_size() == 0) {
    return Status::InvalidArgument("No alter steps provided");
  }

  if (!data_->status_.ok()) {
    return data_->status_;
  }

  AlterTableResponsePB resp;
  RpcController rpc;

  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromMilliseconds(60 * 1000));

  rpc.set_timeout(data_->client_->default_admin_operation_timeout());
  RETURN_NOT_OK(data_->client_->data_->master_proxy_->AlterTable(data_->alter_steps_, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  string alter_name = data_->alter_steps_.has_new_table_name() ?
      data_->alter_steps_.new_table_name() : data_->alter_steps_.table().table_name();
  RETURN_NOT_OK(RetryFunc(deadline,
        "Waiting on Alter Table to be completed",
        "Timeout out waiting for AlterTable",
        boost::bind(&KuduClient::Data::IsAlterTableInProgress,
                    data_->client_->data_.get(),
                    alter_name, _1, _2)));

  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduScanner
////////////////////////////////////////////////////////////

KuduScanner::KuduScanner(KuduTable* table) {
  data_.reset(new KuduScanner::Data(table));
}

KuduScanner::~KuduScanner() {
  Close();
}

Status KuduScanner::SetProjection(const KuduSchema* projection) {
  if (data_->open_) {
    return Status::IllegalState("Projection must be set before Open()");
  }
  data_->projection_ = projection->schema_.get();
  data_->projected_row_size_ = data_->CalculateProjectedRowSize(*data_->projection_);
  return Status::OK();
}

Status KuduScanner::SetBatchSizeBytes(uint32_t batch_size) {
  data_->has_batch_size_bytes_ = true;
  data_->batch_size_bytes_ = batch_size;
  return Status::OK();
}

Status KuduScanner::SetReadMode(ReadMode read_mode) {
  if (data_->open_) {
    return Status::IllegalState("Read mode must be set before Open()");
  }
  if (!tight_enum_test<ReadMode>(read_mode)) {
    return Status::InvalidArgument("Bad read mode");
  }
  data_->read_mode_ = read_mode;
  return Status::OK();
}

Status KuduScanner::SetSnapshot(uint64_t snapshot_timestamp_micros) {
  if (data_->open_) {
    return Status::IllegalState("Snapshot timestamp must be set before Open()");
  }
  // Shift the HT timestamp bits to get well-formed HT timestamp with the logical
  // bits zeroed out.
  data_->snapshot_timestamp_ = snapshot_timestamp_micros << kHtTimestampBitsToShift;
  return Status::OK();
}

Status KuduScanner::SetSelection(KuduClient::ReplicaSelection selection) {
  if (data_->open_) {
    return Status::IllegalState("Replica selection must be set before Open()");
  }
  data_->selection_ = selection;
  return Status::OK();
}

Status KuduScanner::AddConjunctPredicate(const KuduColumnRangePredicate& pred) {
  if (data_->open_) {
    return Status::IllegalState("Predicate must be set before Open()");
  }
  data_->spec_.AddPredicate(*pred.pred_);
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
  Slice start_key = data_->start_key_ ? data_->start_key_->encoded_key() : Slice("INF");
  Slice end_key = data_->end_key_ ? data_->end_key_->encoded_key() : Slice("INF");
  return strings::Substitute("$0: [$1,$2)", data_->table_->name(),
                             start_key.ToDebugString(), end_key.ToDebugString());
}

Status KuduScanner::Open() {
  CHECK(!data_->open_) << "Scanner already open";
  CHECK(data_->projection_ != NULL) << "No projection provided";

  // Find the first tablet.
  data_->spec_encoder_.EncodeRangePredicates(&data_->spec_, false);
  CHECK(!data_->spec_.has_encoded_ranges() ||
        data_->spec_.encoded_ranges().size() == 1);
  if (data_->spec_.has_encoded_ranges()) {
    const EncodedKeyRange* key_range = data_->spec_.encoded_ranges()[0];
    if (key_range->has_lower_bound()) {
      data_->start_key_ = &key_range->lower_bound();
    }
    if (key_range->has_upper_bound()) {
      data_->end_key_ = &key_range->upper_bound();
    }
  }

  VLOG(1) << "Beginning scan " << ToString();

  RETURN_NOT_OK(data_->OpenTablet(data_->start_key_ != NULL
                                  ? data_->start_key_->encoded_key() : Slice()));

  data_->open_ = true;
  return Status::OK();
}

void KuduScanner::Close() {
  if (!data_->open_) return;
  CHECK(data_->proxy_);

  VLOG(1) << "Ending scan " << ToString();
  if (data_->next_req_.scanner_id().empty()) {
    // In the case that the scan matched no rows, and this was determined
    // in the Open() call, then we won't have been assigned a scanner ID
    // at all. So, no need to close on the server side.
    data_->open_ = false;
    return;
  }

  gscoped_ptr<CloseCallback> closer(new CloseCallback);
  closer->scanner_id = data_->next_req_.scanner_id();
  data_->PrepareRequest(KuduScanner::Data::CLOSE);
  data_->next_req_.set_close_scanner(true);
  closer->controller.set_timeout(MonoDelta::FromMilliseconds(data_->kRpcTimeoutMillis));
  data_->proxy_->ScanAsync(data_->next_req_, &closer->response, &closer->controller,
                           boost::bind(&CloseCallback::Callback, closer.get()));
  ignore_result(closer.release());
  data_->next_req_.Clear();
  data_->proxy_.reset();
  data_->open_ = false;
}

bool KuduScanner::HasMoreRows() const {
  CHECK(data_->open_);
  return data_->data_in_open_ || // more data in hand
      data_->last_response_.has_more_results() || // more data in this tablet
      data_->MoreTablets(); // more tablets to scan, possibly with more data
}

Status KuduScanner::NextBatch(vector<KuduRowResult>* rows) {
  // TODO: do some double-buffering here -- when we return this batch
  // we should already have fired off the RPC for the next batch, but
  // need to do some swapping of the response objects around to avoid
  // stomping on the memory the user is looking at.
  CHECK(data_->open_);
  CHECK(data_->proxy_);

  if (data_->data_in_open_) {
    // We have data from a previous scan.
    VLOG(1) << "Extracting data from scan " << ToString();
    data_->data_in_open_ = false;
    return data_->ExtractRows(rows);
  } else if (data_->last_response_.has_more_results()) {
    // More data is available in this tablet.
    //
    // Note that, in this case, we can't fail to a replica on error. Why?
    // Because we're mid-tablet, and we might end up rereading some rows.
    // Only fault tolerant scans can try other replicas here.
    VLOG(1) << "Continuing scan " << ToString();

    data_->controller_.Reset();
    data_->controller_.set_timeout(MonoDelta::FromMilliseconds(data_->kRpcTimeoutMillis));
    data_->PrepareRequest(KuduScanner::Data::CONTINUE);
    RETURN_NOT_OK(data_->proxy_->Scan(data_->next_req_,
                                      &data_->last_response_,
                                      &data_->controller_));
    RETURN_NOT_OK(data_->CheckForErrors());
    return data_->ExtractRows(rows);
  } else if (data_->MoreTablets()) {
    // More data may be available in other tablets.
    //
    // No need to close the current tablet; we scanned all the data so the
    // server closed it for us.
    VLOG(1) << "Scanning next tablet " << ToString();
    RETURN_NOT_OK(data_->OpenTablet(data_->remote_->end_key()));

    // No rows written, the next invocation will pick them up.
    return Status::OK();
  } else {
    // No more data anywhere.
    return Status::OK();
  }
}

} // namespace client
} // namespace kudu
