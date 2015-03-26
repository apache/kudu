// Copyright (c) 2013, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/client.h"

#include <algorithm>
#include <boost/bind.hpp>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <vector>

#include "kudu/client/batcher.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client_builder-internal.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/error-internal.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/table-internal.h"
#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/client/tablet_server-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h" // TODO: remove this include - just needed for default port
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/logging.h"

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
using kudu::master::ListTabletServersRequestPB;
using kudu::master::ListTabletServersResponsePB;
using kudu::master::ListTabletServersResponsePB_Entry;
using kudu::master::ListTablesRequestPB;
using kudu::master::ListTablesResponsePB;
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

MAKE_ENUM_LIMITS(kudu::client::KuduScanner::OrderMode,
                 kudu::client::KuduScanner::UNORDERED,
                 kudu::client::KuduScanner::ORDERED);

namespace kudu {
namespace client {

using internal::Batcher;
using internal::ErrorCollector;
using internal::MetaCache;

static const int kHtTimestampBitsToShift = 12;
static const char* kProgName = "kudu_client";

// We need to reroute all logging to stderr when the client library is
// loaded. GoogleOnceInit() can do that, but there are multiple entry
// points into the client code, and it'd need to be called in each one.
// So instead, let's use a constructor function.
//
// Should this be restricted to just the exported client build? Probably
// not, as any application using the library probably wants stderr logging
// more than file logging.
__attribute__((constructor))
static void InitializeBasicLogging() {
  InitGoogleLoggingSafeBasic(kProgName);
}

void InstallLoggingCallback(const LoggingCallback& cb) {
  RegisterLoggingCallback(cb);
}

void UninstallLoggingCallback() {
  UnregisterLoggingCallback();
}

void SetVerboseLogLevel(int level) {
  FLAGS_v = level;
}

KuduClientBuilder::KuduClientBuilder() {
  data_.reset(new KuduClientBuilder::Data());
}

KuduClientBuilder::~KuduClientBuilder() {
}

KuduClientBuilder& KuduClientBuilder::clear_master_server_addrs() {
  data_->master_server_addrs_.clear();
  return *this;
}

KuduClientBuilder& KuduClientBuilder::master_server_addrs(const vector<string>& addrs) {
  BOOST_FOREACH(const string& addr, addrs) {
    data_->master_server_addrs_.push_back(addr);
  }
  return *this;
}

KuduClientBuilder& KuduClientBuilder::add_master_server_addr(const string& addr) {
  data_->master_server_addrs_.push_back(addr);
  return *this;
}

KuduClientBuilder& KuduClientBuilder::default_admin_operation_timeout(const MonoDelta& timeout) {
  data_->default_admin_operation_timeout_ = timeout;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::default_rpc_timeout(const MonoDelta& timeout) {
  data_->default_rpc_timeout_ = timeout;
  return *this;
}

Status KuduClientBuilder::Build(shared_ptr<KuduClient>* client) {
  shared_ptr<KuduClient> c(new KuduClient());

  // Init messenger.
  MessengerBuilder builder("client");
  RETURN_NOT_OK(builder.Build(&c->data_->messenger_));

  c->data_->master_server_addrs_ = data_->master_server_addrs_;
  c->data_->default_admin_operation_timeout_ = data_->default_admin_operation_timeout_;
  c->data_->default_rpc_timeout_ = data_->default_rpc_timeout_;

  // Let's allow for plenty of time for discovering the master the first
  // time around.
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(c->default_admin_operation_timeout());
  RETURN_NOT_OK_PREPEND(c->data_->SetMasterServerProxy(c.get(), deadline),
                        "Could not locate the leader master");

  c->data_->meta_cache_.reset(new MetaCache(c.get()));
  c->data_->dns_resolver_.reset(new DnsResolver());

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
  return data_->IsCreateTableInProgress(this, table_name, deadline, create_in_progress);
}

Status KuduClient::DeleteTable(const string& table_name) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->DeleteTable(this, table_name, deadline);
}

gscoped_ptr<KuduTableAlterer> KuduClient::NewTableAlterer() {
  return gscoped_ptr<KuduTableAlterer>(new KuduTableAlterer(this));
}

Status KuduClient::IsAlterTableInProgress(const string& table_name,
                                          bool *alter_in_progress) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->IsAlterTableInProgress(this, table_name, deadline, alter_in_progress);
}

Status KuduClient::GetTableSchema(const string& table_name,
                                  KuduSchema* schema) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->GetTableSchema(this,
                               table_name,
                               deadline,
                               schema);
}

Status KuduClient::ListTabletServers(vector<KuduTabletServer*>* tablet_servers) {
  ListTabletServersRequestPB req;
  ListTabletServersResponsePB resp;

  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  Status s =
      data_->SyncLeaderMasterRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>(
          deadline,
          this,
          req,
          &resp,
          NULL,
          &MasterServiceProxy::ListTabletServers);
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  for (int i = 0; i < resp.servers_size(); i++) {
    const ListTabletServersResponsePB_Entry& e = resp.servers(i);
    KuduTabletServer* ts = new KuduTabletServer();
    ts->data_.reset(new KuduTabletServer::Data(e.instance_id().permanent_uuid(),
                                               e.registration().rpc_addresses(0).host()));
    tablet_servers->push_back(ts);
  }
  return Status::OK();
}

Status KuduClient::ListTables(vector<string>* tables,
                              const string& filter) {
  ListTablesRequestPB req;
  ListTablesResponsePB resp;

  if (!filter.empty()) {
    req.set_name_filter(filter);
  }
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  Status s =
      data_->SyncLeaderMasterRpc<ListTablesRequestPB, ListTablesResponsePB>(
          deadline,
          this,
          req,
          &resp,
          NULL,
          &MasterServiceProxy::ListTables);
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  for (int i = 0; i < resp.tables_size(); i++) {
    tables->push_back(resp.tables(i).name());
  }
  return Status::OK();
}

Status KuduClient::TableExists(const string& table_name, bool* exists) {
  std::vector<std::string> tables;
  RETURN_NOT_OK(ListTables(&tables, table_name));
  BOOST_FOREACH(const string& table, tables) {
    if (table == table_name) {
      *exists = true;
      return Status::OK();
    }
  }
  *exists = false;
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

bool KuduClient::IsMultiMaster() const {
  return data_->master_server_addrs_.size() > 1;
}

const MonoDelta& KuduClient::default_admin_operation_timeout() const {
  return data_->default_admin_operation_timeout_;
}

const MonoDelta& KuduClient::default_rpc_timeout() const {
  return data_->default_rpc_timeout_;
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

KuduTableCreator& KuduTableCreator::timeout(const MonoDelta& timeout) {
  data_->timeout_ = timeout;
  return *this;
}

KuduTableCreator& KuduTableCreator::wait(bool wait) {
  data_->wait_ = wait;
  return *this;
}

Status KuduTableCreator::Create() {
  if (!data_->table_name_.length()) {
    return Status::InvalidArgument("Missing table name");
  }
  if (!data_->schema_) {
    return Status::InvalidArgument("Missing schema");
  }

  // Build request.
  CreateTableRequestPB req;
  req.set_name(data_->table_name_);
  if (data_->num_replicas_ >= 1) {
    req.set_num_replicas(data_->num_replicas_);
  }
  RETURN_NOT_OK_PREPEND(SchemaToPB(*data_->schema_->schema_, req.mutable_schema()),
                        "Invalid schema");
  BOOST_FOREACH(const string& key, data_->split_keys_) {
    req.add_pre_split_keys(key);
  }

  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  if (data_->timeout_.Initialized()) {
    deadline.AddDelta(data_->timeout_);
  } else {
    deadline.AddDelta(data_->client_->default_admin_operation_timeout());
  }

  RETURN_NOT_OK_PREPEND(data_->client_->data_->CreateTable(data_->client_,
                                                           req,
                                                           *data_->schema_,
                                                           deadline),
                        strings::Substitute("Error creating table $0 on the master",
                                            data_->table_name_));

  // Spin until the table is fully created, if requested.
  if (data_->wait_) {
    RETURN_NOT_OK(data_->client_->data_->WaitForCreateTableToFinish(data_->client_,
                                                                    data_->table_name_,
                                                                    deadline));
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
    lock_guard<simple_spinlock> l(&data_->lock_);
    data_->NewBatcher(shared_from_this(), &old_batcher);
    InsertOrDie(&data_->flushed_batchers_, old_batcher.get());
  }

  // Send off any buffered data. Important to do this outside of the lock
  // since the callback may itself try to take the lock, in the case that
  // the batch fails "inline" on the same thread.
  old_batcher->FlushAsync(user_callback);
}

bool KuduSession::HasPendingOperations() const {
  lock_guard<simple_spinlock> l(&data_->lock_);
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
  lock_guard<simple_spinlock> l(&data_->lock_);
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
                                               KuduColumnSchema::DataType type,
                                               const void *default_value,
                                               KuduColumnStorageAttributes attributes) {
  if (default_value == NULL) {
    data_->status_ = Status::InvalidArgument("A new column must have a default value",
                                             "Use AddNullableColumn() to add a NULLABLE column");
  }

  AlterTableRequestPB::Step* step = data_->alter_steps_.add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::ADD_COLUMN);
  ColumnStorageAttributes attr_priv(ToInternalEncodingType(attributes.encoding()),
                                    ToInternalCompressionType(attributes.compression()));
  ColumnSchemaToPB(ColumnSchema(name, ToInternalDataType(type), false,
                                default_value, default_value, attr_priv),
                   step->mutable_add_column()->mutable_schema());
  return *this;
}

KuduTableAlterer& KuduTableAlterer::add_nullable_column(const string& name,
                                                        KuduColumnSchema::DataType type,
                                                        KuduColumnStorageAttributes attributes) {
  AlterTableRequestPB::Step* step = data_->alter_steps_.add_alter_schema_steps();
  step->set_type(AlterTableRequestPB::ADD_COLUMN);
  ColumnStorageAttributes attr_priv(ToInternalEncodingType(attributes.encoding()),
                                    ToInternalCompressionType(attributes.compression()));
  ColumnSchemaToPB(ColumnSchema(name, ToInternalDataType(type), true,
                                NULL, NULL, attr_priv),
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

KuduTableAlterer& KuduTableAlterer::timeout(const MonoDelta& timeout) {
  data_->timeout_ = timeout;
  return *this;
}

KuduTableAlterer& KuduTableAlterer::wait(bool wait) {
  data_->wait_ = wait;
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

  MonoDelta timeout = data_->timeout_.Initialized() ?
    data_->timeout_ :
    data_->client_->default_admin_operation_timeout();
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(timeout);
  RETURN_NOT_OK(data_->client_->data_->AlterTable(data_->client_, data_->alter_steps_, deadline));
  if (data_->wait_) {
    string alter_name = data_->alter_steps_.has_new_table_name() ?
        data_->alter_steps_.new_table_name() : data_->alter_steps_.table().table_name();
    RETURN_NOT_OK(data_->client_->data_->WaitForAlterTableToFinish(
        data_->client_, alter_name, deadline));
  }

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

Status KuduScanner::SetOrderMode(OrderMode order_mode) {
  if (data_->open_) {
    return Status::IllegalState("Order mode must be set before Open()");
  }
  if (!tight_enum_test<OrderMode>(order_mode)) {
    return Status::InvalidArgument("Bad order mode");
  }
  data_->order_mode_ = order_mode;
  return Status::OK();
}

Status KuduScanner::SetSnapshotMicros(uint64_t snapshot_timestamp_micros) {
  if (data_->open_) {
    return Status::IllegalState("Snapshot timestamp must be set before Open()");
  }
  // Shift the HT timestamp bits to get well-formed HT timestamp with the logical
  // bits zeroed out.
  data_->snapshot_timestamp_ = snapshot_timestamp_micros << kHtTimestampBitsToShift;
  return Status::OK();
}

Status KuduScanner::SetSnapshotRaw(uint64_t snapshot_timestamp) {
  if (data_->open_) {
    return Status::IllegalState("Snapshot timestamp must be set before Open()");
  }
  // Shift the HT timestamp bits to get well-formed HT timestamp with the logical
  // bits zeroed out.
  data_->snapshot_timestamp_ = snapshot_timestamp;
  return Status::OK();
}

Status KuduScanner::SetSelection(KuduClient::ReplicaSelection selection) {
  if (data_->open_) {
    return Status::IllegalState("Replica selection must be set before Open()");
  }
  data_->selection_ = selection;
  return Status::OK();
}

Status KuduScanner::SetTimeoutMillis(int millis) {
  if (data_->open_) {
    return Status::IllegalState("Timeout must be set before Open()");
  }
  data_->timeout_ = MonoDelta::FromMilliseconds(millis);
  return Status::OK();
}

Status KuduScanner::AddConjunctPredicate(const KuduColumnRangePredicate& pred) {
  if (data_->open_) {
    return Status::IllegalState("Predicate must be set before Open()");
  }
  data_->spec_.AddPredicate(*pred.pred_);
  return Status::OK();
}

Status KuduScanner::AddLowerBound(const Slice& key) {
  gscoped_ptr<EncodedKey> enc_key;
  RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
                  *data_->table_->schema().schema_, &data_->arena_, key, &enc_key));
  data_->spec_.SetLowerBoundKey(enc_key.get());
  data_->pool_.Add(enc_key.release());
  return Status::OK();
}
Status KuduScanner::AddUpperBound(const Slice& key) {
  gscoped_ptr<EncodedKey> enc_key;
  RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
                  *data_->table_->schema().schema_, &data_->arena_, key, &enc_key));
  data_->spec_.SetUpperBoundKey(enc_key.get());
  data_->pool_.Add(enc_key.release());
  return Status::OK();
}

Status KuduScanner::SetCacheBlocks(bool cache_blocks) {
  if (data_->open_) {
    return Status::IllegalState("Block caching must be set before Open()");
  }
  data_->spec_.set_cache_blocks(cache_blocks);
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
  Slice start_key = data_->spec_.lower_bound_key() ?
    data_->spec_.lower_bound_key()->encoded_key() : Slice("INF");
  Slice end_key = data_->spec_.upper_bound_key() ?
    data_->spec_.upper_bound_key()->encoded_key() : Slice("INF");
  return strings::Substitute("$0: [$1,$2]", data_->table_->name(),
                             start_key.ToDebugString(), end_key.ToDebugString());
}

Status KuduScanner::Open() {
  CHECK(!data_->open_) << "Scanner already open";
  CHECK(data_->projection_ != NULL) << "No projection provided";

  // Find the first tablet.
  data_->spec_encoder_.EncodeRangePredicates(&data_->spec_, false);

  VLOG(1) << "Beginning scan " << ToString();

  RETURN_NOT_OK(data_->OpenTablet(data_->spec_.lower_bound_key() != NULL
                                  ? data_->spec_.lower_bound_key()->encoded_key() : Slice()));

  data_->open_ = true;
  return Status::OK();
}

void KuduScanner::Close() {
  if (!data_->open_) return;
  CHECK(data_->proxy_);

  VLOG(1) << "Ending scan " << ToString();

  // Close the scanner on the server-side, if necessary.
  //
  // If the scan did not match any rows, the tserver will not assign a scanner ID.
  // This is reflected in the Open() response. In this case, there is no server-side state
  // to clean up.
  if (!data_->next_req_.scanner_id().empty()) {
    gscoped_ptr<CloseCallback> closer(new CloseCallback);
    closer->scanner_id = data_->next_req_.scanner_id();
    data_->PrepareRequest(KuduScanner::Data::CLOSE);
    data_->next_req_.set_close_scanner(true);
    closer->controller.set_timeout(data_->timeout_);
    data_->proxy_->ScanAsync(data_->next_req_, &closer->response, &closer->controller,
                             boost::bind(&CloseCallback::Callback, closer.get()));
    ignore_result(closer.release());
  }
  data_->proxy_.reset();
  data_->open_ = false;
  return;
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
    VLOG(1) << "Continuing scan " << ToString();

    data_->controller_.Reset();
    data_->controller_.set_timeout(data_->timeout_);
    data_->PrepareRequest(KuduScanner::Data::CONTINUE);
    Status s = data_->proxy_->Scan(data_->next_req_,
                                   &data_->last_response_,
                                   &data_->controller_);
    // If the RPC failed, mark this tablet server as failed.
    // Ordered scans can be restarted at another tablet server via a fresh open.
    // Unordered scans must return an error status up to the client.
    if (!s.ok()) {
      data_->table_->client()->data_->meta_cache_->MarkTSFailed(data_->ts_, s);
      if (data_->order_mode_ == ORDERED) {
        LOG(WARNING) << "Scan at tablet server " << data_->ts_->ToString() << " of tablet "
            << ToString() << " failed, retrying scan elsewhere.";
        // Use the start key of the current tablet as the start key.
        return data_->OpenTablet(data_->remote_->start_key());
      }
      return s;
    }
    if (data_->last_response_.has_encoded_last_row_key()) {
      data_->encoded_last_row_key_ = data_->last_response_.encoded_last_row_key();
    }
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

KuduTabletServer::KuduTabletServer() {
}

KuduTabletServer::~KuduTabletServer() {
}

const string& KuduTabletServer::uuid() const {
  return data_->uuid_;
}

const string& KuduTabletServer::hostname() const {
  return data_->hostname_;
}

} // namespace client
} // namespace kudu
