// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/transactions/txn_system_client.h"

#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/schema.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/transactions/coordinator_rpc.h"
#include "kudu/transactions/participant_rpc.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/net/net_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduSchema;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::internal::MetaCache;
using kudu::tserver::CoordinatorOpPB;
using kudu::tserver::CoordinatorOpResultPB;
using kudu::tserver::ParticipantOpPB;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace transactions {

Status TxnSystemClient::Create(const vector<HostPort>& master_addrs,
                               unique_ptr<TxnSystemClient>* sys_client) {
  vector<string> master_strings;
  for (const auto& hp : master_addrs) {
    master_strings.emplace_back(hp.ToString());
  }
  DCHECK(!master_addrs.empty());
  KuduClientBuilder builder;
  builder.master_server_addrs(master_strings);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(builder.Build(&client));
  sys_client->reset(new TxnSystemClient(std::move(client)));
  return Status::OK();
}

Status TxnSystemClient::CreateTxnStatusTableWithClient(int64_t initial_upper_bound,
                                                       int num_replicas,
                                                       KuduClient* client) {

  const auto& schema = TxnStatusTablet::GetSchema();
  const auto kudu_schema = KuduSchema::FromSchema(schema);

  // Add range partitioning to the transaction status table with an initial
  // upper bound, allowing us to add and drop ranges in the future.
  unique_ptr<KuduPartialRow> lb(new KuduPartialRow(&schema));
  unique_ptr<KuduPartialRow> ub(new KuduPartialRow(&schema));
  RETURN_NOT_OK(lb->SetInt64(TxnStatusTablet::kTxnIdColName, 0));
  RETURN_NOT_OK(ub->SetInt64(TxnStatusTablet::kTxnIdColName, initial_upper_bound));

  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  table_creator->data_->table_type_ = TableTypePB::TXN_STATUS_TABLE;
  // NOTE: we don't set an owner here because, presumably, we're running as a
  // part of the Kudu service -- the Kudu master should default ownership to
  // the currently running user, authorizing us as appropriate in so doing.
  // TODO(awong): ensure that transaction status managers only accept requests
  // when their replicas are leader. For now, ensure this is the case by making
  // them non-replicated.
  return table_creator->schema(&kudu_schema)
      .set_range_partition_columns({ TxnStatusTablet::kTxnIdColName })
      .add_range_partition(lb.release(), ub.release())
      .table_name(TxnStatusTablet::kTxnStatusTableName)
      .num_replicas(num_replicas)
      .wait(true)
      .Create();
}

Status TxnSystemClient::AddTxnStatusTableRangeWithClient(int64_t lower_bound, int64_t upper_bound,
                                                         KuduClient* client) {
  const auto& schema = TxnStatusTablet::GetSchema();
  unique_ptr<KuduPartialRow> lb(new KuduPartialRow(&schema));
  unique_ptr<KuduPartialRow> ub(new KuduPartialRow(&schema));
  RETURN_NOT_OK(lb->SetInt64(TxnStatusTablet::kTxnIdColName, lower_bound));
  RETURN_NOT_OK(ub->SetInt64(TxnStatusTablet::kTxnIdColName, upper_bound));
  unique_ptr<KuduTableAlterer> alterer(
      client->NewTableAlterer(TxnStatusTablet::kTxnStatusTableName));
  return alterer->AddRangePartition(lb.release(), ub.release())
      ->modify_external_catalogs(false)
      ->wait(true)
      ->Alter();
}

Status TxnSystemClient::OpenTxnStatusTable() {
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(TxnStatusTablet::kTxnStatusTableName, &table));

  std::lock_guard<simple_spinlock> l(table_lock_);
  txn_status_table_ = std::move(table);
  return Status::OK();
}

Status TxnSystemClient::BeginTransaction(int64_t txn_id,
                                         const string& user,
                                         uint32_t* txn_keepalive_ms,
                                         int64_t* highest_seen_txn_id,
                                         MonoDelta timeout) {
  CoordinatorOpPB coordinate_txn_op;
  coordinate_txn_op.set_type(CoordinatorOpPB::BEGIN_TXN);
  coordinate_txn_op.set_txn_id(txn_id);
  coordinate_txn_op.set_user(user);
  Synchronizer s;
  CoordinatorOpResultPB result;
  RETURN_NOT_OK(CoordinateTransactionAsync(std::move(coordinate_txn_op),
                                           timeout,
                                           s.AsStatusCallback(),
                                           &result));
  const auto ret = s.Wait();
  if (ret.ok()) {
    DCHECK(result.has_highest_seen_txn_id());
    DCHECK(result.has_keepalive_millis());
    if (txn_keepalive_ms) {
      *txn_keepalive_ms = result.keepalive_millis();
    }
  }
  // The 'highest_seen_tnx_id' field in the 'result' can be set in case of
  // some non-OK cases as well.
  if (result.has_highest_seen_txn_id() && highest_seen_txn_id) {
    *highest_seen_txn_id = result.highest_seen_txn_id();
  }
  return ret;
}

Status TxnSystemClient::RegisterParticipant(int64_t txn_id, const string& participant_id,
                                            const string& user, MonoDelta timeout) {
  CoordinatorOpPB coordinate_txn_op;
  coordinate_txn_op.set_type(CoordinatorOpPB::REGISTER_PARTICIPANT);
  coordinate_txn_op.set_txn_id(txn_id);
  coordinate_txn_op.set_txn_participant_id(participant_id);
  coordinate_txn_op.set_user(user);
  Synchronizer s;
  RETURN_NOT_OK(CoordinateTransactionAsync(std::move(coordinate_txn_op),
                                           timeout,
                                           s.AsStatusCallback()));
  return s.Wait();
}

Status TxnSystemClient::BeginCommitTransaction(int64_t txn_id,
                                               const string& user,
                                               MonoDelta timeout) {
  CoordinatorOpPB coordinate_txn_op;
  coordinate_txn_op.set_type(CoordinatorOpPB::BEGIN_COMMIT_TXN);
  coordinate_txn_op.set_txn_id(txn_id);
  coordinate_txn_op.set_user(user);
  Synchronizer s;
  RETURN_NOT_OK(CoordinateTransactionAsync(std::move(coordinate_txn_op),
                                           timeout,
                                           s.AsStatusCallback()));
  return s.Wait();
}

Status TxnSystemClient::AbortTransaction(int64_t txn_id,
                                         const string& user,
                                         MonoDelta timeout) {
  CoordinatorOpPB coordinate_txn_op;
  coordinate_txn_op.set_type(CoordinatorOpPB::ABORT_TXN);
  coordinate_txn_op.set_txn_id(txn_id);
  coordinate_txn_op.set_user(user);
  Synchronizer s;
  RETURN_NOT_OK(CoordinateTransactionAsync(std::move(coordinate_txn_op),
                                           timeout,
                                           s.AsStatusCallback()));
  return s.Wait();
}

Status TxnSystemClient::GetTransactionStatus(int64_t txn_id,
                                             const string& user,
                                             TxnStatusEntryPB* txn_status,
                                             MonoDelta timeout) {
  DCHECK(txn_status);
  CoordinatorOpPB coordinate_txn_op;
  coordinate_txn_op.set_type(CoordinatorOpPB::GET_TXN_STATUS);
  coordinate_txn_op.set_txn_id(txn_id);
  coordinate_txn_op.set_user(user);
  Synchronizer s;
  CoordinatorOpResultPB result;
  RETURN_NOT_OK(CoordinateTransactionAsync(std::move(coordinate_txn_op),
                                           timeout,
                                           s.AsStatusCallback(),
                                           &result));
  const auto ret = s.Wait();
  if (ret.ok()) {
    // Retrieve the response and set corresponding output parameters.
    DCHECK(!result.has_op_error());
    DCHECK(result.has_txn_status());
    TxnStatusEntryPB ret;
    ret.set_state(result.txn_status().state());
    ret.set_allocated_user(result.mutable_txn_status()->release_user());
    *txn_status = std::move(ret);
  }
  return ret;
}

Status TxnSystemClient::KeepTransactionAlive(int64_t txn_id,
                                             const string& user,
                                             MonoDelta timeout) {
  CoordinatorOpPB coordinate_txn_op;
  coordinate_txn_op.set_type(CoordinatorOpPB::KEEP_TXN_ALIVE);
  coordinate_txn_op.set_txn_id(txn_id);
  coordinate_txn_op.set_user(user);
  Synchronizer s;
  RETURN_NOT_OK(CoordinateTransactionAsync(std::move(coordinate_txn_op),
                                           timeout,
                                           s.AsStatusCallback()));
  return s.Wait();
}

Status TxnSystemClient::CoordinateTransactionAsync(CoordinatorOpPB coordinate_txn_op,
                                                   const MonoDelta& timeout,
                                                   const StatusCallback& cb,
                                                   CoordinatorOpResultPB* result) {
  DCHECK(txn_status_table_);
  const MonoTime deadline = MonoTime::Now() + timeout;
  unique_ptr<TxnStatusTabletContext> ctx(
      new TxnStatusTabletContext({
          txn_status_table(),
          std::move(coordinate_txn_op),
          /*tablet=*/nullptr
      }));

  string partition_key;
  KuduPartialRow row(&TxnStatusTablet::GetSchema());
  DCHECK(ctx->coordinate_txn_op.has_txn_id());
  RETURN_NOT_OK(row.SetInt64(TxnStatusTablet::kTxnIdColName, ctx->coordinate_txn_op.txn_id()));
  RETURN_NOT_OK(ctx->table->partition_schema().EncodeKey(row, &partition_key));

  TxnStatusTabletContext* ctx_raw = ctx.release();
  client_->data_->meta_cache_->LookupTabletByKey(
      ctx_raw->table.get(),
      std::move(partition_key),
      deadline,
      MetaCache::LookupType::kPoint,
      &ctx_raw->tablet,
      // TODO(awong): when we start using C++14, stack-allocate 'ctx' and
      // move capture it.
      [cb, deadline, ctx_raw, result] (const Status& s) {
        // First, take ownership of the context.
        unique_ptr<TxnStatusTabletContext> ctx(ctx_raw);

        // If the lookup failed, run the callback with the error.
        if (PREDICT_FALSE(!s.ok())) {
          cb(s);
          return;
        }
        // NOTE: the CoordinatorRpc frees its own memory upon completion.
        CoordinatorRpc* rpc = CoordinatorRpc::NewRpc(
            std::move(ctx),
            deadline,
            cb,
            result);
        rpc->SendRpc();
      });
  return Status::OK();
}

Status TxnSystemClient::ParticipateInTransaction(const string& tablet_id,
                                                 const ParticipantOpPB& participant_op,
                                                 const MonoDelta& timeout,
                                                 Timestamp* begin_commit_timestamp) {
  Synchronizer sync;
  ParticipateInTransactionAsync(tablet_id, participant_op, timeout,
                                sync.AsStatusCallback(), begin_commit_timestamp);
  return sync.Wait();
}

void TxnSystemClient::ParticipateInTransactionAsync(const string& tablet_id,
                                                    ParticipantOpPB participant_op,
                                                    const MonoDelta& timeout,
                                                    StatusCallback cb,
                                                    Timestamp* begin_commit_timestamp) {
  MonoTime deadline = MonoTime::Now() + timeout;
  unique_ptr<TxnParticipantContext> ctx(
      new TxnParticipantContext({
          client_.get(),
          std::move(participant_op),
          /*tablet*/nullptr,
      }));
  TxnParticipantContext* ctx_raw = ctx.release();
  // TODO(awong): find a clever way around constructing a std::function here
  // (maybe some fancy template magic?). For now, we're forced to pass the raw
  // 'ctx' instead of moving it directly.
  // See https://taylorconor.com/blog/noncopyable-lambdas/ for more details.
  client_->data_->meta_cache_->LookupTabletById(
      client_.get(), tablet_id, deadline, &ctx_raw->tablet,
      [cb = std::move(cb), deadline, ctx_raw, begin_commit_timestamp] (const Status& s) mutable {
        unique_ptr<TxnParticipantContext> unique_ctx(ctx_raw);
        if (PREDICT_FALSE(!s.ok())) {
          cb(s);
          return;
        }
        ParticipantRpc* rpc = ParticipantRpc::NewRpc(
            std::move(unique_ctx),
            deadline,
            std::move(cb),
            begin_commit_timestamp);
        rpc->SendRpc();
      });
}

} // namespace transactions
} // namespace kudu
