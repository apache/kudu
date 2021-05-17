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

#include "kudu/transactions/txn_status_tablet.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/faststring.h"
#include "kudu/util/once.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"

using kudu::tablet::LatchOpCompletionCallback;
using kudu::tablet::OpCompletionCallback;
using kudu::tablet::WriteOpState;
using kudu::tserver::TabletServerErrorPB;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace transactions {

namespace {

int kTxnIdColIdx = -1;
int kEntryTypeColIdx = -1;
int kIdentifierColIdx = -1;
int kMetadataColIdx = -1;
// Initializes the column indices of the transaction status tablet.
Status InitTxnStatusColIdxs() {
  static KuduOnceLambda col_idx_initializer;
  return col_idx_initializer.Init([] {
    const auto& schema = TxnStatusTablet::GetSchemaWithoutIds();
    kTxnIdColIdx = schema.find_column(TxnStatusTablet::kTxnIdColName);
    kEntryTypeColIdx = schema.find_column(TxnStatusTablet::kEntryTypeColName);
    kIdentifierColIdx = schema.find_column(TxnStatusTablet::kIdentifierColName);
    kMetadataColIdx = schema.find_column(TxnStatusTablet::kMetadataColName);
    return Status::OK();
  });
}
// NOTE: these column index getters should only be used once a TxnStatusTablet
// has been constructed.
int TxnIdColIdx() {
  DCHECK_NE(-1, kTxnIdColIdx);
  return kTxnIdColIdx;
}
int EntryTypeColIdx() {
  DCHECK_NE(-1, kEntryTypeColIdx);
  return kEntryTypeColIdx;
}
int IdentifierColIdx() {
  DCHECK_NE(-1, kIdentifierColIdx);
  return kIdentifierColIdx;
}
int MetadataColIdx() {
  DCHECK_NE(-1, kMetadataColIdx);
  return kMetadataColIdx;
}

Schema kTxnStatusSchema;
Schema kTxnStatusSchemaNoIds;
// Populates the schema of the transaction status table.
Status PopulateTxnStatusSchema(SchemaBuilder* builder) {
  RETURN_NOT_OK(builder->AddKeyColumn(TxnStatusTablet::kTxnIdColName, INT64));
  RETURN_NOT_OK(builder->AddKeyColumn(TxnStatusTablet::kEntryTypeColName, INT8));
  RETURN_NOT_OK(builder->AddKeyColumn(TxnStatusTablet::kIdentifierColName, STRING));
  return builder->AddColumn(TxnStatusTablet::kMetadataColName, STRING);
}
// Initializes the static transaction status schema.
Status InitTxnStatusSchemaOnce() {
  static KuduOnceLambda schema_initializer;
  return schema_initializer.Init([] {
    SchemaBuilder builder;
    RETURN_NOT_OK(PopulateTxnStatusSchema(&builder));
    kTxnStatusSchema = builder.Build();
    return Status::OK();
  });
}
Status InitTxnStatusSchemaWithNoIdsOnce() {
  static KuduOnceLambda schema_initializer;
  return schema_initializer.Init([] {
    SchemaBuilder builder;
    RETURN_NOT_OK(PopulateTxnStatusSchema(&builder));
    kTxnStatusSchemaNoIds = builder.BuildWithoutIds();
    return Status::OK();
  });
}

WriteRequestPB kTxnStatusWriteReqPB;
// Initializes the static transaction status tablet write request.
Status InitWriteRequestPBOnce() {
  static KuduOnceLambda write_initializer;
  return write_initializer.Init([] {
    return SchemaToPB(TxnStatusTablet::GetSchemaWithoutIds(),
                      kTxnStatusWriteReqPB.mutable_schema());
  });
}

// Returns a write request for the transaction status tablet of the given ID.
WriteRequestPB BuildWriteReqPB(const string& tablet_id) {
  CHECK_OK(InitWriteRequestPBOnce());
  WriteRequestPB req = kTxnStatusWriteReqPB;
  req.set_tablet_id(tablet_id);
  return req;
}

// Return the values of the keys of the given transaction status tablet row.
void ExtractKeys(const RowBlockRow& row, int64_t* txn_id, int8_t* entry_type, Slice* identifier) {
  const auto& schema = TxnStatusTablet::GetSchemaWithoutIds();
  *txn_id = *schema.ExtractColumnFromRow<INT64>(row, TxnIdColIdx());
  *entry_type = *schema.ExtractColumnFromRow<INT8>(row, EntryTypeColIdx());
  *identifier = *schema.ExtractColumnFromRow<STRING>(row, IdentifierColIdx());
}

template <typename T>
Status ExtractMetadataEntry(const RowBlockRow& row, T* pb) {
  const auto& schema = TxnStatusTablet::GetSchemaWithoutIds();
  const Slice* entry = schema.ExtractColumnFromRow<STRING>(row, MetadataColIdx());
  Status s = pb_util::ParseFromArray(pb, entry->data(), entry->size());
  if (PREDICT_FALSE(!s.ok())) {
    int64_t txn_id;
    int8_t entry_type;
    Slice identifier;
    ExtractKeys(row, &txn_id, &entry_type, &identifier);
    VLOG(2) << Substitute("bad entry: $0", entry->ToString());
    return s.CloneAndPrepend(
        Substitute("unable to parse entry for $0 record of transaction ID $1 ($2)",
                   txn_id, entry_type, identifier.ToString()));
  }
  return Status::OK();
}

Status PopulateTransactionEntryRow(int64_t txn_id, const faststring& entry, KuduPartialRow* row) {
  RETURN_NOT_OK(row->SetInt64(TxnStatusTablet::kTxnIdColName, txn_id));
  RETURN_NOT_OK(row->SetInt8(TxnStatusTablet::kEntryTypeColName, TxnStatusTablet::TRANSACTION));
  RETURN_NOT_OK(row->SetString(TxnStatusTablet::kIdentifierColName, ""));
  return row->SetString(TxnStatusTablet::kMetadataColName, entry);
}

Status PopulateParticipantEntryRow(int64_t txn_id, const string& tablet_id, const faststring& entry,
                                   KuduPartialRow* row) {
  RETURN_NOT_OK(row->SetInt64(TxnStatusTablet::kTxnIdColName, txn_id));
  RETURN_NOT_OK(row->SetInt8(TxnStatusTablet::kEntryTypeColName, TxnStatusTablet::PARTICIPANT));
  RETURN_NOT_OK(row->SetString(TxnStatusTablet::kIdentifierColName, tablet_id));
  return row->SetString(TxnStatusTablet::kMetadataColName, entry);
}

} // anonymous namespace

const char* const TxnStatusTablet::kTxnIdColName = "txn_id";
const char* const TxnStatusTablet::kEntryTypeColName = "entry_type";
const char* const TxnStatusTablet::kIdentifierColName = "identifier";
const char* const TxnStatusTablet::kMetadataColName = "metadata";
const char* const TxnStatusTablet::kTxnStatusTableName = "kudu_system.kudu_transactions";

TxnStatusTablet::TxnStatusTablet(tablet::TabletReplica* tablet_replica)
    : tablet_replica_(DCHECK_NOTNULL(tablet_replica)) {
  CHECK_OK(InitTxnStatusColIdxs());
}

const Schema& TxnStatusTablet::GetSchema() {
  CHECK_OK(InitTxnStatusSchemaOnce());
  return kTxnStatusSchema;
}
const Schema& TxnStatusTablet::GetSchemaWithoutIds() {
  CHECK_OK(InitTxnStatusSchemaWithNoIdsOnce());
  return kTxnStatusSchemaNoIds;
}

Status TxnStatusTablet::VisitTransactions(TransactionsVisitor* visitor) {
  const auto& schema = GetSchemaWithoutIds();
  // There are only TRANSACTION and PARTICIPANT entries today, but this filter
  // is conservative in case we add more entry types in the future.
  faststring record_types;
  record_types.push_back(TRANSACTION);
  record_types.push_back(PARTICIPANT);
  vector<const void*> pred_record_types = { &record_types.at(0), &record_types.at(1) };
  auto pred = ColumnPredicate::InList(schema.column(EntryTypeColIdx()), &pred_record_types);

  ScanSpec spec;
  spec.AddPredicate(pred);
  unique_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet_replica_->tablet()->NewOrderedRowIterator(schema, &iter));
  RETURN_NOT_OK(iter->Init(&spec));

  // Keep track of the current transaction ID so we know when to start a new
  // transaction.
  boost::optional<int64_t> prev_txn_id = boost::none;
  TxnStatusEntryPB prev_status_entry_pb;
  vector<ParticipantIdAndPB> prev_participants;
  RowBlockMemory mem;
  RowBlock block(&iter->schema(), 512, &mem);
  // Iterate over the transaction and participant entries, notifying the
  // visitor once a transaction and all its participants have been found.
  while (iter->HasNext()) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    const size_t nrows = block.nrows();
    for (size_t i = 0; i < nrows; ++i) {
      if (!block.selection_vector()->IsRowSelected(i)) {
        continue;
      }
      const auto& row = block.row(i);
      int64_t txn_id;
      int8_t entry_type;
      Slice identifier;
      ExtractKeys(row, &txn_id, &entry_type, &identifier);
      switch (entry_type) {
        case TRANSACTION: {
          if (PREDICT_FALSE(prev_txn_id && *prev_txn_id == txn_id)) {
            return Status::Corruption(
                Substitute("duplicate transaction entry: $0", txn_id));
          }
          if (prev_txn_id) {
            // We've previously collected the state for a transaction. Signal
            // to the visitor what the state of the previous transaction was.
            visitor->VisitTransactionEntries(*prev_txn_id, std::move(prev_status_entry_pb),
                                             std::move(prev_participants));

            // Sanity check: we're iterating in increasing txn_id order.
            DCHECK_GT(txn_id, *prev_txn_id);
          }
          prev_txn_id = txn_id;
          prev_participants.clear();
          RETURN_NOT_OK(ExtractMetadataEntry(row, &prev_status_entry_pb));
          continue;
        }
        case PARTICIPANT: {
          if (PREDICT_FALSE(!prev_txn_id || *prev_txn_id != txn_id)) {
            return Status::Corruption(
                Substitute("missing transaction status entry for $0$1", txn_id,
                           prev_txn_id ? Substitute(", currently on ID $0", *prev_txn_id) : ""));
          }
          TxnParticipantEntryPB pb;
          RETURN_NOT_OK(ExtractMetadataEntry(row, &pb));
          prev_participants.emplace_back(
              schema.ExtractColumnFromRow<STRING>(row, IdentifierColIdx())->ToString(),
              std::move(pb));
          continue;
        }
        default:
          LOG(DFATAL) << "Unknown entry type: " << entry_type;
          continue;
      }
    }
  }
  if (prev_txn_id) {
    visitor->VisitTransactionEntries(*prev_txn_id, std::move(prev_status_entry_pb),
                                     std::move(prev_participants));
  }
  return Status::OK();
}

Status TxnStatusTablet::AddNewTransaction(int64_t txn_id, const string& user,
                                          int64_t start_timestamp, TabletServerErrorPB* ts_error) {
  WriteRequestPB req = BuildWriteReqPB(tablet_replica_->tablet_id());

  TxnStatusEntryPB entry;
  entry.set_state(OPEN);
  entry.set_user(user);
  entry.set_start_timestamp(start_timestamp);
  entry.set_last_transition_timestamp(start_timestamp);
  faststring metadata_buf;
  pb_util::SerializeToString(entry, &metadata_buf);

  KuduPartialRow row(&GetSchemaWithoutIds());
  RETURN_NOT_OK(PopulateTransactionEntryRow(txn_id, metadata_buf, &row));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::INSERT_IGNORE, row);
  return SyncWrite(req, ts_error);
}

Status TxnStatusTablet::UpdateTransaction(int64_t txn_id, const TxnStatusEntryPB& pb,
                                          TabletServerErrorPB* ts_error) {
  WriteRequestPB req = BuildWriteReqPB(tablet_replica_->tablet_id());

  faststring metadata_buf;
  pb_util::SerializeToString(pb, &metadata_buf);

  KuduPartialRow row(&GetSchemaWithoutIds());
  RETURN_NOT_OK(PopulateTransactionEntryRow(txn_id, metadata_buf, &row));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::UPDATE, row);
  return SyncWrite(req, ts_error);
}

Status TxnStatusTablet::AddNewParticipant(int64_t txn_id, const string& tablet_id,
                                          TabletServerErrorPB* ts_error) {
  WriteRequestPB req = BuildWriteReqPB(tablet_replica_->tablet_id());

  TxnParticipantEntryPB entry;
  entry.set_state(OPEN);
  faststring metadata_buf;
  pb_util::SerializeToString(entry, &metadata_buf);

  KuduPartialRow row(&TxnStatusTablet::GetSchemaWithoutIds());
  PopulateParticipantEntryRow(txn_id, tablet_id, metadata_buf, &row);
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::INSERT_IGNORE, row);
  return SyncWrite(req, ts_error);
}

Status TxnStatusTablet::UpdateParticipant(int64_t txn_id, const string& tablet_id,
                                          const TxnParticipantEntryPB& pb,
                                          TabletServerErrorPB* ts_error) {
  WriteRequestPB req = BuildWriteReqPB(tablet_replica_->tablet_id());

  faststring metadata_buf;
  pb_util::SerializeToString(pb, &metadata_buf);

  KuduPartialRow row(&GetSchemaWithoutIds());
  RETURN_NOT_OK(PopulateParticipantEntryRow(txn_id, tablet_id, metadata_buf, &row));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::UPDATE, row);
  return SyncWrite(req, ts_error);
}

Status TxnStatusTablet::SyncWrite(const WriteRequestPB& req, TabletServerErrorPB* ts_error) {
  DCHECK(req.has_tablet_id());
  DCHECK(req.has_schema());
  CountDownLatch latch(1);
  WriteResponsePB resp;
  unique_ptr<OpCompletionCallback> op_cb(
      new LatchOpCompletionCallback<WriteResponsePB>(&latch, &resp));
  unique_ptr<WriteOpState> op_state(
      new WriteOpState(tablet_replica_,
                       &req,
                       nullptr, // RequestIdPB
                       &resp));
  op_state->set_completion_callback(std::move(op_cb));
  RETURN_NOT_OK(tablet_replica_->SubmitWrite(std::move(op_state)));
  latch.Wait();
  if (resp.has_error()) {
    DCHECK(ts_error);
    *ts_error = std::move(resp.error());
    return StatusFromPB(ts_error->status());
  }
  if (resp.per_row_errors_size() > 0) {
    for (const auto& error : resp.per_row_errors()) {
      LOG(ERROR) << Substitute(
          "row $0: $1", error.row_index(), StatusFromPB(error.error()).ToString());
    }
    return Status::Incomplete(
        Substitute("failed to write $0 rows to transaction status tablet $1",
                   resp.per_row_errors_size(), tablet_replica_->tablet_id()));
  }
  return Status::OK();
}

} // namespace transactions
} // namespace kudu
