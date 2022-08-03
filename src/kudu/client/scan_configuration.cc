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

#include "kudu/client/scan_configuration.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"

namespace kudu {
class PartitionKey;
}  // namespace kudu

using std::unique_ptr;
using std::string;
using std::vector;

namespace kudu {
namespace client {

const uint64_t ScanConfiguration::kNoTimestamp = KuduClient::kNoTimestamp;
const int ScanConfiguration::kHtTimestampBitsToShift = 12;
const char* ScanConfiguration::kDefaultIsDeletedColName = "is_deleted";

ScanConfiguration::ScanConfiguration(KuduTable* table)
    : table_(table),
      projection_(table->schema().schema_),
      client_projection_(KuduSchema::FromSchema(*table->schema().schema_)),
      has_batch_size_bytes_(false),
      batch_size_bytes_(0),
      selection_(KuduClient::CLOSEST_REPLICA),
      read_mode_(KuduScanner::READ_LATEST),
      is_fault_tolerant_(false),
      start_timestamp_(kNoTimestamp),
      snapshot_timestamp_(kNoTimestamp),
      lower_bound_propagation_timestamp_(kNoTimestamp),
      timeout_(MonoDelta::FromMilliseconds(KuduScanner::kScanTimeoutMillis)),
      arena_(256),
      row_format_flags_(KuduScanner::NO_FLAGS) {
}

Status ScanConfiguration::SetProjectedColumnNames(const vector<string>& col_names) {
  const Schema& schema = *table().schema().schema_;
  vector<int> col_indexes;
  col_indexes.reserve(col_names.size());
  for (const string& col_name : col_names) {
    int idx = schema.find_column(col_name);
    if (idx == Schema::kColumnNotFound) {
      return Status::NotFound(strings::Substitute(
            "Column: \"$0\" was not found in the table schema.", col_name));
    }
    col_indexes.push_back(idx);
  }
  return SetProjectedColumnIndexes(col_indexes);
}

Status ScanConfiguration::SetProjectedColumnIndexes(const vector<int>& col_indexes) {
  const Schema* table_schema = table_->schema().schema_;
  vector<ColumnSchema> cols;
  cols.reserve(col_indexes.size());
  for (const int col_index : col_indexes) {
    if (col_index < 0 || col_index >= table_schema->columns().size()) {
      return Status::NotFound(strings::Substitute(
            "Column index: $0 was not found in the table schema.", col_index));
    }
    cols.push_back(table_schema->column(col_index));
  }
  return CreateProjection(cols);
}

Status ScanConfiguration::AddConjunctPredicate(unique_ptr<KuduPredicate> pred) {
  // Take ownership even if returning non-OK status.
  auto* pred_raw_ptr = pred.get();
  predicates_pool_.emplace_back(std::move(pred));
  return pred_raw_ptr->data_->AddToScanSpec(&spec_, &arena_);
}

void ScanConfiguration::AddConjunctPredicate(ColumnPredicate pred) {
  spec_.AddPredicate(std::move(pred));
}

Status ScanConfiguration::AddLowerBound(const KuduPartialRow& key) {
  string encoded;
  RETURN_NOT_OK(key.EncodeRowKey(&encoded));
  return AddLowerBoundRaw(encoded);
}

Status ScanConfiguration::AddUpperBound(const KuduPartialRow& key) {
  string encoded;
  RETURN_NOT_OK(key.EncodeRowKey(&encoded));
  return AddUpperBoundRaw(encoded);
}

Status ScanConfiguration::AddLowerBoundRaw(const Slice& key) {
  // Make a copy of the key.
  EncodedKey* enc_key = nullptr;
  RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
                  *table_->schema().schema_, &arena_, key, &enc_key));
  spec_.SetLowerBoundKey(enc_key);
  return Status::OK();
}

Status ScanConfiguration::AddUpperBoundRaw(const Slice& key) {
  // Make a copy of the key.
  EncodedKey* enc_key = nullptr;
  RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
                  *table_->schema().schema_, &arena_, key, &enc_key));
  spec_.SetExclusiveUpperBoundKey(enc_key);
  return Status::OK();
}

Status ScanConfiguration::AddLowerBoundPartitionKeyRaw(const PartitionKey& pkey) {
  spec_.SetLowerBoundPartitionKey(pkey);
  return Status::OK();
}

Status ScanConfiguration::AddUpperBoundPartitionKeyRaw(const PartitionKey& pkey) {
  spec_.SetExclusiveUpperBoundPartitionKey(pkey);
  return Status::OK();
}

Status ScanConfiguration::SetCacheBlocks(bool cache_blocks) {
  spec_.set_cache_blocks(cache_blocks);
  return Status::OK();
}

Status ScanConfiguration::SetBatchSizeBytes(uint32_t batch_size) {
  has_batch_size_bytes_ = true;
  batch_size_bytes_ = batch_size;
  return Status::OK();
}

Status ScanConfiguration::SetSelection(KuduClient::ReplicaSelection selection) {
  selection_ = selection;
  return Status::OK();
}

Status ScanConfiguration::SetReadMode(KuduScanner::ReadMode read_mode) {
  read_mode_ = read_mode;
  return Status::OK();
}

Status ScanConfiguration::SetFaultTolerant(bool fault_tolerant) {
  if (fault_tolerant) {
    // TODO(yingchun): this will overwrite the user set read mode, maybe it
    //  should return error if there is any conflict.
    RETURN_NOT_OK(SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  }
  is_fault_tolerant_ = fault_tolerant;
  return Status::OK();
}

void ScanConfiguration::SetSnapshotMicros(uint64_t snapshot_timestamp_micros) {
  // Shift the HT timestamp bits to get well-formed HT timestamp with the
  // logical bits zeroed out.
  snapshot_timestamp_ = snapshot_timestamp_micros << kHtTimestampBitsToShift;
}

void ScanConfiguration::SetSnapshotRaw(uint64_t snapshot_timestamp) {
  snapshot_timestamp_ = snapshot_timestamp;
}

Status ScanConfiguration::SetDiffScan(uint64_t start_timestamp, uint64_t end_timestamp) {
  if (start_timestamp == kNoTimestamp) {
    return Status::IllegalState("Start timestamp must be set bigger than 0");
  }
  if (start_timestamp > end_timestamp) {
    return Status::IllegalState("Start timestamp must be less than or equal to "
                                "end timestamp");
  }
  RETURN_NOT_OK(SetFaultTolerant(true));
  RETURN_NOT_OK(SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  start_timestamp_ = start_timestamp;
  snapshot_timestamp_ = end_timestamp;
  return Status::OK();
}

void ScanConfiguration::SetScanLowerBoundTimestampRaw(uint64_t propagation_timestamp) {
  lower_bound_propagation_timestamp_ = propagation_timestamp;
}

void ScanConfiguration::SetTimeoutMillis(int millis) {
  timeout_ = MonoDelta::FromMilliseconds(millis);
}

Status ScanConfiguration::SetRowFormatFlags(uint64_t flags) {
  row_format_flags_ = flags;
  return Status::OK();
}

Status ScanConfiguration::SetLimit(int64_t limit) {
  if (limit < 0) {
    return Status::InvalidArgument("Limit must be non-negative");
  }
  spec_.set_limit(limit);
  return Status::OK();
}

Status ScanConfiguration::AddIsDeletedColumn() {
  CHECK(has_start_timestamp());
  CHECK(has_snapshot_timestamp());

  // Convert the current projection back into ColumnSchemas.
  vector<ColumnSchema> cols;
  cols.reserve(projection_->num_columns() + 1);
  for (size_t i = 0; i < projection_->num_columns(); i++) {
    cols.emplace_back(projection_->column(i));
  }

  // Generate a unique name for the IS_DELETED virtual column.
  string col_name = kDefaultIsDeletedColName;
  while (table_->schema().schema_->find_column(col_name) != Schema::kColumnNotFound) {
    col_name += "_";
  }

  // Add the IS_DELETED virtual column to the list of projected columns.
  bool read_default = false;
  ColumnSchema is_deleted(col_name,
                          IS_DELETED,
                          /*is_nullable=*/false,
                          &read_default);
  cols.emplace_back(std::move(is_deleted));

  return CreateProjection(cols);
}

void ScanConfiguration::OptimizeScanSpec() {
  spec_.OptimizeScan(*table_->schema().schema_,
                     &arena_,
                     /* remove_pushed_predicates */ false);
}

Status ScanConfiguration::CreateProjection(const vector<ColumnSchema>& cols) {
  unique_ptr<Schema> s(new Schema);
  RETURN_NOT_OK(s->Reset(cols, 0));
  projection_ = s.get();
  schemas_pool_.push_back(std::move(s));
  client_projection_ = KuduSchema::FromSchema(*projection_);
  return Status::OK();
}

} // namespace client
} // namespace kudu
