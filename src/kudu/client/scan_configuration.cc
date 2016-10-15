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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"

using std::unique_ptr;
using std::string;
using std::vector;

namespace kudu {
namespace client {

const uint64_t ScanConfiguration::kNoTimestamp = KuduClient::kNoTimestamp;
const int ScanConfiguration::kHtTimestampBitsToShift = 12;

ScanConfiguration::ScanConfiguration(KuduTable* table)
    : table_(table),
      projection_(table->schema().schema_),
      client_projection_(*table->schema().schema_),
      has_batch_size_bytes_(false),
      batch_size_bytes_(0),
      selection_(KuduClient::CLOSEST_REPLICA),
      read_mode_(KuduScanner::READ_LATEST),
      is_fault_tolerant_(false),
      snapshot_timestamp_(kNoTimestamp),
      timeout_(MonoDelta::FromMilliseconds(KuduScanner::kScanTimeoutMillis)),
      arena_(1024, 1024 * 1024),
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

  unique_ptr<Schema> s(new Schema());
  RETURN_NOT_OK(s->Reset(cols, 0));
  projection_ = pool_.Add(s.release());
  client_projection_ = KuduSchema(*projection_);
  return Status::OK();
}

Status ScanConfiguration::AddConjunctPredicate(KuduPredicate* pred) {
  // Take ownership even if we return a bad status.
  pool_.Add(pred);
  return pred->data_->AddToScanSpec(&spec_, &arena_);
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
  gscoped_ptr<EncodedKey> enc_key;
  RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
                  *table_->schema().schema_, &arena_, key, &enc_key));
  spec_.SetLowerBoundKey(enc_key.get());
  pool_.Add(enc_key.release());
  return Status::OK();
}

Status ScanConfiguration::AddUpperBoundRaw(const Slice& key) {
  // Make a copy of the key.
  gscoped_ptr<EncodedKey> enc_key;
  RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
                  *table_->schema().schema_, &arena_, key, &enc_key));
  spec_.SetExclusiveUpperBoundKey(enc_key.get());
  pool_.Add(enc_key.release());
  return Status::OK();
}

Status ScanConfiguration::AddLowerBoundPartitionKeyRaw(const Slice& partition_key) {
  spec_.SetLowerBoundPartitionKey(partition_key);
  return Status::OK();
}

Status ScanConfiguration::AddUpperBoundPartitionKeyRaw(const Slice& partition_key) {
  spec_.SetExclusiveUpperBoundPartitionKey(partition_key);
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
  RETURN_NOT_OK(SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  is_fault_tolerant_ = true;
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

void ScanConfiguration::SetTimeoutMillis(int millis) {
  timeout_ = MonoDelta::FromMilliseconds(millis);
}

Status ScanConfiguration::SetRowFormatFlags(uint64_t flags) {
  row_format_flags_ = flags;
  return Status::OK();
}

void ScanConfiguration::OptimizeScanSpec() {
  spec_.OptimizeScan(*table_->schema().schema_,
                     &arena_,
                     &pool_,
                     /* remove_pushed_predicates */ false);
}

} // namespace client
} // namespace kudu
