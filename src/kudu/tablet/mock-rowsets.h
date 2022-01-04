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
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kudu/common/timestamp.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"

namespace kudu {

namespace fs {
struct IOContext;
}  // namespace fs

namespace tablet {

// Mock implementation of RowSet which just aborts on every call.
class MockRowSet : public RowSet {
 public:
  Status CheckRowPresent(const RowSetKeyProbe& /*probe*/,
                         const fs::IOContext* /*io_context*/,
                         bool* /*present*/, ProbeStats* /*stats*/) const override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  Status MutateRow(Timestamp /*timestamp*/,
                   const RowSetKeyProbe& /*probe*/,
                   const RowChangeList& /*update*/,
                   const consensus::OpId& /*op_id_*/,
                   const fs::IOContext* /*io_context*/,
                   ProbeStats* /*stats*/,
                   OperationResultPB* /*result*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  Status NewRowIterator(const RowIteratorOptions& /*opts*/,
                        std::unique_ptr<RowwiseIterator>* /*out*/) const override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  Status NewCompactionInput(const SchemaPtr& /*projection*/,
                            const MvccSnapshot& /*snap*/,
                            const fs::IOContext* /*io_context*/,
                            std::unique_ptr<CompactionInput>* /*out*/) const override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  Status CountRows(const fs::IOContext* /*io_context*/, rowid_t* /*count*/) const override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  Status CountLiveRows(uint64_t* /*count*/) const override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  std::string ToString() const override {
    LOG(FATAL) << "Unimplemented";
    return "";
  }
  Status DebugDump(std::vector<std::string>* /*lines*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  uint64_t OnDiskSize() const override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }
  uint64_t OnDiskBaseDataSize() const override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }
  uint64_t OnDiskBaseDataColumnSize(const ColumnId& /*col_id*/) const override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }
  uint64_t OnDiskBaseDataSizeWithRedos() const override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }
  std::mutex *compact_flush_lock() override {
    LOG(FATAL) << "Unimplemented";
    return nullptr;
  }
  bool has_been_compacted() const override {
    LOG(FATAL) << "Unimplemented";
    return false;
  }
  void set_has_been_compacted() override {
    LOG(FATAL) << "Unimplemented";
  }
  std::shared_ptr<RowSetMetadata> metadata() override {
    return nullptr;
  }

  size_t DeltaMemStoreSize() const override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

  bool DeltaMemStoreInfo(size_t* /*size_bytes*/, MonoTime* /*creation_time*/) const override {
    LOG(FATAL) << "Unimplemented";
    return false;
  }

  bool DeltaMemStoreEmpty() const override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

  int64_t MinUnflushedLogIndex() const override {
    LOG(FATAL) << "Unimplemented";
    return -1;
  }

  double DeltaStoresCompactionPerfImprovementScore(DeltaCompactionType /*type*/) const override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

  Status FlushDeltas(const fs::IOContext* /*io_context*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  Status MinorCompactDeltaStores(const fs::IOContext* /*io_context*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  Status IsDeletedAndFullyAncient(Timestamp /*ancient_history_mark*/,
                                  bool* /*deleted_and_ancient*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  Status EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp /*ancient_history_mark*/,
                                                     int64_t* /*bytes*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  Status InitUndoDeltas(Timestamp /*ancient_history_mark*/,
                        MonoTime /*deadline*/,
                        const fs::IOContext* /*io_context*/,
                        int64_t* /*delta_blocks_initialized*/,
                        int64_t* /*bytes_in_ancient_undos*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  Status DeleteAncientUndoDeltas(Timestamp /*ancient_history_mark*/,
                                 const fs::IOContext* /*io_context*/,
                                 int64_t* /*blocks_deleted*/,
                                 int64_t* /*bytes_deleted*/) override {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  bool IsAvailableForCompaction() override {
    return true;
  }
};

// Mock which implements GetBounds() with constant provided bonuds.
class MockDiskRowSet : public MockRowSet {
 public:
  MockDiskRowSet(std::string first_key, std::string last_key,
                 uint64_t size = 1000000, uint64_t column_size = 200)
      : first_key_(std::move(first_key)),
        last_key_(std::move(last_key)),
        size_(size),
        column_size_(column_size) {}

  Status GetBounds(std::string* min_encoded_key,
                   std::string* max_encoded_key) const override {
    *min_encoded_key = first_key_;
    *max_encoded_key = last_key_;
    return Status::OK();
  }

  uint64_t OnDiskSize() const override {
    return size_;
  }

  uint64_t OnDiskBaseDataSize() const override {
    return size_;
  }

  uint64_t OnDiskBaseDataColumnSize(const ColumnId& /*col_id*/) const override {
    return column_size_;
  }

  uint64_t OnDiskBaseDataSizeWithRedos() const override {
    return size_;
  }

  std::string ToString() const override {
    return strings::Substitute("mock[$0, $1]",
                               Slice(first_key_).ToDebugString(),
                               Slice(last_key_).ToDebugString());
  }

 private:
  const std::string first_key_;
  const std::string last_key_;
  const uint64_t size_;
  const uint64_t column_size_;
};

// Mock which acts like a MemRowSet and has no known bounds.
class MockMemRowSet : public MockRowSet {
 public:
  Status GetBounds(std::string* /*min_encoded_key*/,
                   std::string* /*max_encoded_key*/) const override {
    return Status::NotSupported("");
  }

 private:
  const std::string first_key_;
  const std::string last_key_;
};

} // namespace tablet
} // namespace kudu
