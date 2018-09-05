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

#include <limits.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/cfile_util.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/deltamemstore.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

namespace clock {
class Clock;
} // namespace clock

namespace tablet {

class RowSetMetadata;

class KuduTabletTest : public KuduTest {
 public:
  explicit KuduTabletTest(const Schema& schema,
                          TabletHarness::Options::ClockType clock_type =
                          TabletHarness::Options::ClockType::LOGICAL_CLOCK)
    : schema_(schema.CopyWithColumnIds()),
      client_schema_(schema),
      clock_type_(clock_type) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    SetUpTestTablet();
  }

  void CreateTestTablet(const std::string& root_dir = "") {
    std::string dir = root_dir.empty() ? GetTestPath("fs_root") : root_dir;
    TabletHarness::Options opts(dir);
    opts.enable_metrics = true;
    opts.clock_type = clock_type_;
    bool first_time = harness_ == NULL;
    harness_.reset(new TabletHarness(schema_, opts));
    CHECK_OK(harness_->Create(first_time));
  }

  void SetUpTestTablet(const std::string& root_dir = "") {
    CreateTestTablet(root_dir);
    CHECK_OK(harness_->Open());
  }

  void TabletReOpen(const std::string& root_dir = "") {
    SetUpTestTablet(root_dir);
  }

  const Schema &schema() const {
    return schema_;
  }

  const Schema &client_schema() const {
    return client_schema_;
  }

  clock::Clock* clock() {
    return harness_->clock();
  }

  FsManager* fs_manager() {
    return harness_->fs_manager();
  }

  void AlterSchema(const Schema& schema) {
    tserver::AlterSchemaRequestPB req;
    req.set_schema_version(tablet()->metadata()->schema_version() + 1);

    AlterSchemaTransactionState tx_state(NULL, &req, NULL);
    ASSERT_OK(tablet()->CreatePreparedAlterSchema(&tx_state, &schema));
    ASSERT_OK(tablet()->AlterSchema(&tx_state));
    tx_state.Finish();
  }

  const std::shared_ptr<Tablet>& tablet() const {
    return harness_->tablet();
  }

  Tablet* mutable_tablet() {
    return harness_->mutable_tablet();
  }

  TabletHarness* harness() {
    return harness_.get();
  }

 protected:
  const Schema schema_;
  const Schema client_schema_;
  const TabletHarness::Options::ClockType clock_type_;

  gscoped_ptr<TabletHarness> harness_;
};

class KuduRowSetTest : public KuduTabletTest {
 public:
  explicit KuduRowSetTest(const Schema& schema)
    : KuduTabletTest(schema) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
    ASSERT_OK(tablet()->metadata()->CreateRowSet(&rowset_meta_));
  }

  Status FlushMetadata() {
    return tablet()->metadata()->Flush();
  }

 protected:
  std::shared_ptr<RowSetMetadata> rowset_meta_;
};

// Iterate through the values without outputting them at the end
// This is strictly a measure of decoding and evaluating predicates
static inline Status SilentIterateToStringList(RowwiseIterator* iter,
                                               int* fetched) {
  const Schema& schema = iter->schema();
  Arena arena(1024);
  RowBlock block(schema, 100, &arena);
  *fetched = 0;
  while (iter->HasNext()) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (size_t i = 0; i < block.nrows(); i++) {
      if (block.selection_vector()->IsRowSelected(i)) {
        (*fetched)++;
      }
    }
  }
  return Status::OK();
}

static inline Status IterateToStringList(RowwiseIterator* iter,
                                         std::vector<std::string>* out,
                                         int limit = INT_MAX) {
  out->clear();
  Schema schema = iter->schema();
  Arena arena(1024);
  RowBlock block(schema, 100, &arena);
  int fetched = 0;
  while (iter->HasNext() && fetched < limit) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (size_t i = 0; i < block.nrows() && fetched < limit; i++) {
      if (block.selection_vector()->IsRowSelected(i)) {
        out->push_back(schema.DebugRow(block.row(i)));
        fetched++;
      }
    }
  }
  return Status::OK();
}

// Performs snapshot reads, under each of the snapshots in 'snaps', and stores
// the results in 'collected_rows'.
static inline void CollectRowsForSnapshots(
    Tablet* tablet,
    const Schema& schema,
    const std::vector<MvccSnapshot>& snaps,
    std::vector<std::vector<std::string>* >* collected_rows) {
  for (const MvccSnapshot& snapshot : snaps) {
    DVLOG(1) << "Snapshot: " <<  snapshot.ToString();
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet->NewRowIterator(schema, snapshot, UNORDERED, &iter));
    ASSERT_OK(iter->Init(NULL));
    auto collector = new std::vector<std::string>();
    ASSERT_OK(IterateToStringList(iter.get(), collector));
    for (const auto& mrs : *collector) {
      DVLOG(1) << "Got from MRS: " << mrs;
    }
    collected_rows->push_back(collector);
  }
}

// Performs snapshot reads, under each of the snapshots in 'snaps', and verifies that
// the results match the ones in 'expected_rows'.
static inline void VerifySnapshotsHaveSameResult(
    Tablet* tablet,
    const Schema& schema,
    const std::vector<MvccSnapshot>& snaps,
    const std::vector<std::vector<std::string>* >& expected_rows) {
  int idx = 0;
  // Now iterate again and make sure we get the same thing.
  for (const MvccSnapshot& snapshot : snaps) {
    DVLOG(1) << "Snapshot: " <<  snapshot.ToString();
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet->NewRowIterator(schema,
                                            snapshot,
                                            UNORDERED,
                                            &iter));
    ASSERT_OK(iter->Init(NULL));
    std::vector<std::string> collector;
    ASSERT_OK(IterateToStringList(iter.get(), &collector));
    ASSERT_EQ(collector.size(), expected_rows[idx]->size());

    for (int i = 0; i < expected_rows[idx]->size(); i++) {
      DVLOG(1) << "Got from DRS: " << collector[i];
      DVLOG(1) << "Expected: " << (*expected_rows[idx])[i];
      ASSERT_EQ((*expected_rows[idx])[i], collector[i]);
    }
    idx++;
  }
}

// Constructs a new iterator for 'rs' with 'opts' and dumps all of its rows into 'out'.
//
// The previous contents of 'out' are cleared.
static inline Status DumpRowSet(const RowSet& rs,
                                const RowIteratorOptions& opts,
                                std::vector<std::string>* out) {
  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(rs.NewRowIterator(opts, &iter));
  RETURN_NOT_OK(iter->Init(nullptr));
  RETURN_NOT_OK(IterateToStringList(iter.get(), out));
  return Status::OK();
}

// Take an un-initialized iterator, Init() it, and iterate through all of its rows.
// The resulting string contains a line per entry.
static inline std::string InitAndDumpIterator(gscoped_ptr<RowwiseIterator> iter) {
  CHECK_OK(iter->Init(NULL));

  std::vector<std::string> out;
  CHECK_OK(IterateToStringList(iter.get(), &out));
  return JoinStrings(out, "\n");
}

// Dump all of the rows of the tablet into the given vector.
static inline Status DumpTablet(const Tablet& tablet,
                         const Schema& projection,
                         std::vector<std::string>* out) {
  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet.NewRowIterator(projection, &iter));
  RETURN_NOT_OK(iter->Init(NULL));
  std::vector<std::string> rows;
  RETURN_NOT_OK(IterateToStringList(iter.get(), &rows));
  std::sort(rows.begin(), rows.end());
  out->swap(rows);
  return Status::OK();
}

// Write a single row to the given RowSetWriter (which may be of the rolling
// or non-rolling variety).
template<class RowSetWriterClass>
static Status WriteRow(const Slice &row_slice, RowSetWriterClass *writer) {
  const Schema &schema = writer->schema();
  DCHECK_EQ(row_slice.size(), schema.byte_size());

  RowBlock block(schema, 1, NULL);
  ConstContiguousRow row(&schema, row_slice.data());
  RowBlockRow dst_row = block.row(0);
  RETURN_NOT_OK(CopyRow(row, &dst_row, reinterpret_cast<Arena*>(NULL)));

  return writer->AppendBlock(block);
}

// Tracks encoded deltas and provides a DeltaIterator-like interface for
// querying them.
//
// This is effectively a poor man's DeltaMemStore, except it allows REINSERTs.
template<typename T>
class MirroredDeltas {
 public:
  // Map of row index to map of timestamp to encoded delta.
  //
  // The inner map is sorted by an order that respects DeltaType. That is, REDO
  // timestamps are sorted in ascending order, while UNDO timestamps are sorted
  // in descending order.
  using ComparatorType = typename std::conditional<T::kTag == REDO,
                                                   std::less<Timestamp>,
                                                   std::greater<Timestamp>>::type;
  using MirroredDeltaMap = std::map<rowid_t,
                                    std::map<Timestamp, faststring, ComparatorType>>;

  explicit MirroredDeltas(const Schema* schema)
      : schema_(schema),
        arena_(1024) {
  }

  // Tracks a new key/delta pair. The key must not already have an associated
  // encoded delta.
  void AddEncodedDelta(const DeltaKey& k, const faststring& changes) {
    auto& existing = all_deltas_[k.row_idx()][k.timestamp()];
    DCHECK_EQ(0, existing.length());
    existing.assign_copy(changes.data(), changes.length());
  }

  // Returns true if all tracked deltas are irrelevant to 'ts', false otherwise.
  bool CheckAllDeltasCulled(Timestamp ts) const {
    for (const auto& e1 : all_deltas_) {
      for (const auto& e2 : e1.second) {
        if (IsDeltaRelevant(ts, e2.first)) {
          return false;
        }
      }
    }
    return true;
  }

  // Returns a string representation of all tracked deltas.
  std::string ToString() const {
    std::string s;
    bool append_delim = false;
    for (const auto& e1 : all_deltas_) {
      for (const auto& e2 : e1.second) {
        if (append_delim) {
          StrAppend(&s, "\n");
        } else {
          append_delim = true;
        }
        StrAppend(&s, e1.first);
        StrAppend(&s, ",");
        StrAppend(&s, e2.first.ToString());
        StrAppend(&s, ": ");
        StrAppend(&s, RowChangeList(e2.second).ToString(*schema_));
      }
    }
    return s;
  }

  // Applies tracked UPDATE and REINSERT values to 'cb'.
  //
  // Deltas not relevant to 'ts' are skipped. The set of rows considered is
  // determined by 'start_row_idx' and the number of rows in 'cb'.
  Status ApplyUpdates(const Schema& projection, Timestamp ts,
                      rowid_t start_row_idx, int col_idx, ColumnBlock* cb) {
    for (int i = 0; i < cb->nrows(); i++) {
      rowid_t row_idx = start_row_idx + i;
      for (const auto& e : all_deltas_[row_idx]) {
        if (!IsDeltaRelevant(ts, e.first)) {
          // No need to keep iterating; all future deltas for this row will also
          // be irrelevant.
          break;
        }
        RowChangeList changes(e.second);
        if (changes.is_delete()) {
          continue;
        }
        RowChangeListDecoder decoder(changes);
        decoder.InitNoSafetyChecks();
        RETURN_NOT_OK(decoder.ApplyToOneColumn(i, cb, projection, col_idx, &arena_));
      }
    }
    return Status::OK();
  }

  // Applies deletions to 'sel_vec' by unselecting all rows whose last tracked
  // delta is a DELETE.
  //
  // Deltas not relevant to 'ts' are skipped. The set of rows considered is
  // determined by 'start_row_idx' and the number of rows in 'sel_vec'.
  Status ApplyDeletes(Timestamp ts, rowid_t start_row_idx, SelectionVector* sel_vec) {
    for (int i = 0; i < sel_vec->nrows(); i++) {
      bool deleted = false;
      for (const auto& e : all_deltas_[start_row_idx + i]) {
        if (!IsDeltaRelevant(ts, e.first)) {
          // No need to keep iterating; all future deltas for this row will also
          // be irrelevant.
          break;
        }
        RowChangeList changes(e.second);
        RowChangeListDecoder decoder(changes);
        decoder.InitNoSafetyChecks();
        decoder.TwiddleDeleteStatus(&deleted);
      }
      if (deleted) {
        sel_vec->SetRowUnselected(i);
      }
    }
    return Status::OK();
  }

  // Transforms and writes deltas into 'deltas', a vector of "delta lists", each
  // of which represents a particular row, and each entry of which is a
  // Timestamp and encoded delta pair. The encoded delta is a Slice into
  // all_deltas_ and should not outlive this class instance.
  //
  // Deltas not relevant to 'ts' are skipped. The set of rows considered is
  // determined by 'start_row_idx' and the number of rows in 'deltas'.
  using DeltaList = std::vector<std::pair<Timestamp, Slice>>;
  void CollectMutations(Timestamp ts, rowid_t start_row_idx,
                        std::vector<DeltaList>* deltas) {
    for (int i = 0; i < deltas->size(); i++) {
      for (const auto& e : all_deltas_[start_row_idx + i]) {
        if (!IsDeltaRelevant(ts, e.first)) {
          // No need to keep iterating; all future deltas for this row will also
          // be irrelevant.
          break;
        }
        (*deltas)[i].emplace_back(e.first, Slice(e.second));
      }
    }
  }

  // Transforms and writes deltas into 'deltas', a vector of delta key to encoded
  // delta pairs. The encoded delta is a Slice into this class instance's arena
  // and should not outlive it.
  //
  // Notably, this function does not compare timestamps; all deltas for the
  // requested rows are returned. The set of rows considered is determined by
  // 'start_row_idx' and 'num_rows'.
  Status FilterColumnIdsAndCollectDeltas(rowid_t start_row_idx, size_t num_rows,
                                         const std::vector<ColumnId>& col_ids,
                                         std::vector<std::pair<DeltaKey, Slice>>* deltas) {
    faststring buf;
    RowChangeListEncoder encoder(&buf);

    for (int i = 0; i < num_rows; i++) {
      rowid_t row_idx = start_row_idx + i;
      for (const auto& e : all_deltas_[row_idx]) {
        encoder.Reset();
        RETURN_NOT_OK(
            RowChangeListDecoder::RemoveColumnIdsFromChangeList(
                RowChangeList(e.second), col_ids, &encoder));
        if (encoder.is_initialized()) {
          RowChangeList changes = encoder.as_changelist();
          Slice relocated;
          CHECK(arena_.RelocateSlice(changes.slice(), &relocated));
          deltas->emplace_back(DeltaKey(row_idx, e.first), relocated);
        }
      }
    }
    return Status::OK();
  }

  const MirroredDeltaMap& all_deltas() const { return all_deltas_; }
  const Schema* schema() const { return schema_; }

 private:
  // Returns true if 'ts' is relevant to 'to_include', false otherwise.
  bool IsDeltaRelevant(Timestamp to_include, Timestamp ts) const;

  // All encoded deltas, arranged in DeltaKey order.
  MirroredDeltaMap all_deltas_;

  // Schema of all encoded deltas.
  const Schema* schema_;

  // Arena used for allocations in ApplyUpdates and FilterColumnIdsAndCollectDeltas.
  Arena arena_;
};

// Returns a sequence of randomly chosen positive integers with the following properties:
// 1. All integers are in the range [0, 'max_integer').
// 2. The number of entries is equal to 'num_integers', or 'max_integer' if
//    'num_integers' is greater than the size of the range in #1.
// 3. An integer cannot repeat.
// 4. The integers are in ascending order.
static inline std::vector<size_t> GetRandomIntegerSequence(
    Random* prng,
    size_t max_integer,
    size_t num_integers) {
  // Clamp the length of the sequence in case the max is too low to supply the
  // desired number of integers.
  size_t num_integers_clamped = std::min(max_integer, num_integers);

  // Pick some integers.
  //
  // We use an ordered set so that the sequence is in ascending order.
  std::set<size_t> integers;
  do {
    integers.emplace(prng->Uniform(max_integer));
  } while (integers.size() < num_integers_clamped);

  return std::vector<size_t>(integers.begin(), integers.end());
}

template <typename T>
void CreateRandomDeltas(const Schema& schema,
                        Random* prng,
                        int num_deltas,
                        std::pair<rowid_t, rowid_t> row_range,
                        std::pair<uint64_t, uint64_t> ts_range,
                        bool allow_reinserts,
                        MirroredDeltas<T>* mirror) {
  DCHECK_GT(row_range.second, row_range.first);
  DCHECK_GT(ts_range.second, ts_range.first);

  // Randomly generate a set of delta keys, then sort them.
  std::unordered_set<DeltaKey,
                     DeltaKeyHashFunctor,
                     DeltaKeyEqualToFunctor<T::kTag>> keys;
  int i = 0;
  while (i < num_deltas) {
    rowid_t row_idx = prng->Uniform(row_range.second - row_range.first) +
                      row_range.first;
    uint64_t ts_val = prng->Uniform(ts_range.second - ts_range.first) +
                      ts_range.first;
    if (EmplaceIfNotPresent(&keys, row_idx, Timestamp(ts_val))) {
      i++;
    }
  }
  std::vector<DeltaKey> sorted_keys(keys.begin(), keys.end());
  std::sort(sorted_keys.begin(), sorted_keys.end(),
            DeltaKeyLessThanFunctor<T::kTag>());

  // Randomly generate deltas using the keys.
  //
  // Because the timestamps are sorted sorted in DeltaType order, we can track
  // the deletion status of each row directly.
  faststring buf;
  RowChangeListEncoder encoder(&buf);
  bool is_deleted;
  boost::optional<rowid_t> prev_row_idx;
  for (i = 0; i < sorted_keys.size(); i++) {
    encoder.Reset();
    const auto& k = sorted_keys[i];

    if (!prev_row_idx || prev_row_idx != k.row_idx()) {
      // New row; reset the deletion status.
      is_deleted = false;
      prev_row_idx = k.row_idx();
    }

    if (is_deleted) {
      // The row is deleted; we must REINSERT it.
      DCHECK(allow_reinserts);
      RowBuilder rb(schema);
      for (int i = 0; i < schema.num_columns(); i++) {
        rb.AddUint32(prng->Next());
      }
      encoder.SetToReinsert(rb.row());
      is_deleted = false;
      VLOG(3) << "REINSERT: " << k.row_idx() << "," << k.timestamp().ToString()
              << ": " << encoder.as_changelist().ToString(schema);
    } else if (prng->Uniform(100) < 90 ||
               (!allow_reinserts &&
                i + 1 < sorted_keys.size() &&
                k.row_idx() == sorted_keys[i + 1].row_idx())) {
      // The row is live and we randomly chose to UPDATE it. Do so to a random
      // assortment of columns.
      //
      // There's a special case here for when we chose to DELETE (see below) but
      // we're not allowed to REINSERT: if this won't be the last delta for this
      // row, we'll generate another UPDATE instead of the DELETE. This is
      // because we've generated the keys up front; if we DELETE now and can't
      // REINSERT, we'd have to discard the remaining keys for this row.
      int num_cols_to_update = std::min(prng->Uniform(5) + 1UL,
                                        schema.num_columns());
      auto idxs_to_update = GetRandomIntegerSequence(prng,
                                                     schema.num_columns(),
                                                     num_cols_to_update);
      for (auto idx : idxs_to_update) {
        uint32_t u32_val = prng->Next();
        auto col_id = schema.column_id(idx);
        const auto& col = schema.column(idx);
        encoder.AddColumnUpdate(col, col_id, &u32_val);
      }
      VLOG(3) << "UPDATE: " << k.row_idx() << "," << k.timestamp().ToString()
              << ": " << encoder.as_changelist().ToString(schema);
    } else {
      // The row is live; DELETE it.
      encoder.SetToDelete();
      is_deleted = true;
      VLOG(3) << "DELETE: " << k.row_idx() << "," << k.timestamp().ToString();
    }

    mirror->AddEncodedDelta(k, buf);
  }
}

static inline Schema GetRandomProjection(const Schema& schema,
                                         Random* prng,
                                         size_t max_cols_to_project) {
  // Set up the projection.
  auto idxs_to_project = GetRandomIntegerSequence(prng,
                                                  schema.num_columns(),
                                                  max_cols_to_project);
  std::vector<ColumnSchema> projected_cols;
  std::vector<ColumnId> projected_col_ids;
  for (auto idx : idxs_to_project) {
    projected_cols.emplace_back(schema.column(idx));
    projected_col_ids.emplace_back(schema.column_id(idx));
  }
  return Schema(projected_cols, projected_col_ids, 0);
}

// Create a DMS and populate it with random deltas.
//
// 'num_deltas' dictates the number of deltas that should be created.
//
// 'row_range' and 'ts_range' constrain the DeltaKeys used in the created deltas.
//
// 'mirror' will be updated with all created deltas.
static inline Status CreateRandomDMS(
    const Schema& schema,
    Random* prng,
    int num_deltas,
    std::pair<rowid_t, rowid_t> row_range,
    std::pair<uint64_t, uint64_t> ts_range,
    MirroredDeltas<DeltaTypeSelector<REDO>>* mirror,
    std::shared_ptr<DeltaMemStore>* dms) {
  DCHECK(mirror);
  DCHECK(dms);

  // Create a smattering of deltas in 'mirror'.
  CreateRandomDeltas(schema, prng, num_deltas,
                     std::move(row_range), std::move(ts_range),
                     /*allow_reinserts=*/ false, mirror);

  // Add them to the DMS.
  std::shared_ptr<DeltaMemStore> local_dms;
  RETURN_NOT_OK(DeltaMemStore::Create(
      0, 0, new log::LogAnchorRegistry(), MemTracker::GetRootTracker(),
      &local_dms));
  RETURN_NOT_OK(local_dms->Init(nullptr));
  consensus::OpId op_id(consensus::MaximumOpId());
  for (const auto& e1 : mirror->all_deltas()) {
    for (const auto& e2 : e1.second) {
      DeltaKey k(e1.first, e2.first);
      RowChangeList changes(e2.second);
      RETURN_NOT_OK(local_dms->Update(e2.first, e1.first,
                                      RowChangeList(e2.second), op_id));
    }
  }

  *dms = std::move(local_dms);
  return Status::OK();
}

// Create a delta file, populate it with random deltas, and return an opened
// DeltaFileReader for it.
//
// 'num_deltas' dictates the number of deltas that should be created.
//
// 'row_range' and 'ts_range' constrain the DeltaKeys used in the created deltas.
//
// 'mirror' will be updated with all created deltas.
template <typename T>
Status CreateRandomDeltaFile(const Schema& schema,
                             FsManager* fs_manager,
                             Random* prng,
                             int num_deltas,
                             std::pair<rowid_t, rowid_t> row_range,
                             std::pair<uint64_t, uint64_t> ts_range,
                             MirroredDeltas<T>* mirror,
                             std::shared_ptr<DeltaFileReader>* delta_reader) {
  DCHECK(mirror);
  DCHECK(delta_reader);

  // Create a smattering of deltas in 'mirror'.
  CreateRandomDeltas(schema, prng, num_deltas,
                     std::move(row_range), std::move(ts_range),
                     /*allow_reinserts=*/ true, mirror);

  // Write them out to a delta file in order.
  std::unique_ptr<fs::WritableBlock> wb;
  RETURN_NOT_OK(fs_manager->CreateNewBlock({}, &wb));
  BlockId block_id = wb->id();
  std::unique_ptr<DeltaFileWriter> writer(new DeltaFileWriter(std::move(wb)));
  RETURN_NOT_OK(writer->Start());
  DeltaStats stats;
  for (const auto& e1 : mirror->all_deltas()) {
    for (const auto& e2 : e1.second) {
      DeltaKey k(e1.first, e2.first);
      RowChangeList changes(e2.second);
      RETURN_NOT_OK(writer->AppendDelta<T::kTag>(k, changes));
      RETURN_NOT_OK(stats.UpdateStats(k.timestamp(), changes));
    }
  }
  writer->WriteDeltaStats(stats);
  RETURN_NOT_OK(writer->Finish());

  // Open a reader for this newly written delta file.
  std::unique_ptr<fs::ReadableBlock> rb;
  RETURN_NOT_OK(fs_manager->OpenBlock(block_id, &rb));
  return DeltaFileReader::Open(std::move(rb), T::kTag,
                               cfile::ReaderOptions(), delta_reader);
}

// Fuzz tests a DeltaStore by generating a fairly random DeltaIterator, using it
// to retrieve deltas from 'store' via several DeltaIterator methods, and
// comparing those deltas with the ones found in 'mirror'. Assumes that both
// 'store' and 'mirror' have been initialized with the same logical deltas.
//
// 'ts_range' controls the timestamp range to be used by the iterator.
//
// If 'test_filter_column_ids_and_collect_deltas' is true, will test that
// DeltaIterator method too.
template <typename T>
void RunDeltaFuzzTest(const DeltaStore& store,
                      Random* prng,
                      MirroredDeltas<T>* mirror,
                      std::pair<uint64_t, uint64_t> ts_range,
                      bool test_filter_column_ids_and_collect_deltas) {
  // Arbitrary constants to control the running time and coverage of the test.
  const int kMaxBatchSize = 1000;
  const int kNumScans = 100;
  const int kMaxColsToProject = 10;
  const int kMaxColsToFilter = 4;

  // Run a series of tests on random timestamps as well as one scan on a
  // snapshot for whom all deltas are relevant.
  for (int i = 0; i < kNumScans + 1; i++) {
    // Pick a timestamp for the iterator. The last iteration will use a snapshot
    // that includes all deltas.
    Timestamp ts;
    if (i < kNumScans) {
      ts = Timestamp(prng->Uniform(ts_range.second - ts_range.first) +
                     ts_range.first);
    } else if (T::kTag == REDO) {
      ts = Timestamp::kMax;
    } else {
      DCHECK(T::kTag == UNDO);
      ts = Timestamp::kMin;
    }

    // Create and initialize the iterator. If none iterator is returned, it's
    // because all deltas in 'store' were irrelevant; verify this.
    Schema projection = GetRandomProjection(*mirror->schema(), prng, kMaxColsToProject);
    SCOPED_TRACE(strings::Substitute("Projection $0", projection.ToString()));
    RowIteratorOptions opts;
    opts.projection = &projection;
    opts.snap_to_include = MvccSnapshot(ts);
    SCOPED_TRACE(strings::Substitute("Timestamp $0", ts.ToString()));
    DeltaIterator* raw_iter;
    Status s = store.NewDeltaIterator(opts, &raw_iter);
    if (s.IsNotFound()) {
      ASSERT_STR_CONTAINS(s.ToString(), "MvccSnapshot outside the range of this delta");
      ASSERT_TRUE(mirror->CheckAllDeltasCulled(ts));
      continue;
    }
    ASSERT_OK(s);
    std::unique_ptr<DeltaIterator> iter(raw_iter);
    ASSERT_OK(iter->Init(nullptr));

    // Run PREPARE_FOR_APPLY method tests in batches, in case there's some bug
    // related to batching.
    ASSERT_OK(iter->SeekToOrdinal(0));
    rowid_t start_row_idx = 0;
    while (iter->HasNext()) {
      int batch_size = prng->Uniform(kMaxBatchSize) + 1;
      SCOPED_TRACE(strings::Substitute("APPLY: batch starting at $0 ($1 rows)",
                                       start_row_idx, batch_size));
      ASSERT_OK(iter->PrepareBatch(batch_size, DeltaIterator::PREPARE_FOR_APPLY));

      // Test ApplyUpdates: all relevant updates are applied to the column block.
      for (int j = 0; j < opts.projection->num_columns(); j++) {
        SCOPED_TRACE(strings::Substitute("Column $0", j));
        ScopedColumnBlock<UINT32> expected_scb(batch_size, false);
        ScopedColumnBlock<UINT32> actual_scb(batch_size, false);
        for (int k = 0; k < batch_size; k++) {
          expected_scb[k] = 0;
          actual_scb[k] = 0;
        }
        ASSERT_OK(mirror->ApplyUpdates(*opts.projection, ts, start_row_idx, j,
                                       &expected_scb));
        ASSERT_OK(iter->ApplyUpdates(j, &actual_scb));
        ASSERT_EQ(expected_scb, actual_scb)
            << "Expected column block: " << expected_scb.ToString()
            << "\nActual column block: " << actual_scb.ToString();
      }

      // Test ApplyDeletes: the selection vector is all true and a row is unset
      // if the last relevant update deleted it.
      {
        SelectionVector expected_sv(batch_size);
        SelectionVector actual_sv(batch_size);
        expected_sv.SetAllTrue();
        actual_sv.SetAllTrue();
        ASSERT_OK(mirror->ApplyDeletes(ts, start_row_idx, &expected_sv));
        ASSERT_OK(iter->ApplyDeletes(&actual_sv));
        ASSERT_EQ(expected_sv, actual_sv);
      }
      start_row_idx += batch_size;
    }

    // Now run PREPARE_FOR_COLLECT method tests.
    ASSERT_OK(iter->SeekToOrdinal(0));
    start_row_idx = 0;
    while (iter->HasNext()) {
      int batch_size = prng->Uniform(kMaxBatchSize - 1) + 1;
      SCOPED_TRACE(strings::Substitute("COLLECT: batch starting at $0 ($1 rows)",
                                       start_row_idx, batch_size));
      ASSERT_OK(iter->PrepareBatch(batch_size, DeltaIterator::PREPARE_FOR_COLLECT));

      // Test CollectMutations: all relevant updates are returned.
      {
        Arena arena(1024);
        std::vector<typename MirroredDeltas<T>::DeltaList> expected_muts(batch_size);
        std::vector<Mutation*> actual_muts(batch_size);
        ASSERT_OK(iter->CollectMutations(&actual_muts, &arena));
        mirror->CollectMutations(ts, start_row_idx, &expected_muts);
        for (int i = 0; i < expected_muts.size(); i++) {
          const auto& expected = expected_muts[i];
          auto* actual = actual_muts[i];

          // Mutations from CollectMutations() are in the opposite timestamp
          // order than what's needed for REDOs or UNDOs.
          Mutation::ReverseMutationList(&actual);

          for (int j = 0; j < expected.size(); j++) {
            ASSERT_TRUE(actual);
            ASSERT_EQ(expected[j].first, actual->timestamp());
            ASSERT_EQ(expected[j].second, actual->changelist().slice());
            actual = actual->next();
          }
          ASSERT_FALSE(actual);
        }
      }

      // Test FilterColumnIdsAndCollectDeltas with a random filter set.
      //
      // Note that this operation only works on a totally inclusive snapshot.
      if (test_filter_column_ids_and_collect_deltas && i == kNumScans) {

        // Create a sequence of column ids to filter.
        auto idxs_to_filter = GetRandomIntegerSequence(prng,
                                                       opts.projection->num_columns(),
                                                       kMaxColsToFilter);
        std::vector<ColumnId> col_ids_to_filter(idxs_to_filter.size());
        for (auto idx : idxs_to_filter) {
          col_ids_to_filter.emplace_back(opts.projection->column_id(idx));
        }

        // Collect and filter, then compare the results.
        Arena arena(1024);
        std::vector<std::pair<DeltaKey, Slice>> expected_deltas;
        std::vector<DeltaKeyAndUpdate> actual_deltas;
        ASSERT_OK(mirror->FilterColumnIdsAndCollectDeltas(
            start_row_idx, batch_size, col_ids_to_filter, &expected_deltas));
        ASSERT_OK(iter->FilterColumnIdsAndCollectDeltas(
            col_ids_to_filter, &actual_deltas, &arena));
        ASSERT_EQ(expected_deltas.size(), actual_deltas.size());
        for (int j = 0; j < expected_deltas.size(); j++) {
          ASSERT_TRUE(expected_deltas[j].first.CompareTo<T::kTag>(
              actual_deltas[j].key) == 0);
          ASSERT_EQ(expected_deltas[j].second, actual_deltas[j].cell);
        }
      }
      start_row_idx += batch_size;
    }
  }
}

} // namespace tablet
} // namespace kudu
