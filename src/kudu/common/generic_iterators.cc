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

#include "kudu/common/generic_iterators.h"

#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/heap/skew_heap.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/column_materialization_context.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/object_pool.h"

namespace boost {
namespace heap {
template <class T> struct compare;
}  // namespace heap
}  // namespace boost

using std::deque;
using std::get;
using std::pair;
using std::sort;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DEFINE_bool(materializing_iterator_do_pushdown, true,
            "Should MaterializingIterator do predicate pushdown");
TAG_FLAG(materializing_iterator_do_pushdown, hidden);
DEFINE_bool(materializing_iterator_decoder_eval, true,
            "Should MaterializingIterator do decoder-level evaluation");
TAG_FLAG(materializing_iterator_decoder_eval, hidden);
TAG_FLAG(materializing_iterator_decoder_eval, runtime);

namespace kudu {
namespace {
void AddIterStats(const RowwiseIterator& iter,
                  vector<IteratorStats>* stats) {
  vector<IteratorStats> iter_stats;
  iter.GetIteratorStats(&iter_stats);
  DCHECK_EQ(stats->size(), iter_stats.size());
  for (int i = 0; i < iter_stats.size(); i++) {
    (*stats)[i] += iter_stats[i];
  }
}
} // anonymous namespace

////////////////////////////////////////////////////////////
// MergeIterator
////////////////////////////////////////////////////////////

// TODO(todd): this should be sized by # bytes, not # rows.
static const int kMergeRowBuffer = 1000;

// MergeIterState wraps a RowwiseIterator for use by the MergeIterator.
// Importantly, it also filters out unselected rows from the wrapped
// RowwiseIterator such that all returned rows are valid.
class MergeIterState : public boost::intrusive::list_base_hook<> {
 public:
  explicit MergeIterState(IterWithBounds iwb)
      : iwb_(std::move(iwb)),
        arena_(1024),
        next_row_idx_(0),
        rows_advanced_(0),
        rows_valid_(0)
  {}

  // Fetches the next row from the iterator's current block, or the iterator's
  // absolute lower bound if a block has not yet been pulled.
  //
  // Does not advance the iterator.
  const RowBlockRow& next_row() const {
    if (read_block_) {
      DCHECK_LT(rows_advanced_, rows_valid_);
      return next_row_;
    }
    DCHECK(decoded_bounds_);
    return decoded_bounds_->lower;
  }

  // Fetches the last row from the iterator's current block, or the iterator's
  // absolute upper bound if a block has not yet been pulled.
  //
  // Does not advance the iterator.
  const RowBlockRow& last_row() const {
    if (read_block_) {
      return last_row_;
    }
    DCHECK(decoded_bounds_);
    return decoded_bounds_->upper;
  }

  // Finishes construction of the MergeIterState by decoding the bounds if they
  // exist. If not, we have to pull a block immediately: after Init() is
  // finished it must be safe to call next_row() and last_row().
  //
  // Decoded bound allocations are done against 'decoded_bounds_arena'.
  Status Init(Arena* decoded_bounds_arena) {
    CHECK_EQ(0, rows_valid_);

    if (iwb_.encoded_bounds) {
      decoded_bounds_.emplace(&schema(), decoded_bounds_arena);
      decoded_bounds_->lower = decoded_bounds_->block.row(0);
      decoded_bounds_->upper = decoded_bounds_->block.row(1);
      RETURN_NOT_OK(schema().DecodeRowKey(
          iwb_.encoded_bounds->first, &decoded_bounds_->lower, decoded_bounds_arena));
      RETURN_NOT_OK(schema().DecodeRowKey(
          iwb_.encoded_bounds->second, &decoded_bounds_->upper, decoded_bounds_arena));
    } else {
      RETURN_NOT_OK(PullNextBlock());
    }

    return Status::OK();
  }

  // Returns true if the underlying iterator is fully exhausted.
  bool IsFullyExhausted() const {
    return rows_valid_ == 0 && !iwb_.iter->HasNext();
  }

  // Advance to the next row in the underlying iterator.
  //
  // If successful, 'pulled_new_block' is true if this block was exhausted and a
  // new block was pulled from the underlying iterator.
  Status Advance(bool* pulled_new_block);

  // Add statistics about the underlying iterator to the given vector.
  void AddStats(vector<IteratorStats>* stats) const {
    AddIterStats(*iwb_.iter, stats);
  }

  // Returns the schema from the underlying iterator.
  const Schema& schema() const {
    return iwb_.iter->schema();
  }

  // Pull the next block from the underlying iterator.
  Status PullNextBlock();

  // Returns true if the current block in the underlying iterator is exhausted.
  bool IsBlockExhausted() const {
    return rows_advanced_ == rows_valid_;
  }

  string ToString() const {
    return Substitute("[$0,$1]: $2",
                      schema().DebugRowKey(next_row()),
                      schema().DebugRowKey(last_row()),
                      iwb_.iter->ToString());
  }

 private:
  // The iterator (and optional bounds) whose rows are to be merged with other
  // iterators.
  //
  // Must already be Init'ed at MergeIterState construction time.
  IterWithBounds iwb_;

  // Allocates memory for read_block_.
  Arena arena_;

  // Optional rowset bounds, decoded during Init().
  struct DecodedBounds {
    // 'block' must be constructed immediately; the bounds themselves can be
    // initialized later.
    DecodedBounds(const Schema* schema, Arena* arena)
        : block(schema, /*nrows=*/2, arena) {}

    RowBlock block;
    RowBlockRow lower;
    RowBlockRow upper;
  };
  boost::optional<DecodedBounds> decoded_bounds_;

  // Current block of buffered rows from the iterator.
  //
  // The memory backing the rows was allocated out of the arena.
  unique_ptr<RowBlock> read_block_;

  // The row currently pointed to by the iterator.
  RowBlockRow next_row_;

  // The last row available in read_block_.
  RowBlockRow last_row_;

  // Row index of next_row_ in read_block_.
  size_t next_row_idx_;

  // Number of rows we've advanced past in read_block_.
  size_t rows_advanced_;

  // Number of valid (selected) rows in read_block_.
  size_t rows_valid_;

  DISALLOW_COPY_AND_ASSIGN(MergeIterState);
};

Status MergeIterState::Advance(bool* pulled_new_block) {
  rows_advanced_++;
  if (IsBlockExhausted()) {
    arena_.Reset();
    RETURN_NOT_OK(PullNextBlock());
    *pulled_new_block = true;
    return Status::OK();
  }

  // Seek to the next selected row.
  SelectionVector *selection = read_block_->selection_vector();
  for (++next_row_idx_; next_row_idx_ < read_block_->nrows(); next_row_idx_++) {
    if (selection->IsRowSelected(next_row_idx_)) {
      next_row_.Reset(read_block_.get(), next_row_idx_);
      break;
    }
  }
  DCHECK_NE(next_row_idx_, read_block_->nrows()) << "No selected rows found!";
  *pulled_new_block = false;
  return Status::OK();
}

Status MergeIterState::PullNextBlock() {
  CHECK_EQ(rows_advanced_, rows_valid_)
      << "should not pull next block until current block is exhausted";

  if (!read_block_) {
    read_block_.reset(new RowBlock(&schema(), kMergeRowBuffer, &arena_));
  }
  while (iwb_.iter->HasNext()) {
    RETURN_NOT_OK(iwb_.iter->NextBlock(read_block_.get()));
    rows_advanced_ = 0;
    // Honor the selection vector of the read_block_, since not all rows are necessarily selected.
    SelectionVector *selection = read_block_->selection_vector();
    DCHECK_EQ(selection->nrows(), read_block_->nrows());
    DCHECK_LE(selection->CountSelected(), read_block_->nrows());
    rows_valid_ = selection->CountSelected();
    VLOG(2) << Substitute("$0/$1 rows selected", rows_valid_, read_block_->nrows());
    if (rows_valid_ == 0) {
      // Short-circuit: this block is entirely unselected and can be skipped.
      continue;
    }

    // Seek next_row_ and last_row_ to the first and last selected rows
    // respectively (which could be identical).
    //
    // We use a signed size_t type to avoid underflowing when finding last_row_.
    //
    // TODO(adar): this can be simplified if there was a BitmapFindLastSet().
    CHECK(selection->FindFirstRowSelected(&next_row_idx_));
    next_row_.Reset(read_block_.get(), next_row_idx_);
    for (ssize_t row_idx = read_block_->nrows() - 1; row_idx >= 0; row_idx--) {
      if (selection->IsRowSelected(row_idx)) {
        last_row_.Reset(read_block_.get(), row_idx);
        VLOG(1) << "Pulled new block: " << ToString();
        return Status::OK();
      }
    }

    LOG(FATAL) << "unreachable code"; // guaranteed by the short-circuit above
  }

  // The underlying iterator is fully exhausted.
  rows_advanced_ = 0;
  rows_valid_ = 0;
  return Status::OK();
}

// An iterator which merges the results of other iterators, comparing
// based on keys.
//
// Three different heaps are used to optimize the merge process. To explain how
// it works, let's start with an explanation of a traditional heap-based merge:
// there exist N sorted lists of elements and the goal is to produce a single
// sorted list containing all of the elements.
//
// To begin:
// - For each list, peek the first element into a per-list buffer.
// - Add all of the lists to a min-heap ordered on the per-list buffers. This
//   means that the heap's top-most entry (accessible in O(1) time) will be the
//   list containing the smallest not-yet-consumed element.
//
// To perform the merge, loop while the min-heap isn't empty:
// - Pop the top-most list from the min-heap.
// - Copy that list's peeked element to the output.
// - Peek the list's next element. If the list is empty, discard it.
// - If the list has more elements, push it back into the min-heap.
//
// This algorithm runs in O(n log n) time and is generally superior to a naive
// O(n^2) merge. However, it requires peeked elements to remain resident in
// memory during the merge.
//
// The MergeIterator's sub-iterators operate much like the lists described
// above: elements correspond to rows and sorting is based on rows' primary
// keys. However, there are several important differences that open the door for
// further optimization:
// 1.  Each sub-iterator corresponds to a Kudu rowset, and DiskRowSets' smallest
//     and largest possible primary keys (i.e. bounds) are known ahead of time.
// 2.  When iterating on DiskRowSets, peeking even one row means decoding a page
//     of columnar data for each projected column. This decoded data remains
//     resident in memory so that we needn't repeat the decoding for each row,
//     but it means we have a strong motivation to minimize the set of peeked
//     sub-iterators in order to minimize memory consumption.
// 2a. Related to #2, there's little reason not to peek more than row at a time,
//     since the predominant source of memory usage is in the decoded pages
//     rather than the buffered rows.
//
// The aggressive peeking allows us to tweak the list model slightly: instead
// of treating sub-iterators as continuous sequences of single rows, we can
// think of each as a sequence of discrete row "runs". Each run has a lower
// bound (the key of the first row in the run) and an upper bound (the key of
// the last row). One run in each sub-iterator is NEXT in that its rows have
// been peeked and are resident in memory. When we draw rows from the
// sub-iterator, we'll draw them from this run. We can use the bounds to
// establish overlapping relationships between runs across sub-iterators. In
// theory, the less overlap, the fewer runs need to be considered when merging.
//
// To exploit this, we need to formally define the concept of a "merge window".
// The window describes, at any given time, the key space interval where we
// expect to find the row with the smallest key. A sub-iterator whose NEXT
// overlaps with the merge window is said to be actively participating in the merge.
//
// The merge window is defined as follows:
// 1.  The window's start is the smallest lower bound of all sub-iterators. We
//     refer to the sub-iterator that owns this lower bound as LOW.
// 2.  The window’s end is the smallest upper bound of all sub-iterators whose
//     lower bounds are less than or equal to LOW's upper bound.
// 2a. The window's end could be LOW's upper bound itself, if it is the smallest
//     upper bound, but this isn't necessarily the case.
// 3.  The merge window's dimensions change as the merge proceeds, though it
//     only ever moves "to the right" (i.e. the window start/end only increase).
//
// Armed with the merge window concept, we can bifurcate the sub-iterators into
// a set whose NEXTs overlap with the merge window and a set whose NEXTs do not.
// We store the first set in a HOT min-heap (ordered by each's NEXT's lower
// bound). Now the merge steady state resembles that of a traditional heap-based
// merge: the top-most sub-iterator is popped from HOT, the lower bound is
// copied to the output and advanced, and the sub-iterator is pushed back to
// HOT. The bifurcation hasn't yielded any algorithmic improvements, but the
// more non-overlapped the input (i.e. the more compacted the tablet), the fewer
// sub-iterators will be in HOT and thus the fewer comparisons and heap motion
// will take place.
//
// How do sub-iterators move between the two sets? At the outset, we examine all
// sub-iterators to find the initial merge window and bifurcate accordingly. In
// the steady state, we need to move to HOT whenever the end of the merge window
// moves; that's a sign that the window may now overlap with a NEXT belonging to
// a sub-iterator in the second set. The end of the merge window moves when a
// sub-iterator is fully exhausted (i.e. all rows have been copied to the
// output), or when a sub-iterator finishes its NEXT and needs to peek again.
//
// But which sub-iterators should be moved? To answer this question efficiently,
// we need two more heaps:
// - COLD: a min-heap for sub-iterators in the second set, ordered by each's
//   NEXT's lower bound. At any given time, the NEXT belonging to the top-most
//   sub-iterator in COLD is nearest the merge window.
// - HOTMAXES: a min-heap for keys. Each entry corresponds to a sub-iterator
//   present in HOT, and specifically, to its NEXT's upper bound. At any given
//   time, the top-most key in HOTMAXES represents the end of the merge window.
//
// When the merge window's end has moved and we need to refill HOT, the top-most
// sub-iterator in COLD is the best candidate. To figure out whether it should
// be moved, we compare its NEXT's lower bound against the top-most key in
// HOTMAXES: if the lower bound is less than or equal to the key, we move the
// sub-iterator from COLD to HOT. On the flip side, when a sub-iterator from HOT
// finishes its NEXT and peeks again, we also need to check whether it has
// exited the merge window. The approach is similar: if its NEXT's lower bound
// is greater than the top-most key in HOTMAXES, it's time to move it to COLD.
//
// There's one more piece to this puzzle: the absolute bounds that are known
// ahead of time are used as stand-ins for NEXT's lower and upper bounds. This
// helps defer peeking for as long as possible, at least until the sub-iterator
// moves from COLD to HOT. After that, peeked memory must remain resident until
// the sub-iterator is fully exhausted.
//
// For another description of this algorithm including pseudocode, see
// https://docs.google.com/document/d/1uP0ubjM6ulnKVCRrXtwT_dqrTWjF9tlFSRk0JN2e_O0/edit#
class MergeIterator : public RowwiseIterator {
 public:
  // Constructs a MergeIterator of the given iterators.
  //
  // The iterators must have matching schemas and should not yet be initialized.
  //
  // Note: the iterators must be constructed using a projection that includes
  // all key columns; otherwise a CHECK will fire at initialization time.
  MergeIterator(MergeIteratorOptions opts, vector<IterWithBounds> iters);

  virtual ~MergeIterator();

  // The passed-in iterators should be already initialized.
  Status Init(ScanSpec *spec) OVERRIDE;

  virtual bool HasNext() const OVERRIDE;

  virtual string ToString() const OVERRIDE;

  virtual const Schema& schema() const OVERRIDE;

  virtual void GetIteratorStats(vector<IteratorStats>* stats) const OVERRIDE;

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

 private:
  void PrepareBatch(RowBlock* dst);
  Status MaterializeBlock(RowBlock* dst);
  Status InitSubIterators(ScanSpec *spec);

  // Moves sub-iterators from cold_ to hot_ if they now overlap with the merge
  // window. Should be called whenever the merge window moves.
  Status RefillHotHeap();

  // Destroys a fully exhausted sub-iterator.
  void DestroySubIterator(MergeIterState* state);

  const MergeIteratorOptions opts_;

  // Initialized during Init.
  unique_ptr<Schema> schema_;

  bool initted_;

  // Index of the IS_DELETED column, or Schema::kColumnNotFound if no such
  // column exists in the schema.
  int is_deleted_col_index_;

  // Holds the sub-iterators until Init is called, at which point this is
  // cleared. This is required because we can't create a MergeIterState of an
  // uninitialized sub-iterator.
  vector<IterWithBounds> orig_iters_;

  // See UnionIterator::iters_lock_ for details on locking. This follows the same
  // pattern.
  mutable rw_spinlock states_lock_;
  boost::intrusive::list<MergeIterState> states_; // NOLINT(*)

  // Statistics (keyed by projection column index) accumulated so far by any
  // fully-consumed sub-iterators.
  vector<IteratorStats> finished_iter_stats_by_col_;

  // The number of iterators, used by ToString().
  const int num_orig_iters_;

  // When the underlying iterators are initialized, each needs its own
  // copy of the scan spec in order to do its own pushdown calculations, etc.
  // The copies are allocated from this pool so they can be automatically freed
  // when the UnionIterator goes out of scope.
  ObjectPool<ScanSpec> scan_spec_copies_;

  // Arena dedicated to MergeIterState bounds decoding.
  //
  // Each MergeIterState has an arena for buffered row data, but it is reset
  // every time a new block is pulled. This single arena ensures that a
  // MergeIterState's decoded bounds remain allocated for its lifetime.
  Arena decoded_bounds_arena_;

  // Min-heap that orders rows by their keys. A call to top() will yield the row
  // with the smallest key.
  struct RowComparator {
    bool operator()(const RowBlockRow& a, const RowBlockRow& b) const {
      // This is counter-intuitive, but it's because boost::heap defaults to
      // a max-heap; the comparator must be inverted to yield a min-heap.
      return a.schema()->Compare(a, b) > 0;
    }
  };
  typedef boost::heap::skew_heap<
    RowBlockRow, boost::heap::compare<RowComparator>> RowMinHeap;

  // Min-heap that orders sub-iterators by their next row key. A call to top()
  // will yield the sub-iterator with the smallest next row key.
  struct MergeIterStateComparator {
    bool operator()(const MergeIterState* a, const MergeIterState* b) const {
      // This is counter-intuitive, but it's because boost::heap defaults to
      // a max-heap; the comparator must be inverted to yield a min-heap.
      return a->schema().Compare(a->next_row(), b->next_row()) > 0;
    }
  };
  typedef boost::heap::skew_heap<
    MergeIterState*, boost::heap::compare<MergeIterStateComparator>> MergeStateMinHeap;

  // The three heaps as described in the algorithm above.
  //
  // Note that the heaps do not "own" any of the objects they contain:
  // - The MergeIterStates in hot_ and cold_ are owned by states_.
  // - The data backing the rows in hotmaxes_ is owned by all states' read_block_.
  //
  // Care must be taken to remove entries from the heaps when the corresponding
  // objects are destroyed.
  //
  // Boost offers a variety of different heap data structures[1]. Perf testing
  // via generic_iterators-test (TestMerge and TestMergeNonOverlapping with
  // num_iters=10, num_lists=1000, and num_rows=1000) shows that while all heaps
  // perform more or less equally well for non-overlapping input, skew heaps
  // outperform the rest for overlapping input. Basic priority queues (i.e.
  // boost::heap::priority_queue and std::priority_queue) were excluded as they
  // do not offer ordered iteration.
  //
  // 1. https://www.boost.org/doc/libs/1_69_0/doc/html/heap/data_structures.html
  MergeStateMinHeap hot_;
  MergeStateMinHeap cold_;
  RowMinHeap hotmaxes_;
};

MergeIterator::MergeIterator(MergeIteratorOptions opts,
                             vector<IterWithBounds> iters)
    : opts_(opts),
      initted_(false),
      orig_iters_(std::move(iters)),
      num_orig_iters_(orig_iters_.size()),
      decoded_bounds_arena_(1024) {
  CHECK_GT(orig_iters_.size(), 0);
}

MergeIterator::~MergeIterator() {
  states_.clear_and_dispose([](MergeIterState* s) { delete s; });
}

Status MergeIterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  // Initialize the iterators and the per-iterator merge states.
  //
  // When this method finishes, orig_iters_ has been cleared and states_ has
  // been populated.
  RETURN_NOT_OK(InitSubIterators(spec));

  // Verify that the schemas match in debug builds.
  //
  // It's important to do the verification after initializing the iterators, as
  // they may not know their own schemas until they've been initialized (in the
  // case of a union of unions).
  schema_.reset(new Schema(states_.front().schema()));
  CHECK_GT(schema_->num_key_columns(), 0);
  finished_iter_stats_by_col_.resize(schema_->num_columns());
#ifndef NDEBUG
  for (const auto& s : states_) {
    if (!s.schema().Equals(*schema_)) {
      return Status::InvalidArgument(
          Substitute("Schemas do not match: $0 vs. $1",
                     schema_->ToString(), s.schema().ToString()));
    }
  }
#endif

  is_deleted_col_index_ = schema_->find_first_is_deleted_virtual_column();
  if (opts_.include_deleted_rows && is_deleted_col_index_ == Schema::kColumnNotFound) {
    return Status::InvalidArgument("Merge iterator cannot deduplicate deleted "
                                   "rows without an IS_DELETED column");
  }

  // Before we copy any rows, clean up any iterators which were empty
  // to start with. Otherwise, HasNext() won't properly return false
  // if we were passed only empty iterators.
  states_.remove_and_dispose_if(
      [](const MergeIterState& s) { return PREDICT_FALSE(s.IsFullyExhausted()); },
      [](MergeIterState* s) { delete s; });

  // Establish the merge window and initialize the three heaps.
  for (auto& s : states_) {
    cold_.push(&s);
  }
  RETURN_NOT_OK(RefillHotHeap());

  initted_ = true;
  return Status::OK();
}

bool MergeIterator::HasNext() const {
  CHECK(initted_);
  return !states_.empty();
}

Status MergeIterator::InitSubIterators(ScanSpec *spec) {
  // Initialize all the sub iterators.
  for (auto& i : orig_iters_) {
    ScanSpec *spec_copy = spec != nullptr ? scan_spec_copies_.Construct(*spec) : nullptr;
    RETURN_NOT_OK(InitAndMaybeWrap(&i.iter, spec_copy));
    unique_ptr<MergeIterState> state(new MergeIterState(std::move(i)));
    RETURN_NOT_OK(state->Init(&decoded_bounds_arena_));
    states_.push_back(*state.release());
  }
  orig_iters_.clear();

  // Since we handle predicates in all the wrapped iterators, we can clear
  // them here.
  if (spec != nullptr) {
    spec->RemovePredicates();
  }
  return Status::OK();
}

Status MergeIterator::RefillHotHeap() {
  VLOG(2) << "Refilling hot heap";
  while (!cold_.empty() &&
         (hotmaxes_.empty() ||
          schema_->Compare(hotmaxes_.top(), cold_.top()->next_row()) >= 0)) {
    MergeIterState* warmest = cold_.top();
    cold_.pop();

    // This will only be true once per sub-iterator, when it becomes hot for the
    // very first time.
    if (warmest->IsBlockExhausted()) {
      RETURN_NOT_OK(warmest->PullNextBlock());

      // After pulling a block, we can't just assume 'warmest' overlaps the
      // merge window: there could have been a huge gap between the pulled block
      // and the sub-iterator's absolute bounds. In other words, although the
      // bounds told us that 'warmest' was the best candidate, the block is the
      // ultimate source of truth.
      //
      // To deal with this, we pretend 'warmest' doesn't overlap and restart the
      // algorithm. In the worst case (little to no gap between the block and
      // the bounds), we'll pop 'warmest' right back out again.
      if (warmest->IsFullyExhausted()) {
        DestroySubIterator(warmest);
      } else {
        cold_.push(warmest);
      }
      continue;
    }
    VLOG(2) << "Became hot: " << warmest->ToString();
    hot_.push(warmest);
    hotmaxes_.push(warmest->last_row());
  }
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Done refilling hot heap";
    for (const auto* c : cold_) {
      VLOG(2) << "Still cold: " << c->ToString();
    }
  }
  return Status::OK();
}

void MergeIterator::DestroySubIterator(MergeIterState* state) {
  DCHECK(state->IsFullyExhausted());

  std::lock_guard<rw_spinlock> l(states_lock_);
  state->AddStats(&finished_iter_stats_by_col_);
  states_.erase_and_dispose(states_.iterator_to(*state),
                            [](MergeIterState* s) { delete s; });
}

Status MergeIterator::NextBlock(RowBlock* dst) {
  VLOG(3) << "Called NextBlock on " << ToString();
  CHECK(initted_);
  DCHECK_SCHEMA_EQ(*dst->schema(), schema());

  PrepareBatch(dst);
  RETURN_NOT_OK(MaterializeBlock(dst));

  return Status::OK();
}

void MergeIterator::PrepareBatch(RowBlock* dst) {
  if (dst->arena()) {
    dst->arena()->Reset();
  }
}

// TODO(todd): this is an obvious spot to add codegen - there's a ton of branching
// and such around the comparisons. A simple experiment indicated there's some
// 2x to be gained.
Status MergeIterator::MaterializeBlock(RowBlock *dst) {
  // We need a vector to track the iterators whose next_row() contains the
  // smallest row key at a given moment during the merge because there may be
  // multiple deleted rows with the same row key across multiple rowsets, and
  // up to one live instance, that we have to deduplicate.
  vector<MergeIterState*> smallest(hot_.size());

  // Initialize the selection vector.
  // MergeIterState only returns selected rows.
  dst->selection_vector()->SetAllTrue();
  size_t dst_row_idx = 0;
  while (dst_row_idx < dst->nrows()) {
    // If the hot heap is empty, we must be out of sub-iterators.
    if (PREDICT_FALSE(hot_.empty())) {
      DCHECK(states_.empty());
      break;
    }

    // TODO(adar): optimize the case where hot_.size == 1.

    // Find the set of sub-iterators whose matching next row keys are the
    // smallest across all sub-iterators.
    //
    // Note: heap ordered iteration isn't the same as a total ordering. For
    // example, the two absolute smallest keys might be in the same sub-iterator
    // rather than in the first two sub-iterators yielded via ordered iteration.
    // However, the goal here is to identify a group of matching keys for the
    // purpose of deduplication, and we're guaranteed that such matching keys
    // cannot exist in the same sub-iterator (i.e. the same rowset).
    smallest.clear();
    for (auto iter = hot_.ordered_begin(); iter != hot_.ordered_end(); ++iter) {
      MergeIterState* state = *iter;
      if (!smallest.empty() &&
          schema_->Compare(state->next_row(), smallest[0]->next_row()) != 0) {
        break;
      }
      smallest.emplace_back(state);
    }

    MergeIterState* row_to_return_iter = nullptr;
    if (!opts_.include_deleted_rows) {
      // Since deleted rows are not included here, there can only be a single
      // instance of any given row key in 'smallest'.
      CHECK_EQ(1, smallest.size()) << "expected only a single smallest row";
      row_to_return_iter = smallest[0];
    } else {
      // Deduplicate any deleted rows. Row instance de-duplication criteria:
      // 1. If there is a non-deleted instance, return that instance.
      // 2. If all rows are deleted, any instance will suffice because we
      //    don't guarantee that we will return valid field values for deleted
      //    rows.
      int live_rows_found = 0;
      for (const auto& s : smallest) {
        bool is_deleted =
            *schema_->ExtractColumnFromRow<IS_DELETED>(s->next_row(), is_deleted_col_index_);
        if (!is_deleted) {
          // We found the single live instance of the row.
          row_to_return_iter = s;
#ifndef NDEBUG
          live_rows_found++; // In DEBUG mode, do a sanity-check count of the live rows.
#else
          break; // In RELEASE mode, short-circuit the loop.
#endif
        }
      }
      DCHECK_LE(live_rows_found, 1) << "expected at most one live row";

      // If all instances of a given row are deleted then return an arbitrary
      // deleted instance.
      if (row_to_return_iter == nullptr) {
        row_to_return_iter = smallest[0];
        DCHECK(*schema_->ExtractColumnFromRow<IS_DELETED>(row_to_return_iter->next_row(),
                                                          is_deleted_col_index_))
            << "expected deleted row";
      }
    }
    VLOG(3) << Substitute("Copying row $0 from $1",
                          dst_row_idx, row_to_return_iter->ToString());
    RowBlockRow dst_row = dst->row(dst_row_idx++);
    RETURN_NOT_OK(CopyRow(row_to_return_iter->next_row(), &dst_row, dst->arena()));

    // Advance all matching sub-iterators and remove any that are exhausted.
    for (auto& s : smallest) {
      bool pulled_new_block;
      RETURN_NOT_OK(s->Advance(&pulled_new_block));
      hot_.pop();

      // Note that hotmaxes_ is not yet popped as it's not necessary to do so if
      // the merge window hasn't changed. Thus, we can avoid some work by
      // deferring it into the cases below.

      if (s->IsFullyExhausted()) {
        hotmaxes_.pop();
        DestroySubIterator(s);

        // This sub-iterator's removal means the end of the merge window may
        // have shifted.
        RETURN_NOT_OK(RefillHotHeap());
      } else if (pulled_new_block) {
        hotmaxes_.pop();

        // This sub-iterator has a new block, which means the end of the merge
        // window may have shifted.
        if (!hotmaxes_.empty() && schema_->Compare(hotmaxes_.top(), s->next_row()) < 0) {
          // The new block lies beyond the new end of the merge window.
          VLOG(2) << "Block finished, became cold: " << s->ToString();
          cold_.push(s);
        } else {
          // The new block is still within the merge window.
          VLOG(2) << "Block finished, still hot: " << s->ToString();
          hot_.push(s);
          hotmaxes_.push(s->last_row());
        }
        RETURN_NOT_OK(RefillHotHeap());
      } else {
        // The sub-iterator's block's upper bound remains the same; the merge
        // window has not changed.
        hot_.push(s);
      }
    }
  }

  // The number of rows actually copied to the destination RowBlock may be less
  // than its original capacity due to deduplication of ghost rows.
  DCHECK_LE(dst_row_idx, dst->nrows());
  if (dst_row_idx < dst->nrows()) {
    dst->Resize(dst_row_idx);
  }

  return Status::OK();
}

string MergeIterator::ToString() const {
  return Substitute("Merge($0 iters)", num_orig_iters_);
}

const Schema& MergeIterator::schema() const {
  CHECK(initted_);
  return *schema_;
}

void MergeIterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  shared_lock<rw_spinlock> l(states_lock_);
  CHECK(initted_);
  *stats = finished_iter_stats_by_col_;

  for (const auto& s : states_) {
    s.AddStats(stats);
  }
}

unique_ptr<RowwiseIterator> NewMergeIterator(
    MergeIteratorOptions opts, vector<IterWithBounds> iters) {
  return unique_ptr<RowwiseIterator>(new MergeIterator(opts, std::move(iters)));
}

////////////////////////////////////////////////////////////
// UnionIterator
////////////////////////////////////////////////////////////

// An iterator which unions the results of other iterators.
// This is different from MergeIterator in that it lays the results out end-to-end
// rather than merging them based on keys. Hence it is more efficient since there is
// no comparison needed, and the key column does not need to be read if it is not
// part of the projection.
class UnionIterator : public RowwiseIterator {
 public:
  // Constructs a UnionIterator of the given iterators.
  //
  // The iterators must have matching schemas and should not yet be initialized.
  explicit UnionIterator(vector<IterWithBounds> iters);

  Status Init(ScanSpec *spec) OVERRIDE;

  bool HasNext() const OVERRIDE;

  string ToString() const OVERRIDE;

  const Schema &schema() const OVERRIDE {
    CHECK(initted_);
    CHECK(schema_.get() != NULL) << "Bad schema in " << ToString();
    return *CHECK_NOTNULL(schema_.get());
  }

  virtual void GetIteratorStats(vector<IteratorStats>* stats) const OVERRIDE;

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

 private:
  void PrepareBatch();
  Status MaterializeBlock(RowBlock* dst);
  void FinishBatch();
  Status InitSubIterators(ScanSpec *spec);

  // Pop the front iterator from iters_ and accumulate its statistics into
  // finished_iter_stats_by_col_.
  void PopFront();

  // Schema: initialized during Init()
  unique_ptr<Schema> schema_;

  bool initted_;

  // Lock protecting 'iters_' and 'finished_iter_stats_by_col_'.
  //
  // Scanners are mostly accessed by the thread doing the scanning, but the HTTP endpoint
  // which lists running scans may occasionally need to read as well.
  //
  // The "owner" thread of the scanner doesn't need to acquire this in read mode, since
  // it's the only thread which might write. However, it does need to acquire in write
  // mode when changing 'iters_'.
  mutable rw_spinlock iters_lock_;
  deque<IterWithBounds> iters_;

  // Statistics (keyed by projection column index) accumulated so far by any
  // fully-consumed sub-iterators.
  vector<IteratorStats> finished_iter_stats_by_col_;

  // When the underlying iterators are initialized, each needs its own
  // copy of the scan spec in order to do its own pushdown calculations, etc.
  // The copies are allocated from this pool so they can be automatically freed
  // when the UnionIterator goes out of scope.
  ObjectPool<ScanSpec> scan_spec_copies_;
};

UnionIterator::UnionIterator(vector<IterWithBounds> iters)
  : initted_(false),
    iters_(std::make_move_iterator(iters.begin()),
           std::make_move_iterator(iters.end())) {
  CHECK_GT(iters_.size(), 0);
}

Status UnionIterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  // Initialize the underlying iterators
  RETURN_NOT_OK(InitSubIterators(spec));

  // Verify that the schemas match in debug builds.
  //
  // It's important to do the verification after initializing the iterators, as
  // they may not know their own schemas until they've been initialized (in the
  // case of a union of unions).
  schema_.reset(new Schema(iters_.front().iter->schema()));
  finished_iter_stats_by_col_.resize(schema_->num_columns());
#ifndef NDEBUG
  for (const auto& i : iters_) {
    if (!i.iter->schema().Equals(*schema_)) {
      return Status::InvalidArgument(
          Substitute("Schemas do not match: $0 vs. $1",
                     schema_->ToString(), i.iter->schema().ToString()));
    }
  }
#endif

  initted_ = true;
  return Status::OK();
}


Status UnionIterator::InitSubIterators(ScanSpec *spec) {
  for (auto& i : iters_) {
    ScanSpec *spec_copy = spec != nullptr ? scan_spec_copies_.Construct(*spec) : nullptr;
    RETURN_NOT_OK(InitAndMaybeWrap(&i.iter, spec_copy));

    // The union iterator doesn't care about these, so let's drop them now to
    // free some memory.
    i.encoded_bounds.reset();
  }
  // Since we handle predicates in all the wrapped iterators, we can clear
  // them here.
  if (spec != nullptr) {
    spec->RemovePredicates();
  }
  return Status::OK();
}

bool UnionIterator::HasNext() const {
  CHECK(initted_);
  for (const auto& i : iters_) {
    if (i.iter->HasNext()) return true;
  }

  return false;
}

Status UnionIterator::NextBlock(RowBlock* dst) {
  CHECK(initted_);
  PrepareBatch();
  RETURN_NOT_OK(MaterializeBlock(dst));
  FinishBatch();
  return Status::OK();
}

void UnionIterator::PrepareBatch() {
  CHECK(initted_);

  while (!iters_.empty() &&
         !iters_.front().iter->HasNext()) {
    PopFront();
  }
}

Status UnionIterator::MaterializeBlock(RowBlock *dst) {
  return iters_.front().iter->NextBlock(dst);
}

void UnionIterator::FinishBatch() {
  if (!iters_.front().iter->HasNext()) {
    // Iterator exhausted, remove it.
    PopFront();
  }
}

void UnionIterator::PopFront() {
  std::lock_guard<rw_spinlock> l(iters_lock_);
  AddIterStats(*iters_.front().iter, &finished_iter_stats_by_col_);
  iters_.pop_front();
}

string UnionIterator::ToString() const {
  string s;
  s.append("Union(");
  s += JoinMapped(iters_, [](const IterWithBounds& i) {
      return i.iter->ToString();
    }, ",");
  s.append(")");
  return s;
}

void UnionIterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  CHECK(initted_);
  shared_lock<rw_spinlock> l(iters_lock_);
  *stats = finished_iter_stats_by_col_;
  if (!iters_.empty()) {
    AddIterStats(*iters_.front().iter, stats);
  }
}

unique_ptr<RowwiseIterator> NewUnionIterator(vector<IterWithBounds> iters) {
  return unique_ptr<RowwiseIterator>(new UnionIterator(std::move(iters)));
}

////////////////////////////////////////////////////////////
// MaterializingIterator
////////////////////////////////////////////////////////////

// An iterator which wraps a ColumnwiseIterator, materializing it into full rows.
//
// Column predicates are pushed down into this iterator. While materializing a
// block, columns with associated predicates are materialized first, and the
// predicates evaluated. If the predicates succeed in filtering out an entire
// batch, then other columns may avoid doing any IO.
class MaterializingIterator : public RowwiseIterator {
 public:
  explicit MaterializingIterator(unique_ptr<ColumnwiseIterator> iter);

  // Initialize the iterator, performing predicate pushdown as described above.
  Status Init(ScanSpec *spec) OVERRIDE;

  bool HasNext() const OVERRIDE;

  string ToString() const OVERRIDE;

  const Schema &schema() const OVERRIDE {
    return iter_->schema();
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE {
    iter_->GetIteratorStats(stats);
  }

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

 private:
  Status MaterializeBlock(RowBlock *dst);

  unique_ptr<ColumnwiseIterator> iter_;

  // List of (column index, predicate) in order of most to least selective, with
  // ties broken by the index.
  vector<std::pair<int32_t, ColumnPredicate>> col_idx_predicates_;

  // List of column indexes without predicates to materialize.
  vector<int32_t> non_predicate_column_indexes_;

  // Set only by test code to disallow pushdown.
  bool disallow_pushdown_for_tests_;
  bool disallow_decoder_eval_;
};

MaterializingIterator::MaterializingIterator(unique_ptr<ColumnwiseIterator> iter)
    : iter_(std::move(iter)),
      disallow_pushdown_for_tests_(!FLAGS_materializing_iterator_do_pushdown),
      disallow_decoder_eval_(!FLAGS_materializing_iterator_decoder_eval) {
}

Status MaterializingIterator::Init(ScanSpec *spec) {
  RETURN_NOT_OK(iter_->Init(spec));

  int32_t num_columns = schema().num_columns();
  col_idx_predicates_.clear();
  non_predicate_column_indexes_.clear();

  if (spec != nullptr && !disallow_pushdown_for_tests_) {
    col_idx_predicates_.reserve(spec->predicates().size());
    non_predicate_column_indexes_.reserve(num_columns - spec->predicates().size());

    for (const auto& col_pred : spec->predicates()) {
      const ColumnPredicate& pred = col_pred.second;
      int col_idx = schema().find_column(pred.column().name());
      if (col_idx == Schema::kColumnNotFound) {
        return Status::InvalidArgument("No such column", col_pred.first);
      }
      VLOG(1) << "Pushing down predicate " << pred.ToString();
      col_idx_predicates_.emplace_back(col_idx, col_pred.second);
    }

    for (int32_t col_idx = 0; col_idx < schema().num_columns(); col_idx++) {
      if (!ContainsKey(spec->predicates(), schema().column(col_idx).name())) {
        non_predicate_column_indexes_.emplace_back(col_idx);
      }
    }

    // Since we'll evaluate these predicates ourselves, remove them from the
    // scan spec so higher layers don't repeat our work.
    spec->RemovePredicates();
  } else {
    non_predicate_column_indexes_.reserve(num_columns);
    for (int32_t col_idx = 0; col_idx < num_columns; col_idx++) {
      non_predicate_column_indexes_.emplace_back(col_idx);
    }
  }

  // Sort the predicates by selectivity so that the most selective are evaluated
  // earlier, with ties broken by the column index.
  sort(col_idx_predicates_.begin(), col_idx_predicates_.end(),
       [] (const pair<int32_t, ColumnPredicate>& left,
           const pair<int32_t, ColumnPredicate>& right) {
          int comp = SelectivityComparator(left.second, right.second);
          return comp ? comp < 0 : left.first < right.first;
       });

  return Status::OK();
}

bool MaterializingIterator::HasNext() const {
  return iter_->HasNext();
}

Status MaterializingIterator::NextBlock(RowBlock* dst) {
  size_t n = dst->row_capacity();
  if (dst->arena()) {
    dst->arena()->Reset();
  }

  RETURN_NOT_OK(iter_->PrepareBatch(&n));
  dst->Resize(n);
  RETURN_NOT_OK(MaterializeBlock(dst));
  RETURN_NOT_OK(iter_->FinishBatch());

  return Status::OK();
}

Status MaterializingIterator::MaterializeBlock(RowBlock *dst) {
  // Initialize the selection vector indicating which rows have been
  // been deleted.
  RETURN_NOT_OK(iter_->InitializeSelectionVector(dst->selection_vector()));

  // It's relatively common to delete large sequential chunks of rows.
  // We can fast-path that case and avoid reading any column data.
  if (!dst->selection_vector()->AnySelected()) {
    DVLOG(1) << "Fast path over " << dst->nrows() << " deleted rows";
    return Status::OK();
  }

  for (const auto& col_pred : col_idx_predicates_) {
    // Materialize the column itself into the row block.
    ColumnBlock dst_col(dst->column_block(get<0>(col_pred)));
    ColumnMaterializationContext ctx(get<0>(col_pred),
                                     &get<1>(col_pred),
                                     &dst_col,
                                     dst->selection_vector());
    // None predicates should be short-circuited in scan spec.
    DCHECK(ctx.pred()->predicate_type() != PredicateType::None);
    if (disallow_decoder_eval_) {
      ctx.SetDecoderEvalNotSupported();
    }
    RETURN_NOT_OK(iter_->MaterializeColumn(&ctx));
    if (ctx.DecoderEvalNotSupported()) {
      get<1>(col_pred).Evaluate(dst_col, dst->selection_vector());
    }

    // If after evaluating this predicate the entire row block has been filtered
    // out, we don't need to materialize other columns at all.
    if (!dst->selection_vector()->AnySelected()) {
      DVLOG(1) << "0/" << dst->nrows() << " passed predicate";
      return Status::OK();
    }
  }

  for (size_t col_idx : non_predicate_column_indexes_) {
    // Materialize the column itself into the row block.
    ColumnBlock dst_col(dst->column_block(col_idx));
    ColumnMaterializationContext ctx(col_idx,
                                     nullptr,
                                     &dst_col,
                                     dst->selection_vector());
    RETURN_NOT_OK(iter_->MaterializeColumn(&ctx));
  }

  DVLOG(1) << dst->selection_vector()->CountSelected() << "/"
           << dst->nrows() << " passed predicate";
  return Status::OK();
}

string MaterializingIterator::ToString() const {
  string s;
  s.append("Materializing(").append(iter_->ToString()).append(")");
  return s;
}

unique_ptr<RowwiseIterator> NewMaterializingIterator(
    unique_ptr<ColumnwiseIterator> iter) {
  return unique_ptr<RowwiseIterator>(
      new MaterializingIterator(std::move(iter)));
}

////////////////////////////////////////////////////////////
// PredicateEvaluatingIterator
////////////////////////////////////////////////////////////

// An iterator which wraps another iterator and evaluates any predicates that the
// wrapped iterator did not itself handle during push down.
class PredicateEvaluatingIterator : public RowwiseIterator {
 public:
  // Construct the evaluating iterator.
  // This is only called from ::InitAndMaybeWrap()
  // REQUIRES: base_iter is already Init()ed.
  explicit PredicateEvaluatingIterator(unique_ptr<RowwiseIterator> base_iter);

  // Initialize the iterator.
  // POSTCONDITION: spec->predicates().empty()
  Status Init(ScanSpec *spec) OVERRIDE;

  virtual Status NextBlock(RowBlock *dst) OVERRIDE;

  bool HasNext() const OVERRIDE;

  string ToString() const OVERRIDE;

  const Schema &schema() const OVERRIDE {
    return base_iter_->schema();
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE {
    base_iter_->GetIteratorStats(stats);
  }

  const vector<ColumnPredicate>& col_predicates() const { return col_predicates_; }

 private:
  unique_ptr<RowwiseIterator> base_iter_;

  // List of predicates in order of most to least selective, with
  // ties broken by the column index.
  vector<ColumnPredicate> col_predicates_;
};

PredicateEvaluatingIterator::PredicateEvaluatingIterator(unique_ptr<RowwiseIterator> base_iter)
    : base_iter_(std::move(base_iter)) {
}

Status PredicateEvaluatingIterator::Init(ScanSpec *spec) {
  // base_iter_ already Init()ed before this is constructed.
  CHECK_NOTNULL(spec);

  // Gather any predicates that the base iterator did not pushdown, and remove
  // the predicates from the spec.
  col_predicates_.clear();
  col_predicates_.reserve(spec->predicates().size());
  for (auto& predicate : spec->predicates()) {
    col_predicates_.emplace_back(predicate.second);
  }
  spec->RemovePredicates();

  // Sort the predicates by selectivity so that the most selective are evaluated
  // earlier, with ties broken by the column index.
  sort(col_predicates_.begin(), col_predicates_.end(),
       [&] (const ColumnPredicate& left, const ColumnPredicate& right) {
          int comp = SelectivityComparator(left, right);
          if (comp != 0) return comp < 0;
          return schema().find_column(left.column().name())
               < schema().find_column(right.column().name());
       });

  return Status::OK();
}

bool PredicateEvaluatingIterator::HasNext() const {
  return base_iter_->HasNext();
}

Status PredicateEvaluatingIterator::NextBlock(RowBlock *dst) {
  RETURN_NOT_OK(base_iter_->NextBlock(dst));

  for (const auto& predicate : col_predicates_) {
    int32_t col_idx = dst->schema()->find_column(predicate.column().name());
    if (col_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument("Unknown column in predicate", predicate.ToString());
    }
    predicate.Evaluate(dst->column_block(col_idx), dst->selection_vector());

    // If after evaluating this predicate, the entire row block has now been
    // filtered out, we don't need to evaluate any further predicates.
    if (!dst->selection_vector()->AnySelected()) {
      break;
    }
  }

  return Status::OK();
}

string PredicateEvaluatingIterator::ToString() const {
  return Substitute("PredicateEvaluating($0)", base_iter_->ToString());
}

Status InitAndMaybeWrap(unique_ptr<RowwiseIterator>* base_iter,
                        ScanSpec *spec) {
  RETURN_NOT_OK((*base_iter)->Init(spec));

  if (spec != nullptr && !spec->predicates().empty()) {
    // Underlying iterator did not accept all predicates. Wrap it.
    unique_ptr<RowwiseIterator> wrapper(new PredicateEvaluatingIterator(std::move(*base_iter)));
    RETURN_NOT_OK(wrapper->Init(spec));
    *base_iter = std::move(wrapper);
  }
  return Status::OK();
}

const vector<ColumnPredicate>& GetIteratorPredicatesForTests(
    const unique_ptr<RowwiseIterator>& iter) {
  PredicateEvaluatingIterator* pred_eval =
      down_cast<PredicateEvaluatingIterator*>(iter.get());
  return pred_eval->col_predicates();
}

} // namespace kudu
