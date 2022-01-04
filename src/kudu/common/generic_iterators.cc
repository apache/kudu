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
#include <numeric>
#include <ostream>
#include <string>
#include <typeinfo>
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
#include "kudu/common/predicate_effectiveness.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
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


typedef std::pair<int32_t, ColumnPredicate> ColumnIdxAndPredicate;

////////////////////////////////////////////////////////////
// MergeIterator
////////////////////////////////////////////////////////////

// This is sized to a power of 2 to improve BitmapCopy performance when copying
// a RowBlock.
//
// TODO(todd): this should be sized by # bytes, not # rows.
static const int kMergeRowBuffer = 1024;

// MergeIterState wraps a RowwiseIterator for use by the MergeIterator.
class MergeIterState : public boost::intrusive::list_base_hook<> {
 public:
  explicit MergeIterState(IterWithBounds iwb)
      : iwb_(std::move(iwb)),
        memory_(1024),
        next_row_idx_(0)
  {}

  // Fetches the next row from the iterator's current block, or the iterator's
  // absolute lower bound if a block has not yet been pulled.
  //
  // Does not advance the iterator.
  const RowBlockRow& next_row() const {
    if (read_block_) {
      DCHECK_LT(next_row_idx_, read_block_->nrows());
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
  // Decoded bound allocations are done against the arena in 'decoded_bounds_memory'.
  Status Init(RowBlockMemory* decoded_bounds_memory) {
    DCHECK(!read_block_);

    if (iwb_.encoded_bounds) {
      decoded_bounds_.emplace(schema().get(), decoded_bounds_memory);
      decoded_bounds_->lower = decoded_bounds_->block.row(0);
      decoded_bounds_->upper = decoded_bounds_->block.row(1);
      RETURN_NOT_OK(schema()->DecodeRowKey(
          iwb_.encoded_bounds->first, &decoded_bounds_->lower, &decoded_bounds_memory->arena));
      RETURN_NOT_OK(schema()->DecodeRowKey(
          iwb_.encoded_bounds->second, &decoded_bounds_->upper, &decoded_bounds_memory->arena));
    } else {
      RETURN_NOT_OK(PullNextBlock());
    }

    return Status::OK();
  }

  // Returns true if the underlying iterator is fully exhausted.
  bool IsFullyExhausted() const {
    return (!read_block_ || read_block_->nrows() == 0) && !iwb_.iter->HasNext();
  }

  // Advances to the next row in the underlying iterator.
  //
  // If successful, 'pulled_new_block' is true if this block was exhausted and a
  // new block was pulled from the underlying iterator.
  Status Advance(size_t num_rows, bool* pulled_new_block);

  // Adds statistics about the underlying iterator to the given vector.
  void AddStats(vector<IteratorStats>* stats) const {
    AddIterStats(*iwb_.iter, stats);
  }

  // Returns the number of rows remaining in the current block.
  size_t remaining_in_block() const {
    DCHECK(read_block_);
    return read_block_->nrows() - next_row_idx_;
  }

  // Returns the schema from the underlying iterator.
  const SchemaPtr schema() const {
    return iwb_.iter->schema();
  }

  // Pulls the next block from the underlying iterator.
  Status PullNextBlock();

  // Copies as many rows as possible from the current block of buffered rows to
  // 'dst' (starting at 'dst_offset').
  //
  // If successful, 'num_rows_copied' will be set to the number of rows copied.
  Status CopyBlock(RowBlock* dst, size_t dst_offset, size_t* num_rows_copied);

  // Returns true if the current block in the underlying iterator is exhausted.
  bool IsBlockExhausted() const {
    return !read_block_ || read_block_->nrows() == next_row_idx_;
  }

  string ToString() const {
    return Substitute("[$0,$1]: $2",
                      schema()->DebugRowKey(next_row()),
                      schema()->DebugRowKey(last_row()),
                      iwb_.iter->ToString());
  }

 private:
  // The iterator (and optional bounds) whose rows are to be merged with other
  // iterators.
  //
  // Must already be Init'ed at MergeIterState construction time.
  const IterWithBounds iwb_;

  // Allocates memory for read_block_.
  RowBlockMemory memory_;

  // Optional rowset bounds, decoded during Init().
  struct DecodedBounds {
    // 'block' must be constructed immediately; the bounds themselves can be
    // initialized later.
    DecodedBounds(const Schema* schema, RowBlockMemory* mem)
        : block(schema, /*nrows=*/2, mem) {}

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

  // The index of the row currently pointed to by the iterator. Guaranteed to be
  // a selected row.
  size_t next_row_idx_;

  DISALLOW_COPY_AND_ASSIGN(MergeIterState);
};

Status MergeIterState::Advance(size_t num_rows, bool* pulled_new_block) {
  DCHECK_GE(read_block_->nrows(), next_row_idx_ + num_rows);

  next_row_idx_ += num_rows;

  // If advancing didn't exhaust this block outright, find the next selected row.
  size_t idx;
  if (!IsBlockExhausted() &&
      read_block_->selection_vector()->FindFirstRowSelected(next_row_idx_, &idx)) {
    next_row_idx_ = idx;
    next_row_.Reset(read_block_.get(), next_row_idx_);
    *pulled_new_block = false;
    return Status::OK();
  }

  // We either exhausted the block outright, or all subsequent rows were
  // deselected. Either way, we need to pull the next block.
  next_row_idx_ = read_block_->nrows();
  memory_.Reset();
  RETURN_NOT_OK(PullNextBlock());
  *pulled_new_block = true;
  return Status::OK();
}

Status MergeIterState::PullNextBlock() {
  CHECK(IsBlockExhausted())
      << "should not pull next block until current block is exhausted";

  if (!read_block_) {
    read_block_.reset(new RowBlock(schema().get(), kMergeRowBuffer, &memory_));
  }
  while (iwb_.iter->HasNext()) {
    RETURN_NOT_OK(iwb_.iter->NextBlock(read_block_.get()));

    SelectionVector* selection = read_block_->selection_vector();
    DCHECK_EQ(selection->nrows(), read_block_->nrows());
    DCHECK_LE(selection->CountSelected(), read_block_->nrows());
    size_t rows_valid = selection->CountSelected();
    VLOG(2) << Substitute("$0/$1 rows selected", rows_valid, read_block_->nrows());
    if (rows_valid == 0) {
      // Short-circuit: this block is entirely unselected and can be skipped.
      continue;
    }

    // Seek next_row_ and last_row_ to the first and last selected rows
    // respectively (which could be identical).

    CHECK(selection->FindFirstRowSelected(0, &next_row_idx_));
    next_row_.Reset(read_block_.get(), next_row_idx_);

    // We use a signed size_t type to avoid underflowing when finding last_row_.
    //
    // TODO(adar): this can be simplified if there was a BitmapFindLastSet().
    for (ssize_t row_idx = read_block_->nrows() - 1; row_idx >= 0; row_idx--) {
      if (selection->IsRowSelected(row_idx)) {
        last_row_.Reset(read_block_.get(), row_idx);
        VLOG(1) << "Pulled new block: " << ToString();
        return Status::OK();
      }
    }

    LOG(FATAL) << "unreachable code"; // guaranteed by the short-circuit above
  }

  VLOG(1) << "Fully exhausted iter " << iwb_.iter->ToString();
  next_row_idx_ = 0;
  read_block_.reset();
  return Status::OK();
}

Status MergeIterState::CopyBlock(RowBlock* dst, size_t dst_offset,
                                 size_t* num_rows_copied) {
  DCHECK(read_block_);
  DCHECK(!IsBlockExhausted());

  size_t num_rows_to_copy = std::min(remaining_in_block(),
                                     dst->nrows() - dst_offset);
  VLOG(3) << Substitute(
      "Copying $0 rows from RowBlock (s:$1,o:$2) to RowBlock (s:$3,o:$4): $5",
      num_rows_to_copy, read_block_->nrows(), next_row_idx_, dst->nrows(),
      dst_offset, ToString());
  RETURN_NOT_OK(read_block_->CopyTo(dst, next_row_idx_,
                                    dst_offset, num_rows_to_copy));
  *num_rows_copied = num_rows_to_copy;
  return Status::OK();
}

// An iterator which merges the results of other iterators, comparing
// based on keys.
//
// Two heaps are used to optimize the merge process. To explain how it works,
// let's start with an explanation of a traditional heap-based merge: there
// exist N sorted lists of elements and the goal is to produce a single sorted
// list containing all of the elements.
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
// we need another heap: we'll define a COLD min-heap for sub-iterators in the
// second set, ordered by each's NEXT's lower bound. At any given time, the
// NEXT belonging to the top-most sub-iterator in COLD is nearest the merge
// window.
//
// When the merge window's end has moved and we need to refill HOT, the
// top-most sub-iterator in COLD is the best candidate. To figure out whether
// it should be moved, we compare its NEXT's lower bound against the upper
// bound in HOT's first iterator: if the lower bound is less than or equal to
// the key, we move the sub-iterator from COLD to HOT. On the flip side, when a
// sub-iterator from HOT finishes its NEXT and peeks again, we also need to
// check whether it has exited the merge window. The approach is similar: if
// its NEXT's lower bound is greater than the upper bound of HOT'S first
// iterator, it's time to move it to COLD.
//
// There's one more piece to this puzzle: the absolute bounds that are known
// ahead of time are used as stand-ins for NEXT's lower and upper bounds. This
// helps defer peeking for as long as possible, at least until the sub-iterator
// moves from COLD to HOT. After that, peeked memory must remain resident until
// the sub-iterator is fully exhausted.
//
// TODO(awong): there is a variant of this algorithm that defines another
// container to further optimize the size of the merge window: HOTMAXES, an
// ordered set for sub-iterators in HOT, ordered by each sub-iterator's upper
// bound. At any given time, the first iterator in HOTMAXES represents the
// optimal end of the merge window, allowing us to move elements onto COLD via
// comparison to the first value of HOTMAXES. Some experiments defined this
// using std::set and found its maintenance sometimes takes more than it nets.
// Experiment with a less memory-hungry data structure (maybe absl::btree?).
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

  virtual const SchemaPtr schema() const OVERRIDE;

  virtual void GetIteratorStats(vector<IteratorStats>* stats) const OVERRIDE;

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

 private:
  // Materializes as much of the next block's worth of data into 'dst' at offset
  // 'dst_row_idx' as possible. Only permitted when there's only one
  // sub-iterator in the hot heap.
  //
  // On success, the selection vector in 'dst' and 'dst_row_idx' are both updated.
  Status MaterializeBlock(RowBlock* dst, size_t* dst_row_idx);

  // Finds the next row and materializes it into 'dst' at offset 'dst_row_idx'.
  //
  // On success, the selection vector in 'dst' and 'dst_row_idx' are both updated.
  Status MaterializeOneRow(RowBlock* dst, size_t* dst_row_idx);

  // Calls Init() on all of sub-iterators, wrapping them in predicate evaluating
  // iterators if necessary and setting up additional per-iterator bookkeeping.
  Status InitSubIterators(ScanSpec *spec);

  // Advances 'state' by 'num_rows_to_advance', destroying it and/or updating
  // 'hot_' and 'cold_' if necessary.
  Status AdvanceAndReheap(MergeIterState* state, size_t num_rows_to_advance);

  // Moves sub-iterators from cold_ to hot_ if they now overlap with the merge
  // window. Should be called whenever the merge window moves.
  Status RefillHotHeap();

  // Destroys a fully exhausted sub-iterator.
  void DestroySubIterator(MergeIterState* state);

  const MergeIteratorOptions opts_;

  // Initialized during Init.
  SchemaPtr schema_;

  bool initted_;

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
  RowBlockMemory decoded_bounds_memory_;

  // Min-heap that orders sub-iterators by their next row key. A call to top()
  // will yield the sub-iterator with the smallest next row key.
  struct MergeIterStateComparator {
    bool operator()(const MergeIterState* a, const MergeIterState* b) const {
      // This is counter-intuitive, but it's because boost::heap defaults to
      // a max-heap; the comparator must be inverted to yield a min-heap.
      return a->schema()->Compare(a->next_row(), b->next_row()) > 0;
    }
  };
  typedef boost::heap::skew_heap<
      MergeIterState*, boost::heap::compare<MergeIterStateComparator>> MergeStateMinHeap;

  // The min-heaps as described in the algorithm above.
  //
  // Note that none of these containers "own" the objects they contain: the
  // MergeIterStates are all owned by states_. Care must be taken to remove
  // entries from the containers in such a way that does not access the
  // corresponding objects, even if they are destroyed (e.g. if an iterator
  // state becomes fully exhausted while still in the containers).
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
};

MergeIterator::MergeIterator(MergeIteratorOptions opts,
                             vector<IterWithBounds> iters)
    : opts_(opts),
      initted_(false),
      orig_iters_(std::move(iters)),
      num_orig_iters_(orig_iters_.size()),
      decoded_bounds_memory_(1024) {
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
  schema_ = states_.front().schema();
  CHECK_GT(schema_->num_key_columns(), 0);
  finished_iter_stats_by_col_.resize(schema_->num_columns());
#ifndef NDEBUG
  for (const auto& s : states_) {
    if (!s.schema()->Equals(*schema_.get())) {
      return Status::InvalidArgument(
          Substitute("Schemas do not match: $0 vs. $1",
                     schema_->ToString(), s.schema()->ToString()));
    }
  }
#endif

  if (opts_.include_deleted_rows &&
      schema_->first_is_deleted_virtual_column_idx() == Schema::kColumnNotFound) {
    return Status::InvalidArgument("Merge iterator cannot deduplicate deleted "
                                   "rows without an IS_DELETED column");
  }

  // Before we copy any rows, clean up any iterators which were empty
  // to start with. Otherwise, HasNext() won't properly return false
  // if we were passed only empty iterators.
  states_.remove_and_dispose_if(
      [](const MergeIterState& s) { return PREDICT_FALSE(s.IsFullyExhausted()); },
      [](MergeIterState* s) { delete s; });

  // Establish the merge window and initialize the ordered iterator containers.
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
    RETURN_NOT_OK(state->Init(&decoded_bounds_memory_));
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

Status MergeIterator::AdvanceAndReheap(MergeIterState* state,
                                       size_t num_rows_to_advance) {
  DCHECK_EQ(state, hot_.top());
  bool pulled_new_block = false;
  RETURN_NOT_OK(state->Advance(num_rows_to_advance, &pulled_new_block));
  hot_.pop();

  if (state->IsFullyExhausted()) {
    DestroySubIterator(state);

    // This sub-iterator's removal means the end of the merge window may have shifted.
    RETURN_NOT_OK(RefillHotHeap());
  } else if (pulled_new_block) {
    // This sub-iterator has a new block, which means the end of the merge window
    // may have shifted.
    if (!hot_.empty() &&
        schema_->Compare(hot_.top()->last_row(), state->next_row()) < 0) {
      // The new block lies beyond the new end of the merge window.
      VLOG(2) << "Block finished, became cold: " << state->ToString();
      cold_.push(state);
    } else {
      // The new block is still within the merge window.
      VLOG(2) << "Block finished, still hot: " << state->ToString();
      hot_.push(state);
    }
    RETURN_NOT_OK(RefillHotHeap());
  } else {
    // The sub-iterator's block's upper bound remains the same; the merge window
    // has not changed.
    hot_.push(state);
  }
  return Status::OK();
}

Status MergeIterator::RefillHotHeap() {
  VLOG(2) << "Refilling hot heap";
  while (!cold_.empty() &&
         (hot_.empty() ||
          schema_->Compare(hot_.top()->last_row(), cold_.top()->next_row()) >= 0)) {
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
  VLOG(3) << "Called NextBlock (" << dst->nrows() << " rows) on " << ToString();
  CHECK(initted_);
  DCHECK_SCHEMA_EQ(*dst->schema(), *schema().get());

  if (dst->arena()) {
    dst->arena()->Reset();
  }

  size_t dst_row_idx = 0;
  while (dst_row_idx < dst->nrows()) {
    // If the hot heap is empty, we must be out of sub-iterators.
    if (PREDICT_FALSE(hot_.empty())) {
      DCHECK(states_.empty());
      break;
    }

    // If there's just one hot sub-iterator, we can copy its entire block
    // instead of copying row-by-row.
    //
    // When N sub-iterators fully overlap, there'll only be one hot sub-iterator
    // when consuming the very last row. A block copy for this case is more
    // overhead than just copying out the last row.
    //
    // TODO(adar): this can be further optimized by "attaching" data to 'dst'
    // rather than copying it.
    if (hot_.size() == 1 && hot_.top()->remaining_in_block() > 1) {
      RETURN_NOT_OK(MaterializeBlock(dst, &dst_row_idx));
    } else {
      RETURN_NOT_OK(MaterializeOneRow(dst, &dst_row_idx));
    }
  }

  if (dst_row_idx < dst->nrows()) {
    dst->Resize(dst_row_idx);
  }

  return Status::OK();
}

Status MergeIterator::MaterializeBlock(RowBlock* dst, size_t* dst_row_idx) {
  DCHECK_EQ(1, hot_.size());

  MergeIterState* state = hot_.top();
  size_t num_rows_copied;
  RETURN_NOT_OK(state->CopyBlock(dst, *dst_row_idx, &num_rows_copied));
  RETURN_NOT_OK(AdvanceAndReheap(state, num_rows_copied));

  // CopyBlock() already updated dst's SelectionVector.
  *dst_row_idx += num_rows_copied;
  return Status::OK();
}

// TODO(todd): this is an obvious spot to add codegen - there's a ton of branching
// and such around the comparisons. A simple experiment indicated there's some
// 2x to be gained.
Status MergeIterator::MaterializeOneRow(RowBlock* dst, size_t* dst_row_idx) {
  // We need a vector to track the iterators whose next_row() contains the
  // smallest row key at a given moment during the merge because there may be
  // multiple deleted rows with the same row key across multiple rowsets, and up
  // to one live instance, that we have to deduplicate.
  vector<MergeIterState*> smallest;
  smallest.reserve(hot_.size());

  // Find the set of sub-iterators whose matching next row keys are the smallest
  // across all sub-iterators.
  //
  // Note: heap ordered iteration isn't the same as a total ordering. For
  // example, the two absolute smallest keys might be in the same sub-iterator
  // rather than in the first two sub-iterators yielded via ordered iteration.
  // However, the goal here is to identify a group of matching keys for the
  // purpose of deduplication, and we're guaranteed that such matching keys
  // cannot exist in the same sub-iterator (i.e. the same rowset).
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
          *schema_->ExtractColumnFromRow<IS_DELETED>(
              s->next_row(), schema_->first_is_deleted_virtual_column_idx());
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
      DCHECK(*schema_->ExtractColumnFromRow<IS_DELETED>(
          row_to_return_iter->next_row(), schema_->first_is_deleted_virtual_column_idx()))
          << "expected deleted row";
    }
  }
  VLOG(3) << Substitute("Copying row $0 from $1",
                        *dst_row_idx, row_to_return_iter->ToString());
  RowBlockRow dst_row = dst->row(*dst_row_idx);
  RETURN_NOT_OK(CopyRow(row_to_return_iter->next_row(), &dst_row, dst->arena()));

  // Advance all matching sub-iterators and remove any that are exhausted.
  // Since we're advancing iterators of the same row starting with hot_.top(),
  // each of these calls is effectively being called on hot_.top(), since the
  // reheaping will put each top iterator farther down the hot heap.
  for (auto& s : smallest) {
    RETURN_NOT_OK(AdvanceAndReheap(s, /*num_rows_to_advance=*/1));
  }

  dst->selection_vector()->SetRowSelected(*dst_row_idx);
  (*dst_row_idx)++;
  return Status::OK();
}

string MergeIterator::ToString() const {
  return Substitute("Merge($0 iters)", num_orig_iters_);
}

const SchemaPtr MergeIterator::schema() const {
  CHECK(initted_);
  return schema_;
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

  const SchemaPtr schema() const OVERRIDE {
    CHECK(initted_);
    CHECK(schema_.get() != NULL) << "Bad schema in " << ToString();
    return schema_;
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
  SchemaPtr schema_;

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
  schema_ = iters_.front().iter->schema();
  finished_iter_stats_by_col_.resize(schema_->num_columns());
#ifndef NDEBUG
  for (const auto& i : iters_) {
    if (!i.iter->schema()->Equals(*schema_.get())) {
      return Status::InvalidArgument(
          Substitute("Schemas do not match: $0 vs. $1",
                     schema_->ToString(), i.iter->schema()->ToString()));
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

  const SchemaPtr schema() const OVERRIDE {
    return iter_->schema();
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE {
    iter_->GetIteratorStats(stats);
    predicates_effectiveness_ctx_.PopulateIteratorStatsWithDisabledPredicates(
        col_idx_predicates_, stats);
  }

  virtual Status NextBlock(RowBlock* dst) OVERRIDE;

  const IteratorPredicateEffectivenessContext& effectiveness_context() const {
    return predicates_effectiveness_ctx_;
  }

 private:
  Status MaterializeBlock(RowBlock *dst);

  unique_ptr<ColumnwiseIterator> iter_;

  // List of (column index, predicate) in order of most to least selective, with
  // ties broken by the index.
  vector<ColumnIdxAndPredicate> col_idx_predicates_;

  // List of column indexes without predicates to materialize.
  vector<int32_t> non_predicate_column_indexes_;

  // Predicate effective contexts help disable ineffective column predicates.
  IteratorPredicateEffectivenessContext predicates_effectiveness_ctx_;

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

  const int32_t num_columns = schema()->num_columns();
  col_idx_predicates_.clear();
  non_predicate_column_indexes_.clear();
  if (PREDICT_FALSE(!disallow_pushdown_for_tests_) && spec != nullptr) {
    col_idx_predicates_.reserve(spec->predicates().size());
    DCHECK_GE(num_columns, spec->predicates().size());
    non_predicate_column_indexes_.reserve(num_columns - spec->predicates().size());

    for (const auto& col_pred : spec->predicates()) {
      const ColumnPredicate& pred = col_pred.second;
      int col_idx = schema()->find_column(pred.column().name());
      if (col_idx == Schema::kColumnNotFound) {
        return Status::InvalidArgument("No such column", col_pred.first);
      }
      VLOG(1) << "Pushing down predicate " << pred.ToString();
      col_idx_predicates_.emplace_back(col_idx, col_pred.second);
    }

    for (int32_t col_idx = 0; col_idx < num_columns; col_idx++) {
      if (!ContainsKey(spec->predicates(), schema()->column(col_idx).name())) {
        non_predicate_column_indexes_.emplace_back(col_idx);
      }
    }

    // Since we'll evaluate these predicates ourselves, remove them from the
    // scan spec so higher layers don't repeat our work.
    spec->RemovePredicates();
  } else {
    non_predicate_column_indexes_.resize(num_columns);
    std::iota(non_predicate_column_indexes_.begin(),
              non_predicate_column_indexes_.end(), 0);
  }

  // Sort the predicates by selectivity so that the most selective are evaluated
  // earlier, with ties broken by the column index.
  sort(col_idx_predicates_.begin(), col_idx_predicates_.end(),
       [] (const ColumnIdxAndPredicate& left,
           const ColumnIdxAndPredicate& right) {
          int comp = SelectivityComparator(left.second, right.second);
          return comp ? comp < 0 : left.first < right.first;
       });

  // Important to initialize the effectiveness contexts after sorting the predicates
  // to get right order of predicate indices.
  for (int pred_idx = 0; pred_idx < col_idx_predicates_.size(); pred_idx++) {
    const auto* predicate = &col_idx_predicates_[pred_idx].second;
    if (IsColumnPredicateDisableable(predicate->predicate_type())) {
      predicates_effectiveness_ctx_.AddDisableablePredicate(pred_idx, predicate);
    }
  }
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

  predicates_effectiveness_ctx_.IncrementNextBlockCount();
  for (int i = 0; i < col_idx_predicates_.size(); i++) {
    const auto& col_pred = col_idx_predicates_[i];
    const auto& col_idx = get<0>(col_pred);
    const auto& predicate = get<1>(col_pred);

    // Materialize the column itself into the row block.
    ColumnBlock dst_col(dst->column_block(col_idx));
    ColumnMaterializationContext ctx(col_idx,
                                     &predicate,
                                     &dst_col,
                                     dst->selection_vector());
    // None predicates should be short-circuited in scan spec.
    DCHECK(ctx.pred()->predicate_type() != PredicateType::None);
    auto* effectiveness_ctx = IsColumnPredicateDisableable(predicate.predicate_type()) ?
                              &predicates_effectiveness_ctx_[i] : nullptr;
    // Predicate evaluation will be disabled for a predicate already determined to be ineffective
    // both at decoder level and outside.
    //
    // Indicate whether the predicate has been disabled or not. If the predicate is not disableable
    // (currently only Bloom filter predicates are disableable), both of these are false.
    bool disableable_predicate_disabled =
        effectiveness_ctx && !effectiveness_ctx->IsPredicateEnabled();
    bool disableable_predicate_enabled =
        effectiveness_ctx && effectiveness_ctx->IsPredicateEnabled();

    if (disallow_decoder_eval_ || disableable_predicate_disabled) {
      // This column predicate is determined to be ineffective, hence disable the decoder level
      // evaluation.
      ctx.SetDecoderEvalNotSupported();
    }

    // Determine the number of rows filtered out by this predicate, if disableable.
    //
    // Currently we don't have a mechanism to get precise number of rows filtered out by a
    // particular predicate and adding such a stat could in fact slow down the filtering.
    // This count of rows filtered out is not precise as the rows filtered out by predicates
    // earlier in the sort order get more credit than the ones later in the order. Nevertheless it
    // still helps measure whether the predicate is effective in filtering out rows.
    auto num_rows_before = disableable_predicate_enabled ?
                           dst->selection_vector()->CountSelected() : 0;

    RETURN_NOT_OK(iter_->MaterializeColumn(&ctx));
    if (ctx.DecoderEvalNotSupported() && !disableable_predicate_disabled) {
      predicate.Evaluate(dst_col, dst->selection_vector());
    }
    if (disableable_predicate_enabled) {
      auto num_rows_rejected = num_rows_before - dst->selection_vector()->CountSelected();
      DCHECK_GE(num_rows_rejected, 0);
      DCHECK_LE(num_rows_rejected, num_rows_before);
      effectiveness_ctx->rows_rejected += num_rows_rejected;
      effectiveness_ctx->rows_read += dst->nrows();
    }

    // If after evaluating this predicate the entire row block has been filtered
    // out, we don't need to materialize other columns at all.
    if (!dst->selection_vector()->AnySelected()) {
      DVLOG(1) << "0/" << dst->nrows() << " passed predicate";
      return Status::OK();
    }
  }
  predicates_effectiveness_ctx_.DisableIneffectivePredicates();

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

  const SchemaPtr schema() const OVERRIDE {
    return base_iter_->schema();
  }

  virtual void GetIteratorStats(std::vector<IteratorStats>* stats) const OVERRIDE {
    base_iter_->GetIteratorStats(stats);
    predicates_effectiveness_ctx_.PopulateIteratorStatsWithDisabledPredicates(
        col_idx_predicates_, stats);
  }

  // Return the column predicates tracked by this iterator. Only valid for the lifetime of
  // the iterator.
  vector<const ColumnPredicate*> col_predicates() const {
    vector<const ColumnPredicate*> result;
    result.reserve(col_idx_predicates_.size());
    std::transform(col_idx_predicates_.begin(),
                   col_idx_predicates_.end(),
                   std::back_inserter(result),
                   [](const ColumnIdxAndPredicate& idx_and_pred) { return &idx_and_pred.second; });
    return result;
  }

  const IteratorPredicateEffectivenessContext& effectiveness_context() const {
    return predicates_effectiveness_ctx_;
  }

 private:

  unique_ptr<RowwiseIterator> base_iter_;

  // List of (column index, predicate) in order of most to least selective, with
  // ties broken by the column index.
  vector<ColumnIdxAndPredicate> col_idx_predicates_;

  // Predicate effective contexts help disable ineffective column predicates.
  IteratorPredicateEffectivenessContext predicates_effectiveness_ctx_;
};

PredicateEvaluatingIterator::PredicateEvaluatingIterator(unique_ptr<RowwiseIterator> base_iter)
    : base_iter_(std::move(base_iter)) {
}

Status PredicateEvaluatingIterator::Init(ScanSpec *spec) {
  // base_iter_ already Init()ed before this is constructed.
  CHECK_NOTNULL(spec);

  // Gather any predicates that the base iterator did not pushdown, and remove
  // the predicates from the spec.
  col_idx_predicates_.clear();
  col_idx_predicates_.reserve(spec->predicates().size());
  for (const auto& col_pred : spec->predicates()) {
    const auto& col_name = col_pred.first;
    const ColumnPredicate& pred = col_pred.second;
    DCHECK_EQ(col_name, pred.column().name());
    int col_idx = schema()->find_column(col_name);
    if (col_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument("No such column", col_name);
    }
    VLOG(1) << "Pushing down predicate " << pred.ToString();
    col_idx_predicates_.emplace_back(col_idx, pred);
  }
  spec->RemovePredicates();

  // Sort the predicates by selectivity so that the most selective are evaluated
  // earlier, with ties broken by the column index.
  sort(col_idx_predicates_.begin(), col_idx_predicates_.end(),
       [] (const ColumnIdxAndPredicate& left,
           const ColumnIdxAndPredicate& right) {
         int comp = SelectivityComparator(left.second, right.second);
         return comp ? comp < 0 : left.first < right.first;
       });

  // Important to initialize the effectiveness contexts after sorting the predicates
  // to get right order of predicate indices.
  for (int pred_idx = 0; pred_idx < col_idx_predicates_.size(); pred_idx++) {
    const auto* predicate = &col_idx_predicates_[pred_idx].second;
    if (IsColumnPredicateDisableable(predicate->predicate_type())) {
      predicates_effectiveness_ctx_.AddDisableablePredicate(pred_idx, predicate);
    }
  }

  return Status::OK();
}

bool PredicateEvaluatingIterator::HasNext() const {
  return base_iter_->HasNext();
}

Status PredicateEvaluatingIterator::NextBlock(RowBlock *dst) {
  RETURN_NOT_OK(base_iter_->NextBlock(dst));

  predicates_effectiveness_ctx_.IncrementNextBlockCount();
  for (int i = 0; i < col_idx_predicates_.size(); i++) {
    const auto& col_idx = col_idx_predicates_[i].first;
    const auto& predicate = col_idx_predicates_[i].second;
    DCHECK_NE(col_idx, Schema::kColumnNotFound);

    auto* effectiveness_ctx = IsColumnPredicateDisableable(predicate.predicate_type()) ?
                              &predicates_effectiveness_ctx_[i] : nullptr;
    if (effectiveness_ctx && !effectiveness_ctx->IsPredicateEnabled()) {
      // Column predicate determined to be disabled.
      continue;
    }

    // Determine the number of rows filtered out by this predicate.
    //
    // Currently we don't have a mechanism to get precise number of rows filtered out by a
    // particular predicate and adding such stat could in fact slow down the filtering.
    // This count of rows filtered out is not precise as the rows filtered out by predicates
    // earlier in the sort order get more credit than the ones later in the order. Nevertheless it
    // still helps measure whether the predicate is effective in filtering out rows.
    auto num_rows_before = effectiveness_ctx ?
                           dst->selection_vector()->CountSelected() : 0;

    predicate.Evaluate(dst->column_block(col_idx), dst->selection_vector());

    if (effectiveness_ctx) {
      auto num_rows_rejected = num_rows_before - dst->selection_vector()->CountSelected();
      DCHECK_GE(num_rows_rejected, 0);
      DCHECK_LE(num_rows_rejected, num_rows_before);
      effectiveness_ctx->rows_rejected += num_rows_rejected;
      effectiveness_ctx->rows_read += dst->nrows();
    }

    // If after evaluating this predicate, the entire row block has now been
    // filtered out, we don't need to evaluate any further predicates.
    if (!dst->selection_vector()->AnySelected()) {
      break;
    }
  }
  predicates_effectiveness_ctx_.DisableIneffectivePredicates();

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

vector<const ColumnPredicate*> GetIteratorPredicatesForTests(
    const unique_ptr<RowwiseIterator>& iter) {
  PredicateEvaluatingIterator* pred_eval =
      down_cast<PredicateEvaluatingIterator*>(iter.get());
  return pred_eval->col_predicates();
}

const IteratorPredicateEffectivenessContext& GetIteratorPredicateEffectivenessCtxForTests(
    const std::unique_ptr<RowwiseIterator>& iter) {
  auto* iter_ptr = iter.get();
  // Using dynamic_cast like below is not recommended but okay considering following reasons:
  // - This function is only used for tests
  // - PredicateEvaluatingIterator and MaterializingIterator are hidden from public access.
  // - Introducing effectiveness context for base RowwiseIterator would be unnecessary
  //   for other derived classes of RowwiseIterator.
  if (auto* pred_iter = dynamic_cast<PredicateEvaluatingIterator*>(iter_ptr)) {
    return pred_iter->effectiveness_context();
  }
  if (auto* pred_iter = dynamic_cast<MaterializingIterator*>(iter_ptr)) {
    return pred_iter->effectiveness_context();
  }
  LOG(FATAL) << "Effectiveness context not available for iterator type: "
             << typeid(iter_ptr).name();
}

} // namespace kudu
