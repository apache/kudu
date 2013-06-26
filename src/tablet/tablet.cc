// Copyright (c) 2012, Cloudera, inc.
#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <tr1/memory>
#include <algorithm>
#include <vector>

#include "cfile/cfile.h"
#include "common/iterator.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "gutil/atomicops.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/compaction.h"
#include "tablet/tablet.h"
#include "tablet/tablet-util.h"
#include "tablet/diskrowset.h"
#include "util/bloom_filter.h"
#include "util/env.h"

DEFINE_bool(tablet_do_dup_key_checks, true,
            "Whether to check primary keys for duplicate on insertion. "
            "Use at your own risk!");

namespace kudu { namespace tablet {

using std::string;
using std::vector;
using std::tr1::shared_ptr;
using base::subtle::Barrier_AtomicIncrement;

const char *kRowSetPrefix = "rowset_";

string Tablet::GetRowSetPath(const string &tablet_dir,
                            int rowset_idx) {
  return StringPrintf("%s/rowset_%010d",
                      tablet_dir.c_str(),
                      rowset_idx);
}

Tablet::Tablet(const Schema &schema,
               const string &dir)
  : schema_(schema),
    key_schema_(schema.CreateKeyProjection()),
    dir_(dir),
    memrowset_(new MemRowSet(schema)),
    next_rowset_idx_(0),
    env_(Env::Default()),
    open_(false) {
}

Status Tablet::CreateNew() {
  CHECK(!open_) << "already open";
  RETURN_NOT_OK(env_->CreateDir(dir_));
  // TODO: write a metadata file into the tablet dir
  return Status::OK();
}

Status Tablet::Open() {
  CHECK(!open_) << "already open";
  // TODO: track a state_ variable, ensure tablet is open, etc.

  // for now, just list the children, to make sure the dir exists.
  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(dir_, &children));

  BOOST_FOREACH(const string &child, children) {
    // Skip hidden files (also '.' and '..')
    if (child[0] == '.') continue;

    string absolute_path = env_->JoinPathSegments(dir_, child);

    string suffix;
    if (TryStripPrefixString(child, kRowSetPrefix, &suffix)) {
      // The file should be named 'rowset_<N>'. N here is the index
      // of the rowset (indicating the order in which it was flushed).
      int32_t rowset_idx;
      if (!safe_strto32(suffix.c_str(), &rowset_idx)) {
        return Status::IOError(string("Bad rowset file: ") + absolute_path);
      }

      shared_ptr<DiskRowSet> rowset;
      Status s = DiskRowSet::Open(env_, schema_, absolute_path, &rowset);
      if (!s.ok()) {
        LOG(ERROR) << "Failed to open rowset " << absolute_path << ": "
                   << s.ToString();
        return s;
      }
      rowsets_.push_back(rowset);

      next_rowset_idx_ = std::max(next_rowset_idx_, rowset_idx + 1);
    } else {
      LOG(WARNING) << "ignoring unknown file: " << absolute_path;
    }
  }

  open_ = true;

  return Status::OK();
}

BloomFilterSizing Tablet::bloom_sizing() const {
  // TODO: make this configurable
  return BloomFilterSizing::BySizeAndFPRate(64*1024, 0.01f);
}

Status Tablet::NewRowIterator(const Schema &projection,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  // Yield current rows.
  MvccSnapshot snap(mvcc_);
  return NewRowIterator(projection, snap, iter);
}


Status Tablet::NewRowIterator(const Schema &projection,
                              const MvccSnapshot &snap,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  vector<shared_ptr<RowwiseIterator> > iters;
  RETURN_NOT_OK(CaptureConsistentIterators(projection, snap, &iters));

  iter->reset(new UnionIterator(iters));

  return Status::OK();
}


Status Tablet::Insert(const Slice &data) {
  CHECK(open_) << "must Open() first!";

  RowSetKeyProbe probe(key_schema_, data.data());

  // The order of the various locks is critical!
  // See comment block in MutateRow(...) below for details.
  ScopedRowLock rowlock(&lock_manager_, probe.encoded_key(), LockManager::LOCK_EXCLUSIVE);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  // First, ensure that it is a unique key by checking all the open
  // RowSets
  if (FLAGS_tablet_do_dup_key_checks) {
    bool present;
    RETURN_NOT_OK(tablet_util::CheckRowPresentInAnyRowSet(
                    rowsets_, probe, &present));
    if (present) {
      return Status::AlreadyPresent("key already present");
    }
  }

  // TODO: the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  // Now try to insert into memrowset. The memrowset itself will return
  // AlreadyPresent if it has already been inserted there.
  ScopedTransaction tx(&mvcc_);
  return memrowset_->Insert(tx.txid(), data);
}

Status Tablet::MutateRow(const void *key,
                         const RowChangeList &update) {
  // TODO: use 'probe' when calling UpdateRow on each rowset.
  RowSetKeyProbe probe(key_schema_, key);

  // The order of the next three lines is critical!
  //
  // Row-lock before ScopedTransaction:
  // -------------------------------------
  // We must take the row-lock before we assign a transaction ID in order to ensure
  // that within each row, transaction IDs only move forward. If we took a txid before
  // getting the row lock, we could have the following situation:
  //
  //   Thread 1         |  Thread 2
  //   ----------------------
  //   Start tx 1       |
  //                    |  Start tx 2
  //                    |  Obtain row lock
  //                    |  Update row
  //                    |  Commit tx 2
  //   Obtain row lock  |
  //   Delete row       |
  //   Commit tx 1
  //
  // This would cause the mutation list to look like: @t1: DELETE, @t2: UPDATE
  // which is invalid, since we expect to be able to be able to replay mutations
  // in increasing txid order on a given row.
  //
  // This requirement is basically two-phase-locking: the order in which row locks
  // are acquired for transactions determines their serialization order. If/when
  // we support multi-row serializable transactions, we'll have to acquire _all_
  // row locks before obtaining a txid.
  //
  // component_lock_ before ScopedTransaction:
  // -------------------------------------
  // Obtaining the txid inside of component_lock_ ensures that, in AtomicSwapRowSets,
  // we can cleanly differentiate a set of transactions that saw the "old" rowsets
  // vs the "new" rowsets. If we created the txid before taking the lock, then
  // the in-flight transaction could either have mutated the old rowsets or the new.
  //
  // There may be a more "fuzzy" way of doing this barrier which would cause less of
  // a locking hiccup during the swap, but let's keep things simple for now.
  //
  // RowLock before component_lock
  // ------------------------------
  // It currently doesn't matter which order these happen, but it makes more sense
  // to logically lock the rows before doing anything on the "physical" layer.
  // It is critical, however, that we're consistent with this choice between here
  // and Insert() or else there's a possibility of deadlock.
  ScopedRowLock rowlock(&lock_manager_, probe.encoded_key(), LockManager::LOCK_EXCLUSIVE);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
  ScopedTransaction tx(&mvcc_);

  // First try to update in memrowset.
  Status s = memrowset_->MutateRow(tx.txid(), probe, update);
  if (s.ok() || !s.IsNotFound()) {
    // if it succeeded, or if an error occurred, return.
    return s;
  }

  // TODO: could iterate the rowsets in a smart order
  // based on recent statistics - eg if a rowset is getting
  // updated frequently, pick that one first.
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_) {
    s = rs->MutateRow(tx.txid(), probe, update);
    if (s.ok() || !s.IsNotFound()) {
      // if it succeeded, or if an error occurred, return.
      return s;
    }
  }

  return Status::NotFound("key not found");
}

void Tablet::AtomicSwapRowSets(const RowSetVector old_rowsets,
                               const shared_ptr<RowSet> &new_rowset,
                               MvccSnapshot *snap_under_lock = NULL) {
  RowSetVector new_rowsets;
  boost::lock_guard<percpu_rwlock> lock(component_lock_);

  // O(n^2) diff algorithm to collect the set of rowsets excluding
  // the rowsets that were included in the compaction
  int num_replaced = 0;

  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_) {
    // Determine if it should be removed
    bool should_remove = false;
    BOOST_FOREACH(const shared_ptr<RowSet> &l_input, old_rowsets) {
      if (l_input == rs) {
        should_remove = true;
        num_replaced++;
        break;
      }
    }
    if (!should_remove) {
      new_rowsets.push_back(rs);
    }
  }

  CHECK_EQ(num_replaced, old_rowsets.size());

  if (new_rowset != NULL) {
    // Then push the new rowset on the end.
    new_rowsets.push_back(new_rowset);
  }

  rowsets_.swap(new_rowsets);

  if (snap_under_lock != NULL) {
    *snap_under_lock = MvccSnapshot(mvcc_);

    // We expect that there are no transactions in flight, since we hold component_lock_
    // in exclusive mode. For our compaction logic to be correct, we need to ensure that
    // no mutations in the 'old_rowsets' are associated with transactions that are
    // uncommitted in 'snap_under_lock'. If there were an in-flight transaction in
    // 'snap_under_lock', it would be possible that it wrote some mutations into
    // 'old_rowsets'.
    //
    // This property is ensured by the ordering between shared-locking 'component_lock_'
    // and creating the ScopedTransaction during mutations.  The transaction should be
    // started only after the 'component_lock' is taken, and committed before it is
    // released.
    CHECK_EQ(snap_under_lock->num_transactions_in_flight(), 0);
  }
}

Status Tablet::DeleteCompactionInputs(const RowSetsInCompaction &input) {
  BOOST_FOREACH(const shared_ptr<RowSet> &l_input, input.rowsets()) {
    LOG(INFO) << "Removing compaction input rowset " << l_input->ToString();
    RETURN_NOT_OK(l_input->Delete());
  }
  return Status::OK();
}

Status Tablet::Flush() {
  CHECK(open_);

  RowSetsInCompaction input;
  uint64_t start_insert_count;

  // Step 1. Freeze the old memrowset by blocking readers and swapping
  // it in as a new rowset, replacing it with an empty one.

  // TODO(perf): there's a memrowset.Freeze() call which we might be able to
  // use to improve iteration performance during the flush. The old design
  // used this, but not certain whether it's still doable with the new design.

  LOG(INFO) << "Flush: entering stage 1 (freezing old memrowset from inserts)";
  shared_ptr<MemRowSet> old_ms(new MemRowSet(schema_));
  {
    // Lock the component_lock_ in exclusive mode.
    // This shuts out any concurrent readers or writers for as long
    // as the swap takes.
    boost::lock_guard<percpu_rwlock> lock(component_lock_);

    start_insert_count = memrowset_->debug_insert_count();

    // swap in a new memrowset
    old_ms.swap(memrowset_);

    if (old_ms->empty()) {
      // flushing empty memrowset is a no-op
      LOG(INFO) << "Flush requested on empty memrowset";
      return Status::OK();
    }

    // Mark the memrowset rowset as locked, so compactions won't consider it
    // for inclusion in any concurrent compactions.
    shared_ptr<boost::mutex::scoped_try_lock> ms_lock(
      new boost::mutex::scoped_try_lock(*old_ms->compact_flush_lock()));
    CHECK(ms_lock->owns_lock());
    input.AddRowSet(old_ms, ms_lock);

    // Put it back on the rowset list.
    rowsets_.push_back(old_ms);
  }
  if (flush_hooks_) RETURN_NOT_OK(flush_hooks_->PostSwapNewMemRowSet());

  input.DumpToLog();
  LOG(INFO) << "Memstore in-memory size: " << old_ms->memory_footprint() << " bytes";


  RETURN_NOT_OK(DoCompactionOrFlush(input));

  // Sanity check that no insertions happened during our flush.
  CHECK_EQ(start_insert_count, old_ms->debug_insert_count())
    << "Sanity check failed: insertions continued in memrowset "
    << "after flush was triggered! Aborting to prevent dataloss.";

  return Status::OK();
}

void Tablet::SetCompactionHooksForTests(
  const shared_ptr<Tablet::CompactionFaultHooks> &hooks) {
  compaction_hooks_ = hooks;
}

void Tablet::SetFlushHooksForTests(
  const shared_ptr<Tablet::FlushFaultHooks> &hooks) {
  flush_hooks_ = hooks;
}

void Tablet::SetFlushCompactCommonHooksForTests(
  const shared_ptr<Tablet::FlushCompactCommonHooks> &hooks) {
  common_hooks_ = hooks;
}

static bool CompareBySize(const shared_ptr<RowSet> &a,
                          const shared_ptr<RowSet> &b) {
  return a->EstimateOnDiskSize() < b->EstimateOnDiskSize();
}


Status Tablet::PickRowSetsToCompact(RowSetsInCompaction *picked) const {
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
  CHECK_EQ(picked->num_rowsets(), 0);

  vector<shared_ptr<RowSet> > tmp_rowsets;
  tmp_rowsets.assign(rowsets_.begin(), rowsets_.end());

  // Sort the rowsets by their on-disk size
  std::sort(tmp_rowsets.begin(), tmp_rowsets.end(), CompareBySize);
  uint64_t accumulated_size = 0;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, tmp_rowsets) {
    uint64_t this_size = rs->EstimateOnDiskSize();
    if (picked->num_rowsets() < 2 || this_size < accumulated_size * 2) {
      // Grab the compact_flush_lock: this prevents any other concurrent
      // compaction from selecting this same rowset, and also ensures that
      // we don't select a rowset which is currently in the middle of being
      // flushed.
      shared_ptr<boost::mutex::scoped_try_lock> lock(
        new boost::mutex::scoped_try_lock(*rs->compact_flush_lock()));
      if (!lock->owns_lock()) {
        LOG(INFO) << "Unable to select " << rs->ToString() << " for compaction: it is busy";
        continue;
      }

      // Push the lock on our scoped list, so we unlock when done.
      picked->AddRowSet(rs, lock);
      accumulated_size += this_size;
    } else {
      break;
    }
  }

  return Status::OK();
}

Status Tablet::DoCompactionOrFlush(const RowSetsInCompaction &input) {
  AtomicWord my_index = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  string new_rowset_dir = GetRowSetPath(dir_, my_index);
  string tmp_rowset_dir = new_rowset_dir + ".compact-tmp";

  LOG(INFO) << "Compaction: entering phase 1 (flushing snapshot)";

  MvccSnapshot flush_snap(mvcc_);
  VLOG(1) << "Flushing with MVCC snapshot: " << flush_snap.ToString();

  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostTakeMvccSnapshot());

  shared_ptr<CompactionInput> merge;
  RETURN_NOT_OK(input.CreateCompactionInput(flush_snap, schema_, &merge));

  DiskRowSetWriter drsw(env_, schema_, tmp_rowset_dir, bloom_sizing());
  RETURN_NOT_OK(drsw.Open());
  RETURN_NOT_OK(kudu::tablet::Flush(merge.get(), flush_snap, &drsw));
  RETURN_NOT_OK(drsw.Finish());

  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostWriteSnapshot());

  // Though unlikely, it's possible that all of the input rows were actually
  // GCed in this compaction. In that case, we don't actually want to reopen.
  bool gced_all_input = drsw.written_count() == 0;
  if (gced_all_input) {
    LOG(INFO) << "Compaction resulted in no output rows (all input rows were GCed!)";
    LOG(INFO) << "Removing all input rowsets.";
    AtomicSwapRowSets(input.rowsets(),
                      shared_ptr<RowSet>(reinterpret_cast<RowSet *>(NULL)), NULL);
    // Remove old rowsets
    DeleteCompactionInputs(input);
    return Status::OK();
  }

  // Open the written-out snapshot as a new rowset.
  shared_ptr<DiskRowSet> new_rowset;
  Status s = DiskRowSet::Open(env_, schema_, tmp_rowset_dir, &new_rowset);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open snapshot compaction results in " << tmp_rowset_dir
                 << ": " << s.ToString();
    return s;
  }

  // Finished Phase 1. Start duplicating any new updates into the new on-disk rowset.
  //
  // During Phase 1, we may have missed some updates which came into the input rowsets
  // while we were writing. So, we can't immediately start reading from the on-disk
  // rowset alone. Starting here, we continue to read from the original rowset(s), but
  // mirror updates to both the input and the output data.
  //
  LOG(INFO) << "Compaction: entering phase 2 (starting to duplicate updates in new rowset)";
  shared_ptr<DuplicatingRowSet> inprogress_rowset(new DuplicatingRowSet(input.rowsets(), new_rowset));
  MvccSnapshot snap2;
  AtomicSwapRowSets(input.rowsets(), inprogress_rowset, &snap2);

  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostSwapInDuplicatingRowSet());

  // Phase 2. Some updates may have come in during Phase 1 which are only reflected in the
  // memrowset, but not in the new rowset. Here we re-scan the memrowset, copying those
  // missed updates into the new rowset's DeltaTracker.
  //
  // TODO: is there some bug here? Here's a potentially bad scenario:
  // - during flush, txid 1 updates a flushed row
  // - At the beginning of step 4, txid 2 updates the same flushed row, followed by ~1000
  //   more updates against the new rowset. This causes the new rowset to flush its deltas
  //   before txid 1 is transferred to it.
  // - Now the redos_0 deltafile in the new rowset includes txid 2-1000, and the DMS is empty.
  // - This code proceeds, and pushes txid1 into the DMS.
  // - DMS eventually flushes again, and redos_1 includes an _earlier_ update than redos_0.
  // At read time, since we apply updates from the redo logs in order, we might end up reading
  // the earlier data instead of the later data.
  //
  // Potential solutions:
  // 1) don't apply the changes in step 4 directly into the new rowset's DMS. Instead, reserve
  //    redos_0 for these edits, and write them directly to that file, even though it will likely
  //    be very small.
  // 2) at read time, as deltas are applied, keep track of the max txid for each of the columns
  //    and don't let an earlier update overwrite a later one.
  // 3) don't allow DMS to flush in an in-progress rowset.
  LOG(INFO) << "Compaction Phase 2: carrying over any updates which arrived during Phase 1";
  LOG(INFO) << "Phase 2 snapshot: " << snap2.ToString();
  RETURN_NOT_OK(input.CreateCompactionInput(snap2, schema_, &merge));
  RETURN_NOT_OK(merge->Init()); // TODO: why is init required here but not above?
  RETURN_NOT_OK(ReupdateMissedDeltas(merge.get(), flush_snap, snap2, new_rowset->delta_tracker_.get()));


  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostReupdateMissedDeltas());

  // ------------------------------
  // Flush to tmp was successful. Rename it to its real location.

  RETURN_NOT_OK(new_rowset->RenameRowSetDir(new_rowset_dir));

  LOG(INFO) << "Successfully flush/compacted " << drsw.written_count() << " rows";

  // Replace the compacted rowsets with the new on-disk rowset.
  AtomicSwapRowSets(boost::assign::list_of(inprogress_rowset), new_rowset);

  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostSwapNewRowSet());

  // Remove old rowsets
  DeleteCompactionInputs(input);

  return Status::OK();
}

Status Tablet::Compact() {
  CHECK(open_);

  LOG(INFO) << "Compaction: entering stage 1 (collecting rowsets)";
  RowSetsInCompaction input;
  // Step 1. Capture the rowsets to be merged
  RETURN_NOT_OK(PickRowSetsToCompact(&input));
  if (input.num_rowsets() < 2) {
    LOG(INFO) << "Not enough rowsets to run compaction! Aborting...";
    return Status::OK();
  }
  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostSelectIterators());

  input.DumpToLog();

  return DoCompactionOrFlush(input);
}

Status Tablet::DebugDump(vector<string> *lines) {
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  LOG_STRING(INFO, lines) << "Dumping tablet:";
  LOG_STRING(INFO, lines) << "---------------------------";

  LOG_STRING(INFO, lines) << "MRS " << memrowset_->ToString() << ":";
  RETURN_NOT_OK(memrowset_->DebugDump(lines));

  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_) {
    LOG_STRING(INFO, lines) << "RowSet " << rs->ToString() << ":";
    RETURN_NOT_OK(rs->DebugDump(lines));
  }

  return Status::OK();
}

Status Tablet::CaptureConsistentIterators(
  const Schema &projection,
  const MvccSnapshot &snap,
  vector<shared_ptr<RowwiseIterator> > *iters) const {
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<shared_ptr<RowwiseIterator> > ret;

  // Grab the memrowset iterator.
  shared_ptr<RowwiseIterator> ms_iter(memrowset_->NewRowIterator(projection, snap));
  ret.push_back(ms_iter);

  // Grab all rowset iterators.
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_) {
    shared_ptr<RowwiseIterator> row_it(rs->NewRowIterator(projection, snap));
    ret.push_back(row_it);
  }

  // Swap results into the parameters.
  ret.swap(*iters);
  return Status::OK();
}

Status Tablet::CountRows(uint64_t *count) const {
  // First grab a consistent view of the components of the tablet.
  shared_ptr<MemRowSet> memrowset;
  vector<shared_ptr<RowSet> > rowsets;
  {
    boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
    memrowset = memrowset_;
    rowsets = rowsets_;
  }

  // Now sum up the counts.
  *count = memrowset->entry_count();

  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, rowsets) {
    rowid_t l_count;
    RETURN_NOT_OK(rowset->CountRows(&l_count));
    *count += l_count;
  }

  return Status::OK();
}

size_t Tablet::num_rowsets() const {
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
  return rowsets_.size();
}

} // namespace table
} // namespace kudu
