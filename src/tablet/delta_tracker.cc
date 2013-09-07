// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/thread/shared_mutex.hpp>
#include <tr1/memory>
#include <string>

#include "gutil/strings/strip.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/status.h"
#include "tablet/deltafile.h"
#include "tablet/delta_compaction.h"
#include "tablet/delta_store.h"
#include "tablet/diskrowset.h"

namespace kudu { namespace tablet {

using metadata::RowSetMetadata;
using std::string;
using std::tr1::shared_ptr;

namespace {

// DeltaIterator that simply combines together other DeltaIterators,
// applying deltas from each in order.
class DeltaIteratorMerger : public DeltaIterator {
 public:
  // Create a new DeltaIterator which combines the deltas from
  // all of the input delta stores.
  //
  // If only one store is input, this will automatically return an unwrapped
  // iterator for greater efficiency.
  static shared_ptr<DeltaIterator> Create(
    const vector<shared_ptr<DeltaStore> > &stores,
    const Schema &projection,
    const MvccSnapshot &snapshot);

  ////////////////////////////////////////////////////////////
  // Implementations of DeltaIterator
  ////////////////////////////////////////////////////////////
  virtual Status Init();
  virtual Status SeekToOrdinal(rowid_t idx);
  virtual Status PrepareBatch(size_t nrows);
  virtual Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst);
  virtual Status ApplyDeletes(SelectionVector *sel_vec);
  virtual Status CollectMutations(vector<Mutation *> *dst, Arena *arena);
  virtual string ToString() const;

 private:
  explicit DeltaIteratorMerger(const vector<shared_ptr<DeltaIterator> > &iters);

  vector<shared_ptr<DeltaIterator> > iters_;
};

DeltaIteratorMerger::DeltaIteratorMerger(const vector<shared_ptr<DeltaIterator> > &iters)
  : iters_(iters) {
}

Status DeltaIteratorMerger::Init() {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->Init());
  }
  return Status::OK();
}

Status DeltaIteratorMerger::SeekToOrdinal(rowid_t idx) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->SeekToOrdinal(idx));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::PrepareBatch(size_t nrows) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->PrepareBatch(nrows));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->ApplyUpdates(col_to_apply, dst));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::ApplyDeletes(SelectionVector *sel_vec) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->ApplyDeletes(sel_vec));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::CollectMutations(vector<Mutation *> *dst, Arena *arena) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->CollectMutations(dst, arena));
  }
  // TODO: do we need to do some kind of sorting here to deal with out-of-order
  // txids?
  return Status::OK();
}


string DeltaIteratorMerger::ToString() const {
  string ret;
  ret.append("DeltaIteratorMerger(");

  bool first = true;
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    ret.append(iter->ToString());
  }
  ret.append(")");
  return ret;
}


shared_ptr<DeltaIterator> DeltaIteratorMerger::Create(
  const vector<shared_ptr<DeltaStore> > &stores,
  const Schema &projection,
  const MvccSnapshot &snapshot) {
  vector<shared_ptr<DeltaIterator> > delta_iters;

  BOOST_FOREACH(const shared_ptr<DeltaStore> &store, stores) {
    shared_ptr<DeltaIterator> iter(store->NewDeltaIterator(projection, snapshot));
    delta_iters.push_back(iter);
  }

  if (delta_iters.size() == 1) {
    // If we only have one input to the "merge", we can just directly
    // return that iterator.
    return delta_iters[0];
  }

  return shared_ptr<DeltaIterator>(new DeltaIteratorMerger(delta_iters));
}

} // anonymous namespace


DeltaTracker::DeltaTracker(const shared_ptr<RowSetMetadata>& rowset_metadata,
                           const Schema &schema,
                           rowid_t num_rows) :
  rowset_metadata_(rowset_metadata),
  schema_(schema),
  num_rows_(num_rows),
  open_(false),
  dms_(new DeltaMemStore(schema))
{}


// Open any previously flushed DeltaFiles in this rowset
Status DeltaTracker::Open() {
  CHECK(delta_stores_.empty()) << "should call before opening any readers";
  CHECK(!open_);

  for (size_t idx = 0; idx < rowset_metadata_->delta_blocks_count(); ++idx) {
    size_t dsize = 0;
    shared_ptr<RandomAccessFile> dfile;
    Status s = rowset_metadata_->OpenDeltaDataBlock(idx, &dfile, &dsize);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to open delta file " << idx << ": "
                 << s.ToString();
      return s;
    }

    gscoped_ptr<DeltaFileReader> dfr;
    s = DeltaFileReader::Open("---", dfile, dsize, schema_, &dfr);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to open delta file " << idx << ": "
                 << s.ToString();
      return s;
    }

    LOG(INFO) << "Successfully opened delta file " << idx;
    delta_stores_.push_back(shared_ptr<DeltaStore>(dfr.release()));
  }

  open_ = true;
  return Status::OK();
}

Status DeltaTracker::MakeCompactionInput(size_t start_idx, size_t end_idx,
                                         gscoped_ptr<DeltaCompactionInput> *out) {
  CHECK(open_);
  CHECK_LE(start_idx, end_idx);
  CHECK_LT(end_idx, delta_stores_.size());
  vector<shared_ptr<DeltaCompactionInput> > inputs;
  for (size_t idx = start_idx; idx <= end_idx; ++idx) {
    DeltaFileReader *dfr = down_cast<DeltaFileReader *>(delta_stores_[idx].get());
    LOG(INFO) << "Preparing to compact delta file: " << dfr->path();
    gscoped_ptr<DeltaCompactionInput> dci;
    RETURN_NOT_OK(DeltaCompactionInput::Open(*dfr, &dci));
    inputs.push_back(shared_ptr<DeltaCompactionInput>(dci.release()));
  }
  out->reset(DeltaCompactionInput::Merge(inputs));
  return Status::OK();
}

Status DeltaTracker::CompactStores(size_t start_idx, size_t end_idx,
                                   const shared_ptr<WritableFile> &data_writer) {
  gscoped_ptr<DeltaCompactionInput> inputs_merge;
  RETURN_NOT_OK(MakeCompactionInput(start_idx, end_idx, &inputs_merge));
  LOG(INFO) << "Compacting " << (start_idx - end_idx + 1) << " delta files.";
  DeltaFileWriter dfw(schema_, data_writer);
  RETURN_NOT_OK(dfw.Start());
  RETURN_NOT_OK(FlushDeltaCompactionInput(inputs_merge.get(), &dfw));
  RETURN_NOT_OK(dfw.Finish());
  LOG(INFO) << "Succesfully compacted the specified delta files.";
  return Status::OK();
}

void DeltaTracker::CollectStores(vector<shared_ptr<DeltaStore> > *deltas) const {
  boost::lock_guard<boost::shared_mutex> lock(component_lock_);
  deltas->assign(delta_stores_.begin(), delta_stores_.end());
  deltas->push_back(dms_);
}

shared_ptr<DeltaIterator> DeltaTracker::NewDeltaIterator(const Schema &schema,
                                                                  const MvccSnapshot &snap) const {
  std::vector<shared_ptr<DeltaStore> > stores;
  CollectStores(&stores);
  return DeltaIteratorMerger::Create(stores, schema, snap);
}

ColumnwiseIterator *DeltaTracker::WrapIterator(const shared_ptr<ColumnwiseIterator> &base,
                                               const MvccSnapshot &mvcc_snap) const {
  return new DeltaApplier(base, NewDeltaIterator(base->schema(), mvcc_snap));
}


Status DeltaTracker::Update(txid_t txid,
                            rowid_t row_idx,
                            const RowChangeList &update,
                            MutationResult * result) {
  // TODO: can probably lock this more fine-grained.
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);
  DCHECK_LT(row_idx, num_rows_);

  Status s = dms_->Update(txid, row_idx, update);
  if (s.ok()) {
    // TODO once deltas have ids (probably after delta compaction) we need to
    // change this to use the id from the DeltaMemStore and not the delta stores
    // size.
    result->AddDeltaRowStoreMutation(row_idx, rowset_metadata_->id(), delta_stores_.size());
  }
  return s;
}

Status DeltaTracker::CheckRowDeleted(rowid_t row_idx, bool *deleted) const {
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);

  DCHECK_LT(row_idx, num_rows_);

  *deleted = false;
  // Check if the row has a deletion in DeltaMemStore.
  RETURN_NOT_OK(dms_->CheckRowDeleted(row_idx, deleted));
  if (*deleted) {
    return Status::OK();
  }

  // Then check backwards through the list of trackers.
  BOOST_REVERSE_FOREACH(const shared_ptr<DeltaStore> &ds, delta_stores_) {
    RETURN_NOT_OK(ds->CheckRowDeleted(row_idx, deleted));
    if (*deleted) {
      return Status::OK();
    }
  }

  return Status::OK();
}

Status DeltaTracker::FlushDMS(const DeltaMemStore &dms,
                              gscoped_ptr<DeltaFileReader> *dfr) {
  uint32_t deltafile_idx = next_deltafile_idx_++;

  // Open file for write.
  BlockId block_id;
  shared_ptr<WritableFile> data_writer;
  Status s = rowset_metadata_->NewDeltaDataBlock(&data_writer, &block_id);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open delta output block " << block_id.ToString() << ": "
                 << s.ToString();
    return s;
  }

  DeltaFileWriter dfw(schema_, data_writer);
  s = dfw.Start();
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open delta output block " << block_id.ToString() << ": "
                 << s.ToString();
    return s;
  }
  RETURN_NOT_OK(dms.FlushToFile(&dfw));
  RETURN_NOT_OK(dfw.Finish());
  LOG(INFO) << "Flushed delta block: " << block_id.ToString();

  // Now re-open for read
  size_t data_size = 0;
  shared_ptr<RandomAccessFile> data_reader;
  RETURN_NOT_OK(rowset_metadata_->OpenDataBlock(block_id, &data_reader, &data_size));
  RETURN_NOT_OK(DeltaFileReader::Open(block_id.ToString(), data_reader, data_size, schema_, dfr));
  LOG(INFO) << "Reopened delta block for read: " << block_id.ToString();

  RETURN_NOT_OK(rowset_metadata_->CommitDeltaDataBlock(deltafile_idx, block_id));
  s = rowset_metadata_->Flush();
  if (!s.ok()) {
    LOG(ERROR) << "Unable to commit Delta block metadata for: " << block_id.ToString();
    return s;
  }

  return Status::OK();
}

Status DeltaTracker::Flush() {
  // First, swap out the old DeltaMemStore with a new one,
  // and add it to the list of delta stores to be reflected
  // in reads.
  shared_ptr<DeltaMemStore> old_dms;
  size_t count;
  {
    // Lock the component_lock_ in exclusive mode.
    // This shuts out any concurrent readers or writers.
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);

    count = dms_->Count();
    if (count == 0) {
      // No need to flush if there are no deltas.
      return Status::OK();
    }

    old_dms = dms_;
    dms_.reset(new DeltaMemStore(schema_));

    delta_stores_.push_back(old_dms);
  }

  LOG(INFO) << "Flushing " << count << " deltas...";

  // Now, actually flush the contents of the old DMS.
  // TODO: need another lock to prevent concurrent flushers
  // at some point.
  gscoped_ptr<DeltaFileReader> dfr;
  Status s = FlushDMS(*old_dms, &dfr);
  CHECK(s.ok())
    << "Failed to flush DMS: " << s.ToString()
    << "\nTODO: need to figure out what to do with error handling "
    << "if this fails -- we end up with a DeltaMemStore permanently "
    << "in the store list. For now, abort.";


  // Now, re-take the lock and swap in the DeltaFileReader in place of
  // of the DeltaMemStore
  {
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);
    size_t idx = delta_stores_.size() - 1;

    CHECK_EQ(delta_stores_[idx], old_dms)
      << "Another thread modified the delta store list during flush";
    delta_stores_[idx].reset(dfr.release());
  }

  return Status::OK();

  // TODO: wherever we write stuff, we should write to a tmp path
  // and rename to final path!
}

////////////////////////////////////////////////////////////
// Delta merger
////////////////////////////////////////////////////////////

} // namespace tablet
} // namespace kudu
