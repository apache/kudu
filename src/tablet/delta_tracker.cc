// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/thread/locks.hpp>
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
#include "tablet/tablet.pb.h"

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
    const Schema* projection,
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
  virtual Status FilterColumnsAndAppend(const metadata::ColumnIndexes& col_indexes,
                                        vector<DeltaKeyAndUpdate>* out,
                                        Arena* arena);
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

struct DeltaKeyUpdateComparator {
  bool operator() (const DeltaKeyAndUpdate& a, const DeltaKeyAndUpdate &b) {
    return a.key.CompareTo(b.key) < 0;
  }
};

Status DeltaIteratorMerger::FilterColumnsAndAppend(const metadata::ColumnIndexes& col_indexes,
                                                   vector<DeltaKeyAndUpdate>* out,
                                                   Arena* arena) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator>& iter, iters_) {
    RETURN_NOT_OK(iter->FilterColumnsAndAppend(col_indexes, out, arena));
  }
  std::sort(out->begin(), out->end(), DeltaKeyUpdateComparator());
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
  const Schema* projection,
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
  open_(false)
{}


// Open any previously flushed DeltaFiles in this rowset
Status DeltaTracker::Open() {
  CHECK(delta_stores_.empty()) << "should call before opening any readers";
  CHECK(!open_);

  int64_t max_id = -1;
  for (size_t idx = 0; idx < rowset_metadata_->redo_delta_blocks_count(); ++idx) {
    size_t dsize = 0;
    shared_ptr<RandomAccessFile> dfile;
    int64_t delta_id;
    Status s = rowset_metadata_->OpenRedoDeltaDataBlock(idx,
                                                        &dfile,
                                                        &dsize,
                                                        &delta_id);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to open delta file " << idx << ": "
                 << s.ToString();
      return s;
    }

    gscoped_ptr<DeltaFileReader> dfr;
    s = DeltaFileReader::Open("---", dfile, dsize, delta_id, &dfr);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to open delta file " << idx << ": "
                 << s.ToString();
      return s;
    }

    max_id = std::max(max_id, dfr->id());
    LOG(INFO) << "Successfully opened delta file " << idx;
    delta_stores_.push_back(shared_ptr<DeltaStore>(dfr.release()));
  }

  // the id of the first DeltaMemStore is the max id of the current ones +1
  dms_.reset(new DeltaMemStore(max_id + 1, schema_));
  open_ = true;
  return Status::OK();
}

Status DeltaTracker::MakeCompactionInput(size_t start_idx, size_t end_idx,
                                         vector<shared_ptr<DeltaStore> > *target_stores,
                                         vector<int64_t> *target_ids,
                                         gscoped_ptr<DeltaCompactionInput> *out) {
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);

  CHECK(open_);
  CHECK_LE(start_idx, end_idx);
  CHECK_LT(end_idx, delta_stores_.size());
  vector<shared_ptr<DeltaCompactionInput> > inputs;
  for (size_t idx = start_idx; idx <= end_idx; ++idx) {
    shared_ptr<DeltaStore> &delta_store = delta_stores_[idx];
    DeltaFileReader *dfr = down_cast<DeltaFileReader *>(delta_store.get());
    LOG(INFO) << "Preparing to compact delta file: " << dfr->path();
    gscoped_ptr<DeltaCompactionInput> dci;
    RETURN_NOT_OK(DeltaCompactionInput::Open(*dfr, &schema_, &dci));
    inputs.push_back(shared_ptr<DeltaCompactionInput>(dci.release()));
    target_stores->push_back(delta_store);
    target_ids->push_back(delta_store->id());
  }
  out->reset(DeltaCompactionInput::Merge(schema_, inputs));
  return Status::OK();
}

Status DeltaTracker::AtomicUpdateStores(size_t start_idx, size_t end_idx,
                                        const vector<shared_ptr<DeltaStore> > &expected_stores,
                                        gscoped_ptr<DeltaFileReader> new_store) {
  boost::lock_guard<boost::shared_mutex> lock(component_lock_);

  // First check that delta stores match the old stores
  vector<shared_ptr<DeltaStore > >::const_iterator it = expected_stores.begin();
  for (size_t idx = start_idx; idx <= end_idx; ++idx) {
    CHECK_EQ(it->get(), delta_stores_[idx].get());
    ++it;
  }

  // Remove the old stores
  vector<shared_ptr<DeltaStore> >::iterator erase_start = delta_stores_.begin() + start_idx;
  vector<shared_ptr<DeltaStore> >::iterator erase_end = delta_stores_.begin() + end_idx + 1;
  delta_stores_.erase(erase_start, erase_end);

  // Insert the new store
  delta_stores_.push_back(shared_ptr<DeltaStore>(new_store.release()));
  return Status::OK();
}

Status DeltaTracker::Compact() {
  if (CountDeltaStores() <= 1) {
    return Status::OK();
  }
  return CompactStores(0, -1);
}

Status DeltaTracker::CompactStores(int start_idx, int end_idx) {
  // Prevent concurrent compactions or a compaction concurrent with a flush
  //
  // TODO(perf): this could be more fine grained
  boost::lock_guard<boost::mutex> l(compact_flush_lock_);

  if (end_idx == -1) {
    end_idx = delta_stores_.size() - 1;
  }

  CHECK_LE(start_idx, end_idx);
  CHECK_LT(end_idx, delta_stores_.size());
  CHECK(open_);
  BlockId block_id;
  shared_ptr<WritableFile> data_writer;

  // Open a writer for the new destination delta block
  RETURN_NOT_OK(rowset_metadata_->NewDeltaDataBlock(&data_writer, &block_id));

  // Merge and compact the stores and write and output to "data_writer"
  vector<shared_ptr<DeltaStore> > compacted_stores;
  vector<int64_t> compacted_ids;
  RETURN_NOT_OK(DoCompactStores(start_idx, end_idx, data_writer,
                &compacted_stores, &compacted_ids));
  int64_t compacted_id = compacted_ids.back();

  // Open the new deltafile
  gscoped_ptr<DeltaFileReader> dfr;
  size_t data_size = 0;
  shared_ptr<RandomAccessFile> data_reader;
  RETURN_NOT_OK(rowset_metadata_->OpenDataBlock(block_id, &data_reader,
                                                &data_size));
  RETURN_NOT_OK(DeltaFileReader::Open(block_id.ToString(), data_reader,
                                      data_size, compacted_id, &dfr));

  // Update delta_stores_, removing the compacted delta files and inserted the new
  RETURN_NOT_OK(AtomicUpdateStores(start_idx, end_idx, compacted_stores, dfr.Pass()));
  LOG(INFO) << "Opened delta block for read: " << block_id.ToString();

  RETURN_NOT_OK(rowset_metadata_->AtomicRemoveRedoDeltaDataBlocks(start_idx, end_idx,
                                                                  compacted_ids));
  RETURN_NOT_OK(rowset_metadata_->CommitRedoDeltaDataBlock(compacted_id, block_id));
  Status s = rowset_metadata_->Flush();
  if (!s.ok()) {
    LOG(ERROR) << "Unable to commit delta data block metadata for "
               << block_id.ToString() << ": " << s.ToString();
    return s;
  }
  return Status::OK();
}

Status DeltaTracker::DoCompactStores(size_t start_idx, size_t end_idx,
         const shared_ptr<WritableFile> &data_writer,
         vector<shared_ptr<DeltaStore> > *compacted_stores,
         vector<int64_t> *compacted_ids) {
  gscoped_ptr<DeltaCompactionInput> inputs_merge;
  RETURN_NOT_OK(MakeCompactionInput(start_idx, end_idx, compacted_stores,
                                    compacted_ids, &inputs_merge));
  LOG(INFO) << "Compacting " << (end_idx - start_idx + 1) << " delta files.";
  DeltaFileWriter dfw(inputs_merge->schema(), data_writer);
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

shared_ptr<DeltaIterator> DeltaTracker::NewDeltaIterator(const Schema* schema,
                                                         const MvccSnapshot &snap) const {
  std::vector<shared_ptr<DeltaStore> > stores;
  CollectStores(&stores);
  return DeltaIteratorMerger::Create(stores, schema, snap);
}

shared_ptr<DeltaIterator> DeltaTracker::NewDeltaFileIterator(const Schema* schema,
                                                             const MvccSnapshot& snap,
                                                             int64_t* last_store_id) const {
  std::vector<shared_ptr<DeltaStore> > stores;
  {
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);
    // TODO perf: is this really needed? Will check
    // DeltaIteratorMerger::Create()
    stores.assign(delta_stores_.begin(), delta_stores_.end());
  }
  *last_store_id = stores.back()->id();
  return DeltaIteratorMerger::Create(stores, schema, snap);
}

ColumnwiseIterator *DeltaTracker::WrapIterator(const shared_ptr<ColumnwiseIterator> &base,
                                               const MvccSnapshot &mvcc_snap) const {
  return new DeltaApplier(base, NewDeltaIterator(&base->schema(), mvcc_snap));
}


Status DeltaTracker::Update(txid_t txid,
                            rowid_t row_idx,
                            const RowChangeList &update,
                            MutationResultPB* result) {
  // TODO: can probably lock this more fine-grained.
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);
  DCHECK_LT(row_idx, num_rows_);

  Status s = dms_->Update(txid, row_idx, update);
  if (s.ok()) {
    MutationTargetPB* target = result->add_mutations();
    target->set_rs_id(rowset_metadata_->id());
    target->set_delta_id(dms_->id());
  }
  return s;
}

Status DeltaTracker::CheckRowDeleted(rowid_t row_idx, bool *deleted,
                                     ProbeStats* stats) const {
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
    stats->deltas_consulted++;
    RETURN_NOT_OK(ds->CheckRowDeleted(row_idx, deleted));
    if (*deleted) {
      return Status::OK();
    }
  }

  return Status::OK();
}

Status DeltaTracker::FlushDMS(const DeltaMemStore &dms,
                              gscoped_ptr<DeltaFileReader> *dfr) {

  // Open file for write.
  BlockId block_id;
  shared_ptr<WritableFile> data_writer;
  Status s = rowset_metadata_->NewDeltaDataBlock(&data_writer, &block_id);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open delta output block " << block_id.ToString() << ": "
                 << s.ToString();
    return s;
  }

  DeltaFileWriter dfw(dms.schema(), data_writer);
  s = dfw.Start();
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open delta output block " << block_id.ToString() << ": "
                 << s.ToString();
    return s;
  }
  RETURN_NOT_OK(dms.FlushToFile(&dfw));
  RETURN_NOT_OK(dfw.Finish());
  LOG(INFO) << "Flushed delta block: " << block_id.ToString();
  VLOG(1) << "Delta block " << block_id.ToString() << " schema: " << dms.schema().ToString();

  // Now re-open for read
  size_t data_size = 0;
  shared_ptr<RandomAccessFile> data_reader;
  RETURN_NOT_OK(rowset_metadata_->OpenDataBlock(block_id, &data_reader, &data_size));
  RETURN_NOT_OK(DeltaFileReader::Open(block_id.ToString(), data_reader, data_size, dms.id(), dfr));
  LOG(INFO) << "Reopened delta block for read: " << block_id.ToString();

  RETURN_NOT_OK(rowset_metadata_->CommitRedoDeltaDataBlock(dms.id(), block_id));
  s = rowset_metadata_->Flush();
  if (!s.ok()) {
    LOG(ERROR) << "Unable to commit Delta block metadata for: " << block_id.ToString();
    return s;
  }

  return Status::OK();
}

Status DeltaTracker::Flush() {
  boost::lock_guard<boost::mutex> l(compact_flush_lock_);

  // First, swap out the old DeltaMemStore a new one,
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
      // Ensure that the DeltaMemStore is using the latest schema.
      RETURN_NOT_OK(dms_->AlterSchema(schema_));
      return Status::OK();
    }

    // Swap the DeltaMemStore to use the new schema
    old_dms = dms_;
    dms_.reset(new DeltaMemStore(old_dms->id() + 1, schema_));

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

void DeltaTracker::SetDMS(const shared_ptr<DeltaMemStore>& dms) {
  boost::lock_guard<boost::shared_mutex> lock(component_lock_);
  dms_ = dms;
}

Status DeltaTracker::AlterSchema(const Schema& schema) {
  bool require_update;
  {
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);
    if ((require_update = !schema_.Equals(schema))) {
      schema_ = schema;
    }
  }
  if (!require_update) {
    return Status::OK();
  }
  return Flush();
}

size_t DeltaTracker::DeltaMemStoreSize() const {
  return dms_->memory_footprint();
}

size_t DeltaTracker::CountDeltaStores() const {
  boost::lock_guard<boost::shared_mutex> lock(component_lock_);
  return delta_stores_.size();
}

const Schema& DeltaTracker::schema() const {
  return dms_->schema();
}

////////////////////////////////////////////////////////////
// Delta merger
////////////////////////////////////////////////////////////

} // namespace tablet
} // namespace kudu
