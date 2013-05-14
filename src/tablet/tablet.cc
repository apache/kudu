// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <tr1/memory>
#include <vector>

#include "cfile/cfile.h"
#include "common/iterator.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/compaction.h"
#include "tablet/tablet.h"
#include "tablet/tablet-util.h"
#include "tablet/layer.h"
#include "util/bloom_filter.h"
#include "util/env.h"

DEFINE_bool(tablet_do_dup_key_checks, true,
            "Whether to check primary keys for duplicate on insertion. "
            "Use at your own risk!");

namespace kudu { namespace tablet {

using std::string;
using std::vector;
using std::tr1::shared_ptr;


const char *kLayerPrefix = "layer_";

string Tablet::GetLayerPath(const string &tablet_dir,
                            int layer_idx) {
  return StringPrintf("%s/layer_%010d",
                      tablet_dir.c_str(),
                      layer_idx);
}

Tablet::Tablet(const Schema &schema,
               const string &dir) :
  schema_(schema),
  dir_(dir),
  memstore_(new MemStore(schema)),
  next_layer_idx_(0),
  env_(Env::Default()),
  open_(false)
{
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
    if (TryStripPrefixString(child, kLayerPrefix, &suffix)) {
      // The file should be named 'layer_<N>'. N here is the index
      // of the layer (indicating the order in which it was flushed).
      uint32_t layer_idx;
      if (!safe_strtou32(suffix.c_str(), &layer_idx)) {
        return Status::IOError(string("Bad layer file: ") + absolute_path);
      }

      shared_ptr<Layer> layer;
      Status s = Layer::Open(env_, schema_, absolute_path, &layer);
      if (!s.ok()) {
        LOG(ERROR) << "Failed to open layer " << absolute_path << ": "
                   << s.ToString();
        return s;
      }
      layers_.push_back(layer);

      next_layer_idx_ = std::max(next_layer_idx_,
                                 (size_t)layer_idx + 1);
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
                              gscoped_ptr<RowwiseIterator> *iter) const
{
  vector<shared_ptr<RowwiseIterator> > iters;
  RETURN_NOT_OK(CaptureConsistentIterators(projection, snap, &iters));

  iter->reset(new UnionIterator(iters));

  return Status::OK();
}


Status Tablet::Insert(const Slice &data) {
  CHECK(open_) << "must Open() first!";

  LayerKeyProbe probe(schema_, data.data());

  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  // First, ensure that it is a unique key by checking all the open
  // Layers
  if (FLAGS_tablet_do_dup_key_checks) {
    bool present;
    RETURN_NOT_OK(tablet_util::CheckRowPresentInAnyLayer(
                    layers_, probe, &present));
    if (present) {
      return Status::AlreadyPresent("key already present");
    }
  }

  // TODO: the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  // Now try to insert into memstore. The memstore itself will return
  // AlreadyPresent if it has already been inserted there.
  ScopedTransaction tx(&mvcc_);
  return memstore_->Insert(tx.txid(), data);
}

Status Tablet::UpdateRow(const void *key,
                         const RowChangeList &update) {
  ScopedTransaction tx(&mvcc_);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  // First try to update in memstore.
  Status s = memstore_->UpdateRow(tx.txid(), key, update);
  if (s.ok() || !s.IsNotFound()) {
    // if it succeeded, or if an error occurred, return.
    return s;
  }

  // TODO: could iterate the layers in a smart order
  // based on recent statistics - eg if a layer is getting
  // updated frequently, pick that one first.
  BOOST_FOREACH(shared_ptr<LayerInterface> &l, layers_) {
    s = l->UpdateRow(tx.txid(), key, update);
    if (s.ok() || !s.IsNotFound()) {
      // if it succeeded, or if an error occurred, return.
      return s;
    }
  }

  return Status::NotFound("key not found");
}

void Tablet::AtomicSwapLayers(const LayerVector old_layers,
                              const shared_ptr<LayerInterface> &new_layer,
                              MvccSnapshot *snap_under_lock = NULL) {
  LayerVector new_layers;
  boost::lock_guard<percpu_rwlock> lock(component_lock_);

  // O(n^2) diff algorithm to collect the set of layers excluding
  // the layers that were included in the compaction
  int num_replaced = 0;

  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, layers_) {
    // Determine if it should be removed
    bool should_remove = false;
    BOOST_FOREACH(const shared_ptr<LayerInterface> &l_input, old_layers) {
      if (l_input == l) {
        should_remove = true;
        num_replaced++;
        break;
      }
    }
    if (!should_remove) {
      new_layers.push_back(l);
    }
  }

  CHECK_EQ(num_replaced, old_layers.size());

  // Then push the new layer on the end.
  new_layers.push_back(new_layer);

  layers_.swap(new_layers);

  if (snap_under_lock != NULL) {
    // TODO: is there some race here where transactions that are "uncommitted"
    // in this snapshot may still end up writing into the "old" layer? probably,
    // since transaction scope is outside of the update lock scope. Instead, we
    // probably want to just wait around for a while after doing the swap, until
    // all live transactions that might have written into the pre-swap layer are
    // committed, and then use that state.
    *snap_under_lock = MvccSnapshot(mvcc_);
  }
}

Status Tablet::Flush() {
  CHECK(open_);

  LayersInCompaction input;
  uint64_t start_insert_count;

  // Step 1. Freeze the old memstore by blocking readers and swapping
  // it in as a new layer, replacing it with an empty one.

  // TODO(perf): there's a memstore.Freeze() call which we might be able to
  // use to improve iteration performance during the flush. The old design
  // used this, but not certain whether it's still doable with the new design.

  LOG(INFO) << "Flush: entering stage 1 (freezing old memstore from inserts)";
  shared_ptr<MemStore> old_ms(new MemStore(schema_));
  {
    // Lock the component_lock_ in exclusive mode.
    // This shuts out any concurrent readers or writers for as long
    // as the swap takes.
    boost::lock_guard<percpu_rwlock> lock(component_lock_);

    start_insert_count = memstore_->debug_insert_count();

    // swap in a new memstore
    old_ms.swap(memstore_);

    if (old_ms->empty()) {
      // flushing empty memstore is a no-op
      LOG(INFO) << "Flush requested on empty memstore";
      return Status::OK();
    }

    // Mark the memstore layer as locked, so compactions won't consider it
    // for inclusion in any concurrent compactions.
    shared_ptr<boost::mutex::scoped_try_lock> ms_lock(
      new boost::mutex::scoped_try_lock(*old_ms->compact_flush_lock()));
    CHECK(ms_lock->owns_lock());
    input.AddLayer(old_ms, ms_lock);

    // Put it back on the layer list.
    layers_.push_back(old_ms);
  }
  if (flush_hooks_) RETURN_NOT_OK(flush_hooks_->PostSwapNewMemStore());

  input.DumpToLog();
  LOG(INFO) << "Memstore in-memory size: " << old_ms->memory_footprint() << " bytes";


  RETURN_NOT_OK(DoCompactionOrFlush(input));

  // Sanity check that no insertions happened during our flush.
  CHECK_EQ(start_insert_count, old_ms->debug_insert_count())
    << "Sanity check failed: insertions continued in memstore "
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

static bool CompareBySize(const shared_ptr<LayerInterface> &a,
                          const shared_ptr<LayerInterface> &b) {
  return a->EstimateOnDiskSize() < b->EstimateOnDiskSize();
}


Status Tablet::PickLayersToCompact(LayersInCompaction *picked) const
{
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
  CHECK_EQ(picked->num_layers(), 0);

  vector<shared_ptr<LayerInterface> > tmp_layers;
  tmp_layers.assign(layers_.begin(), layers_.end());

  // Sort the layers by their on-disk size
  std::sort(tmp_layers.begin(), tmp_layers.end(), CompareBySize);
  uint64_t accumulated_size = 0;
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, tmp_layers) {
    uint64_t this_size = l->EstimateOnDiskSize();
    if (picked->num_layers() < 2 || this_size < accumulated_size * 2) {
      // Grab the compact_flush_lock: this prevents any other concurrent
      // compaction from selecting this same layer, and also ensures that
      // we don't select a layer which is currently in the middle of being
      // flushed.
      shared_ptr<boost::mutex::scoped_try_lock> lock(
        new boost::mutex::scoped_try_lock(*l->compact_flush_lock()));
      if (!lock->owns_lock()) {
        LOG(INFO) << "Unable to select " << l->ToString() << " for compaction: it is busy";
        continue;
      }

      // Push the lock on our scoped list, so we unlock when done.
      picked->AddLayer(l, lock);
      accumulated_size += this_size;
    } else {
      break;
    }
  }

  return Status::OK();
}

Status Tablet::DoCompactionOrFlush(const LayersInCompaction &input) {
  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + ".compact-tmp";

  LOG(INFO) << "Compaction: entering phase 1 (flushing snapshot)";
  MvccSnapshot flush_snap(mvcc_);

  shared_ptr<CompactionInput> merge;
  RETURN_NOT_OK(input.CreateCompactionInput(flush_snap, schema_, &merge));

  LayerWriter lw(env_, schema_, tmp_layer_dir, bloom_sizing());
  RETURN_NOT_OK(lw.Open());
  RETURN_NOT_OK(kudu::tablet::Flush(merge.get(), &lw));
  RETURN_NOT_OK(lw.Finish());

  // Open the written-out snapshot as a new layer.
  shared_ptr<Layer> new_layer;
  Status s = Layer::Open(env_, schema_, tmp_layer_dir, &new_layer);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open snapshot compaction results in " << tmp_layer_dir
                 << ": " << s.ToString();
    return s;
  }

  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostWriteSnapshot());

  // Finished Phase 1. Start duplicating any new updates into the new on-disk layer.
  //
  // During Phase 1, we may have missed some updates which came into the input layers
  // while we were writing. So, we can't immediately start reading from the on-disk
  // layer alone. Starting here, we continue to read from the original layer(s), but
  // mirror updates to both the input and the output data.
  //
  LOG(INFO) << "Compaction: entering phase 2 (starting to duplicate updates in new layer)";
  shared_ptr<DuplicatingLayer> inprogress_layer(new DuplicatingLayer(input.layers(), new_layer));
  MvccSnapshot snap2;
  AtomicSwapLayers(input.layers(), inprogress_layer, &snap2);

  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostSwapInDuplicatingLayer());


  // Phase 2. Some updates may have come in during Phase 1 which are only reflected in the
  // memstore, but not in the new layer. Here we re-scan the memstore, copying those
  // missed updates into the new layer's DeltaTracker.
  //
  // TODO: is there some bug here? Here's a potentially bad scenario:
  // - during flush, txid 1 updates a flushed row
  // - At the beginning of step 4, txid 2 updates the same flushed row, followed by ~1000
  //   more updates against the new layer. This causes the new layer to flush its deltas
  //   before txid 1 is transferred to it.
  // - Now the redos_0 deltafile in the new layer includes txid 2-1000, and the DMS is empty.
  // - This code proceeds, and pushes txid1 into the DMS.
  // - DMS eventually flushes again, and redos_1 includes an _earlier_ update than redos_0.
  // At read time, since we apply updates from the redo logs in order, we might end up reading
  // the earlier data instead of the later data.
  //
  // Potential solutions:
  // 1) don't apply the changes in step 4 directly into the new layer's DMS. Instead, reserve
  //    redos_0 for these edits, and write them directly to that file, even though it will likely
  //    be very small.
  // 2) at read time, as deltas are applied, keep track of the max txid for each of the columns
  //    and don't let an earlier update overwrite a later one.
  // 3) don't allow DMS to flush in an in-progress layer.
  LOG(INFO) << "Compaction Phase 2: carrying over any updates which arrived during Phase 1";
  RETURN_NOT_OK(input.CreateCompactionInput(snap2, schema_, &merge));
  RETURN_NOT_OK(merge->Init()); // TODO: why is init required here but not above?
  RETURN_NOT_OK(ReupdateMissedDeltas(merge.get(), flush_snap, snap2, new_layer->delta_tracker_.get()));


  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostReupdateMissedDeltas());

  // ------------------------------
  // Flush to tmp was successful. Rename it to its real location.

  RETURN_NOT_OK(new_layer->RenameLayerDir(new_layer_dir));

  LOG(INFO) << "Successfully flush/compacted " << lw.written_count() << " rows";

  // Replace the compacted layers with the new on-disk layer.
  AtomicSwapLayers(boost::assign::list_of(inprogress_layer), new_layer);

  if (common_hooks_) RETURN_NOT_OK(common_hooks_->PostSwapNewLayer());

  // Remove old layers
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l_input, input.layers()) {
    LOG(INFO) << "Removing compaction input layer " << l_input->ToString();
    RETURN_NOT_OK(l_input->Delete());
  }

  return Status::OK();
}

Status Tablet::Compact()
{
  CHECK(open_);

  LOG(INFO) << "Compaction: entering stage 1 (collecting layers)";
  LayersInCompaction input;
  // Step 1. Capture the layers to be merged
  RETURN_NOT_OK(PickLayersToCompact(&input));
  if (input.num_layers() < 2) {
    LOG(INFO) << "Not enough layers to run compaction! Aborting...";
    return Status::OK();
  }
  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostSelectIterators());

  input.DumpToLog();

  return DoCompactionOrFlush(input);
}

Status Tablet::CaptureConsistentIterators(
  const Schema &projection,
  const MvccSnapshot &snap,
  vector<shared_ptr<RowwiseIterator> > *iters) const
{
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<shared_ptr<RowwiseIterator> > ret;

  // Grab the memstore iterator.
  shared_ptr<RowwiseIterator> ms_iter(memstore_->NewRowIterator(projection, snap));
  ret.push_back(ms_iter);

  // Grab all layer iterators.
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, layers_) {
    shared_ptr<RowwiseIterator> row_it(l->NewRowIterator(projection, snap));
    ret.push_back(row_it);
  }

  // Swap results into the parameters.
  ret.swap(*iters);
  return Status::OK();
}

Status Tablet::CountRows(uint64_t *count) const {
  // First grab a consistent view of the components of the tablet.
  shared_ptr<MemStore> memstore;
  vector<shared_ptr<LayerInterface> > layers;
  {
    boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
    memstore = memstore_;
    layers = layers_;
  }

  // Now sum up the counts.
  *count = memstore->entry_count();

  BOOST_FOREACH(const shared_ptr<LayerInterface> &layer, layers) {
    rowid_t l_count;
    RETURN_NOT_OK(layer->CountRows(&l_count));
    *count += l_count;
  }

  return Status::OK();
}

size_t Tablet::num_layers() const {
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
  return layers_.size();
}

} // namespace table
} // namespace kudu
