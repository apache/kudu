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
                              gscoped_ptr<RowwiseIterator> *iter) const
{
  vector<shared_ptr<RowwiseIterator> > iters;
  RETURN_NOT_OK(CaptureConsistentIterators(projection, &iters));

  iter->reset(new UnionIterator(iters));

  return Status::OK();
}


Status Tablet::Insert(const Slice &data) {
  CHECK(open_) << "must Open() first!";

  LayerKeyProbe probe(schema_, data.data());

  boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());

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
  return memstore_->Insert(data);
}

Status Tablet::UpdateRow(const void *key,
                         const RowChangeList &update) {
  boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());

  // First try to update in memstore.
  Status s = memstore_->UpdateRow(key, update);
  if (s.ok() || !s.IsNotFound()) {
    // if it succeeded, or if an error occurred, return.
    return s;
  }

  // TODO: could iterate the layers in a smart order
  // based on recent statistics - eg if a layer is getting
  // updated frequently, pick that one first.
  BOOST_FOREACH(shared_ptr<LayerInterface> &l, layers_) {
    s = l->UpdateRow(key, update);
    if (s.ok() || !s.IsNotFound()) {
      // if it succeeded, or if an error occurred, return.
      return s;
    }
  }

  return Status::NotFound("key not found");
}

void Tablet::AtomicSwapLayers(const LayerVector old_layers,
                              const shared_ptr<LayerInterface> &new_layer) {
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
}

Status Tablet::Flush() {
  CHECK(open_);

  if (flush_hooks_) RETURN_NOT_OK(flush_hooks_->PreFlush());

  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + Layer::kTmpLayerSuffix;

  uint64_t start_insert_count;

  shared_ptr<MemStore> old_ms(new MemStore(schema_));
  boost::mutex::scoped_try_lock old_ms_lock;

  LOG(INFO) << "Flush: entering stage 1 (freezing old memstore from inserts)";

  // Step 1. Freeze the old memstore by blocking readers and swapping
  // it in as a new layer.
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
    // for inclusion.
    old_ms_lock.swap(boost::mutex::scoped_try_lock(*old_ms->compact_flush_lock()));
    CHECK(old_ms_lock.owns_lock());
    layers_.push_back(old_ms);
  }

  if (flush_hooks_) RETURN_NOT_OK(flush_hooks_->PostSwapNewMemStore());


  // At this point:
  //   Inserts: go into the new memstore
  //   Updates: can go to either memstore as appropriate
  //   Deletes: TODO
  //
  // Crucially, the *key set* of the old memstore has been frozen,
  // so we can flush it to disk without it changing under us.

  // Step 2. Flush the key column of the old memstore. This doesn't need
  // any lock, since updates go on concurrently.
  // TODO: deletes are a little bit tricky here -- need to make sure
  // that deletes at this point are just marking entries in the btree
  // rather than actually deleting them.
  LOG(INFO) << "Flush: entering stage 2 (flushing keys)";
  LOG(INFO) << "Memstore in-memory size: " << old_ms->memory_footprint() << " bytes";

  Schema keys_only = schema_.CreateKeyProjection();

  shared_ptr<RowwiseIterator> iter(old_ms->NewIterator(keys_only));
  RETURN_NOT_OK(iter->Init(NULL));

  LayerWriter out(env_, schema_, tmp_layer_dir, bloom_sizing());
  RETURN_NOT_OK(out.Open());
  RETURN_NOT_OK(out.FlushProjection(keys_only, iter.get(), false, true));

  if (flush_hooks_) RETURN_NOT_OK(flush_hooks_->PostFlushKeys());

  // Step 3. Freeze old memstore contents.
  // Because the key column exists on disk, we can create a new layer
  // which does positional updating instead of in-place updating,
  // and swap that in.

  LOG(INFO) << "Flush: entering stage 3 (freezing old memstore from in-place updates)";

  shared_ptr<FlushInProgressLayer> inprogress_layer;
  RETURN_NOT_OK(FlushInProgressLayer::Open(
                  env_, schema_, tmp_layer_dir, old_ms,
                  &inprogress_layer));

  uint64_t start_update_count;
  AtomicSwapLayers(boost::assign::list_of(old_ms), inprogress_layer);

  // We shouldn't have any more updates after this point.
  start_update_count = old_ms->debug_update_count();
  old_ms->Freeze();

  if (flush_hooks_) RETURN_NOT_OK(flush_hooks_->PostFreezeOldMemStore());

  LOG(INFO) << "Flush: entering stage 4 (flushing the rest of the columns)";

  // Step 4. Flush the non-key columns
  Schema non_keys = schema_.CreateNonKeyProjection();
  iter.reset(old_ms->NewIterator(non_keys));
  RETURN_NOT_OK(iter->Init(NULL));
  RETURN_NOT_OK(out.FlushProjection(non_keys, iter.get(), false, false));
  RETURN_NOT_OK(out.Finish());


  // Sanity check that no mutations happened during our flush.
  CHECK_EQ(start_insert_count, old_ms->debug_insert_count())
    << "Sanity check failed: insertions continued in memstore "
    << "after flush was triggered! Aborting to prevent dataloss.";
  CHECK_EQ(start_update_count, old_ms->debug_update_count())
    << "Sanity check failed: updates continued in memstore "
    << "after flush was triggered! Aborting to prevent dataloss.";

  // Flush to tmp was successful. Finish the flush.
  // TODO: move this to be a method in FlushInProgressLayer
  RETURN_NOT_OK(env_->RenameFile(tmp_layer_dir, new_layer_dir));

  shared_ptr<Layer> final_layer;
  Status s = Layer::Open(env_, schema_, new_layer_dir, &final_layer);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open flush result " << new_layer_dir
                 << ": " << s.ToString();
    return s;
  }

  final_layer->set_delta_tracker(inprogress_layer->delta_tracker_);
  AtomicSwapLayers(boost::assign::list_of(inprogress_layer), final_layer);

  LOG(INFO) << "Successfully flushed " << out.written_count() << " rows";

  if (flush_hooks_) RETURN_NOT_OK(flush_hooks_->PostOpenNewLayer());

  // unlock these now. Although no other thread should ever see them,
  // we trigger pthread assertions in DEBUG mode if we don't unlock.
  old_ms_lock.unlock();

  return Status::OK();

  // TODO: explicitly track iterators somehow so that a slow
  // memstore reader can't hold on to too much memory in the tablet.
}

void Tablet::SetCompactionHooksForTests(
  const shared_ptr<Tablet::CompactionFaultHooks> &hooks) {
  compaction_hooks_ = hooks;
}

void Tablet::SetFlushHooksForTests(
  const shared_ptr<Tablet::FlushFaultHooks> &hooks) {
  flush_hooks_ = hooks;
}

static bool CompareBySize(const shared_ptr<LayerInterface> &a,
                          const shared_ptr<LayerInterface> &b) {
  return a->EstimateOnDiskSize() < b->EstimateOnDiskSize();
}

Status Tablet::PickLayersToCompact(
  LayerVector *out_layers,
  LockVector *out_locks) const
{
  boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());
  CHECK_EQ(out_layers->size(), 0);

  vector<shared_ptr<LayerInterface> > tmp_layers;
  tmp_layers.assign(layers_.begin(), layers_.end());

  // Sort the layers by their on-disk size
  std::sort(tmp_layers.begin(), tmp_layers.end(), CompareBySize);
  uint64_t accumulated_size = 0;
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, tmp_layers) {
    uint64_t this_size = l->EstimateOnDiskSize();
    if (out_layers->size() < 2 || this_size < accumulated_size * 2) {
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
      out_locks->push_back(lock);
      out_layers->push_back(l);
      accumulated_size += this_size;
    } else {
      break;
    }
  }

  return Status::OK();
}


Status Tablet::Compact()
{
  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PreCompaction());

  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + ".compact-tmp";

  Schema keys_only = schema_.CreateKeyProjection();

  LOG(INFO) << "Compaction: entering stage 1 (collecting layers)";

  // Step 1. Capture the layers to be merged
  LayerVector input_layers;
  LockVector compact_locks;
  RETURN_NOT_OK(PickLayersToCompact(&input_layers, &compact_locks));
  if (input_layers.size() < 2) {
    LOG(INFO) << "Not enough layers to run compaction! Aborting...";
    return Status::OK();
  }

  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostSelectIterators());

  // Dump the selected layers to the log, and collect corresponding iterators.
  vector<shared_ptr<RowwiseIterator> > key_iters;
  LOG(INFO) << "Selected " << input_layers.size() << " layers to compact:";
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, input_layers) {
    shared_ptr<RowwiseIterator> row_it(l->NewRowIterator(keys_only));

    LOG(INFO) << l->ToString() << "(" << l->EstimateOnDiskSize() << " bytes)";
    key_iters.push_back(row_it);
  }

  // Step 2. Merge their key columns.
  LOG(INFO) << "Compaction: entering stage 2 (compacting keys)";

  MergeIterator merge_keys(keys_only, key_iters);
  RETURN_NOT_OK(merge_keys.Init(NULL));

  LayerWriter out(env_, schema_, tmp_layer_dir, bloom_sizing());
  RETURN_NOT_OK(out.Open());
  RETURN_NOT_OK(out.FlushProjection(keys_only, &merge_keys, true, true));

  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostMergeKeys());
  
  // Step 3. Swap in the UpdateDuplicatingLayer
  shared_ptr<CompactionInProgressLayer> inprogress_layer;
  RETURN_NOT_OK(CompactionInProgressLayer::Open(
                  env_, schema_, tmp_layer_dir, input_layers,
                  &inprogress_layer));
  AtomicSwapLayers(input_layers, inprogress_layer);

  // Step 4. Merge non-keys
  LOG(INFO) << "Compaction: entering stage 4 (compacting other columns)";

  vector<shared_ptr<RowwiseIterator> > full_iters;
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, input_layers) {
    shared_ptr<RowwiseIterator> row_it(l->NewRowIterator(schema_));
    VLOG(2) << "adding " << row_it->ToString() << " for compaction stage 2";
    full_iters.push_back(row_it);
  }

  MergeIterator merge_full(schema_, full_iters);
  RETURN_NOT_OK(merge_full.Init(NULL));
  Schema non_keys = schema_.CreateNonKeyProjection();
  RETURN_NOT_OK(out.FlushProjection(non_keys, &merge_full, true, false));
  RETURN_NOT_OK(out.Finish());

  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostMergeNonKeys());

  // ------------------------------
  // Flush to tmp was successful. Rename it to its real location.

  RETURN_NOT_OK(env_->RenameFile(tmp_layer_dir, new_layer_dir));
  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostRenameFile());

  LOG(INFO) << "Successfully compacted " << out.written_count() << " rows";

  // Open it.
  shared_ptr<Layer> new_layer;
  RETURN_NOT_OK(Layer::Open(env_, schema_, new_layer_dir, &new_layer));
  new_layer->set_delta_tracker(inprogress_layer->delta_tracker_);

  // Replace the compacted layers with the new on-disk layer.
  AtomicSwapLayers(boost::assign::list_of(inprogress_layer),
                   new_layer);

  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostSwapNewLayer());

  // Remove old layers
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l_input, input_layers) {
    LOG(INFO) << "Removing compaction input layer " << l_input->ToString();
    RETURN_NOT_OK(l_input->Delete());
  }

  if (compaction_hooks_) RETURN_NOT_OK(compaction_hooks_->PostCompaction());

  return Status::OK();

}

Status Tablet::CaptureConsistentIterators(
  const Schema &projection,
  vector<shared_ptr<RowwiseIterator> > *iters) const
{
  boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<shared_ptr<RowwiseIterator> > ret;

  // Grab the memstore iterator.
  shared_ptr<RowwiseIterator> ms_iter(memstore_->NewRowIterator(projection));
  ret.push_back(ms_iter);

  // Grab all layer iterators.
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, layers_) {
    shared_ptr<RowwiseIterator> row_it(l->NewRowIterator(projection));
    ret.push_back(row_it);
  }

  // Swap results into the parameters.
  ret.swap(*iters);
  return Status::OK();
}

Status Tablet::CountRows(size_t *count) const {
  // First grab a consistent view of the components of the tablet.
  shared_ptr<MemStore> memstore;
  vector<shared_ptr<LayerInterface> > layers;
  {
    boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());
    memstore = memstore_;
    layers = layers_;
  }

  // Now sum up the counts.
  *count = memstore->entry_count();

  BOOST_FOREACH(const shared_ptr<LayerInterface> &layer, layers) {
    size_t l_count;
    RETURN_NOT_OK(layer->CountRows(&l_count));
    *count += l_count;
  }

  return Status::OK();
}

size_t Tablet::num_layers() const {
  boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());
  return layers_.size();
}

} // namespace table
} // namespace kudu
