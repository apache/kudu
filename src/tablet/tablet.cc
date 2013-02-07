// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <tr1/memory>
#include <vector>

#include "cfile/cfile.h"
#include "common/iterator.h"
#include "common/merge_iterator.h"
#include "common/schema.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/tablet.h"
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

      Layer *layer;
      Status s = Layer::Open(env_, schema_, absolute_path, &layer);
      if (!s.ok()) {
        LOG(ERROR) << "Failed to open layer " << absolute_path << ": "
                   << s.ToString();
        return s;
      }
      layers_.push_back(shared_ptr<Layer>(layer));

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


Status Tablet::Insert(const Slice &data) {
  CHECK(open_) << "must Open() first!";

  LayerKeyProbe probe(schema_, data.data());

  boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());

  // First, ensure that it is a unique key by checking all the open
  // Layers
  if (FLAGS_tablet_do_dup_key_checks) {
    BOOST_FOREACH(shared_ptr<LayerInterface> &layer, layers_) {
      bool present;
      VLOG(1) << "checking for key in layer " << layer->ToString();
      RETURN_NOT_OK(layer->CheckRowPresent(probe, &present));
      if (present) {
        return Status::AlreadyPresent("key already present");
      }
    }
  }

  // TODO: the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  // Now try to insert into memstore. The memstore itself will return
  // AlreadyPresent if it has already been inserted there.
  return memstore_->Insert(data);
}

Status Tablet::UpdateRow(const void *key,
                         const RowDelta &update) {
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


Status Tablet::Flush() {
  CHECK(open_);

  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + ".tmp";

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

  scoped_ptr<MemStore::Iterator> iter(old_ms->NewIterator(keys_only));
  RETURN_NOT_OK(iter->Init());

  LayerWriter out(env_, schema_, tmp_layer_dir, bloom_sizing());
  RETURN_NOT_OK(out.Open());
  RETURN_NOT_OK(out.FlushProjection(keys_only, iter.get(), false, true));

  // Step 3. Freeze old memstore contents.
  // Because the key column exists on disk, we can create a new layer
  // which does positional updating instead of in-place updating,
  // and swap that in.

  LOG(INFO) << "Flush: entering stage 3 (freezing old memstore from in-place updates)";

  Layer *partially_flushed_layer;
  RETURN_NOT_OK(Layer::CreatePartiallyFlushed(
                  env_, schema_, tmp_layer_dir, old_ms,
                  &partially_flushed_layer));

  // Mark the partially flushed layer as locked, so compactions won't consider it
  // for inclusion.
  boost::mutex::scoped_try_lock partial_layer_lock(
    *partially_flushed_layer->compact_flush_lock());
  CHECK(partial_layer_lock.owns_lock());

  uint64_t start_update_count;
  {
    // Swap it in under the lock.
    boost::lock_guard<percpu_rwlock> lock(component_lock_);
    CHECK_EQ(layers_.back(), old_ms)
      << "Layer components changed during flush";
    layers_.back().reset(partially_flushed_layer);

    // We shouldn't have any more updates after this point.
    start_update_count = old_ms->debug_update_count();
  }

  LOG(INFO) << "Flush: entering stage 4 (flushing the rest of the columns)";

  // Step 4. Flush the non-key columns
  Schema non_keys = schema_.CreateNonKeyProjection();
  iter.reset(old_ms->NewIterator(non_keys));
  RETURN_NOT_OK(iter->Init());
  RETURN_NOT_OK(out.FlushProjection(non_keys, iter.get(), false, false));
  RETURN_NOT_OK(out.Finish());


  // Sanity check that no mutations happened during our flush.
  CHECK_EQ(start_insert_count, old_ms->debug_insert_count())
    << "Sanity check failed: insertions continued in memstore "
    << "after flush was triggered! Aborting to prevent dataloss.";
  CHECK_EQ(start_update_count, old_ms->debug_update_count())
    << "Sanity check failed: updates continued in memstore "
    << "after flush was triggered! Aborting to prevent dataloss.";


  // Flush to tmp was successful. Rename it to its real location.
  RETURN_NOT_OK(env_->RenameFile(tmp_layer_dir, new_layer_dir));

  LOG(INFO) << "Successfully flushed " << out.written_count() << " rows";

  // Open it.
  // TODO: this should actually just swap-out the BaseData, probably - otherwise
  // we'll lose any updates that happened during the flush process.
  // TODO: add a test which verifies no lost updates
  Layer *new_layer;
  RETURN_NOT_OK(Layer::Open(env_, schema_, new_layer_dir, &new_layer));

  // Replace the memstore layer with the on-disk layer.
  // Because this is a shared pointer, and iterators hold a shared_ptr
  // to the MemStore as well, the actual memstore wlil get cleaned
  // up when the last iterator is destructed.
  {
    boost::lock_guard<percpu_rwlock> lock(component_lock_);

    // unlock these now. Although no other thread should ever see them,
    // we trigger pthread assertions in DEBUG mode if we don't unlock.
    old_ms_lock.unlock();
    partial_layer_lock.unlock();
    layers_.back().reset(new_layer);
  }
  return Status::OK();

  // TODO: explicitly track iterators somehow so that a slow
  // memstore reader can't hold on to too much memory in the tablet.
}

static bool CompareBySize(const shared_ptr<LayerInterface> &a,
                          const shared_ptr<LayerInterface> &b) {
  return a->EstimateOnDiskSize() < b->EstimateOnDiskSize();
}

Status Tablet::Compact()
{
  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + ".compact-tmp";

  vector<shared_ptr<RowIteratorInterface> > key_iters;
  vector<shared_ptr<LayerInterface> > tmp_layers;
  vector<shared_ptr<LayerInterface> > input_layers;

  Schema keys_only = schema_.CreateKeyProjection();

  LOG(INFO) << "Compaction: entering stage 1 (collecting layers)";

  uint64_t accumulated_size = 0;
  vector<shared_ptr<boost::mutex::scoped_try_lock> > compact_locks;

  // Step 1. Capture the layers to be merged
  {
    boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());

    tmp_layers.assign(layers_.begin(), layers_.end());

    // Sort the layers by their on-disk size
    std::sort(tmp_layers.begin(), tmp_layers.end(), CompareBySize);

    BOOST_FOREACH(const shared_ptr<LayerInterface> &l, tmp_layers) {
      uint64_t this_size = l->EstimateOnDiskSize();
      if (input_layers.size() < 2 || this_size < accumulated_size * 2) {
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
        compact_locks.push_back(lock);
        accumulated_size += this_size;
        input_layers.push_back(l);
      } else {
        break;
      }
    }
  }

  tmp_layers.clear();

  if (input_layers.size() < 2) {
    LOG(INFO) << "Not enough layers to run compaction! Aborting...";
    return Status::OK();
  }

  LOG(INFO) << "Selected " << input_layers.size() << " layers to compact"
            << " (" << accumulated_size << " bytes):";

  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, input_layers) {
    shared_ptr<RowIteratorInterface> row_it(l->NewRowIterator(keys_only));

    LOG(INFO) << l->ToString() << "(" << l->EstimateOnDiskSize() << " bytes)";
    key_iters.push_back(row_it);
  }


  LOG(INFO) << "Compaction: entering stage 2 (compacting keys)";

  // Step 2. Merge their key columns.
  MergeIterator merge_keys(keys_only, key_iters);
  RETURN_NOT_OK(merge_keys.Init());

  LayerWriter out(env_, schema_, tmp_layer_dir, bloom_sizing());
  RETURN_NOT_OK(out.Open());
  RETURN_NOT_OK(out.FlushProjection(keys_only, &merge_keys, true, true));

  
  // Step 3. Swap in the UpdateDuplicatingLayer

  // ------ TODO ---------

  // Step 4. Merge non-keys
  LOG(INFO) << "Compaction: entering stage 4 (compacting other columns)";

  vector<shared_ptr<RowIteratorInterface> > full_iters;
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, input_layers) {
    shared_ptr<RowIteratorInterface> row_it(l->NewRowIterator(schema_));
    VLOG(2) << "adding " << row_it->ToString() << " for compaction stage 2";
    full_iters.push_back(row_it);
  }

  MergeIterator merge_full(schema_, full_iters);
  RETURN_NOT_OK(merge_full.Init());
  Schema non_keys = schema_.CreateNonKeyProjection();
  RETURN_NOT_OK(out.FlushProjection(non_keys, &merge_full, true, false));

  RETURN_NOT_OK(out.Finish());

  // ------------------------------
  // Flush to tmp was successful. Rename it to its real location.

  RETURN_NOT_OK(env_->RenameFile(tmp_layer_dir, new_layer_dir));

  LOG(INFO) << "Successfully compacted " << out.written_count() << " rows";

  // Open it.
  Layer *new_layer;
  RETURN_NOT_OK(Layer::Open(env_, schema_, new_layer_dir, &new_layer));

  // Replace the compacted layers with the new on-disk layer.
  {
    boost::lock_guard<percpu_rwlock> lock(component_lock_);

    // O(n^2) diff algorithm to collect the set of layers excluding
    // the layers that were included in the compaction
    vector<shared_ptr<LayerInterface> > new_layers;
    new_layers.push_back(shared_ptr<LayerInterface>(new_layer));
    BOOST_FOREACH(const shared_ptr<LayerInterface> &l, layers_) {
      bool was_in_compaction = false;
      BOOST_FOREACH(const shared_ptr<LayerInterface> &l_input, input_layers) {
        if (l_input == l) {
          was_in_compaction = true;
          break;
        }
      }
      if (!was_in_compaction) {
        new_layers.push_back(l);
      }
    }

    layers_.swap(new_layers);
  }

  // Remove old layers
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l_input, input_layers) {
    LOG(INFO) << "Removing compaction input layer " << l_input->ToString();
    RETURN_NOT_OK(l_input->Delete());
  }


  return Status::OK();

}

Status Tablet::CaptureConsistentIterators(
  const Schema &projection,
  deque<shared_ptr<RowIteratorInterface> > *iters) const
{
  boost::lock_guard<simple_spinlock> lock(component_lock_.get_lock());

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  deque<shared_ptr<RowIteratorInterface> > ret;

  // Grab the memstore iterator.
  // TODO: when we add concurrent flush, need to add all snapshot
  // memstore iterators.
  shared_ptr<RowIteratorInterface> ms_iter(
    memstore_->NewIterator(projection));
  RETURN_NOT_OK(ms_iter->Init());
  VLOG(2) << "adding " << ms_iter->ToString();

  ret.push_back(ms_iter);

  // Grab all layer iterators.
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, layers_) {
    shared_ptr<RowIteratorInterface> row_it(l->NewRowIterator(projection));

    // TODO(perf): may be more efficient to _not_ init them, and instead make the
    // caller do a seek. Otherwise we're always seeking down the left side
    // of our b-trees to find the first key, even if we're about to seek
    // somewhere else.
    RETURN_NOT_OK(row_it->Init());
    RETURN_NOT_OK(row_it->SeekToStart());
    VLOG(2) << "adding " << row_it->ToString();
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

////////////////////////////////////////////////////////////
// Tablet::RowIterator
////////////////////////////////////////////////////////////

Tablet::RowIterator::RowIterator(const Tablet &tablet,
                                 const Schema &projection) :
  tablet_(&tablet),
  projection_(projection)
{}

Status Tablet::RowIterator::Init() {
  CHECK(sub_iters_.empty());

  RETURN_NOT_OK(projection_.GetProjectionFrom(
                  tablet_->schema(), &projection_mapping_));

  RETURN_NOT_OK(tablet_->CaptureConsistentIterators(
                  projection_, &sub_iters_));

  return Status::OK();
}

bool Tablet::RowIterator::HasNext() const {
  BOOST_FOREACH(const shared_ptr<RowIteratorInterface> &iter, sub_iters_) {
    if (iter->HasNext()) return true;
  }

  return false;
}

Status Tablet::RowIterator::CopyNextRows(size_t *nrows, RowBlock *dst) {

  while (!sub_iters_.empty() &&
         !sub_iters_.front()->HasNext()) {
    sub_iters_.pop_front();
  }
  if (sub_iters_.empty()) {
    *nrows = 0;
    return Status::OK();
  }

  shared_ptr<RowIteratorInterface> &iter = sub_iters_.front();
  VLOG(1) << "Copying up to " << (*nrows) << " rows from " << iter->ToString();


  RETURN_NOT_OK(iter->CopyNextRows(nrows, dst));
  if (!iter->HasNext()) {
    // Iterator exhausted, remove it.
    sub_iters_.pop_front();
  }
  return Status::OK();
}


} // namespace table
} // namespace kudu
