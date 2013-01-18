// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <tr1/memory>
#include <vector>

#include "cfile/cfile.h"
#include "common/schema.h"
#include "common/iterator.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/tablet.h"
#include "tablet/layer.h"
#include "util/env.h"

namespace kudu { namespace tablet {

using std::string;
using std::vector;
using std::tr1::shared_ptr;


const char *kLayerPrefix = "layer_";

static string GetLayerPath(const string &tablet_dir,
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

Status Tablet::Insert(const Slice &data) {
  CHECK(open_) << "must Open() first!";

  boost::shared_lock<boost::shared_mutex> lock(component_lock_);

  // First, ensure that it is a unique key by checking all the open
  // Layers
  BOOST_FOREACH(shared_ptr<LayerInterface> &layer, layers_) {
    bool present;
    VLOG(1) << "checking for key in layer " << layer->ToString();
    RETURN_NOT_OK(layer->CheckRowPresent(data.data(), &present));
    if (present) {
      return Status::AlreadyPresent("key already present");
    }
  }

  // Now try to insert into memstore. The memstore itself will return
  // AlreadyPresent if it has already been inserted there.
  return memstore_->Insert(data);
}

Status Tablet::UpdateRow(const void *key,
                         const RowDelta &update) {
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);

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

  uint64_t start_insert_count;

  shared_ptr<MemStore> old_ms(new MemStore(schema_));

  // Step 1. Freeze the old memstore by blocking readers and swapping
  // it in as a new layer.
  {
    // Lock the component_lock_ in exclusive mode.
    // This shuts out any concurrent readers or writers for as long
    // as the swap takes.
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);

    start_insert_count = memstore_->debug_insert_count();

    // swap in a new memstore
    old_ms.swap(memstore_);

    if (old_ms->empty()) {
      // flushing empty memstore is a no-op
      LOG(INFO) << "Flush requested on empty memstore";
      return Status::OK();
    }

    shared_ptr<LayerInterface> old_ms_layer(new MemStoreBaseData(old_ms));
    layers_.push_back(old_ms_layer);
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

  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + ".tmp";


  boost::lock_guard<boost::shared_mutex> lock(component_lock_);

  //Schema keys_only = schema_; // schema_.CreateKeyProjection();
  scoped_ptr<MemStore::Iterator> iter(old_ms->NewIterator(schema_));
  RETURN_NOT_OK(iter->Init());


  // TODO: will need to think carefully about handling concurrent
  // updates during the flush process. For initial prototype, ignore
  // this tricky bit.

  // 1. Flush new layer to temporary directory.

  LayerWriter out(env_, schema_, tmp_layer_dir);
  RETURN_NOT_OK(out.Open());
  RETURN_NOT_OK(out.FlushProjection(schema_, iter.get()));
  RETURN_NOT_OK(out.Finish());

  // Sanity check that no mutations happened during our flush.
  CHECK_EQ(start_insert_count, old_ms->debug_insert_count())
    << "Sanity check failed: mutations continued in memstore "
    << "after flush was triggered! Aborting to prevent dataloss.";

  // Flush to tmp was successful. Rename it to its real location.
  RETURN_NOT_OK(env_->RenameFile(tmp_layer_dir, new_layer_dir));

  LOG(INFO) << "Successfully flushed " << out.written_count() << " rows";

  // Open it.
  Layer *new_layer;
  RETURN_NOT_OK(Layer::Open(env_, schema_, new_layer_dir, &new_layer));

  // Replace the memstore layer with the on-disk layer.
  // Because this is a shared pointer, and iterators hold a shared_ptr
  // to the MemStore as well, the actual memstore wlil get cleaned
  // up when the last iterator is destructed.
  layers_[layers_.size() - 1].reset(new_layer);
  return Status::OK();

  // TODO: explicitly track iterators somehow so that a slow
  // memstore reader can't hold on to too much memory in the tablet.
}

Status Tablet::CaptureConsistentIterators(
  const Schema &projection,
  deque<shared_ptr<RowIteratorInterface> > *iters) const
{
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);

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
    boost::shared_lock<boost::shared_mutex> lock(component_lock_);
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

Status Tablet::RowIterator::CopyNextRows(
  size_t *nrows, uint8_t *dst, Arena *dst_arena)
{

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


  RETURN_NOT_OK(iter->CopyNextRows(nrows, dst, dst_arena));
  if (!iter->HasNext()) {
    // Iterator exhausted, remove it.
    sub_iters_.pop_front();
  }
  return Status::OK();
}


} // namespace table
} // namespace kudu
