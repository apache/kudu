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

      shared_ptr<Layer> layer(new Layer(env_, schema_, absolute_path));
      Status s = layer->Open();
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

  // Lock the component_lock_ in exclusive mode.
  // This shuts out any concurrent readers or writers.
  // TODO: this is a very bad implementation which locks
  // out other accessors for the duration of the flush, which
  // might be significantly long. A better approach would be
  // to do this in multiple phases, and only taking the lock
  // during the transitions between the phases. See 'flushing.txt'
  boost::lock_guard<boost::shared_mutex> lock(component_lock_);

  // swap in a new memstore
  scoped_ptr<MemStore> old_ms(new MemStore(schema_));
  old_ms.swap(memstore_);

  // TODO: there may be outstanding iterators on old_ms here.
  // We need to either make them take a shared_ptr so it gets
  // reference counted, or do something like RCU to free the
  // memstore when the last iterator is closed.
  // TODO: add a unit test which shows this issue (concurrent
  // scanners with flushes)

  if (old_ms->empty()) {
    // flushing empty memstore is a no-op
    LOG(INFO) << "Flush requested on empty memstore";
    return Status::OK();
  }

  uint64_t start_mutate_count = old_ms->debug_mutate_count();

  // TODO: will need to think carefully about handling concurrent
  // updates during the flush process. For initial prototype, ignore
  // this tricky bit.

  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + ".tmp";
  // 1. Flush new layer to temporary directory.

  LayerWriter out(env_, schema_, tmp_layer_dir);
  RETURN_NOT_OK(out.Open());

  scoped_ptr<MemStore::Iterator> iter(old_ms->NewIterator());
  CHECK(iter->HasNext()) << "old memstore yielded invalid iterator";

  // TODO: convert over to the batched API
  int written = 0;
  while (iter->HasNext()) {
    Slice s = iter->GetCurrentRow();
    Status status = out.WriteRow(s);
    if (!status.ok()) {
      LOG(ERROR) << "Unable to write row " << written << " to " <<
        dir_ << ": " << status.ToString();
      return status;
    }
    iter->Next();
    written++;
  }

  RETURN_NOT_OK(out.Finish());

  // Sanity check that no mutations happened during our flush.
  CHECK_EQ(start_mutate_count, old_ms->debug_mutate_count())
    << "Sanity check failed: mutations continued in memstore "
    << "after flush was triggered! Aborting to prevent dataloss.";

  // Flush to tmp was successful. Rename it to its real location.
  RETURN_NOT_OK(env_->RenameFile(tmp_layer_dir, new_layer_dir));

  LOG(INFO) << "Successfully flushed " << written << " rows";

  // Open it.
  shared_ptr<Layer> new_layer(new Layer(env_, schema_, new_layer_dir));
  RETURN_NOT_OK(new_layer->Open());
  layers_.push_back(new_layer);
  return Status::OK();
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
