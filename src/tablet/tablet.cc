// Copyright (c) 2012, Cloudera, inc.


#include <boost/foreach.hpp>
#include <tr1/memory>
#include <vector>

#include "cfile/cfile.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/tablet.h"
#include "tablet/layer.h"
#include "util/env.h"

namespace kudu { namespace tablet {

using boost::ptr_deque;
using std::string;
using std::vector;
using std::tr1::shared_ptr;


const string kLayerPrefix = "layer_";

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
  // TODO: read metadata file, open layer readers for flushed files.
  // TODO: track a state_ variable, ensure tablet is open, etc.

  // for now, just list the children, to make sure the dir exists.
  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(dir_, &children));

  BOOST_FOREACH(const string &child, children) {
    string suffix;
    if (TryStripPrefixString(child, kLayerPrefix, &suffix)) {
      uint32_t layer_idx;
      if (!safe_strtou32(child.c_str(), &layer_idx)) {
        return Status::IOError(string("Bad layer file: ") + child);
      }

      next_layer_idx_ = std::max(next_layer_idx_,
                                 (size_t)layer_idx + 1);

    } else {
      LOG(WARNING) << "ignoring unknown file in " << dir_  << ": " << child;
    }
  }

  open_ = true;

  return Status::OK();
}

Status Tablet::Insert(const Slice &data) {
  CHECK(open_) << "must Open() first!";

  // First, ensure that it is a unique key by checking all the open
  // Layers
  BOOST_FOREACH(Layer &layer, layers_) {
    bool present;
    VLOG(1) << "checking for key in layer " << layer.ToString();
    RETURN_NOT_OK(layer.CheckRowPresent(data.data(), &present));
    if (present) {
      return Status::AlreadyPresent("key already present");
    }
  }

  // Now try to insert into memstore. The memstore itself will return
  // AlreadyPresent if it has already been inserted there.
  // TODO: check concurrency
  return memstore_->Insert(data);
}

Status Tablet::Flush() {
  CHECK(open_);

  // swap in a new memstore
  scoped_ptr<MemStore> old_ms(new MemStore(schema_));
  old_ms.swap(memstore_);

  // TODO: will need to think carefully about handling concurrent
  // updates during the flush process. For initial prototype, ignore
  // this tricky bit.

  string new_layer_dir = GetLayerPath(dir_, next_layer_idx_++);
  string tmp_layer_dir = new_layer_dir + ".tmp";
  // 1. Flush new layer to temporary directory.

  LayerWriter out(env_, schema_, tmp_layer_dir);
  RETURN_NOT_OK(out.Open());

  scoped_ptr<MemStore::Iterator> iter(old_ms->NewIterator());
  CHECK(iter->IsValid()) << "old memstore yielded invalid iterator";

  int written = 0;
  while (iter->IsValid()) {
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


  // Flush to tmp was successful. Rename it to its real location.
  RETURN_NOT_OK(env_->RenameFile(tmp_layer_dir, new_layer_dir));

  // Open it.
  std::auto_ptr<Layer> new_layer(new Layer(env_, schema_, new_layer_dir));
  RETURN_NOT_OK(new_layer->Open());
  layers_.push_back(new_layer.release());
  return Status::OK();
}

Status Tablet::CaptureConsistentIterators(
  const Schema &projection,
  ptr_deque<MemStore::Iterator> *ms_iters,
  ptr_deque<Layer::RowIterator> *layer_iters) const
{
  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  ptr_deque<MemStore::Iterator> ms_ret;
  ptr_deque<Layer::RowIterator> layer_ret;

  // Grab the memstore iterator.
  // TODO: when we add concurrent flush, need to add all snapshot
  // memstore iterators.
  ms_ret.push_back(memstore_->NewIterator());

  // Grab all layer iterators.
  BOOST_FOREACH(const Layer &l, layers_) {
    std::auto_ptr<Layer::RowIterator> row_it(l.NewRowIterator(projection));
    RETURN_NOT_OK(row_it->Init());
    RETURN_NOT_OK(row_it->SeekToOrdinal(0));
    layer_ret.push_back(row_it);
  }

  // Swap results into the parameters.
  ms_ret.swap(*ms_iters);
  layer_ret.swap(*layer_iters);
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
  CHECK(memstore_iters_.empty() && layer_iters_.empty());

  RETURN_NOT_OK(projection_.GetProjectionFrom(
                  tablet_->schema(), &projection_mapping_));

  RETURN_NOT_OK(tablet_->CaptureConsistentIterators(
                  projection_, &memstore_iters_, &layer_iters_));

  return Status::OK();
}

bool Tablet::RowIterator::HasNext() const {
  BOOST_FOREACH(const MemStore::Iterator &iter, memstore_iters_) {
    if (iter.IsValid()) return true;
  }
  BOOST_FOREACH(const Layer::RowIterator &iter, layer_iters_) {
    if (iter.HasNext()) return true;
  }

  return false;
}

Status Tablet::RowIterator::CopyNextRows(
  size_t *nrows, void *dst, Arena *dst_arena)
{
  if (!memstore_iters_.empty()) {
    return CopyNextFromMemStore(nrows, dst, dst_arena);
  } else {
    return CopyNextFromLayers(nrows, dst, dst_arena);
  }
}

Status Tablet::RowIterator::CopyNextFromMemStore(
  size_t *nrows, void *dst_v, Arena *dst_arena)
{
  while (!memstore_iters_.empty() &&
         !memstore_iters_.front().IsValid()) {
    memstore_iters_.pop_front();
  }

  if (memstore_iters_.empty()) {
    *nrows = 0;
    return Status::OK();
  }

  MemStore::Iterator *iter = &(memstore_iters_.front());

  uint8_t *dst = reinterpret_cast<uint8_t *>(CHECK_NOTNULL(dst_v));
  size_t fetched = 0;
  for (size_t i = 0; i < *nrows && iter->IsValid(); i++) {
    // Copy the row into the destination, including projection
    // and relocating slices.
    // TODO: can we share some code here with CopyRowToArena() from row.h
    // or otherwise put this elsewhere?
    Slice s = iter->GetCurrentRow();
    for (size_t proj_col_idx = 0; proj_col_idx < projection_mapping_.size(); proj_col_idx++) {
      size_t src_col_idx = projection_mapping_[proj_col_idx];
      void *dst_cell = dst + projection_.column_offset(proj_col_idx);
      const void *src_cell = s.data() + tablet_->schema().column_offset(src_col_idx);
      RETURN_NOT_OK(projection_.column(proj_col_idx).CopyCell(dst_cell, src_cell, dst_arena));
    }

    // advance to next row
    fetched++;
    dst += projection_.byte_size();
    iter->Next();
  }

  if (!iter->IsValid()) {
    // memstore iter exhausted - remove it
    memstore_iters_.pop_front();
  }

  *nrows = fetched;
  return Status::OK();
}

Status Tablet::RowIterator::CopyNextFromLayers(
  size_t *nrows, void *dst, Arena *dst_arena)
{
  while (!layer_iters_.empty() &&
         !layer_iters_.front().HasNext()) {
    layer_iters_.pop_front();
  }
  if (layer_iters_.empty()) {
    *nrows = 0;
    return Status::OK();
  }

  Layer::RowIterator *iter = &(layer_iters_.front());
  RETURN_NOT_OK(iter->CopyNextRows(
                  nrows, reinterpret_cast<char *>(dst), dst_arena));
  if (!iter->HasNext()) {
    // memstore iter exhausted - remove it
    layer_iters_.pop_front();
  }
  return Status::OK();
}


} // namespace table
} // namespace kudu
