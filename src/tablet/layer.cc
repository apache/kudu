// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <tr1/memory>
#include <vector>

#include "common/schema.h"
#include "cfile/cfile.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/deltafile.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/status.h"

namespace kudu { namespace tablet {

using cfile::CFileReader;
using cfile::ReaderOptions;
using std::auto_ptr;
using std::string;
using std::tr1::shared_ptr;

static const char *kDeltaPrefix = "delta_";
static const char *kColumnPrefix = "col_";

// Return the path at which the given column's cfile
// is stored within the layer directory.
static string GetColumnPath(const string &dir,
                            int col_idx) {
  return dir + "/" + kColumnPrefix +
    boost::lexical_cast<string>(col_idx);
}

// Return the path at which the given delta file
// is stored within the layer directory.
static string GetDeltaPath(const string &dir,
                           int delta_idx) {
  return dir + "/" + kDeltaPrefix +
    boost::lexical_cast<string>(delta_idx);
}

Status LayerWriter::Open() {
  CHECK(cfile_writers_.empty());

  // Create the directory for the new layer
  RETURN_NOT_OK(env_->CreateDir(dir_));


  for (int i = 0; i < schema_.num_columns(); i++) {
    const ColumnSchema &col = schema_.column(i);

    // TODO: allow options to be configured, perhaps on a per-column
    // basis as part of the schema. For now use defaults.
    //
    // Also would be able to set encoding here, or do something smart
    // to figure out the encoding on the fly.
    cfile::WriterOptions opts;

    // Index the key column by its value.
    if (i < schema_.num_key_columns()) {
      opts.write_validx = true;
    }
    // Index all columns by ordinal position, so we can match up
    // the corresponding rows.
    opts.write_posidx = true;

    string path = GetColumnPath(dir_, i);

    // Open file for write.
    WritableFile *out;
    Status s = env_->NewWritableFile(path, &out);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to open output file for column " <<
        col.ToString() << " at path " << path << ": " << 
        s.ToString();
      return s;
    }

    // Construct a shared_ptr so that, if the writer construction
    // fails, we don't leak the file descriptor.
    shared_ptr<WritableFile> out_shared(out);

    // Create the CFile writer itself.
    std::auto_ptr<cfile::Writer> writer(new cfile::Writer(
                                          opts,
                                          col.type_info().type(),
                                          cfile::GetDefaultEncoding(col.type_info().type()),
                                          out_shared));

    s = writer->Start();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Start() writer for column " <<
        col.ToString() << " at path " << path << ": " << 
        s.ToString();
      return s;
    }

    LOG(INFO) << "Opened CFile writer for column " <<
      col.ToString() << " at path " << path;
    cfile_writers_.push_back(writer.release());
  }

  return Status::OK();
}

Status LayerWriter::Finish() {
  for (int i = 0; i < schema_.num_columns(); i++) {
    RETURN_NOT_OK(cfile_writers_[i].Finish());
  }
  return Status::OK();
}


////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status Layer::Open() {
  CHECK(!open_) << "Already open!";
  CHECK(cfile_readers_.empty()) << "Invalid state: should have no readers";

  RETURN_NOT_OK(OpenBaseCFileReaders());
  RETURN_NOT_OK(OpenDeltaFileReaders());

  open_ = true;
  return Status::OK();
}

// Open the CFileReaders for the "base data" in this layer.
Status Layer::OpenBaseCFileReaders() {
  CHECK(cfile_readers_.empty()) << "Should call this only before opening";
  CHECK(!open_);

  // TODO: somehow pass reader options in schema
  ReaderOptions opts;
  for (int i = 0; i < schema_.num_columns(); i++) {
    string path = GetColumnPath(dir_, i);

    RandomAccessFile *raf_ptr;
    Status s = env_->NewRandomAccessFile(path, &raf_ptr);
    if (!s.ok()) {
      LOG(WARNING) << "Could not open cfile at path "
                   << path << ": " << s.ToString();
      return s;
    }
    shared_ptr<RandomAccessFile> raf(raf_ptr);

    uint64_t file_size;
    s = env_->GetFileSize(path, &file_size);
    if (!s.ok()) {
      LOG(WARNING) << "Could not get cfile length at path "
                   << path << ": " << s.ToString();
      return s;
    }

    auto_ptr<CFileReader> reader(
      new CFileReader(opts, raf, file_size));
    s = reader->Init();
    if (!s.ok()) {
      LOG(WARNING) << "Failed to Init() cfile reader for "
                   << path << ": " << s.ToString();
      return s;
    }

    cfile_readers_.push_back(reader.release());
    LOG(INFO) << "Successfully opened cfile for column " <<
      schema_.column(i).ToString() << " at " << path;
  }

  return Status::OK();
}

// Open any previously flushed DeltaFiles in this layer
Status Layer::OpenDeltaFileReaders() {
  CHECK(delta_trackers_.empty()) << "should call before opening any readers";
  CHECK(!open_);

  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(dir_, &children));
  BOOST_FOREACH(const string &child, children) {
    // Skip hidden files (also '.' and '..')
    if (child[0] == '.') continue;

    string absolute_path = env_->JoinPathSegments(dir_, child);

    string suffix;
    if (TryStripPrefixString(child, kDeltaPrefix, &suffix)) {
      // The file should be named 'delta_<N>'. N here is the index
      // of the delta file (indicating the order in which it was flushed).
      uint32_t delta_idx;
      if (!safe_strtou32(suffix.c_str(), &delta_idx)) {
        return Status::IOError(string("Bad delta file: ") + absolute_path);
      }

      DeltaFileReader *dfr;
      Status s = DeltaFileReader::Open(env_, absolute_path, schema_, &dfr);
      if (!s.ok()) {
        LOG(ERROR) << "Failed to open delta file " << absolute_path << ": "
                   << s.ToString();
        return s;
      }
      LOG(INFO) << "Successfully opened delta file " << absolute_path;

      delta_trackers_.push_back(dfr);

      next_delta_idx_ = std::max(next_delta_idx_,
                                 delta_idx + 1);
    } else if (TryStripPrefixString(child, kColumnPrefix, &suffix)) {
      // expected: column data
    } else {
      LOG(WARNING) << "ignoring unknown file: " << absolute_path;
    }
  }

  return Status::OK();
}


Layer::RowIterator *Layer::NewRowIterator(const Schema &projection) const {
  return new RowIterator(this, projection);
}

Status Layer::NewColumnIterator(size_t col_idx, Layer::ColumnIterator **iter) const {
  CHECK(open_);
  CHECK_LT(col_idx, cfile_readers_.size());

  auto_ptr<Layer::ColumnIterator> new_iter(new Layer::ColumnIterator(this, col_idx));
  RETURN_NOT_OK(new_iter->Init());

  *iter = new_iter.release();
  return Status::OK();
}

Status Layer::NewColumnIterator(size_t col_idx, scoped_ptr<Layer::ColumnIterator> *iter) const {
  ColumnIterator *iter_ptr;
  RETURN_NOT_OK(NewColumnIterator(col_idx, &iter_ptr));
  iter->reset(iter_ptr);
  return Status::OK();
}

Status Layer::NewBaseColumnIterator(size_t col_idx, CFileIterator **iter) const {
  CHECK(open_);
  CHECK_LT(col_idx, cfile_readers_.size());

  return cfile_readers_[col_idx].NewIterator(iter);
}

Status Layer::UpdateRow(const void *key,
                        const RowDelta &update) {
  scoped_ptr<CFileIterator> key_iter;
  RETURN_NOT_OK( NewBaseColumnIterator(0, &key_iter) );

  // TODO: check bloom filter

  bool exact;
  RETURN_NOT_OK( key_iter->SeekAtOrAfter(key, &exact) );
  if (!exact) {
    return Status::NotFound("not present in storefile (failed seek)");
  }

  uint32_t row_idx = key_iter->GetCurrentOrdinal();

  VLOG(1) << "updating row " << row_idx;
  {
    boost::shared_lock<boost::shared_mutex> lock(component_lock_);

    dms_->Update(row_idx, update);
  }

  return Status::OK();
}

Status Layer::CheckRowPresent(const void *key,
                              bool *present) {
  scoped_ptr<CFileIterator> key_iter;
  RETURN_NOT_OK( NewBaseColumnIterator(0, &key_iter) );

  // TODO: insert use of bloom filters here

  Status s = key_iter->SeekAtOrAfter(key, present);
  if (s.IsNotFound()) {
    // In the case that the key comes past the end of the file, Seek
    // will return NotFound. In that case, it is OK from this function's
    // point of view - just a non-present key.
    *present = false;
    return Status::OK();
  }
  return s;
}

Status Layer::FlushDMS(const DeltaMemStore &dms,
                       DeltaFileReader **dfr) {
  int delta_idx = next_delta_idx_++;
  string path = GetDeltaPath(dir_, delta_idx);

  // Open file for write.
  WritableFile *out;
  Status s = env_->NewWritableFile(path, &out);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open output file for delta level " <<
      delta_idx << " at path " << path << ": " << 
      s.ToString();
    return s;
  }

  // Construct a shared_ptr so that, if the writer construction
  // fails, we don't leak the file descriptor.
  shared_ptr<WritableFile> out_shared(out);

  DeltaFileWriter dfw(schema_, out_shared);

  s = dfw.Start();
  if (!s.ok()) {
    LOG(WARNING) << "Unable to start delta file writer for path " <<
      path;
    return s;
  }
  RETURN_NOT_OK(dms.FlushToFile(&dfw));
  RETURN_NOT_OK(dfw.Finish());
  LOG(INFO) << "Flushed delta file: " << path;

  // Now re-open for read
  RETURN_NOT_OK(DeltaFileReader::Open(env_, path, schema_, dfr));
  LOG(INFO) << "Reopened delta file for read: " << path;

  return Status::OK();
}

Status Layer::FlushDeltas() {
  // First, swap out the old DeltaMemStore with a new one,
  // and add it to the list of delta trackers to be reflected
  // in reads.
  DeltaMemStore *old_dms;
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

    unique_ptr<DeltaMemStore> old_dms_unique(dms_.release());
    dms_.reset(new DeltaMemStore(schema_));
    old_dms = old_dms_unique.release();

    delta_trackers_.push_back(old_dms);
  }

  LOG(INFO) << "Flushing " << count << " deltas...";

  // Now, actually flush the contents of the old DMS.
  // TODO: need another lock to prevent concurrent flushers
  // at some point.
  DeltaFileReader *dfr;
  Status s = FlushDMS(*old_dms, &dfr);
  CHECK(s.ok())
    << "TODO: need to figure out what to do with error handling "
    << "if this fails -- we end up with a DeltaMemStore permanently "
    << "in the tracker list. For now, abort.";


  // Now, re-take the lock and swap in the DeltaFileReader in place of
  // of the DeltaMemStore
  {
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);
    size_t idx = delta_trackers_.size() - 1;

    CHECK_EQ(&delta_trackers_[idx], old_dms)
      << "Another thread modified the delta tracker list during flush";
    delta_trackers_.replace(idx, dfr);
  }

  return Status::OK();

  // TODO: wherever we write stuff, we should write to a tmp path
  // and rename to final path!
}

////////////////////////////////////////////////////////////
// Layer Iterators
////////////////////////////////////////////////////////////

////////////////////
// Column Iterator
////////////////////

Layer::ColumnIterator::ColumnIterator(const Layer *layer,
                               size_t col_idx) :
  layer_(layer),
  col_idx_(col_idx)
{
  CHECK_NOTNULL(layer);
  CHECK_LT(col_idx, layer_->schema().num_columns());
}

Status Layer::ColumnIterator::Init() {
  CHECK(base_iter_.get() == NULL) << "Already initialized: " << base_iter_;

  RETURN_NOT_OK(layer_->NewBaseColumnIterator(col_idx_, &base_iter_));
  return Status::OK();
}

Status Layer::ColumnIterator::SeekToOrdinal(uint32_t ord_idx) {
  return base_iter_->SeekToOrdinal(ord_idx);
}

Status Layer::ColumnIterator::SeekAtOrAfter(const void *key, bool *exact_match) {
  return base_iter_->SeekAtOrAfter(key, exact_match);
}

uint32_t Layer::ColumnIterator::GetCurrentOrdinal() const {
  return base_iter_->GetCurrentOrdinal();
}

Status Layer::ColumnIterator::CopyNextValues(size_t *n, ColumnBlock *dst) {
  uint32_t start_row = GetCurrentOrdinal();

  // Copy as many rows as possible from the base data.
  RETURN_NOT_OK(base_iter_->CopyNextValues(n, dst));
  size_t base_copied = *n;


  ColumnBlock dst_block_valid = dst->SliceRange(0, base_copied);

  {
    boost::shared_lock<boost::shared_mutex> lock(layer_->component_lock_);
    // Apply deltas from memory.
    layer_->dms_->ApplyUpdates(col_idx_, start_row, &dst_block_valid);

    // Apply updates from all flushed deltas
    // TODO: this causes the delta files to re-seek. this is no good.
    // Instead, we should keep some kind of DeltaBlockIterator with the
    // column iterator, so they iterate "with" each other and maintain the
    // necessary state.
    BOOST_FOREACH(const DeltaTrackerInterface &dt, layer_->delta_trackers_) {
      RETURN_NOT_OK(dt.ApplyUpdates(col_idx_, start_row, &dst_block_valid));
    }
  }

  return Status::OK();
}

bool Layer::ColumnIterator::HasNext() const {
  return base_iter_->HasNext();
}


////////////////////
// Row Iterator
////////////////////
Status Layer::RowIterator::Init() {
  CHECK(!initted_);

  RETURN_NOT_OK(projection_.GetProjectionFrom(
                  reader_->schema(), &projection_mapping_));

  // Setup Key Iterator.

  // Only support single key column for now.
  CHECK_EQ(reader_->schema().num_key_columns(), 1);
  int key_col = 0;

  RETURN_NOT_OK(reader_->NewBaseColumnIterator(key_col, &key_iter_));

  // Setup column iterators.

  for (size_t i = 0; i < projection_.num_columns(); i++) {
    size_t col_in_layer = projection_mapping_[i];

    ColumnIterator *iter;
    RETURN_NOT_OK(reader_->NewColumnIterator(col_in_layer, &iter));
    col_iters_.push_back(iter);
  }

  initted_ = true;
  return Status::OK();
}

Status Layer::RowIterator::CopyNextRows(
  size_t *nrows, char *dst, Arena *dst_arena)
{
  DCHECK(initted_);
  DCHECK(dst) << "null dst";
  DCHECK(dst_arena) << "null dst_arena";

  // Copy the projected columns into 'dst'
  size_t stride = projection_.byte_size();
  int proj_col_idx = 0;
  int fetched_prev_col = -1;

  BOOST_FOREACH(ColumnIterator &col_iter, col_iters_) {
    const TypeInfo &tinfo = projection_.column(proj_col_idx).type_info();
    ColumnBlock dst_block(tinfo,
                          dst + projection_.column_offset(proj_col_idx),
                          stride, *nrows, dst_arena);

    size_t fetched = *nrows;
    RETURN_NOT_OK(col_iter.CopyNextValues(&fetched, &dst_block));

    // Sanity check that all iters match up
    if (proj_col_idx > 0) {
      CHECK(fetched == fetched_prev_col) <<
        "Column " << proj_col_idx << " only fetched "
                  << fetched << " rows whereas the previous "
                  << "columns fetched " << fetched_prev_col;
    }
    fetched_prev_col = fetched;

    if (fetched == 0) {
      DCHECK_EQ(proj_col_idx, 0) << "all columns should end at the same time!";
      return Status::NotFound("end of input");
    }
    proj_col_idx++;
  }

  *nrows = fetched_prev_col;
  return Status::OK();
}


} // namespace tablet
} // namespace kudu
