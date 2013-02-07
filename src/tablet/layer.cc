// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <tr1/memory>
#include <vector>

#include "common/iterator.h"
#include "common/schema.h"
#include "cfile/bloomfile.h"
#include "cfile/cfile.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/deltafile.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/status.h"

namespace kudu { namespace tablet {

using cfile::CFileReader;
using cfile::ReaderOptions;
using std::auto_ptr;
using std::string;
using std::tr1::shared_ptr;

static const char *kDeltaPrefix = "delta_";
static const char *kColumnPrefix = "col_";
static const char *kBloomFileName = "bloom";

// Return the path at which the given column's cfile
// is stored within the layer directory.
string Layer::GetColumnPath(const string &dir,
                            int col_idx) {
  return dir + "/" + kColumnPrefix +
    boost::lexical_cast<string>(col_idx);
}

// Return the path at which the given delta file
// is stored within the layer directory.
string Layer::GetDeltaPath(const string &dir,
                           int delta_idx) {
  return dir + "/" + kDeltaPrefix +
    boost::lexical_cast<string>(delta_idx);
}

// Return the path at which the bloom filter
// is stored within the layer directory.
string Layer::GetBloomPath(const string &dir) {
  return dir + "/" + kBloomFileName;
}

// Utility class for pulling batches of rows from an iterator, storing
// the resulting data in local scope.
//
// This is probably useful more generally, but for now just lives here
// for use within the various Flush calls.
class ScopedBatchReader {
public:
  ScopedBatchReader(RowIteratorInterface *src_iter,
                    bool need_arena) :
    iter_(src_iter)
  {
    if (need_arena) {
      arena_.reset(new Arena(16*1024, 256*1024));
    }

    int buf_size = sizeof(buf_);
    batch_size_ = buf_size / iter_->schema().byte_size();

    // TODO: handle big rows
    CHECK_GE(batch_size_, 1) << "could not fit a row from schema: "
                             << src_iter->schema().ToString()
                             << " in " << buf_size << " bytes";

    block_.reset(new RowBlock(iter_->schema(), &buf_[0],
                              batch_size_, arena_.get()));
  }

  bool HasNext() {
    return iter_->HasNext();
  }

  Status NextBatch(size_t *nrows) {
    if (arena_ != NULL) arena_->Reset();

    *nrows = batch_size_;
    return iter_->CopyNextRows(nrows, block_.get());
  }

  void *col_ptr(size_t col_idx) {
    return &buf_[iter_->schema().column_offset(col_idx)];
  }

  uint8_t *row_ptr(size_t row_idx) {
    return block_->row_ptr(row_idx);
  }

private:
  RowIteratorInterface *iter_;
  scoped_ptr<Arena> arena_;
  scoped_ptr<RowBlock> block_;

  // Use a small buffer here -- otherwise we end up with larger-than-requested
  // blocks in the CFile. This could be considered a bug in the CFile writer.
  // If flush performance is bad, could consider fetching in larger batches.
  // TODO: look at above - puts a minimum on real block size
  uint8_t buf_[32768];
  size_t batch_size_;
};

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

    string path = Layer::GetColumnPath(dir_, i);

    // Open file for write.
    shared_ptr<WritableFile> out;
    Status s = env_util::OpenFileForWrite(env_, path, &out);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to open output file for column " <<
        col.ToString() << " at path " << path << ": " << 
        s.ToString();
      return s;
    }

    // Create the CFile writer itself.
    std::auto_ptr<cfile::Writer> writer(new cfile::Writer(
                                          opts,
                                          col.type_info().type(),
                                          cfile::GetDefaultEncoding(col.type_info().type()),
                                          out));

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

Status LayerWriter::InitBloomFileWriter(scoped_ptr<BloomFileWriter> *bfw) const {
  string path(Layer::GetBloomPath(dir_));
  shared_ptr<WritableFile> file;
  RETURN_NOT_OK( env_util::OpenFileForWrite(env_, path, &file) );
  bfw->reset(new BloomFileWriter(file, bloom_sizing_));
  return bfw->get()->Start();
}


Status LayerWriter::FlushProjection(const Schema &projection,
                                    RowIteratorInterface *src_iter,
                                    bool need_arena,
                                    bool write_bloom) {
  const Schema &iter_schema = src_iter->schema();

  CHECK(!finished_);
  vector<size_t> orig_projection;
  vector<size_t> iter_projection;
  projection.GetProjectionFrom(schema_, &orig_projection);
  projection.GetProjectionFrom(iter_schema, &iter_projection);

  faststring encoded_key_buf; // for blooms
  scoped_ptr<BloomFileWriter> bfw;
  if (write_bloom) {
    InitBloomFileWriter(&bfw);
  }


  ScopedBatchReader batcher(src_iter, need_arena);

  size_t written = 0;
  while (batcher.HasNext()) {
    // Read a batch from the iterator.
    size_t nrows;
    RETURN_NOT_OK(batcher.NextBatch(&nrows));
    CHECK_GT(nrows, 0);

    // Write the batch to the each of the columns
    for (int proj_col = 0; proj_col < projection.num_columns(); proj_col++) {
      size_t orig_col = orig_projection[proj_col];
      size_t iter_col = iter_projection[proj_col];

      size_t stride = iter_schema.byte_size();
      const void *p = batcher.col_ptr(iter_col);
      RETURN_NOT_OK( cfile_writers_[orig_col].AppendEntries(p, nrows, stride) );
    }

    // Write the batch to the bloom, if applicable
    if (write_bloom) {
      const uint8_t *row = batcher.row_ptr(0);
      for (size_t i = 0; i < nrows; i++) {
        // TODO: performance might be better if we actually batch this -
        // encode a bunch of key slices, then pass them all in one go.

        // Encode the row into sortable form
        encoded_key_buf.clear();
        Slice row_slice(row, iter_schema.byte_size());
        iter_schema.EncodeComparableKey(row_slice, &encoded_key_buf);

        // Insert the encoded row into the bloom.
        Slice encoded_key_slice(encoded_key_buf);
        RETURN_NOT_OK( bfw->AppendKeys(&encoded_key_slice, 1) );

        // Advance.
        row += iter_schema.byte_size();
      }
    }

    written += nrows;
  }

  // Finish columns.
  for (int proj_col = 0; proj_col < projection.num_columns(); proj_col++) {
    size_t orig_col = orig_projection[proj_col];
    CHECK_EQ(column_flushed_counts_[orig_col], 0);
    column_flushed_counts_[orig_col] = written;

    RETURN_NOT_OK(cfile_writers_[orig_col].Finish());
  }

  // Finish bloom.
  if (write_bloom) {
    RETURN_NOT_OK(bfw->Finish());
  }


  return Status::OK();
}

Status LayerWriter::Finish() {
  CHECK(!finished_);
  for (int i = 0; i < schema_.num_columns(); i++) {
    CHECK_EQ(column_flushed_counts_[i], column_flushed_counts_[0])
      << "Uneven flush. Column " << schema_.column(i).ToString() << " didn't match count "
      << "of column " << schema_.column(0).ToString();
    if (!cfile_writers_[i].finished()) {
      RETURN_NOT_OK(cfile_writers_[i].Finish());
    }
  }

  finished_ = true;

  return Status::OK();
}


////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status Layer::Open(Env *env,
                   const Schema &schema,
                   const string &layer_dir,
                   Layer **layer) {
  auto_ptr<Layer> l(new Layer(env, schema, layer_dir));

  RETURN_NOT_OK(l->OpenBaseCFileReaders());
  RETURN_NOT_OK(l->OpenDeltaFileReaders());
  l->open_ = true;

  *layer = l.release();
  return Status::OK();
}



Status Layer::CreatePartiallyFlushed(Env *env,
                                     const Schema &schema,
                                     const string &layer_dir,
                                     shared_ptr<MemStore> &memstore,
                                     Layer **layer) {
  auto_ptr<Layer> l(new Layer(env, schema, layer_dir));

  auto_ptr<KeysFlushedBaseData> lbd(
    new KeysFlushedBaseData(env, layer_dir, schema, memstore));

  RETURN_NOT_OK(lbd->Open());
  l->base_data_.reset(lbd.release());
  l->open_ = true;

  *layer = l.release();
  return Status::OK();
}


// Open the CFileReaders for the "base data" in this layer.
// TODO: rename me
Status Layer::OpenBaseCFileReaders() {
  std::auto_ptr<CFileBaseData> new_base(
    new CFileBaseData(env_, dir_, schema_));
  RETURN_NOT_OK(new_base->Open());

  base_data_.reset(new_base.release());

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

      delta_trackers_.push_back(shared_ptr<DeltaTrackerInterface>(dfr));

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


RowIteratorInterface *Layer::NewRowIterator(const Schema &projection) const {
  CHECK(open_);

  // TODO: locking
  RowIteratorInterface *base_iter = base_data_->NewRowIterator(projection);

  std::vector<shared_ptr<DeltaTrackerInterface> > deltas(delta_trackers_);
  deltas.push_back(dms_);

  return new DeltaMergingIterator(base_iter, deltas, schema_, projection);
}

Status Layer::UpdateRow(const void *key,
                        const RowDelta &update) {
  CHECK(open_);

  // TODO: can probably lock this more fine-grained.
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);

  if (base_data_->is_updatable_in_place()) {
    return base_data_->UpdateRow(key, update);
  } else {
    uint32_t row_idx;
    RETURN_NOT_OK(base_data_->FindRow(key, &row_idx));
    dms_->Update(row_idx, update);
  }

  return Status::OK();
}

Status Layer::CheckRowPresent(const LayerKeyProbe &probe,
                              bool *present) const {
  CHECK(open_);
  return base_data_->CheckRowPresent(probe, present);
}

Status Layer::CountRows(size_t *count) const {
  CHECK(open_);

  return base_data_->CountRows(count);
}

uint64_t Layer::EstimateOnDiskSize() const {
  CHECK(open_);
  // TODO: should probably add the delta trackers as well.
  return base_data_->EstimateOnDiskSize();
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

    delta_trackers_.push_back(old_dms);
  }

  LOG(INFO) << "Flushing " << count << " deltas...";

  // Now, actually flush the contents of the old DMS.
  // TODO: need another lock to prevent concurrent flushers
  // at some point.
  DeltaFileReader *dfr;
  Status s = FlushDMS(*old_dms, &dfr);
  CHECK(s.ok())
    << "Failed to flush DMS: " << s.ToString()
    << "\nTODO: need to figure out what to do with error handling "
    << "if this fails -- we end up with a DeltaMemStore permanently "
    << "in the tracker list. For now, abort.";


  // Now, re-take the lock and swap in the DeltaFileReader in place of
  // of the DeltaMemStore
  {
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);
    size_t idx = delta_trackers_.size() - 1;

    CHECK_EQ(delta_trackers_[idx], old_dms)
      << "Another thread modified the delta tracker list during flush";
    delta_trackers_[idx].reset(dfr);
  }

  return Status::OK();

  // TODO: wherever we write stuff, we should write to a tmp path
  // and rename to final path!
}


Status Layer::Delete() {
  // TODO: actually rm -rf, not just rename!
  return env_->RenameFile(dir_, dir_ + ".deleted");
}

////////////////////////////////////////////////////////////
// Layer Iterators
////////////////////////////////////////////////////////////

Status DeltaMergingIterator::Init() {
  RETURN_NOT_OK(base_iter_->Init());
  RETURN_NOT_OK(
    projection_.GetProjectionFrom(src_schema_, &projection_mapping_));
  return Status::OK();
}

Status DeltaMergingIterator::CopyNextRows(size_t *nrows, RowBlock *dst)
{
  // Get base data
  RETURN_NOT_OK(base_iter_->CopyNextRows(nrows, dst));
  size_t old_cur_row = cur_row_;
  cur_row_ += *nrows;

  if (*nrows == 0) {
    // TODO: does this happen?
    return Status::OK();
  }

  // Apply updates
  BOOST_FOREACH(shared_ptr<DeltaTrackerInterface> &tracker, delta_trackers_) {
    for (size_t proj_col_idx = 0; proj_col_idx < projection_.num_columns(); proj_col_idx++) {
      ColumnBlock dst_col = dst->column_block(proj_col_idx, *nrows);
      size_t src_col_idx = projection_mapping_[proj_col_idx];

      RETURN_NOT_OK(tracker->ApplyUpdates(src_col_idx, old_cur_row, &dst_col));
    }
  }

  return Status::OK();
}

} // namespace tablet
} // namespace kudu
