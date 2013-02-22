// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <tr1/memory>

#include "cfile/bloomfile.h"
#include "cfile/cfile.h"
#include "tablet/layer.h"
#include "tablet/layer-basedata.h"


DEFINE_bool(consult_bloom_filters, true, "Whether to consult bloom filters on row presence checks");

namespace kudu { namespace tablet {

using cfile::ReaderOptions;
using std::auto_ptr;
using std::tr1::shared_ptr;

////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////

static Status OpenReader(Env *env, string dir, size_t col_idx,
                         CFileReader **new_reader) {
  string path = Layer::GetColumnPath(dir, col_idx);

  // TODO: somehow pass reader options in schema
  ReaderOptions opts;

  RandomAccessFile *raf_ptr;
  Status s = env->NewRandomAccessFile(path, &raf_ptr);
  if (!s.ok()) {
    LOG(WARNING) << "Could not open cfile at path "
                 << path << ": " << s.ToString();
    return s;
  }
  shared_ptr<RandomAccessFile> raf(raf_ptr);

  uint64_t file_size;
  s = env->GetFileSize(path, &file_size);
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

  *new_reader = reader.release();
  return Status::OK();
}

////////////////////////////////////////////////////////////
// CFile Base
////////////////////////////////////////////////////////////

CFileBaseData::CFileBaseData(Env *env,
                             const string &dir,
                             const Schema &schema) :
  env_(env),
  dir_(dir),
  schema_(schema)
{}


Status CFileBaseData::OpenAllColumns() {
  return OpenColumns(schema_.num_columns());
}

Status CFileBaseData::OpenKeyColumns() {
  return OpenColumns(schema_.num_key_columns());
}

Status CFileBaseData::OpenColumns(size_t num_cols) {
  CHECK_LE(num_cols, schema_.num_columns());

  RETURN_NOT_OK( OpenBloomReader() );

  readers_.resize(num_cols);

  for (int i = 0; i < num_cols; i++) {
    if (readers_[i] != NULL) {
      // Already open.
      continue;
    }

    CFileReader *reader;
    RETURN_NOT_OK(OpenReader(env_, dir_, i, &reader));
    readers_[i].reset(reader);
    LOG(INFO) << "Successfully opened cfile for column " <<
      schema_.column(i).ToString() << " in " << dir_;;
  }

  return Status::OK();
}


Status CFileBaseData::OpenBloomReader() {
  if (bloom_reader_ != NULL) {
    return Status::OK();
  }

  BloomFileReader *rdr;
  Status s = BloomFileReader::Open(env_, Layer::GetBloomPath(dir_), &rdr);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open bloom file in " << dir_ << ": "
                 << s.ToString();
  } else {
    bloom_reader_.reset(rdr);
  }

  return Status::OK();
}


Status CFileBaseData::NewColumnIterator(size_t col_idx, CFileIterator **iter) const {
  CHECK_LT(col_idx, readers_.size());

  return CHECK_NOTNULL(readers_[col_idx].get())->NewIterator(iter);
}


RowIteratorInterface *CFileBaseData::NewRowIterator(const Schema &projection) const {
  return new CFileBaseData::RowIterator(shared_from_this(), projection);
}

Status CFileBaseData::CountRows(size_t *count) const {
  const shared_ptr<cfile::CFileReader> &reader = readers_[0];
  return reader->CountRows(count);
}

uint64_t CFileBaseData::EstimateOnDiskSize() const {
  uint64_t ret = 0;
  BOOST_FOREACH(const shared_ptr<CFileReader> &reader, readers_) {
    ret += reader->file_size();
  }
  return ret;
}

Status CFileBaseData::FindRow(const void *key, uint32_t *idx) const {
  CFileIterator *key_iter;
  RETURN_NOT_OK( NewColumnIterator(0, &key_iter) );
  scoped_ptr<CFileIterator> key_iter_scoped(key_iter); // free on return

  // TODO: check bloom filter

  bool exact;
  RETURN_NOT_OK( key_iter->SeekAtOrAfter(key, &exact) );
  if (!exact) {
    return Status::NotFound("not present in storefile (failed seek)");
  }

  *idx = key_iter->GetCurrentOrdinal();
  return Status::OK();
}

Status CFileBaseData::CheckRowPresent(const LayerKeyProbe &probe, bool *present) const {
  if (bloom_reader_ != NULL && FLAGS_consult_bloom_filters) {
    Status s = bloom_reader_->CheckKeyPresent(probe.bloom_probe(), present);
    if (s.ok() && !*present) {
      return Status::OK();
    } else if (!s.ok()) {
      LOG(WARNING) << "Unable to query bloom: " << s.ToString()
                   << " (disabling bloom for this layer from this point forward)";
      const_cast<CFileBaseData *>(this)->bloom_reader_.reset(NULL);
      // Continue with the slow path
    }
  }

  uint32_t junk;
  Status s = FindRow(probe.raw_key(), &junk);
  if (s.IsNotFound()) {
    // In the case that the key comes past the end of the file, Seek
    // will return NotFound. In that case, it is OK from this function's
    // point of view - just a non-present key.
    *present = false;
    return Status::OK();
  }
  *present = true;
  return s;
}


////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////

Status CFileBaseData::RowIterator::Init() {
  CHECK(!initted_);

  RETURN_NOT_OK(projection_.GetProjectionFrom(
                  base_data_->schema(), &projection_mapping_));

  // Setup Key Iterator.

  // Only support single key column for now.
  CHECK_EQ(base_data_->schema().num_key_columns(), 1);
  int key_col = 0;

  CFileIterator *tmp;
  RETURN_NOT_OK(base_data_->NewColumnIterator(key_col, &tmp));
  key_iter_.reset(tmp);

  // Setup column iterators.

  for (size_t i = 0; i < projection_.num_columns(); i++) {
    size_t col_in_layer = projection_mapping_[i];

    CFileIterator *iter;
    RETURN_NOT_OK(base_data_->NewColumnIterator(col_in_layer, &iter));
    col_iters_.push_back(iter);
  }

  initted_ = true;

  // TODO: later, Init() will probably take some kind of predicate,
  // which would tell us where to seek to.
  return SeekToOrdinal(0);
}

Status CFileBaseData::RowIterator::SeekToOrdinal(uint32_t ord_idx) {
  DCHECK(initted_);
  BOOST_FOREACH(CFileIterator &col_iter, col_iters_) {
    RETURN_NOT_OK(col_iter.SeekToOrdinal(ord_idx));
  }

  return Status::OK();
}


Status CFileBaseData::RowIterator::CopyNextRows(
  size_t *nrows, RowBlock *dst)
{
  DCHECK(initted_);
  DCHECK(dst) << "null dst";
  DCHECK(dst->arena()) << "null dst_arena";

  // Copy the projected columns into 'dst'
  int proj_col_idx = 0;
  int fetched_prev_col = -1;

  BOOST_FOREACH(CFileIterator &col_iter, col_iters_) {
    ColumnBlock dst_col = dst->column_block(proj_col_idx, *nrows);

    size_t fetched = *nrows;
    RETURN_NOT_OK(col_iter.CopyNextValues(&fetched, &dst_col));

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

