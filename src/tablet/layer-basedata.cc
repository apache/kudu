// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <tr1/memory>

#include "cfile/cfile.h"
#include "cfile/seek_flags.h"
#include "tablet/layer.h"
#include "tablet/layer-basedata.h"

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
// KeysFlushed base
////////////////////////////////////////////////////////////

Status KeysFlushedBaseData::Open() {
  CHECK(!open_);

  CFileReader *reader;
  RETURN_NOT_OK(OpenReader(env_, dir_, 0, &reader));
  key_reader_.reset(reader);

  open_ = true;
  return Status::OK();
}

Status KeysFlushedBaseData::FindRow(const void *key, uint32_t *idx) const {
  CHECK(open_);

  CFileIterator *key_iter;
  RETURN_NOT_OK( key_reader_->NewIterator(&key_iter) );
  scoped_ptr<CFileIterator> key_iter_scoped(key_iter); // free on return

  // TODO: check bloom filter, or perhaps check the memstore here as a filter?

  bool exact;
  RETURN_NOT_OK( key_iter->SeekAtOrAfter(key, &exact, 0) );
  if (!exact) {
    return Status::NotFound("not present in storefile (failed seek)");
  }

  *idx = key_iter->GetCurrentOrdinal();
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
  schema_(schema),
  open_(false)
{}


Status CFileBaseData::Open() {
  CHECK(readers_.empty()) << "Should call this only before opening";
  CHECK(!open_);

  for (int i = 0; i < schema_.num_columns(); i++) {
    CFileReader *reader;
    RETURN_NOT_OK(OpenReader(env_, dir_, i, &reader));
    readers_.push_back(reader);
    LOG(INFO) << "Successfully opened cfile for column " <<
      schema_.column(i).ToString() << " in " << dir_;;
  }

  open_ = true;

  return Status::OK();
}


Status CFileBaseData::NewColumnIterator(size_t col_idx, CFileIterator **iter) const {
  CHECK(open_);
  CHECK_LT(col_idx, readers_.size());

  return readers_[col_idx].NewIterator(iter);
}


RowIteratorInterface *CFileBaseData::NewRowIterator(const Schema &projection) const {
  return new CFileBaseData::RowIterator(this, projection);
}

Status CFileBaseData::CountRows(size_t *count) const {
  const cfile::CFileReader &reader = readers_[0];
  return reader.CountRows(count);
}

Status CFileBaseData::FindRow(const void *key, uint32_t *idx) const {
  CFileIterator *key_iter;
  RETURN_NOT_OK( NewColumnIterator(0, &key_iter) );
  scoped_ptr<CFileIterator> key_iter_scoped(key_iter); // free on return

  // TODO: check bloom filter

  bool exact;
  RETURN_NOT_OK( key_iter->SeekAtOrAfter(key, &exact, 0) );
  if (!exact) {
    return Status::NotFound("not present in storefile (failed seek)");
  }

  *idx = key_iter->GetCurrentOrdinal();
  return Status::OK();
}

Status CFileBaseData::CheckRowPresent(const void *key, bool *present) const {
  // TODO: use bloom

  CFileIterator *key_iter;
  RETURN_NOT_OK( NewColumnIterator(0, &key_iter) );
  scoped_ptr<CFileIterator> key_iter_scoped(key_iter); // free on return

  Status s = key_iter->SeekAtOrAfter(key, present,
                                     cfile::SEEK_FORCE_EXACT_MATCH);
  if (s.IsNotFound()) {
    *present = false;
    return Status::OK();
  }
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

