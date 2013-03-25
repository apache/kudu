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
using std::tr1::shared_ptr;

////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////

static Status OpenReader(Env *env, string dir, size_t col_idx,
                         gscoped_ptr<CFileReader> *new_reader) {
  string path = Layer::GetColumnPath(dir, col_idx);

  // TODO: somehow pass reader options in schema
  ReaderOptions opts;
  return CFileReader::Open(env, path, opts, new_reader);
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

CFileBaseData::~CFileBaseData() {
}


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

    gscoped_ptr<CFileReader> reader;
    RETURN_NOT_OK(OpenReader(env_, dir_, i, &reader));
    readers_[i].reset(reader.release());
    LOG(INFO) << "Successfully opened cfile for column " <<
      schema_.column(i).ToString() << " in " << dir_;;
  }

  return Status::OK();
}


Status CFileBaseData::OpenBloomReader() {
  if (bloom_reader_ != NULL) {
    return Status::OK();
  }

  Status s = BloomFileReader::Open(env_, Layer::GetBloomPath(dir_), &bloom_reader_);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open bloom file in " << dir_ << ": "
                 << s.ToString();
    // Continue without bloom.
  }

  return Status::OK();
}


Status CFileBaseData::NewColumnIterator(size_t col_idx, CFileIterator **iter) const {
  CHECK_LT(col_idx, readers_.size());

  return CHECK_NOTNULL(readers_[col_idx].get())->NewIterator(iter);
}


CFileBaseData::Iterator *CFileBaseData::NewIterator(const Schema &projection) const {
  return new CFileBaseData::Iterator(shared_from_this(), projection);
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
  gscoped_ptr<CFileIterator> key_iter_scoped(key_iter); // free on return

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

Status CFileBaseData::Iterator::Init() {
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

Status CFileBaseData::Iterator::SeekToOrdinal(uint32_t ord_idx) {
  DCHECK(initted_);
  BOOST_FOREACH(CFileIterator &col_iter, col_iters_) {
    RETURN_NOT_OK(col_iter.SeekToOrdinal(ord_idx));
  }

  prepared_ = false;

  return Status::OK();
}

Status CFileBaseData::Iterator::PrepareBatch(size_t *n) {
  size_t n_out = 0;

  bool first = true;
  for (size_t idx = 0; idx < col_iters_.size(); idx++) {
    CFileIterator &col_iter = col_iters_[idx];
    size_t this_col_n = *n;
    Status s = col_iter.PrepareBatch(&this_col_n);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to prepare column " << idx << ": " << s.ToString();
      return s;
    }
    if (first) {
      n_out = this_col_n;
    } else {
      if (this_col_n != n_out) {
        // TODO: better actionable error message indicating which column name and row offset
        // triggered the issue.
        return Status::Corruption(
          StringPrintf("Column %zd had different length %zd compared to prior columns, "
                       "which had length %zd", idx, this_col_n, n_out));
      }
    }
  }

  if (n_out == 0) {
    return Status::NotFound("end of input");
  }

  prepared_ = true;
  *n = n_out;

  return Status::OK();
}

Status CFileBaseData::Iterator::MaterializeColumn(size_t col_idx, ColumnBlock *dst) {
  CHECK(prepared_);
  DCHECK_LT(col_idx, col_iters_.size());

  CFileIterator &iter = col_iters_[col_idx];
  return iter.Scan(dst);
}

Status CFileBaseData::Iterator::FinishBatch() {
  CHECK(prepared_);
  prepared_ = false;

  for (size_t i = 0; i < col_iters_.size(); i++) {
    Status s = col_iters_[i].FinishBatch();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to FinishBatch() on column " << i;
      return s;
    }
  }

  return Status::OK();
}

} // namespace tablet
} // namespace kudu

