// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <tr1/memory>
#include <algorithm>
#include <vector>

#include "cfile/bloomfile.h"
#include "cfile/cfile.h"
#include "cfile/cfile_util.h"
#include "common/scan_spec.h"
#include "gutil/algorithm.h"
#include "tablet/diskrowset.h"
#include "tablet/cfile_set.h"


DEFINE_bool(consult_bloom_filters, true, "Whether to consult bloom filters on row presence checks");

namespace kudu { namespace tablet {

using cfile::ReaderOptions;
using cfile::DefaultColumnValueIterator;
using metadata::RowSetMetadata;
using std::tr1::shared_ptr;

////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////

static Status OpenReader(const shared_ptr<RowSetMetadata>& rowset_metadata, size_t col_idx,
                         gscoped_ptr<CFileReader> *new_reader) {
  uint64_t data_size = 0;
  shared_ptr<RandomAccessFile> data_reader;
  RETURN_NOT_OK(rowset_metadata->OpenColumnDataBlock(col_idx, &data_reader, &data_size));

  // TODO: somehow pass reader options in schema
  ReaderOptions opts;
  return CFileReader::Open(data_reader, data_size, opts, new_reader);
}

////////////////////////////////////////////////////////////
// CFile Base
////////////////////////////////////////////////////////////

CFileSet::CFileSet(const shared_ptr<RowSetMetadata>& rowset_metadata)
  : rowset_metadata_(rowset_metadata) {
}

CFileSet::~CFileSet() {
}


Status CFileSet::Open() {
  RETURN_NOT_OK(OpenBloomReader());

  if (schema().num_key_columns() > 1) {
    RETURN_NOT_OK(OpenAdHocIndexReader());
  }

  readers_.resize(schema().num_columns());
  for (int i = 0; i < schema().num_columns(); i++) {
    if (readers_[i] != NULL) {
      // Already open.
      continue;
    }

    gscoped_ptr<CFileReader> reader;
    RETURN_NOT_OK(OpenReader(rowset_metadata_, i, &reader));
    readers_[i].reset(reader.release());
    LOG(INFO) << "Successfully opened cfile for column "
              << schema().column(i).ToString()
              << " in " << rowset_metadata_->ToString();
  }

  // Determine the upper and lower key bounds for this CFileSet.
  RETURN_NOT_OK(LoadMinMaxKeys());

  return Status::OK();
}

Status CFileSet::OpenAdHocIndexReader() {
  if (ad_hoc_idx_reader_ != NULL) {
    return Status::OK();
  }

  uint64_t data_size = 0;
  shared_ptr<RandomAccessFile> data_reader;
  RETURN_NOT_OK(rowset_metadata_->OpenAdHocIndexDataBlock(&data_reader, &data_size));

  ReaderOptions opts;
  return CFileReader::Open(data_reader, data_size, opts, &ad_hoc_idx_reader_);
}


Status CFileSet::OpenBloomReader() {
  if (bloom_reader_ != NULL) {
    return Status::OK();
  }

  uint64_t data_size = 0;
  shared_ptr<RandomAccessFile> data_reader;
  RETURN_NOT_OK(rowset_metadata_->OpenBloomDataBlock(&data_reader, &data_size));

  Status s = BloomFileReader::Open(data_reader, data_size, &bloom_reader_);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open bloom file in " << rowset_metadata_->ToString() << ": "
                 << s.ToString();
    // Continue without bloom.
  }

  return Status::OK();
}

Status CFileSet::LoadMinMaxKeys() {
  CFileReader *key_reader = key_index_reader();
  if (!key_reader->GetMetadataEntry(DiskRowSet::kMinKeyMetaEntryName, &min_encoded_key_)) {
    return Status::Corruption("No min key found", ToString());
  }
  if (!key_reader->GetMetadataEntry(DiskRowSet::kMaxKeyMetaEntryName, &max_encoded_key_)) {
    return Status::Corruption("No max key found", ToString());
  }
  if (Slice(min_encoded_key_).compare(max_encoded_key_) > 0) {
    return Status::Corruption(StringPrintf("Min key %s > max key %s",
                                           Slice(min_encoded_key_).ToDebugString().c_str(),
                                           Slice(max_encoded_key_).ToDebugString().c_str()),
                              ToString());
  }

  return Status::OK();
}

CFileReader *CFileSet::key_index_reader() {
  return ad_hoc_idx_reader_ ? ad_hoc_idx_reader_.get() : readers_[0].get();
}

Status CFileSet::NewColumnIterator(size_t col_idx, CFileIterator **iter) const {
  CHECK_LT(col_idx, readers_.size());

  return CHECK_NOTNULL(readers_[col_idx].get())->NewIterator(iter);
}

Status CFileSet::NewAdHocIndexIterator(CFileIterator **iter) const {
  return CHECK_NOTNULL(ad_hoc_idx_reader_.get())->NewIterator(iter);
}


CFileSet::Iterator *CFileSet::NewIterator(const Schema &projection) const {
  return new CFileSet::Iterator(shared_from_this(), projection);
}

Status CFileSet::CountRows(rowid_t *count) const {
  const shared_ptr<cfile::CFileReader> &reader = readers_[0];
  return reader->CountRows(count);
}

Status CFileSet::GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const {
  *min_encoded_key = Slice(min_encoded_key_);
  *max_encoded_key = Slice(max_encoded_key_);
  return Status::OK();
}

uint64_t CFileSet::EstimateOnDiskSize() const {
  uint64_t ret = 0;
  BOOST_FOREACH(const shared_ptr<CFileReader> &reader, readers_) {
    ret += reader->file_size();
  }
  return ret;
}

Status CFileSet::FindRow(const RowSetKeyProbe &probe, rowid_t *idx) const {
  if (bloom_reader_ != NULL && FLAGS_consult_bloom_filters) {
    bool present;
    Status s = bloom_reader_->CheckKeyPresent(probe.bloom_probe(), &present);
    if (s.ok() && !present) {
      return Status::NotFound("not present in bloom filter");
    } else if (!s.ok()) {
      LOG(WARNING) << "Unable to query bloom: " << s.ToString()
                   << " (disabling bloom for this rowset from this point forward)";
      const_cast<CFileSet *>(this)->bloom_reader_.reset(NULL);
      // Continue with the slow path
    }
  }

  CFileIterator *key_iter = NULL;
  RETURN_NOT_OK(NewKeyIterator(&key_iter));

  gscoped_ptr<CFileIterator> key_iter_scoped(key_iter); // free on return

  bool exact;
  RETURN_NOT_OK(key_iter->SeekAtOrAfter(probe.encoded_key(), &exact));
  if (!exact) {
    return Status::NotFound("not present in storefile (failed seek)");
  }

  *idx = key_iter->GetCurrentOrdinal();
  return Status::OK();
}

Status CFileSet::CheckRowPresent(const RowSetKeyProbe &probe, bool *present,
                                 rowid_t *rowid) const {

  Status s = FindRow(probe, rowid);
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

Status CFileSet::NewKeyIterator(CFileIterator **key_iter) const {
  if (schema().num_key_columns() > 1) {
    RETURN_NOT_OK(NewAdHocIndexIterator(*&key_iter));
  } else {
    RETURN_NOT_OK(NewColumnIterator(0, *&key_iter));
  }
  return Status::OK();
}



////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////

class CFileSetIteratorProjector {
 public:
  // Used by CFileSet::Iterator::Init() to create the ColumnIterators
  static Status Project(const CFileSet *base_data, const Schema& projection,
                        ptr_vector<ColumnIterator> *col_iters) {
    CFileSetIteratorProjector projector(base_data, projection, col_iters);
    return projector.Run();
  }

 private:
  CFileSetIteratorProjector(const CFileSet *base_data, const Schema& projection,
                            ptr_vector<ColumnIterator> *col_iters)
    : projection_(projection), base_data_(base_data), col_iters_(col_iters) {
  }

  Status Run() {
    return projection_.GetProjectionMapping(base_data_->schema(), this);
  }

 private:
  friend class ::kudu::Schema;

  Status ProjectAdaptedColumn(size_t proj_idx, size_t base_idx) {
    // TODO: Create an iterator to adapt the type of the base data to the one in the projection
    return Status::NotSupported("Column Value Adaptor not implemented");
  }

  Status ProjectBaseColumn(size_t proj_col_idx, size_t base_col_idx) {
    CFileIterator *base_iter;
    RETURN_NOT_OK(base_data_->NewColumnIterator(base_col_idx, &base_iter));
    col_iters_->push_back(base_iter);
    return Status::OK();
  }

  Status ProjectDefaultColumn(size_t proj_col_idx) {
    // Create an iterator with the default column of the projection
    const ColumnSchema& col_schema = projection_.column(proj_col_idx);
    col_iters_->push_back(new DefaultColumnValueIterator(
        col_schema.type_info().type(), col_schema.default_value()));
    return Status::OK();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CFileSetIteratorProjector);

  const Schema& projection_;
  const CFileSet *base_data_;
  ptr_vector<ColumnIterator> *col_iters_;
};


Status CFileSet::Iterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  // Setup Key Iterator
  CFileIterator *tmp;
  RETURN_NOT_OK(base_data_->NewKeyIterator(&tmp));
  key_iter_.reset(tmp);

  // Setup column iterators.
  RETURN_NOT_OK(CFileSetIteratorProjector::Project(base_data_.get(), projection_, &col_iters_));

  // If there is a range predicate on the key column, push that down into an
  // ordinal range.
  RETURN_NOT_OK(PushdownRangeScanPredicate(spec));

  initted_ = true;

  // Don't actually seek -- we'll seek when we first actually read the
  // data.
  cur_idx_ = lower_bound_idx_;
  Unprepare(); // Reset state.
  return Status::OK();
}

Status CFileSet::Iterator::PushdownRangeScanPredicate(ScanSpec *spec) {
  CHECK_GT(row_count_, 0);

  lower_bound_idx_ = 0;
  // since upper_bound_idx is _inclusive_, subtract 1 from row_count
  upper_bound_idx_ = row_count_ - 1;

  if (spec == NULL) {
    // No predicate.
    return Status::OK();
  }

  if (!spec->has_encoded_ranges()) {
    VLOG(1) << "No predicates that can be pushed down!";
    return Status::OK();
  }

  Schema key_schema = base_data_->schema().CreateKeyProjection();
  BOOST_FOREACH(const EncodedKeyRange *range, spec->encoded_ranges()) {
    if (range->has_lower_bound()) {
      bool exact;
      Status s = key_iter_->SeekAtOrAfter(range->lower_bound(), &exact);
      if (s.IsNotFound()) {
        // The lower bound is after the end of the key range.
        // Thus, no rows will pass the predicate, so we set the lower bound
        // to the end of the file.
        lower_bound_idx_ = row_count_;
        return Status::OK();
      }
      RETURN_NOT_OK(s);

      lower_bound_idx_ = std::max(lower_bound_idx_, key_iter_->GetCurrentOrdinal());
      VLOG(1) << "Pushed lower bound value "
              << range->lower_bound().Stringify(key_schema)
              << " as row_idx >= " << lower_bound_idx_;
    }
    if (range->has_upper_bound()) {
      bool exact;
      Status s = key_iter_->SeekAtOrAfter(range->upper_bound(), &exact);
      if (s.IsNotFound()) {
        // The upper bound is after the end of the key range - the existing upper bound
        // at EOF is correct.
      } else {
        RETURN_NOT_OK(s);

        if (exact) {
          upper_bound_idx_ = std::min(upper_bound_idx_,
                                      key_iter_->GetCurrentOrdinal());
        } else {
          upper_bound_idx_ = std::min(upper_bound_idx_,
                                      key_iter_->GetCurrentOrdinal() - 1);
        }

        VLOG(1) << "Pushed upper bound value "
                << range->upper_bound().Stringify(key_schema)
                << " as row_idx <= " << upper_bound_idx_;
        }
    }
  }
  return Status::OK();
}

Status CFileSet::Iterator::SeekToOrdinal(rowid_t ord_idx) {
  DCHECK(initted_);
  if (ord_idx < row_count_) {
    BOOST_FOREACH(ColumnIterator& col_iter, col_iters_) {
      RETURN_NOT_OK(col_iter.SeekToOrdinal(ord_idx));
    }
  } else {
    DCHECK_EQ(ord_idx, row_count_);
    // Seeking CFileIterator to EOF causes a NotFound error here.
    // TODO: consider allowing CFileIterator to seek exactly to EOF.
  }
  cur_idx_ = ord_idx;

  Unprepare();

  return Status::OK();
}


void CFileSet::Iterator::Unprepare() {
  prepared_count_ = 0;
  cols_prepared_.assign(col_iters_.size(), false);
}

Status CFileSet::Iterator::PrepareBatch(size_t *n) {
  DCHECK_EQ(prepared_count_, 0) << "Already prepared";

  size_t remaining = upper_bound_idx_ + 1 - cur_idx_;
  if (*n > remaining) {
    *n = remaining;
  }

  prepared_count_ = *n;

  // Lazily prepare the first column when it is materialized.
  return Status::OK();
}


Status CFileSet::Iterator::PrepareColumn(size_t idx) {
  if (cols_prepared_[idx]) {
    // Already prepared in this batch.
    return Status::OK();
  }

  ColumnIterator& col_iter = col_iters_[idx];
  size_t n = prepared_count_;

  if (!col_iter.seeked() || col_iter.GetCurrentOrdinal() != cur_idx_) {
    // Either this column has not yet been accessed, or it was accessed
    // but then skipped in a prior block (e.g because predicates on other
    // columns completely eliminated the block).
    //
    // Either way, we need to seek it to the correct offset.
    RETURN_NOT_OK(col_iter.SeekToOrdinal(cur_idx_));
  }

  Status s = col_iter.PrepareBatch(&n);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to prepare column " << idx << ": " << s.ToString();
    return s;
  }

  if (n != prepared_count_) {
    return Status::Corruption(
      StringPrintf("Column %zd (%s) didn't yield enough rows at offset %zd: expected "
                   "%zd but only got %zd", idx, projection_.column(idx).ToString().c_str(),
                   cur_idx_, prepared_count_, n));
  }

  cols_prepared_[idx] = true;

  return Status::OK();
}

Status CFileSet::Iterator::InitializeSelectionVector(SelectionVector *sel_vec) {
  sel_vec->SetAllTrue();
  return Status::OK();
}

Status CFileSet::Iterator::MaterializeColumn(size_t col_idx, ColumnBlock *dst) {
  CHECK_EQ(prepared_count_, dst->nrows());
  DCHECK_LT(col_idx, col_iters_.size());

  RETURN_NOT_OK(PrepareColumn(col_idx));
  ColumnIterator& iter = col_iters_[col_idx];
  return iter.Scan(dst);
}

Status CFileSet::Iterator::FinishBatch() {
  CHECK_GT(prepared_count_, 0);

  for (size_t i = 0; i < col_iters_.size(); i++) {
    if (cols_prepared_[i]) {
      Status s = col_iters_[i].FinishBatch();
      if (!s.ok()) {
        LOG(WARNING) << "Unable to FinishBatch() on column " << i;
        return s;
      }
    }
  }

  cur_idx_ += prepared_count_;
  Unprepare();

  return Status::OK();
}


void CFileSet::Iterator::GetIOStatistics(vector<ColumnIterator::IOStatistics> *stats) {
  stats->clear();
  stats->reserve(col_iters_.size());
  BOOST_FOREACH(const ColumnIterator& iter, col_iters_) {
    stats->push_back(iter.io_statistics());
  }
}

} // namespace tablet
} // namespace kudu

