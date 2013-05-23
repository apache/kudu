// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <tr1/memory>
#include <vector>

#include "common/generic_iterators.h"
#include "common/iterator.h"
#include "common/schema.h"
#include "cfile/bloomfile.h"
#include "cfile/cfile.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/compaction.h"
#include "tablet/diskrowset.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/status.h"

namespace kudu { namespace tablet {

using cfile::CFileReader;
using cfile::ReaderOptions;
using std::string;
using std::tr1::shared_ptr;

const char *DiskRowSet::kDeltaPrefix = "delta_";
const char *DiskRowSet::kColumnPrefix = "col_";
const char *DiskRowSet::kBloomFileName = "bloom";
const char *DiskRowSet::kTmpRowSetSuffix = ".tmp";

// Return the path at which the given column's cfile
// is stored within the rowset directory.
string DiskRowSet::GetColumnPath(const string &dir,
                            int col_idx) {
  return dir + "/" + kColumnPrefix +
    boost::lexical_cast<string>(col_idx);
}

// Return the path at which the given delta file
// is stored within the rowset directory.
string DiskRowSet::GetDeltaPath(const string &dir,
                           int delta_idx) {
  return dir + "/" + kDeltaPrefix +
    boost::lexical_cast<string>(delta_idx);
}

// Return the path at which the bloom filter
// is stored within the rowset directory.
string DiskRowSet::GetBloomPath(const string &dir) {
  return dir + "/" + kBloomFileName;
}

Status DiskRowSetWriter::Open() {
  CHECK(cfile_writers_.empty());

  // Create the directory for the new rowset
  RETURN_NOT_OK(env_->CreateDir(dir_));

  // Open columns.
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

    string path = DiskRowSet::GetColumnPath(dir_, i);

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
    gscoped_ptr<cfile::Writer> writer(new cfile::Writer(
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

  // Open bloom filter.
  RETURN_NOT_OK(InitBloomFileWriter());

  return Status::OK();
}

Status DiskRowSetWriter::InitBloomFileWriter() {
  string path(DiskRowSet::GetBloomPath(dir_));
  shared_ptr<WritableFile> file;
  RETURN_NOT_OK( env_util::OpenFileForWrite(env_, path, &file) );
  bloom_writer_.reset(new BloomFileWriter(file, bloom_sizing_));
  return bloom_writer_->Start();
}

Status DiskRowSetWriter::WriteRow(const Slice &row) {
  CHECK(!finished_);
  DCHECK_EQ(row.size(), schema_.byte_size());


  // TODO(perf): this is a kind of slow implementation since it
  // causes an extra unnecessary copy, and only appends one
  // at a time. Would be nice to change RowBlock so that it can
  // be used in scenarios where it just points to existing memory.
  RowBlock block(schema_, 1, NULL);

  ConstContiguousRow row_slice(schema_, row.data());

  RowBlockRow dst_row = block.row(0);
  dst_row.CopyCellsFrom(schema_, row_slice);

  return AppendBlock(block);
}

Status DiskRowSetWriter::AppendBlock(const RowBlock &block) {
  DCHECK_EQ(block.schema().num_columns(), schema_.num_columns());
  CHECK(!finished_);

  // Write the batch to each of the columns
  for (int i = 0; i < schema_.num_columns(); i++) {
    // TODO: need to look at the selection vector here and only append the
    // selected rows?
    ColumnBlock column = block.column_block(i);
    RETURN_NOT_OK(
      cfile_writers_[i].AppendEntries(column.data(), block.nrows(), column.stride()));
  }

  // Write the batch to the bloom
  for (size_t i = 0; i < block.nrows(); i++) {
    // TODO: performance might be better if we actually batch this -
    // encode a bunch of key slices, then pass them all in one go.
    RowBlockRow row = block.row(i);

    // Encode the row into sortable form
    tmp_buf_.clear();
    schema_.EncodeComparableKey(row, &tmp_buf_);

    // Insert the encoded row into the bloom.
    Slice encoded_key_slice(tmp_buf_);
    RETURN_NOT_OK( bloom_writer_->AppendKeys(&encoded_key_slice, 1) );
  }

  written_count_ += block.nrows();

  return Status::OK();
}

Status DiskRowSetWriter::Finish() {
  CHECK(!finished_);
  for (int i = 0; i < schema_.num_columns(); i++) {
    cfile::Writer &writer = cfile_writers_[i];
    Status s = writer.Finish();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish writer for column " <<
        schema_.column(i).ToString() << ": " << s.ToString();
      return s;
    }
  }

  // Finish bloom.
  Status s = bloom_writer_->Finish();
  if (!s.ok()) {
    LOG(WARNING) << "Unable to Finish bloom filter writer: " << s.ToString();
    return s;
  }

  finished_ = true;

  return Status::OK();
}


////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status DiskRowSet::Open(Env *env,
                   const Schema &schema,
                   const string &rowset_dir,
                   shared_ptr<DiskRowSet> *rowset) {
  shared_ptr<DiskRowSet> rs(new DiskRowSet(env, schema, rowset_dir));

  RETURN_NOT_OK(rs->Open());

  rowset->swap(rs);
  return Status::OK();
}


DiskRowSet::DiskRowSet(Env *env,
             const Schema &schema,
             const string &rowset_dir) :
    env_(env),
    schema_(schema),
    dir_(rowset_dir),
    open_(false),
    delta_tracker_(new DeltaTracker(env, schema, rowset_dir))
{}


Status DiskRowSet::Open() {
  gscoped_ptr<CFileSet> new_base(
    new CFileSet(env_, dir_, schema_));
  RETURN_NOT_OK(new_base->OpenAllColumns());

  base_data_.reset(new_base.release());
  RETURN_NOT_OK(delta_tracker_->Open());

  open_ = true;

  return Status::OK();
}

Status DiskRowSet::FlushDeltas() {
  return delta_tracker_->Flush();
}

RowwiseIterator *DiskRowSet::NewRowIterator(const Schema &projection,
                                       const MvccSnapshot &mvcc_snap) const {
  CHECK(open_);
  //boost::shared_lock<boost::shared_mutex> lock(component_lock_);
  // TODO: need to add back some appropriate locking?

  shared_ptr<ColumnwiseIterator> base_iter(base_data_->NewIterator(projection));
  return new MaterializingIterator(
    shared_ptr<ColumnwiseIterator>(delta_tracker_->WrapIterator(base_iter,
                                                                mvcc_snap)));
}

CompactionInput *DiskRowSet::NewCompactionInput(const MvccSnapshot &snap) const  {
  return CompactionInput::Create(*this, snap);
}

Status DiskRowSet::UpdateRow(txid_t txid,
                        const void *key,
                        const RowChangeList &update) {
  CHECK(open_);

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->FindRow(key, &row_idx));
  delta_tracker_->Update(txid, row_idx, update);

  return Status::OK();
}

Status DiskRowSet::CheckRowPresent(const RowSetKeyProbe &probe,
                              bool *present) const {
  CHECK(open_);

  return base_data_->CheckRowPresent(probe, present);
}

Status DiskRowSet::CountRows(rowid_t *count) const {
  CHECK(open_);

  return base_data_->CountRows(count);
}

uint64_t DiskRowSet::EstimateOnDiskSize() const {
  CHECK(open_);
  // TODO: should probably add the delta trackers as well.
  return base_data_->EstimateOnDiskSize();
}


Status DiskRowSet::Delete() {
  string tmp_path = dir_ + ".deleting";
  RETURN_NOT_OK(env_->RenameFile(dir_, tmp_path));
  return env_->DeleteRecursively(tmp_path);
}

Status DiskRowSet::RenameRowSetDir(const string &new_dir) {
  RETURN_NOT_OK(env_->RenameFile(dir_, new_dir));
  dir_ = new_dir;
  return Status::OK();
}

////////////////////////////////////////

DuplicatingRowSet::DuplicatingRowSet(const vector<shared_ptr<RowSet> > &old_rowsets,
                                   const shared_ptr<RowSet> &new_rowset) :
  old_rowsets_(old_rowsets),
  new_rowset_(new_rowset)
{
  CHECK_GT(old_rowsets_.size(), 0);
  always_locked_.lock();
}

DuplicatingRowSet::~DuplicatingRowSet() {
  always_locked_.unlock();
}

string DuplicatingRowSet::ToString() const {
  string ret;
  ret.append("DuplicatingRowSet([");
  bool first = true;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, old_rowsets_) {
    if (!first) {
      ret.append(", ");
    }
    first = false;
    ret.append(rs->ToString());
  }
  ret.append("] + ");
  ret.append(new_rowset_->ToString());
  ret.append(")");
  return ret;
}

RowwiseIterator *DuplicatingRowSet::NewRowIterator(const Schema &projection,
                                                  const MvccSnapshot &snap) const {
  // Use the original rowset.
  if (old_rowsets_.size() == 1) {
    return old_rowsets_[0]->NewRowIterator(projection, snap);
  } else {
    // Union between them

    vector<shared_ptr<RowwiseIterator> > iters;
    BOOST_FOREACH(const shared_ptr<RowSet> &rowset, old_rowsets_) {
      shared_ptr<RowwiseIterator> iter(rowset->NewRowIterator(projection, snap));
      iters.push_back(iter);
    }

    return new UnionIterator(iters);
  }
}

CompactionInput *DuplicatingRowSet::NewCompactionInput(const MvccSnapshot &snap) const  {
  LOG(FATAL) << "duplicating rowsets do not act as compaction input";
  return NULL;
}


Status DuplicatingRowSet::UpdateRow(txid_t txid,
                                       const void *key,
                                       const RowChangeList &update) {
  ConstContiguousRow row_key(schema(), key);

  // First update the new rowset
  RETURN_NOT_OK(new_rowset_->UpdateRow(txid, key, update));

  // If it succeeded there, we also need to mirror into the old rowset.
  // Duplicate the update to both the relevant input rowset and the output rowset.
  // First propagate to the relevant input rowset.
  bool updated = false;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, old_rowsets_) {
    Status s = rowset->UpdateRow(txid, key, update);
    if (s.ok()) {
      updated = true;
      break;
    } else if (!s.IsNotFound()) {
      LOG(ERROR) << "Unable to update key "
                 << schema().CreateKeyProjection().DebugRow(row_key)
                 << " (failed on rowset " << rowset->ToString() << "): "
                 << s.ToString();
      return s;
    }
  }

  if (!updated) {
    LOG(DFATAL) << "Found row in compaction output but not in any input rowset: "
                << schema().CreateKeyProjection().DebugRow(row_key);
    return Status::NotFound("not found in any input rowset of compaction");
  }

  return Status::OK();
}

Status DuplicatingRowSet::CheckRowPresent(const RowSetKeyProbe &probe,
                              bool *present) const {
  *present = false;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, old_rowsets_) {
    RETURN_NOT_OK(rowset->CheckRowPresent(probe, present));
    if (present) {
      return Status::OK();
    }
  }
  return Status::OK();
}

Status DuplicatingRowSet::CountRows(rowid_t *count) const {
  return new_rowset_->CountRows(count);
}

uint64_t DuplicatingRowSet::EstimateOnDiskSize() const {
  // The actual value of this doesn't matter, since it won't be selected
  // for compaction.
  return new_rowset_->EstimateOnDiskSize();
}

Status DuplicatingRowSet::Delete() {
  LOG(FATAL) << "Unsupported op";
  return Status::NotSupported("");
}

} // namespace tablet
} // namespace kudu
