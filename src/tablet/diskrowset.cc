// Copyright (c) 2012, Cloudera, inc.

#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <tr1/memory>
#include <algorithm>
#include <vector>

#include "common/generic_iterators.h"
#include "common/iterator.h"
#include "common/schema.h"
#include "cfile/bloomfile.h"
#include "cfile/cfile.h"
#include "cfile/type_encodings.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/compaction.h"
#include "tablet/diskrowset.h"
#include "util/status.h"

namespace kudu { namespace tablet {

using cfile::CFileReader;
using cfile::ReaderOptions;
using metadata::RowSetMetadata;
using std::string;
using std::tr1::shared_ptr;

const char *DiskRowSet::kMinKeyMetaEntryName = "min_key";
const char *DiskRowSet::kMaxKeyMetaEntryName = "max_key";

Status DiskRowSetWriter::Open() {
  CHECK(cfile_writers_.empty());

  // Create the metadata for the new rowset
  RETURN_NOT_OK(rowset_metadata_->Create());

  // Open columns.
  for (int i = 0; i < schema_.num_columns(); i++) {
    const ColumnSchema &col = schema_.column(i);

    // TODO: allow options to be configured, perhaps on a per-column
    // basis as part of the schema. For now use defaults.
    //
    // Also would be able to set encoding here, or do something smart
    // to figure out the encoding on the fly.
    cfile::WriterOptions opts;

    // Index all columns by ordinal position, so we can match up
    // the corresponding rows.
    opts.write_posidx = true;

    // If the schema has a single PK and this is the PK col
    if (i == 0 && schema_.num_key_columns() == 1) {
      opts.write_validx = true;
    }

    // Open file for write.
    shared_ptr<WritableFile> data_writer;
    Status s = rowset_metadata_->NewColumnDataBlock(i, &data_writer);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to open output file for column " << col.ToString() << ": "
                   << s.ToString();
      return s;
    }

    // Create the CFile writer itself.
    gscoped_ptr<cfile::Writer> writer(new cfile::Writer(
                                        opts,
                                        col.type_info().type(),
                                        col.is_nullable(),
                                        cfile::TypeEncodingInfo::GetDefaultEncoding(col.type_info().type()),
                                        data_writer));

    s = writer->Start();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Start() writer for column " << col.ToString() << ": "
                   << s.ToString();
      return s;
    }

    LOG(INFO) << "Opened CFile writer for column " << col.ToString();
    cfile_writers_.push_back(writer.release());
  }

  // Open bloom filter.
  RETURN_NOT_OK(InitBloomFileWriter());

  if (schema_.num_key_columns() > 1) {
    // Open ad-hoc index writer
    RETURN_NOT_OK(InitAdHocIndexWriter());
  }

  return Status::OK();
}

Status DiskRowSetWriter::InitBloomFileWriter() {
  shared_ptr<WritableFile> data_writer;
  RETURN_NOT_OK(rowset_metadata_->NewBloomDataBlock(&data_writer));
  bloom_writer_.reset(new BloomFileWriter(data_writer, bloom_sizing_));
  return bloom_writer_->Start();
}

Status DiskRowSetWriter::InitAdHocIndexWriter() {
  shared_ptr<WritableFile> data_writer;
  RETURN_NOT_OK(rowset_metadata_->NewAdHocIndexDataBlock(&data_writer));
  // TODO: allow options to be configured, perhaps on a per-column
  // basis as part of the schema. For now use defaults.
  //
  // Also would be able to set encoding here, or do something smart
  // to figure out the encoding on the fly.
  cfile::WriterOptions opts;

  // Index all columns by value
  opts.write_validx = true;

  // no need to index positions
  opts.write_posidx = false;

  // Create the CFile writer for the ad-hoc index.
  ad_hoc_index_writer_.reset(new cfile::Writer(
      opts,
      STRING,
      false,
      cfile::PREFIX,
      data_writer));
  return ad_hoc_index_writer_->Start();
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

  // If this is the very first block, encode the first key and save it as metadata
  // in the index column.
  if (written_count_ == 0) {
    Slice enc_key = schema_.EncodeComparableKey(block.row(0), &last_encoded_key_);
    key_index_writer()->AddMetadataPair(DiskRowSet::kMinKeyMetaEntryName, enc_key);
  }

  // Write the batch to each of the columns
  for (int i = 0; i < schema_.num_columns(); i++) {
    // TODO: need to look at the selection vector here and only append the
    // selected rows?
    ColumnBlock column = block.column_block(i);
    if (column.is_nullable()) {
      RETURN_NOT_OK(cfile_writers_[i].AppendNullableEntries(column.null_bitmap(),
          column.data(), column.nrows()));
    } else {
      RETURN_NOT_OK(cfile_writers_[i].AppendEntries(column.data(), column.nrows()));
    }
  }

  // Write the batch to the ad hoc index if we're using one
  if (ad_hoc_index_writer_ != NULL) {
    for (int i = 0; i < block.nrows(); i++) {
      // TODO merge this loop with the bloom loop below to
      // avoid re-encoding the keys
      Slice enc_key = schema_.EncodeComparableKey(block.row(i), &last_encoded_key_);
      RETURN_NOT_OK(ad_hoc_index_writer_->AppendEntries(&enc_key, 1));
    }
  }

  // Write the batch to the bloom
  for (size_t i = 0; i < block.nrows(); i++) {
    // TODO: performance might be better if we actually batch this -
    // encode a bunch of key slices, then pass them all in one go.
    RowBlockRow row = block.row(i);
    // Insert the encoded key into the bloom.
    Slice enc_key = schema_.EncodeComparableKey(row, &last_encoded_key_);
    RETURN_NOT_OK( bloom_writer_->AppendKeys(&enc_key, 1) );
  }

  written_count_ += block.nrows();

  return Status::OK();
}

Status DiskRowSetWriter::Finish() {
  CHECK(!finished_);

  // Save the last encoded (max) key
  if (written_count_ > 0) {
    CHECK_GT(last_encoded_key_.size(), 0);
    key_index_writer()->AddMetadataPair(DiskRowSet::kMaxKeyMetaEntryName,
                                        Slice(last_encoded_key_));
  }

  for (int i = 0; i < schema_.num_columns(); i++) {
    cfile::Writer &writer = cfile_writers_[i];
    Status s = writer.Finish();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish writer for column " <<
        schema_.column(i).ToString() << ": " << s.ToString();
      return s;
    }
  }

  if (ad_hoc_index_writer_ != NULL) {
    // Finish bloom.
    Status s = ad_hoc_index_writer_->Finish();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish ad hoc index writer: " << s.ToString();
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

cfile::Writer *DiskRowSetWriter::key_index_writer() {
  return ad_hoc_index_writer_ ? ad_hoc_index_writer_.get() : &cfile_writers_[0];
}

////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status DiskRowSet::Open(const shared_ptr<RowSetMetadata>& rowset_metadata,
                        const Schema &schema,
                        shared_ptr<DiskRowSet> *rowset)
{
  shared_ptr<DiskRowSet> rs(new DiskRowSet(rowset_metadata, schema));

  RETURN_NOT_OK(rs->Open());

  rowset->swap(rs);
  return Status::OK();
}


DiskRowSet::DiskRowSet(const shared_ptr<RowSetMetadata>& rowset_metadata,
                       const Schema &schema) :
    rowset_metadata_(rowset_metadata),
    schema_(schema),
    open_(false)
{}


Status DiskRowSet::Open() {
  gscoped_ptr<CFileSet> new_base(new CFileSet(rowset_metadata_, schema_));
  RETURN_NOT_OK(new_base->Open());

  base_data_.reset(new_base.release());

  rowid_t num_rows;
  RETURN_NOT_OK(base_data_->CountRows(&num_rows));
  delta_tracker_.reset(new DeltaTracker(rowset_metadata_, schema_, num_rows));
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

Status DiskRowSet::MutateRow(txid_t txid,
                             const RowSetKeyProbe &probe,
                             const RowChangeList &update) {
  CHECK(open_);

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->FindRow(probe, &row_idx));

  // It's possible that the row key exists in this DiskRowSet, but it has
  // in fact been Deleted already. Check with the delta tracker to be sure.
  bool deleted;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(row_idx, &deleted));
  if (deleted) {
    return Status::NotFound("row not found");
  }

  delta_tracker_->Update(txid, row_idx, update);

  return Status::OK();
}

Status DiskRowSet::CheckRowPresent(const RowSetKeyProbe &probe,
                              bool *present) const {
  CHECK(open_);

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->CheckRowPresent(probe, present, &row_idx));
  if (!*present) {
    // If it wasn't in the base data, then it's definitely not in the rowset.
    return Status::OK();
  }

  // Otherwise it might be in the base data but deleted.
  bool deleted = false;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(row_idx, &deleted));
  *present = !deleted;
  return Status::OK();
}

Status DiskRowSet::CountRows(rowid_t *count) const {
  CHECK(open_);

  return base_data_->CountRows(count);
}

Status DiskRowSet::GetBounds(Slice *min_encoded_key,
                             Slice *max_encoded_key) const {
  CHECK(open_);
  return base_data_->GetBounds(min_encoded_key, max_encoded_key);
}

uint64_t DiskRowSet::EstimateOnDiskSize() const {
  CHECK(open_);
  // TODO: should probably add the delta trackers as well.
  return base_data_->EstimateOnDiskSize();
}

Status DiskRowSet::DebugDump(vector<string> *lines) {
  // Using CompactionInput to dump our data is an easy way of seeing all the
  // rows and deltas.
  gscoped_ptr<CompactionInput> input(
    NewCompactionInput(MvccSnapshot::CreateSnapshotIncludingAllTransactions()));
  return DebugDumpCompactionInput(input.get(), lines);
}

} // namespace tablet
} // namespace kudu
