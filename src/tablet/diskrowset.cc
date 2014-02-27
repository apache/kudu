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
#include "gutil/stl_util.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "consensus/opid_anchor_registry.h"
#include "tablet/compaction.h"
#include "tablet/diskrowset.h"
#include "tablet/delta_compaction.h"
#include "util/status.h"

namespace kudu { namespace tablet {

using cfile::CFileReader;
using cfile::ReaderOptions;
using log::OpIdAnchorRegistry;
using metadata::RowSetMetadata;
using metadata::RowSetMetadataVector;
using metadata::TabletMetadata;
using metadata::ColumnIndexes;
using std::string;
using std::tr1::shared_ptr;

const char *DiskRowSet::kMinKeyMetaEntryName = "min_key";
const char *DiskRowSet::kMaxKeyMetaEntryName = "max_key";

Status DiskRowSetWriter::Open() {
  CHECK(cfile_writers_.empty());

  // Open columns.
  for (int i = 0; i < schema().num_columns(); i++) {
    const ColumnSchema &col = schema().column(i);

    // TODO: allow options to be configured, perhaps on a per-column
    // basis as part of the schema. For now use defaults.
    //
    // Also would be able to set encoding here, or do something smart
    // to figure out the encoding on the fly.
    cfile::WriterOptions opts;

    // Index all columns by ordinal position, so we can match up
    // the corresponding rows.
    opts.write_posidx = true;

    /// Set the column storage attributes.
    opts.storage_attributes = col.attributes();

    // If the schema has a single PK and this is the PK col
    if (i == 0 && schema().num_key_columns() == 1) {
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

  if (schema().num_key_columns() > 1) {
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

  opts.storage_attributes = ColumnStorageAttributes(PREFIX_ENCODING);

  // Create the CFile writer for the ad-hoc index.
  ad_hoc_index_writer_.reset(new cfile::Writer(
      opts,
      STRING,
      false,
      data_writer));
  return ad_hoc_index_writer_->Start();

}

Status DiskRowSetWriter::AppendBlock(const RowBlock &block) {
  DCHECK_EQ(block.schema().num_columns(), schema().num_columns());
  CHECK(!finished_);

  // If this is the very first block, encode the first key and save it as metadata
  // in the index column.
  if (written_count_ == 0) {
    Slice enc_key = schema().EncodeComparableKey(block.row(0), &last_encoded_key_);
    key_index_writer()->AddMetadataPair(DiskRowSet::kMinKeyMetaEntryName, enc_key);
  }

  // Write the batch to each of the columns
  for (int i = 0; i < schema().num_columns(); i++) {
    // TODO: need to look at the selection vector here and only append the
    // selected rows?
    ColumnBlock column = block.column_block(i);
    if (column.is_nullable()) {
      RETURN_NOT_OK(cfile_writers_[i]->AppendNullableEntries(column.null_bitmap(),
          column.data(), column.nrows()));
    } else {
      RETURN_NOT_OK(cfile_writers_[i]->AppendEntries(column.data(), column.nrows()));
    }
  }

  // Write the batch to the ad hoc index if we're using one
  if (ad_hoc_index_writer_ != NULL) {
    for (int i = 0; i < block.nrows(); i++) {
      // TODO merge this loop with the bloom loop below to
      // avoid re-encoding the keys
      Slice enc_key = schema().EncodeComparableKey(block.row(i), &last_encoded_key_);
      RETURN_NOT_OK(ad_hoc_index_writer_->AppendEntries(&enc_key, 1));
    }
  }

  // Write the batch to the bloom
  for (size_t i = 0; i < block.nrows(); i++) {
    // TODO: performance might be better if we actually batch this -
    // encode a bunch of key slices, then pass them all in one go.
    RowBlockRow row = block.row(i);
    // Insert the encoded key into the bloom.
    Slice enc_key = schema().EncodeComparableKey(row, &last_encoded_key_);
    RETURN_NOT_OK(bloom_writer_->AppendKeys(&enc_key, 1));
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

  for (int i = 0; i < schema().num_columns(); i++) {
    cfile::Writer *writer = cfile_writers_[i];
    Status s = writer->Finish();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish writer for column " <<
        schema().column(i).ToString() << ": " << s.ToString();
      return s;
    }
  }

  if (ad_hoc_index_writer_ != NULL) {
    // Finish ad hoc index.
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
  return ad_hoc_index_writer_ ? ad_hoc_index_writer_.get() : cfile_writers_[0];
}

size_t DiskRowSetWriter::written_size() const {
  size_t size = 0;
  BOOST_FOREACH(const cfile::Writer *writer, cfile_writers_) {
    size += writer->written_size();
  }

  if (bloom_writer_) {
    size += bloom_writer_->written_size();
  }

  if (ad_hoc_index_writer_) {
    size += ad_hoc_index_writer_->written_size();
  }

  return size;
}

DiskRowSetWriter::~DiskRowSetWriter() {
  STLDeleteElements(&cfile_writers_);
}

RollingDiskRowSetWriter::RollingDiskRowSetWriter(TabletMetadata* tablet_metadata,
                                                 const Schema &schema,
                                                 const BloomFilterSizing &bloom_sizing,
                                                 size_t target_rowset_size)
  : state_(kInitialized),
    tablet_metadata_(DCHECK_NOTNULL(tablet_metadata)),
    schema_(schema),
    bloom_sizing_(bloom_sizing),
    target_rowset_size_(target_rowset_size),
    row_idx_in_cur_drs_(0),
    output_index_(0),
    written_count_(0),
    written_size_(0) {
  CHECK(schema.has_column_ids());
}

Status RollingDiskRowSetWriter::Open() {
  CHECK_EQ(state_, kInitialized);

  RETURN_NOT_OK(RollWriter());
  state_ = kStarted;
  return Status::OK();
}

Status RollingDiskRowSetWriter::RollWriter() {
  // Close current writer if it is open
  RETURN_NOT_OK(FinishCurrentWriter());

  RETURN_NOT_OK(tablet_metadata_->CreateRowSet(&cur_drs_metadata_, schema_));

  cur_writer_.reset(new DiskRowSetWriter(cur_drs_metadata_.get(), bloom_sizing_));
  RETURN_NOT_OK(cur_writer_->Open());

  shared_ptr<WritableFile> undo_data_file;
  shared_ptr<WritableFile> redo_data_file;
  RETURN_NOT_OK(cur_drs_metadata_->NewDeltaDataBlock(&undo_data_file, &cur_undo_ds_block_id_));
  RETURN_NOT_OK(cur_drs_metadata_->NewDeltaDataBlock(&redo_data_file, &cur_redo_ds_block_id_));
  cur_undo_writer_.reset(new DeltaFileWriter(schema_, undo_data_file));
  cur_redo_writer_.reset(new DeltaFileWriter(schema_, redo_data_file));
  cur_undo_delta_stats.reset(new DeltaStats(schema_.num_columns()));
  cur_redo_delta_stats.reset(new DeltaStats(schema_.num_columns()));

  row_idx_in_cur_drs_ = 0;

  RETURN_NOT_OK(cur_undo_writer_->Start());
  return cur_redo_writer_->Start();
}

Status RollingDiskRowSetWriter::AppendBlock(const RowBlock &block) {
  DCHECK_EQ(state_, kStarted);
  if (cur_writer_->written_size() > target_rowset_size_) {
    RETURN_NOT_OK(RollWriter());
  }

  RETURN_NOT_OK(cur_writer_->AppendBlock(block));

  written_count_ += block.nrows();

  row_idx_in_cur_drs_ += block.nrows();

  return Status::OK();
}

Status RollingDiskRowSetWriter::AppendUndoDeltas(rowid_t row_idx_in_block,
                                                 Mutation* undo_delta_head,
                                                 rowid_t* row_idx) {
  return AppendDeltas<UNDO>(row_idx_in_block, undo_delta_head,
                            row_idx,
                            cur_undo_writer_.get(),
                            cur_undo_delta_stats.get());
}

Status RollingDiskRowSetWriter::AppendRedoDeltas(rowid_t row_idx_in_block,
                                                 Mutation* redo_delta_head,
                                                 rowid_t* row_idx) {
  return AppendDeltas<REDO>(row_idx_in_block, redo_delta_head,
                            row_idx,
                            cur_redo_writer_.get(),
                            cur_redo_delta_stats.get());
}

template<DeltaType Type>
Status RollingDiskRowSetWriter::AppendDeltas(rowid_t row_idx_in_block,
                                             Mutation* delta_head,
                                             rowid_t* row_idx,
                                             DeltaFileWriter* writer,
                                             DeltaStats* delta_stats) {
  *row_idx = row_idx_in_cur_drs_ + row_idx_in_block;
  for (const Mutation *mut = delta_head; mut != NULL; mut = mut->next()) {
    DeltaKey undo_key(*row_idx, mut->timestamp());
    RETURN_NOT_OK(writer->AppendDelta<Type>(undo_key, mut->changelist()));
    delta_stats->UpdateStats(mut->timestamp(), schema_, mut->changelist());
  }
  return Status::OK();
}

Status RollingDiskRowSetWriter::FinishCurrentWriter() {
  if (!cur_writer_) {
    return Status::OK();
  }
  CHECK_EQ(state_, kStarted);

  cur_undo_writer_->WriteDeltaStats(*cur_undo_delta_stats);
  cur_redo_writer_->WriteDeltaStats(*cur_redo_delta_stats);

  RETURN_NOT_OK(cur_writer_->Finish());
  RETURN_NOT_OK(cur_undo_writer_->Finish());
  RETURN_NOT_OK(cur_redo_writer_->Finish());

  // If the writer is not null _AND_ we've written something to the undo
  // delta store commit the undo delta block.
  if (cur_undo_writer_.get() != NULL &&
      cur_undo_delta_stats->min_timestamp().CompareTo(Timestamp::kMax) != 0) {
    cur_drs_metadata_->CommitUndoDeltaDataBlock(0, cur_undo_ds_block_id_);
  }

  // If the writer is not null _AND_ we've written something to the redo
  // delta store commit the redo delta block.
  if (cur_redo_writer_.get() != NULL &&
      cur_redo_delta_stats->min_timestamp().CompareTo(Timestamp::kMax) != 0) {
    cur_drs_metadata_->CommitRedoDeltaDataBlock(0, cur_redo_ds_block_id_);
  }

  written_size_ += cur_writer_->written_size();

  written_drs_metas_.push_back(cur_drs_metadata_);

  cur_writer_.reset(NULL);
  cur_undo_writer_.reset(NULL);
  cur_redo_writer_.reset(NULL);

  cur_drs_metadata_.reset();

  return Status::OK();
}

Status RollingDiskRowSetWriter::Finish() {
  DCHECK_EQ(state_, kStarted);

  RETURN_NOT_OK(FinishCurrentWriter());
  state_ = kFinished;
  return Status::OK();
}

void RollingDiskRowSetWriter::GetWrittenRowSetMetadata(RowSetMetadataVector* metas) const {
  CHECK_EQ(state_, kFinished);
  metas->assign(written_drs_metas_.begin(), written_drs_metas_.end());
}

RollingDiskRowSetWriter::~RollingDiskRowSetWriter() {
}

////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status DiskRowSet::Open(const shared_ptr<RowSetMetadata>& rowset_metadata,
                        log::OpIdAnchorRegistry* opid_anchor_registry,
                        shared_ptr<DiskRowSet> *rowset) {
  shared_ptr<DiskRowSet> rs(new DiskRowSet(rowset_metadata, opid_anchor_registry));

  RETURN_NOT_OK(rs->Open());

  rowset->swap(rs);
  return Status::OK();
}

DiskRowSet::DiskRowSet(const shared_ptr<RowSetMetadata>& rowset_metadata,
                       OpIdAnchorRegistry* opid_anchor_registry)
  : rowset_metadata_(rowset_metadata),
    open_(false),
    opid_anchor_registry_(opid_anchor_registry) {
}

Status DiskRowSet::Open() {
  gscoped_ptr<CFileSet> new_base(new CFileSet(rowset_metadata_));
  RETURN_NOT_OK(new_base->Open());

  base_data_.reset(new_base.release());

  rowid_t num_rows;
  RETURN_NOT_OK(base_data_->CountRows(&num_rows));
  delta_tracker_.reset(new DeltaTracker(rowset_metadata_, schema(), num_rows,
                                        opid_anchor_registry_));
  RETURN_NOT_OK(delta_tracker_->Open());

  open_ = true;

  return Status::OK();
}

Status DiskRowSet::FlushDeltas() {
  return delta_tracker_->Flush();
}

Status DiskRowSet::MinorCompactDeltaStores() {
  return delta_tracker_->Compact();
}

MajorDeltaCompaction* DiskRowSet::NewMajorDeltaCompaction(RowSetColumnUpdater* updater,
                                                          int64_t* last_store_id) const {
  CHECK(open_);

  shared_ptr<DeltaIterator> delta_iter = delta_tracker_->NewDeltaFileIterator(
    &schema(), MvccSnapshot::CreateSnapshotIncludingAllTransactions(), last_store_id);
  return new MajorDeltaCompaction(delta_iter, updater);
}

void DiskRowSet::SetDMSFrom(DiskRowSet* rs) {
  // Concurrency issues are handled by the DeltaTracker::SetDMS
  delta_tracker_->SetDMS(rs->delta_tracker()->dms_);
}

RowwiseIterator *DiskRowSet::NewRowIterator(const Schema *projection,
                                            const MvccSnapshot &mvcc_snap) const {
  CHECK(open_);
  //boost::shared_lock<boost::shared_mutex> lock(component_lock_);
  // TODO: need to add back some appropriate locking?

  shared_ptr<ColumnwiseIterator> base_iter(base_data_->NewIterator(projection));
  return new MaterializingIterator(
    shared_ptr<ColumnwiseIterator>(delta_tracker_->WrapIterator(base_iter,
                                                                mvcc_snap)));
}

CompactionInput *DiskRowSet::NewCompactionInput(const Schema* projection,
                                                const MvccSnapshot &snap) const  {
  return CompactionInput::Create(*this, projection, snap);
}

Status DiskRowSet::MutateRow(Timestamp timestamp,
                             const RowSetKeyProbe &probe,
                             const RowChangeList &update,
                             const consensus::OpId& op_id,
                             ProbeStats* stats,
                             MutationResultPB* result) {
  CHECK(open_);

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->FindRow(probe, &row_idx, stats));

  // It's possible that the row key exists in this DiskRowSet, but it has
  // in fact been Deleted already. Check with the delta tracker to be sure.
  bool deleted;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(row_idx, &deleted, stats));
  if (deleted) {
    return Status::NotFound("row not found");
  }

  RETURN_NOT_OK(delta_tracker_->Update(timestamp, row_idx, update, op_id, result));

  return Status::OK();
}

Status DiskRowSet::CheckRowPresent(const RowSetKeyProbe &probe,
                                   bool* present,
                                   ProbeStats* stats) const {
  CHECK(open_);

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->CheckRowPresent(probe, present, &row_idx, stats));
  if (!*present) {
    // If it wasn't in the base data, then it's definitely not in the rowset.
    return Status::OK();
  }

  // Otherwise it might be in the base data but deleted.
  bool deleted = false;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(row_idx, &deleted, stats));
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

size_t DiskRowSet::DeltaMemStoreSize() const {
  CHECK(open_);
  return delta_tracker_->DeltaMemStoreSize();
}

size_t DiskRowSet::CountDeltaStores() const {
  CHECK(open_);
  return delta_tracker_->CountRedoDeltaStores();
}

Status DiskRowSet::AlterSchema(const Schema& schema) {
  return delta_tracker_->AlterSchema(schema);
}

Status DiskRowSet::DebugDump(vector<string> *lines) {
  // Using CompactionInput to dump our data is an easy way of seeing all the
  // rows and deltas.
  gscoped_ptr<CompactionInput> input(
    NewCompactionInput(&schema(), MvccSnapshot::CreateSnapshotIncludingAllTransactions()));
  return DebugDumpCompactionInput(input.get(), lines);
}

} // namespace tablet
} // namespace kudu
