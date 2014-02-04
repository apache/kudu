// Copyright (c) 2013, Cloudera, inc.

#include "benchmarks/tpch/local_line_item_dao.h"

#include "common/partial_row.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "tablet/tablet.h"
#include "benchmarks/tpch/tpch-schemas.h"

namespace kudu {

using metadata::QuorumPB;
using metadata::TabletMasterBlockPB;
using metadata::TabletMetadata;

void LocalLineItemDAO::Init() {
  // Hard-coded master block
  TabletMasterBlockPB master_block;
  master_block.set_tablet_id("tpch1");
  master_block.set_block_a("9865b0f142ed4d1aaa7dac6eddf281e4");
  master_block.set_block_b("b0f65c47c2a84dcf9ec4e95dd63f4393");

  // Try to load it. If it was not found, create a new one.
  Schema s = SchemaBuilder(tpch::CreateLineItemSchema()).Build();
  gscoped_ptr<kudu::metadata::TabletMetadata> metadata;

  QuorumPB quorum;
  CHECK_OK(TabletMetadata::LoadOrCreate(&fs_manager_,
                                        master_block,
                                        "tpch1",
                                        s,
                                        quorum,
                                        "",
                                        "",
                                        &metadata));

  tablet_.reset(new tablet::Tablet(metadata.Pass()));
  CHECK_OK(tablet_->Open());
}

void LocalLineItemDAO::WriteLine(const PartialRow& row) {
  CHECK_OK(tablet_->Insert(&tx_ctx_, row.as_contiguous_row()));
  tx_ctx_.Reset();
}

void LocalLineItemDAO::MutateLine(const ConstContiguousRow &row, const faststring &mutations) {
  // Call MutateRow with context, rb, schema and a list of changes such as:
  // RowChangeListEncoder(schema_, &update_buf).AddColumnUpdate(col_idx, &new_val);
  // CHECK_OK(tablet_->MutateRow(&dummy, rb.row(), schema_, RowChangeList(update_buf)));
}

void LocalLineItemDAO::FinishWriting() {
  CHECK_OK(tablet_->Flush());
}

void LocalLineItemDAO::OpenScanner(const Schema &query_schema, ScanSpec *spec) {
  CHECK_OK(tablet_->NewRowIterator(query_schema, &current_iter_));
  CHECK_OK(current_iter_->Init(spec));
}

bool LocalLineItemDAO::HasMore() {
  return current_iter_->HasNext();
}

void LocalLineItemDAO::GetNext(RowBlock *block) {
  CHECK_OK(RowwiseIterator::CopyBlock(current_iter_.get(), block));
}

bool LocalLineItemDAO::IsTableEmpty() {
  return tablet_->num_rowsets() == 0;
}

LocalLineItemDAO::~LocalLineItemDAO() {

}

} // namespace kudu
