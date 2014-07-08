// Copyright (c) 2013, Cloudera, inc.

#include "benchmarks/tpch/local_line_item_dao.h"

#include "benchmarks/tpch/tpch-schemas.h"
#include "client/schema.h"
#include "common/partial_row.h"
#include "server/logical_clock.h"
#include "tablet/tablet.h"

namespace kudu {

using client::KuduSchema;
using client::KuduColumnRangePredicate;
using metadata::QuorumPB;
using metadata::TabletMasterBlockPB;
using metadata::TabletMetadata;
using std::vector;

LocalLineItemDAO::LocalLineItemDAO(const string &path)
    : fs_manager_(kudu::Env::Default(), path),
      schema_(tpch::CreateLineItemSchema()) {
  Status s = fs_manager_.Open();
  if (s.IsNotFound()) {
    CHECK_OK(fs_manager_.CreateInitialFileSystemLayout());
    CHECK_OK(fs_manager_.Open());
  }
}

void LocalLineItemDAO::Init() {
  // Hard-coded master block
  TabletMasterBlockPB master_block;
  master_block.set_tablet_id("tpch1");
  master_block.set_block_a("9865b0f142ed4d1aaa7dac6eddf281e4");
  master_block.set_block_b("b0f65c47c2a84dcf9ec4e95dd63f4393");

  // Try to load it. If it was not found, create a new one.
  scoped_refptr<kudu::metadata::TabletMetadata> metadata;

  QuorumPB quorum;
  CHECK_OK(TabletMetadata::LoadOrCreate(&fs_manager_,
                                        master_block,
                                        "tpch1",
                                        // Build schema with column ids
                                        SchemaBuilder(*schema_.schema_).Build(),
                                        quorum,
                                        "",
                                        "",
                                        &metadata));

  scoped_refptr<server::Clock> clock(
      server::LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp));
  tablet_.reset(new tablet::Tablet(metadata, clock, NULL,
                                   new log::OpIdAnchorRegistry()));
  CHECK_OK(tablet_->Open());
}

void LocalLineItemDAO::WriteLine(boost::function<void(PartialRow*)> f) {
  PartialRow row(schema_.schema_.get());
  f(&row);
  WriteLine(row);
}

void LocalLineItemDAO::MutateLine(boost::function<void(PartialRow*)> f) {
  PartialRow row(schema_.schema_.get());
  f(&row);
  MutateLine(row);
}

void LocalLineItemDAO::WriteLine(const PartialRow& row) {
  // TODO: This code should use InsertUnlocked().
  ConstContiguousRow ccrow(*row.schema(), row.row_data_);
  CHECK_OK(tablet_->InsertForTesting(&tx_state_, ccrow));
  tx_state_.Reset();
}

void LocalLineItemDAO::MutateLine(const PartialRow& row) {
  LOG(FATAL) << "Updates not implemented on local DAO";
  // Call MutateRow with context, rb, schema and a list of changes such as:
  // RowChangeListEncoder(schema_, &update_buf).AddColumnUpdate(col_idx, &new_val);
  // CHECK_OK(tablet_->MutateRow(&dummy, rb.row(), schema_, RowChangeList(update_buf)));
}

void LocalLineItemDAO::FinishWriting() {
  CHECK_OK(tablet_->Flush());
}

void LocalLineItemDAO::OpenScanner(const KuduSchema& query_schema,
                                   const vector<KuduColumnRangePredicate>& preds) {
  CHECK_OK(tablet_->NewRowIterator(*query_schema.schema_, &current_iter_));
  current_iter_spec_.reset(new ScanSpec());
  BOOST_FOREACH(const KuduColumnRangePredicate& pred, preds) {
    current_iter_spec_->AddPredicate(*pred.pred_);
  }
  CHECK_OK(current_iter_->Init(current_iter_spec_.get()));
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
