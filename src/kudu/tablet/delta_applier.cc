// Copyright (c) 2014, Cloudera, inc.

#include "kudu/tablet/delta_applier.h"

#include <string>
#include <vector>

#include "kudu/common/iterator.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace tablet {

  // Construct. The base_iter and delta_iter should not be Initted.
DeltaApplier::DeltaApplier(const shared_ptr<ColumnwiseIterator>& base_iter,
                           const shared_ptr<DeltaIterator>& delta_iter)
  : base_iter_(base_iter),
    delta_iter_(delta_iter) {
}

DeltaApplier::~DeltaApplier() {
}

Status DeltaApplier::Init(ScanSpec *spec) {
  RETURN_NOT_OK(base_iter_->Init(spec));
  RETURN_NOT_OK(delta_iter_->Init());
  RETURN_NOT_OK(delta_iter_->SeekToOrdinal(0));
  return Status::OK();
}


string DeltaApplier::ToString() const {
  string s;
  s.append("DeltaApplier(");
  s.append(base_iter_->ToString());
  s.append(" + ");
  s.append(delta_iter_->ToString());
  s.append(")");
  return s;
}

const Schema &DeltaApplier::schema() const {
  return base_iter_->schema();
}

void DeltaApplier::GetIteratorStats(std::vector<IteratorStats>* stats) const {
  return base_iter_->GetIteratorStats(stats);
}

bool DeltaApplier::HasNext() const {
  return base_iter_->HasNext();
}

Status DeltaApplier::PrepareBatch(size_t *nrows) {
  RETURN_NOT_OK(base_iter_->PrepareBatch(nrows));
  if (*nrows == 0) {
    return Status::NotFound("no more rows left");
  }

  RETURN_NOT_OK(delta_iter_->PrepareBatch(*nrows));
  return Status::OK();
}

Status DeltaApplier::FinishBatch() {
  return base_iter_->FinishBatch();
}

Status DeltaApplier::InitializeSelectionVector(SelectionVector *sel_vec) {
  RETURN_NOT_OK(base_iter_->InitializeSelectionVector(sel_vec));
  return delta_iter_->ApplyDeletes(sel_vec);
}

Status DeltaApplier::MaterializeColumn(size_t col_idx, ColumnBlock *dst) {
  // Copy the base data.
  RETURN_NOT_OK(base_iter_->MaterializeColumn(col_idx, dst));

  // Apply all the updates for this column.
  RETURN_NOT_OK(delta_iter_->ApplyUpdates(col_idx, dst));
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
