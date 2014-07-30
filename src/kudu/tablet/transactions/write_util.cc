// Copyright (c) 2013, Cloudera, inc.

#include "kudu/tablet/transactions/write_util.h"

#include "kudu/common/row.h"
#include "kudu/tablet/transactions/write_transaction.h"

namespace kudu {
namespace tablet {

// TODO: remove when the old Tablet::Insert(uint8_t*) is dead.
const ConstContiguousRow* ProjectRowForInsert(WriteTransactionState* tx_state,
                                              const Schema* tablet_schema,
                                              const RowProjector& row_projector,
                                              const uint8_t *user_row_ptr) {
  const ConstContiguousRow* row;
  if (row_projector.is_identity()) {
    row = new ConstContiguousRow(tablet_schema, user_row_ptr);
  } else {
    uint8_t *rowbuf = new uint8_t[ContiguousRowHelper::row_size(*tablet_schema)];
    tx_state->AddArrayToAutoReleasePool(rowbuf);
    ConstContiguousRow src_row(row_projector.base_schema(), user_row_ptr);
    ContiguousRow proj_row(tablet_schema, rowbuf);
    CHECK_OK(row_projector.ProjectRowForWrite(src_row, &proj_row, static_cast<Arena*>(NULL)));
    row = new ConstContiguousRow(proj_row);
  }
  DCHECK(row->schema()->has_column_ids());
  return tx_state->AddToAutoReleasePool(row);
}

// TODO: remove when the old Tablet::Mutate() is dead.
RowChangeList ProjectMutation(WriteTransactionState *tx_state,
                              const DeltaProjector& delta_projector,
                              const RowChangeList &user_mutation) {
  if (delta_projector.is_identity()) {
    return user_mutation;
  } else {
    faststring rclbuf;
    CHECK_OK(RowChangeListDecoder::ProjectUpdate(delta_projector, user_mutation, &rclbuf));
    RowChangeList mutation(rclbuf);
    tx_state->AddToAutoReleasePool(rclbuf.release());
    return mutation;
  }
}

}  // namespace tablet
}  // namespace kudu
