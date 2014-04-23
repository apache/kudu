// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/write_util.h"

#include "common/partial_row.h"
#include "common/wire_protocol.h"
#include "common/row_changelist.h"
#include "common/row_operations.h"
#include "common/row.h"
#include "common/schema.h"
#include "consensus/consensus.pb.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tablet/transactions/write_transaction.h"
#include "util/locks.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using boost::shared_lock;
using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;


Status CreatePreparedInsertsAndMutates(Tablet* tablet,
                                       WriteTransactionContext* tx_ctx,
                                       const Schema& client_schema,
                                       const RowOperationsPB& ops_pb) {
  TRACE("PREPARE: Acquiring component lock");
  // acquire the component lock. this is more like "tablet lock" and is used
  // to prevent AlterSchema and other operations that requires exclusive access
  // to the tablet.
  gscoped_ptr<shared_lock<rw_semaphore> > component_lock_(
      new shared_lock<rw_semaphore>(*tablet->component_lock()));
  tx_ctx->set_component_lock(component_lock_.Pass());

  TRACE("Projecting inserts");
  // Now that the schema is fixed, we can project the operations into that schema.
  vector<DecodedRowOperation> decoded_ops;
  if (ops_pb.rows().size() > 0) {
    RowOperationsPBDecoder dec(&ops_pb,
                               &client_schema,
                               tablet->schema_unlocked().get(),
                               tx_ctx->arena());
    Status s = dec.DecodeOperations(&decoded_ops);
    if (!s.ok()) {
      // TODO: is MISMATCHED_SCHEMA always right here? probably not.
      tx_ctx->completion_callback()->set_error(s,
                                               TabletServerErrorPB::MISMATCHED_SCHEMA);
      return s;
    }
  }

  // Now acquire row locks and prepare everything for apply
  TRACE("PREPARE: Running prepare for $0 operations", decoded_ops.size());
  BOOST_FOREACH(const DecodedRowOperation& op, decoded_ops) {
    gscoped_ptr<PreparedRowWrite> row_write;

    switch (op.type) {
      case RowOperationsPB::INSERT:
      {
        // TODO pass 'row_ptr' to the PreparedRowWrite once we get rid of the
        // old API that has a Mutate method that receives the row as a reference.
        // TODO: allocating ConstContiguousRow is kind of a waste since it is just
        // a {schema, ptr} pair itself and probably cheaper to copy around.
        ConstContiguousRow *row = tx_ctx->AddToAutoReleasePool(
          new ConstContiguousRow(*tablet->schema_unlocked().get(), op.row_data));
        RETURN_NOT_OK(tablet->CreatePreparedInsert(tx_ctx, row, &row_write));
        break;
      }
      case RowOperationsPB::UPDATE:
      case RowOperationsPB::DELETE:
      {
        const uint8_t* row_key_ptr = op.row_data;
        const RowChangeList& mutation = op.changelist;

        // TODO pass 'row_key_ptr' to the PreparedRowWrite once we get rid of the
        // old API that has a Mutate method that receives the row as a reference.
        // TODO: allocating ConstContiguousRow is kind of a waste since it is just
        // a {schema, ptr} pair itself and probably cheaper to copy around.
        ConstContiguousRow* row_key = new ConstContiguousRow(tablet->key_schema(), row_key_ptr);
        row_key = tx_ctx->AddToAutoReleasePool(row_key);
        RETURN_NOT_OK(tablet->CreatePreparedMutate(tx_ctx, row_key, mutation, &row_write));
        break;
      }
      default:
        LOG(FATAL) << "Bad type: " << op.type;
    }

    tx_ctx->add_prepared_row(row_write.Pass());
  }

  return Status::OK();
}

// TODO: remove when the old Tablet::Insert(uint8_t*) is dead.
const ConstContiguousRow* ProjectRowForInsert(WriteTransactionContext* tx_ctx,
                                              const Schema* tablet_schema,
                                              const RowProjector& row_projector,
                                              const uint8_t *user_row_ptr) {
  const ConstContiguousRow* row;
  if (row_projector.is_identity()) {
    row = new ConstContiguousRow(*tablet_schema, user_row_ptr);
  } else {
    uint8_t *rowbuf = new uint8_t[ContiguousRowHelper::row_size(*tablet_schema)];
    tx_ctx->AddArrayToAutoReleasePool(rowbuf);
    ConstContiguousRow src_row(row_projector.base_schema(), user_row_ptr);
    ContiguousRow proj_row(*tablet_schema, rowbuf);
    CHECK_OK(row_projector.ProjectRowForWrite(src_row, &proj_row, static_cast<Arena*>(NULL)));
    row = new ConstContiguousRow(proj_row);
  }
  DCHECK(row->schema().has_column_ids());
  return tx_ctx->AddToAutoReleasePool(row);
}

// TODO: remove when the old Tablet::Mutate() is dead.
RowChangeList ProjectMutation(WriteTransactionContext *tx_ctx,
                              const DeltaProjector& delta_projector,
                              const RowChangeList &user_mutation) {
  if (delta_projector.is_identity()) {
    return user_mutation;
  } else {
    faststring rclbuf;
    CHECK_OK(RowChangeListDecoder::ProjectUpdate(delta_projector, user_mutation, &rclbuf));
    RowChangeList mutation(rclbuf);
    tx_ctx->AddToAutoReleasePool(rclbuf.release());
    return mutation;
  }
}

}  // namespace tablet
}  // namespace kudu
