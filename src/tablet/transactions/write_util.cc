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

Status DecodeRowBlock(WriteTransactionContext* tx_ctx,
                      RowwiseRowBlockPB* block_pb,
                      const Schema& tablet_key_projection,
                      bool is_inserts_block,
                      Schema* client_schema,
                      std::vector<const uint8_t*>* row_block) {
  // Extract the schema of the row block
  Status s = ColumnPBsToSchema(block_pb->schema(), client_schema);
  if (!s.ok()) {
    tx_ctx->completion_callback()->set_error(
        s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }


  // Check that the schema sent by the user matches the key projection of the tablet.
  Schema client_key_projection = client_schema->CreateKeyProjection();
  if (!client_key_projection.Equals(tablet_key_projection)) {
    s = Status::InvalidArgument("Mismatched key projection schema, expected",
                                          tablet_key_projection.ToString());
    if (!s.ok()) {
      tx_ctx->completion_callback()->set_error(
          s, TabletServerErrorPB::MISMATCHED_SCHEMA);
      return s;
    }
  }

  // Extract the row block
  if (is_inserts_block) {
    s = ExtractRowsFromRowBlockPB(*client_schema, block_pb, row_block);
  } else {
    s = ExtractRowsFromRowBlockPB(client_key_projection, block_pb, row_block);
  }


  if (!s.ok()) {
    tx_ctx->completion_callback()->set_error(
        s, TabletServerErrorPB::INVALID_ROW_BLOCK);
    return s;
  }

  return Status::OK();
}

Status CreatePreparedInsertsAndMutates(Tablet* tablet,
                                       WriteTransactionContext* tx_ctx,
                                       gscoped_ptr<Schema> client_schema,
                                       const RowOperationsPB& to_insert_rows,
                                       gscoped_ptr<Schema> mutates_client_schema,
                                       const vector<const uint8_t *>& to_mutate,
                                       const vector<const RowChangeList *>& mutations) {

  TRACE("PREPARE: Acquiring component lock");
  // acquire the component lock. this is more like "tablet lock" and is used
  // to prevent AlterSchema and other operations that requires exclusive access
  // to the tablet.
  gscoped_ptr<shared_lock<rw_semaphore> > component_lock_(
      new shared_lock<rw_semaphore>(*tablet->component_lock()));
  tx_ctx->set_component_lock(component_lock_.Pass());


  // Now that the schema is fixed, we can project the inserts into that schema.
  vector<DecodedRowOperation> to_insert;
  if (to_insert_rows.rows().size() > 0) {
    RowOperationsPBDecoder dec(&to_insert_rows,
                               client_schema.get(),
                               tablet->schema_ptr(),
                               tx_ctx->arena());
    Status s = dec.DecodeOperations(&to_insert);
    if (!s.ok()) {
      tx_ctx->completion_callback()->set_error(s,
                                               TabletServerErrorPB::MISMATCHED_SCHEMA);
      return s;
    }
  }

  // Taking schema_ptr here is safe because we know that the schema won't change,
  // due to the lock above.
  DeltaProjector delta_projector(mutates_client_schema.get(), tablet->schema_ptr());
  if (to_mutate.size() > 0 && !delta_projector.is_identity()) {
    Status s = tablet->schema().VerifyProjectionCompatibility(*mutates_client_schema);
    if (!s.ok()) {
      tx_ctx->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
      return s;
    }

    s = mutates_client_schema->GetProjectionMapping(tablet->schema(), &delta_projector);
    if (!s.ok()) {
      tx_ctx->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
      return s;
    }
  }

  // Now acquire row locks and prepare everything for apply
  TRACE("PREPARE: Acquiring row locks ($0 insertions, $1 mutations)",
        to_insert.size(), to_mutate.size());
  BOOST_FOREACH(const DecodedRowOperation& op, to_insert) {
    // TODO pass 'row_ptr' to the PreparedRowWrite once we get rid of the
    // old API that has a Mutate method that receives the row as a reference.
    // TODO: allocating ConstContiguousRow is kind of a waste since it is just
    // a {schema, ptr} pair itself and probably cheaper to copy around.
    ConstContiguousRow *row = tx_ctx->AddToAutoReleasePool(
      new ConstContiguousRow(*tablet->schema_ptr(), op.row_data));
    gscoped_ptr<PreparedRowWrite> row_write;
    RETURN_NOT_OK(tablet->CreatePreparedInsert(tx_ctx, row, &row_write));
    tx_ctx->add_prepared_row(row_write.Pass());
  }

  int i = 0;
  BOOST_FOREACH(const uint8_t* row_key_ptr, to_mutate) {
    // TODO pass 'row_key_ptr' to the PreparedRowWrite once we get rid of the
    // old API that has a Mutate method that receives the row as a reference.
    ConstContiguousRow* row_key = new ConstContiguousRow(tablet->key_schema(), row_key_ptr);
    row_key = tx_ctx->AddToAutoReleasePool(row_key);

    const RowChangeList* mutation = ProjectMutation(tx_ctx, delta_projector, mutations[i]);

    gscoped_ptr<PreparedRowWrite> row_write;
    RETURN_NOT_OK(tablet->CreatePreparedMutate(tx_ctx, row_key, mutation, &row_write));
    tx_ctx->add_prepared_row(row_write.Pass());
    ++i;
  }

  return Status::OK();
}

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

const RowChangeList* ProjectMutation(WriteTransactionContext *tx_ctx,
                                     const DeltaProjector& delta_projector,
                                     const RowChangeList *user_mutation) {
  const RowChangeList* mutation;
  if (delta_projector.is_identity()) {
    mutation = user_mutation;
  } else {
    faststring rclbuf;
    CHECK_OK(RowChangeListDecoder::ProjectUpdate(delta_projector, *user_mutation, &rclbuf));
    mutation = new RowChangeList(rclbuf);
    tx_ctx->AddToAutoReleasePool(rclbuf.release());
    tx_ctx->AddToAutoReleasePool(mutation);
  }
  return mutation;
}

}  // namespace tablet
}  // namespace kudu
