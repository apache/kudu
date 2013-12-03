// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_
#define KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_

#include <vector>

#include "util/status.h"
#include "tablet/tablet.pb.h"
#include "tserver/tserver.pb.h"

namespace kudu {
class ConstContiguousRow;
class DeltaProjector;
class RowChangeList;
class RowProjector;
class Schema;

namespace rpc {
class RpcContext;
}

namespace tserver {
class WriteResponsePB;
}

namespace tablet {

class WriteTransactionContext;

// Sets up the client visible PB error based on a Status.
void SetupClientError(tserver::TabletServerErrorPB* error,
                      const Status& s,
                      tserver::TabletServerErrorPB::Code code);

// Decodes the row block and sets up a client error if something fails.
Status DecodeRowBlockAndSetupClientErrors(RowwiseRowBlockPB* block_pb,
                                          tserver::WriteResponsePB* resp,
                                          rpc::RpcContext* context,
                                          const Schema& tablet_key_projection,
                                          bool is_inserts_block,
                                          Schema* client_schema,
                                          std::vector<const uint8_t*>* row_block);

// Calculates type of the mutation based on the set fields and number of targets.
MutationResultPB::MutationTypePB MutationType(const MutationResultPB* result);

// Return a row that is the Projection of the 'user_row_ptr' on the 'tablet_schema'.
// The returned ConstContiguousRow is added to the AutoReleasePool of the 'tx_ctx'.
// No projection is performed if the two schemas are the same.
const ConstContiguousRow* ProjectRowForInsert(WriteTransactionContext* tx_ctx,
                                              const Schema& tablet_schema,
                                              const RowProjector& row_projector,
                                              const uint8_t *user_row_ptr);

// Return a mutation that is the Projection of the 'user_mutation' on the 'tablet_schema'.
// The returned RowChangeList is added to the AutoReleasePool of the 'tx_ctx'.
// No projection is performed if the two schemas are the same.
const RowChangeList* ProjectMutation(WriteTransactionContext *tx_ctx,
                                     const DeltaProjector& delta_projector,
                                     const RowChangeList *user_mutation);

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_ */
