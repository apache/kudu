// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_
#define KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_

#include <vector>

#include "kudu/util/status.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"

namespace kudu {
class ConstContiguousRow;
class DeltaProjector;
class RowChangeList;
class RowProjector;
class Schema;

namespace tserver {
class WriteResponsePB;
}

namespace tablet {
class Tablet;

class WriteTransactionState;

// Return a row that is the Projection of the 'user_row_ptr' on the 'tablet_schema'.
// The 'tablet_schema' pointer will be referenced by the returned row, so must
// remain valid as long as the row.
// The returned ConstContiguousRow is added to the AutoReleasePool of the 'tx_state'.
// No projection is performed if the two schemas are the same.
//
// TODO: this is now only used by the testing code path
ConstContiguousRow ProjectRowForInsert(WriteTransactionState* tx_state,
                                       const Schema* tablet_schema,
                                       const RowProjector& row_projector,
                                       const uint8_t* user_row_ptr);



// Return a mutation that is the Projection of the 'user_mutation' on the 'tablet_schema'.
// The returned RowChangeList's data is added to the AutoReleasePool of the 'tx_state'.
// No projection is performed if the two schemas are the same.
// TODO: use the tx_state's arena instead.
RowChangeList ProjectMutation(WriteTransactionState *tx_state,
                              const DeltaProjector& delta_projector,
                              const RowChangeList &user_mutation);

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_ */
