// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TRANSACTION_CONTEXT_H_
#define KUDU_TABLET_TRANSACTION_CONTEXT_H_

#include <string>
#include <vector>

#include "common/row.h"
#include "common/row_changelist.h"
#include "common/wire_protocol.h"
#include "tablet/mvcc.h"
#include "tablet/rowset.h"
#include "tablet/tablet.pb.h"

namespace kudu {
namespace tablet {

// A transaction context for a batch of inserts/mutates.
//
// Used when logging to WAL in that we keep track of where inserts/updates
// were applied and add that information to the commit message that is stored
// on the WAL.
//
// NOTE: TransactionContext does not copy any of the data, such as rows,
// row_keys or row change lists. These are expected to not be destructed during
// the lifetime of TransactionContext (or at least not before they are used to
// persist the Commit message).
//
// NOTE: this class isn't thread safe.
//
// TODO later add owning scoped transaction, the locks (either inside or out of
// ScopedTransaction, and the RpcContext, but for now this is minimal.
class TransactionContext {

 public:
  TransactionContext()
      : unsuccessful_ops_(0) {
  }

  // Adds an applied insert to this TransactionContext, including the
  // id of the MemRowSet to which it was applied.
  Status AddInsert(const txid_t &tx_id,
                   int64_t mrs_id);

  // Adds a failed insert to this TransactionContext, including the status
  // explaining why the insert failed.
  void AddFailedInsert(const Status &status);

  // Adds an applied mutation to this TransactionContext, including the
  // tablet id, the mvcc transaction id, the mutation that was applied
  // and the delta stores that were mutated.
  Status AddMutation(const txid_t &tx_id,
                     gscoped_ptr<MutationResultPB> result);

  // Adds a failed mutation to this TransactionContext, including the status
  // explaining why it failed.
  void AddFailedMutation(const Status &status);

  bool is_all_success() const {
    return unsuccessful_ops_ == 0;
  }

  // Returns the result of this transaction in its protocol buffers
  // form.
  const TxResultPB& Result() const {
    return result_pb_;
  }

  void Reset();

 private:
  DISALLOW_COPY_AND_ASSIGN(TransactionContext);

  Status SetOrCheckTxId(const txid_t& tx_id);

  txid_t tx_id_;
  TxResultPB result_pb_;
  int32_t unsuccessful_ops_;
};

// Calculates type of the mutation based on the set fields and number of targets.
MutationResultPB::MutationTypePB MutationType(const MutationResultPB* result);

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TRANSACTION_CONTEXT_H_ */
