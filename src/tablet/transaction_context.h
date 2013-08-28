// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TRANSACTION_CONTEXT_H_
#define KUDU_TABLET_TRANSACTION_CONTEXT_H_

#include <string>
#include <vector>

#include "common/row.h"
#include "common/row_changelist.h"
#include "tablet/mvcc.h"
#include "tablet/rowset.h"

namespace kudu {
namespace tablet {

class Operation;
class InsertOp;
class MutationOp;

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

  // Adds an applied insert to this TransactionContext, including the tablet,
  // the mvcc transaction id, the row that was applied and the MemRowSet id
  // where it was applied to.
  void AddInsert(const string &tablet_id,
                 const txid_t &tx_id,
                 const ConstContiguousRow& row,
                 int64_t mrs_id);

  // Adds a failed insert to this TransactionContext, including the status
  // explaining why the insert failed.
  //
  // TransactionContext does not copy the passed row, caller is expected not to
  // free the row for the duration of the validity of this TransactionContext.
  void AddFailedInsert(const string &tablet_id,
                       const ConstContiguousRow& row,
                       const Status &status);

  // Adds an applied mutation to this TransactionContext, including the
  // tablet id, the mvcc transaction id, the mutation that was applied
  // and the delta stores that were mutated.
  void AddMutation(const string &tablet_id,
                   const txid_t &tx_id,
                   const RowChangeList &update,
                   gscoped_ptr<MutationResult> result);

  // Adds a failed mutation to this TransactionContext, including the status
  // explaining why it failed.
  void AddFailedMutation(const string &tablet_id,
                         const RowChangeList &update,
                         gscoped_ptr<MutationResult> result,
                         const Status &status);

  bool is_all_success() const {
    return unsuccessful_ops_ == 0;
  }

  // Returns all operations that were performed in this transaction.
  const vector<const Operation *> &operations() const {
    return operations_;
  }

  void Reset();

  ~TransactionContext();

 private:
  DISALLOW_COPY_AND_ASSIGN(TransactionContext);

  vector<const Operation *> operations_;
  int32_t unsuccessful_ops_;
};

class Operation {
 public:

  const string &tablet_id() const {
    return tablet_id_;
  }

  const txid_t &tx_id() const {
    return tx_id_;
  }

  const Status &status() const {
    return status_;
  }

  bool is_success() const {
    return status_.ok();
  }

  virtual ~Operation() {}

 protected:
  Operation(const string &tablet_id,
            const txid_t &tx_id,
            Status status)
      : tablet_id_(tablet_id),
        tx_id_(tx_id),
        status_(status) {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Operation);

  friend class TransactionContext;

  string tablet_id_;
  txid_t tx_id_;
  Status status_;
};

class InsertOp : public Operation {
 public:

  const ConstContiguousRow &row() const {
    return row_;
  }

  int64_t mrs_id() const {
    return mrs_id_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(InsertOp);

  friend class TransactionContext;

  InsertOp(const string &tablet_id,
           const txid_t &tx_id,
           const ConstContiguousRow& row,
           int64_t mrs_id)
      : Operation(tablet_id, tx_id, Status::OK()),
        row_(row),
        mrs_id_(mrs_id) {
  }

  // ctor for failed inserts
  InsertOp(const string &tablet_id,
           const ConstContiguousRow& row,
           Status status)
      : Operation(tablet_id, txid_t::kInvalidTxId, status),
        row_(row),
        mrs_id_(-1) {
    DCHECK(!status.ok());
  }

  ConstContiguousRow row_;
  int64_t mrs_id_;
};

class MutationOp : public Operation {
 public:

  const RowChangeList &update() const {
    return update_;
  }

  const MutationResult *result() const {
    return result_.get();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(MutationOp);

  friend class TransactionContext;

  MutationOp(const string &tablet_id,
             const txid_t &tx_id,
             const RowChangeList &update,
             gscoped_ptr<MutationResult> result)
      : Operation(tablet_id, tx_id, Status::OK()),
        update_(update),
        result_(result.Pass()) {
  }

  // ctor for failed mutations
  MutationOp(const string &tablet_id,
             const RowChangeList &update,
             gscoped_ptr<MutationResult> result,
             Status status)
      : Operation(tablet_id, txid_t::kInvalidTxId, status),
        update_(update),
        result_(result.Pass()) {
    DCHECK(!status.ok());
  }

  RowChangeList update_;
  gscoped_ptr<MutationResult> result_;
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TRANSACTION_CONTEXT_H_ */
