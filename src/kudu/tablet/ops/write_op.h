// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/bitset.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;
class rw_semaphore;

namespace rpc {
class RequestIdPB;
}

namespace tablet {

class ScopedOp;
class TabletReplica;
class TxResultPB;
struct RowOp;
struct TabletComponents;

// Privileges required for write operations.
enum WritePrivilegeType {
  INSERT,
  UPDATE,
  DELETE,
};
static constexpr size_t kWritePrivilegeMax = WritePrivilegeType::DELETE + 1;
std::string WritePrivilegeToString(const WritePrivilegeType& type);

typedef FixedBitSet<WritePrivilegeType, kWritePrivilegeMax> WritePrivileges;

// Adds the required privileges for the given op types to 'privileges'.
void AddWritePrivilegesForRowOperations(const RowOperationsPB::Type& op_type,
                                        WritePrivileges* privileges);

struct WriteAuthorizationContext {
  // Checks that the requested operations can be performed with the given
  // privileges, returning a NotAuthorized error if not.
  Status CheckPrivileges() const;

  // Write privileges with which to authorize.
  WritePrivileges write_privileges;

  // Write operations for a given write request. This is populated while
  // decoding a write request.
  RowOpTypes requested_op_types;
};

// A OpState for a batch of inserts/mutates. This class holds and
// owns most everything related to an op, including:
// - A RowOp structure for each of the rows being inserted or mutated, which itself
//   contains:
//   - decoded/projected data
//   - row lock reference
//   - result of this particular insert/mutate operation, once executed
// - the Replicate and Commit PB messages
//
// All the op related pointers are owned by this class
// and destroyed on Reset() or by the destructor.
//
// IMPORTANT: All the acquired locks will not be released unless the OpState
// is either destroyed or Reset() or release_locks() is called. Beware of this
// or else there will be lock leaks.
//
// Used when logging to WAL in that we keep track of where inserts/updates
// were applied and add that information to the commit message that is stored
// on the WAL.
//
// NOTE: this class isn't thread safe.
class WriteOpState : public OpState {
 public:
  // A write op for a given replica and the given request. Optionally takes
  // 'response' if this write is meant to populate a response (e.g. this is a
  // leader op), and a set of write privileges to check before performing the
  // ops in a request.
  WriteOpState(TabletReplica* tablet_replica,
               const tserver::WriteRequestPB* request,
               const rpc::RequestIdPB* request_id,
               tserver::WriteResponsePB* response = nullptr,
               boost::optional<WriteAuthorizationContext> authz_ctx = boost::none);
  ~WriteOpState();

  // Returns the result of this op in its protocol buffers form. The op result
  // holds information on exactly which memory stores were mutated in the
  // context of this op and can be used to perform recovery.
  //
  // This releases part of the state of the op, and will crash
  // if called more than once.
  void ReleaseTxResultPB(TxResultPB* result) const;

  // Returns the original client request for this op, if there was
  // one.
  const tserver::WriteRequestPB* request() const override {
    return request_;
  }

  // Returns the prepared response to the client that will be sent when this op
  // is completed, if this op was started by a client.
  tserver::WriteResponsePB* response() const override {
    return response_;
  }

  // Returns the state associated with authorizing this op, or 'none' if no
  // authorization is necessary.
  const boost::optional<WriteAuthorizationContext>& authz_context() const {
    return authz_context_;
  }

  // Set the MVCC op associated with this Write operation.
  // This must be called exactly once, after the timestamp was acquired.
  // This also copies the timestamp from the MVCC op into the
  // WriteOpState object.
  void SetMvccTx(std::unique_ptr<ScopedOp> mvcc_op);

  // Set the Tablet components that this op will write into.
  // Called exactly once at the beginning of Apply, before applying its
  // in-memory edits.
  void set_tablet_components(const scoped_refptr<const TabletComponents>& components);

  // Take a shared lock on the given schema lock.
  // This is required prior to decoding rows so that the schema does
  // not change in between performing the projection and applying
  // the writes.
  void AcquireSchemaLock(rw_semaphore* schema_lock);

  // Release the already-acquired schema lock.
  void ReleaseSchemaLock();

  void ReleaseMvccTxn(Op::OpResult result);

  void set_schema_at_decode_time(const Schema* schema) {
    std::lock_guard<simple_spinlock> l(op_state_lock_);
    schema_at_decode_time_ = schema;
  }

  const Schema* schema_at_decode_time() const {
    std::lock_guard<simple_spinlock> l(op_state_lock_);
    return schema_at_decode_time_;
  }

  const TabletComponents* tablet_components() const {
    return tablet_components_.get();
  }

  // Notifies the MVCC manager that this operation is about to start applying
  // its in-memory edits. After this method is called, the op _must_ Commit()
  // within a bounded amount of time (there may be other threads blocked on
  // it).
  void StartApplying();

  // Commits or aborts the MVCC op and releases all locks held.
  //
  // In the case of COMMITTED, this method makes the inserts and mutations
  // performed by this op visible to other op.
  //
  // Must be called exactly once.
  // REQUIRES: StartApplying() was called.
  //
  // Note: request_ and response_ are set to NULL after this method returns.
  void CommitOrAbort(Op::OpResult result);

  // Returns all the prepared row writes for this op. Usually called on the
  // apply phase to actually make changes to the tablet.
  const std::vector<RowOp*>& row_ops() const {
    return row_ops_;
  }

  // Return the ProbeStats object collecting statistics for op index 'i'.
  ProbeStats* mutable_op_stats(int i) {
    DCHECK_LT(i, row_ops_.size());
    DCHECK(stats_array_);
    return &stats_array_[i];
  }

  // Set the 'row_ops' member based on the given decoded operations.
  void SetRowOps(std::vector<DecodedRowOperation> decoded_ops);

  void UpdateMetricsForOp(const RowOp& op);

  // Resets this OpState, releasing all locks, destroying all prepared writes,
  // clearing the op result _and_ committing the current Mvcc op.
  void Reset();

  std::string ToString() const override;

 private:
  // Releases all the row locks acquired by this op.
  void ReleaseRowLocks();

  // Reset the RPC request, response, and row_ops_ (which refers to data
  // from the request).
  void ResetRpcFields();

  // An owned version of the response, for follower ops.
  tserver::WriteResponsePB owned_response_;

  // The lifecycle of these pointers request and response, is not managed by this class.
  // These pointers are never null: 'request_' is always set on construction and 'response_' is
  // either set to the response passed on the ctor, if there is one, or to point to
  // 'owned_response_' if there isn't.
  const tserver::WriteRequestPB* request_;
  tserver::WriteResponsePB* response_;

  // Encapsulates state required to authorize a write request. If 'none', then
  // no authorization is required.
  boost::optional<WriteAuthorizationContext> authz_context_;

  // The row operations which are decoded from the request during Prepare().
  // Protected by superclass's op_state_lock_.
  std::vector<RowOp*> row_ops_;

  // Array of ProbeStats for each of the operations in 'row_ops_'.
  // Allocated from this op's arena during SetRowOps().
  ProbeStats* stats_array_ = nullptr;

  // The MVCC op, set up during PREPARE phase
  std::unique_ptr<ScopedOp> mvcc_op_;

  // The tablet components, acquired at the same time as mvcc_op_ is set.
  scoped_refptr<const TabletComponents> tablet_components_;

  // A lock held on the tablet's schema. Prevents concurrent schema change
  // from racing with a write.
  shared_lock<rw_semaphore> schema_lock_;

  // The Schema of the tablet when the op was first decoded. This is verified
  // at APPLY time to ensure we don't have races against schema change.
  // Protected by superclass's op_state_lock_.
  const Schema* schema_at_decode_time_;

  DISALLOW_COPY_AND_ASSIGN(WriteOpState);
};

// Executes a write op.
class WriteOp : public Op {
 public:
  WriteOp(std::unique_ptr<WriteOpState> state, consensus::DriverType type);

  WriteOpState* state() override { return state_.get(); }
  const WriteOpState* state() const override { return state_.get(); }

  void NewReplicateMsg(std::unique_ptr<consensus::ReplicateMsg>* replicate_msg) override;

  // Executes a Prepare for a write op.
  //
  // Decodes the operations in the request and acquires row locks for each of
  // the affected rows. This results in adding 'RowOp' objects for each of the
  // operations into the WriteOpState.
  //
  // Returns an error if the request contains an operation that is malformed
  // or isn't authorized.
  Status Prepare() override;

  void AbortPrepare() override;

  // Actually starts the Mvcc op and assigns a timestamp to this op.
  Status Start() override;

  // Executes an Apply for a write op.
  //
  // Actually applies inserts/mutates into the tablet. After these start being
  // applied, the op must run to completion as there is currently no means of
  // undoing an update.
  //
  // After completing the inserts/mutates, the row locks and the mvcc op can be
  // released, allowing other op to update the same rows. However the
  // component lock must not be released until the commit msg, which indicates
  // where each of the inserts/mutates were applied, is persisted to stable
  // storage. Because of this ApplyTask must enqueue a CommitTask before
  // releasing both the row locks and deleting the MvccOp as we need to make
  // sure that Commits that touch the same set of rows are persisted in order,
  // for recovery.
  // This, of course, assumes that commits are executed in the same order they
  // are placed in the queue (but not necessarily in the same order of the
  // original requests) which is already a requirement of the consensus
  // algorithm.
  Status Apply(std::unique_ptr<consensus::CommitMsg>* commit_msg) override;

  // If result == COMMITTED, commits the mvcc op and updates the metrics, if
  // result == ABORTED aborts the mvcc op.
  void Finish(OpResult result) override;

  std::string ToString() const override;

 private:
  void UpdatePerRowErrors();

  // this op's start time
  MonoTime start_time_;

  std::unique_ptr<WriteOpState> state_;

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteOp);
};

}  // namespace tablet
}  // namespace kudu

