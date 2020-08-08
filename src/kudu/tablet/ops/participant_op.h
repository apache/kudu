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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/status.h"

namespace kudu {
class rw_semaphore;

namespace consensus {
class OpId;
} // namespace consensus

namespace tablet {
class TabletReplica;

// An OpState for an update to transaction participant state.
class ParticipantOpState : public OpState {
 public:
  // Creates op state for the given tablet replica with the given request and
  // response.
  // TODO(awong): track this on the RPC results tracker.
  ParticipantOpState(TabletReplica* tablet_replica,
                     TxnParticipant* txn_participant,
                     const tserver::ParticipantRequestPB* request,
                     tserver::ParticipantResponsePB* response = nullptr);
  const tserver::ParticipantRequestPB* request() const override {
    return request_;
  }
  tserver::ParticipantResponsePB* response() const override {
    return response_;
  }
  std::string ToString() const override;

  // Takes a reference to the transaction associated with this request in the
  // underlying tablet's transaction participant, creating the transaction if
  // it doesn't already exist. Locks the transaction for writes.
  void AcquireTxnAndLock();

  // Performs the transaction state change requested by this op. Must be called
  // while the transaction lock is held, i.e. between the calls to
  // AcquireTxnAndLock() and ReleaseTxn().
  //
  // Anchors the given 'op_id' in the WAL, ensuring that subsequent bootstraps
  // of the tablet's WAL will leave the transaction in the appropriate state.
  Status PerformOp(const consensus::OpId& op_id);

  // Releases the transaction and its lock.
  void ReleaseTxn();

  // Returns the transaction ID for this op.
  int64_t txn_id() {
    return request_->op().txn_id();
  }
 private:
  friend class ParticipantOp;

  // Returns an error if the transaction is not in an appropriate state for
  // the state change requested by this op.
  Status ValidateOp() const;

  // The particpant being mutated. This may differ from the one we'd get from
  // TabletReplica if, for instance, we're bootstrapping a new Tablet.
  TxnParticipant* txn_participant_;

  const tserver::ParticipantRequestPB* request_;
  tserver::ParticipantResponsePB* response_;

  scoped_refptr<Txn> txn_;
  std::unique_lock<rw_semaphore> txn_lock_;
};

// Op that executes a transaction state change in the transaction participant.
class ParticipantOp : public Op {
 public:
  ParticipantOp(std::unique_ptr<ParticipantOpState> state,
                consensus::DriverType type)
      : Op(type, Op::PARTICIPANT_OP),
        state_(std::move(state)) {}
  OpState* state() override { return state_.get(); }
  const OpState* state() const override { return state_.get(); }
  void NewReplicateMsg(std::unique_ptr<consensus::ReplicateMsg>* replicate_msg) override;

  // Takes a reference to the requested transaction, creating it if necessary.
  // Locks its state and checks that the requested operation is valid.
  Status Prepare() override;

  // Register the op.
  Status Start() override;

  // Perform the state change.
  Status Apply(consensus::CommitMsg** commit_msg) override;

  // Release the transaction reference and the lock on its state. If this was
  // the only op referencing the transaction and it was left in the
  // kInitializing state (e.g. we tried to start the transaction in this op but
  // aborted before applying), removes the transaction from those tracked by
  // the underlying TxnParticipant.
  void Finish(OpResult result) override;

  std::string ToString() const override;

 private:
  std::unique_ptr<ParticipantOpState> state_;
};

} // namespace tablet
} // namespace kudu
