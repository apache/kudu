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
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {
namespace tablet {
class TabletReplica;
class OpCompletionCallback;
class OpState;

// All metrics associated with a Op.
struct OpMetrics {
  OpMetrics();
  void Reset();
  int successful_inserts;
  int insert_ignore_errors;
  int successful_upserts;
  int successful_updates;
  int successful_deletes;
  uint64_t commit_wait_duration_usec;
};

// Base class for ops.
// There are different implementations for different types (Write, AlterSchema, etc.).
// OpDriver implementations use Ops along with Consensus to execute
// and replicate operations in a consensus configuration.
class Op {
 public:

  enum OpType {
    WRITE_OP,
    ALTER_SCHEMA_OP,
  };

  enum TraceType {
    NO_TRACE_OPS = 0,
    TRACE_OPS = 1
  };

  enum OpResult {
    COMMITTED,
    ABORTED
  };

  Op(consensus::DriverType type, OpType op_type);

  // Returns the OpState for this op.
  virtual OpState* state() = 0;
  virtual const OpState* state() const = 0;

  // Returns whether this op is being executed on the leader or on a
  // replica.
  consensus::DriverType type() const { return type_; }

  // Returns this op's type.
  OpType op_type() const { return op_type_; }

  // Builds the ReplicateMsg for this op.
  virtual void NewReplicateMsg(std::unique_ptr<consensus::ReplicateMsg>* replicate_msg) = 0;

  // Executes the prepare phase of this op, the actual actions
  // of this phase depend on the op type, but usually are limited
  // to what can be done without actually changing data structures and without
  // side-effects.
  virtual Status Prepare() = 0;

  // Aborts the prepare phase.
  virtual void AbortPrepare() {}

  // Actually starts an op, assigning a timestamp to the op.
  // LEADER replicas execute this in or right after Prepare(), while FOLLOWER/LEARNER
  // replicas execute this right before the Apply() phase as the op's
  // timestamp is only available on the LEADER's commit message.
  // Once Started(), state might have leaked to other replicas/local log and the
  // op can't be cancelled without issuing an abort message.
  virtual Status Start() = 0;

  // Executes the Apply() phase of the op, the actual actions of
  // this phase depend on the op type, but usually this is the
  // method where data-structures are changed.
  virtual Status Apply(std::unique_ptr<consensus::CommitMsg>* commit_msg) = 0;

  // Executed after the op has been applied and the commit message has
  // been appended to the log (though it might not be durable yet), or if the
  // op was aborted.
  // Implementations are expected to perform cleanup on this method, the driver
  // will reply to the client after this method call returns.
  // 'result' will be either COMMITTED or ABORTED, letting implementations
  // know what was the final status of the op.
  virtual void Finish(OpResult result) {}

  // Each implementation should have its own ToString() method.
  virtual std::string ToString() const = 0;

  virtual ~Op() {}

 private:
  const consensus::DriverType type_;
  const OpType op_type_;
};

class OpState {
 public:

  // Returns the request PB associated with this op. May be NULL if
  // the op's state has been reset.
  virtual const google::protobuf::Message* request() const { return NULL; }

  // Returns the response PB associated with this op, or NULL.
  // This will only return a non-null object for leader-side ops.
  virtual google::protobuf::Message* response() const { return NULL; }

  // Returns whether the results of the op are being tracked.
  bool are_results_tracked() const {
    return result_tracker_.get() != nullptr && has_request_id();
  }

  rpc::ResultTracker* result_tracker() const { return result_tracker_.get(); }

  void SetResultTracker(const scoped_refptr<rpc::ResultTracker> result_tracker) {
    result_tracker_ = result_tracker;
  }

  // Sets the ConsensusRound for this op, if this op is
  // being executed through the consensus system.
  void set_consensus_round(const scoped_refptr<consensus::ConsensusRound>& consensus_round) {
    consensus_round_ = consensus_round;
    op_id_ = consensus_round_->id();
  }

  // Returns the ConsensusRound being used, if this op is being
  // executed through the consensus system or NULL if it's not.
  consensus::ConsensusRound* consensus_round() {
    return consensus_round_.get();
  }

  TabletReplica* tablet_replica() const {
    return tablet_replica_;
  }

  // Return metrics related to this transaction.
  const OpMetrics& metrics() const {
    return op_metrics_;
  }

  OpMetrics* mutable_metrics() {
    return &op_metrics_;
  }

  void set_completion_callback(
      std::unique_ptr<OpCompletionCallback> completion_clbk) {
    completion_clbk_ = std::move(completion_clbk);
  }

  // Returns the completion callback.
  OpCompletionCallback* completion_callback() {
    return DCHECK_NOTNULL(completion_clbk_.get());
  }

  // Sets a heap object to be managed by this op's AutoReleasePool.
  template<class T>
  T* AddToAutoReleasePool(T* t) {
    return pool_.Add(t);
  }

  // Sets an array heap object to be managed by this op's AutoReleasePool.
  template<class T>
  T* AddArrayToAutoReleasePool(T* t) {
    return pool_.AddArray(t);
  }

  // Return the arena associated with this op.
  // NOTE: this is not a thread-safe arena!
  Arena* arena() {
    return &arena_;
  }

  // Each implementation should have its own ToString() method.
  virtual std::string ToString() const = 0;

  // Sets the timestamp for the op
  virtual void set_timestamp(const Timestamp& timestamp) {
    // make sure we set the timestamp only once
    std::lock_guard<simple_spinlock> l(op_state_lock_);
    DCHECK_EQ(timestamp_, Timestamp::kInvalidTimestamp);
    timestamp_ = timestamp;
  }

  Timestamp timestamp() const {
    std::lock_guard<simple_spinlock> l(op_state_lock_);
    DCHECK(timestamp_ != Timestamp::kInvalidTimestamp);
    return timestamp_;
  }

  bool has_timestamp() const {
    std::lock_guard<simple_spinlock> l(op_state_lock_);
    return timestamp_ != Timestamp::kInvalidTimestamp;
  }

  consensus::OpId* mutable_op_id() {
    return &op_id_;
  }

  const consensus::OpId& op_id() const {
    return op_id_;
  }

  ExternalConsistencyMode external_consistency_mode() const {
    return external_consistency_mode_;
  }

  // Returns where the op associated with this OpState had an
  // associated op id.
  bool has_request_id() const {
    return request_id_.has_client_id();
  }

  // Returns the request id for the op associated with this OpState.
  // Not all ops will have a request id so users of this method should call
  // 'has_request_id()' first to make sure it is set.
  const rpc::RequestIdPB& request_id() const {
    return request_id_;
  }

 protected:
  explicit OpState(TabletReplica* tablet_replica);
  virtual ~OpState();

  OpMetrics op_metrics_;

  // The TabletReplica that is coordinating this op.
  TabletReplica* const tablet_replica_;

  // The result tracker that will cache the result of this op.
  scoped_refptr<rpc::ResultTracker> result_tracker_;

  // Optional callback to be called once the op completes.
  std::unique_ptr<OpCompletionCallback> completion_clbk_;

  AutoReleasePool pool_;

  // This op's timestamp. Protected by op_state_lock_.
  Timestamp timestamp_;

  // The clock error when timestamp_ was read.
  uint64_t timestamp_error_;

  Arena arena_;

  // This OpId stores the canonical "anchor" OpId for this op.
  consensus::OpId op_id_;

  // The client's id for this op, if there is one.
  rpc::RequestIdPB request_id_;

  scoped_refptr<consensus::ConsensusRound> consensus_round_;

  // The defined consistency mode for this op.
  ExternalConsistencyMode external_consistency_mode_;

  // Lock that protects access to op state.
  mutable simple_spinlock op_state_lock_;
};

// A parent class for the callback that gets called when ops
// complete.
//
// This must be set in the OpState if the op initiator is to
// be notified of when an op completes. The callback belongs to the
// op context and is deleted along with it.
//
// NOTE: this is a concrete class so that we can use it as a default implementation
// which avoids callers having to keep checking for NULL.
class OpCompletionCallback {
 public:

  OpCompletionCallback();

  // Allows to set an error for this op and a mapping to a server level code.
  // Calling this method does not mean the op is completed.
  void set_error(const Status& status, tserver::TabletServerErrorPB::Code code);

  void set_error(const Status& status);

  bool has_error() const;

  const Status& status() const;

  const tserver::TabletServerErrorPB::Code error_code() const;

  // Subclasses should override this.
  virtual void OpCompleted();

  virtual ~OpCompletionCallback();

 protected:
  Status status_;
  tserver::TabletServerErrorPB::Code code_;
};

// OpCompletionCallback implementation that can be waited on.
// Helper to make async ops, sync.
// This is templated to accept any response PB that has a TabletServerError
// 'error' field and to set the error before performing the latch countdown.
// The callback does *not* take ownership of either latch or response.
template<class ResponsePB>
class LatchOpCompletionCallback : public OpCompletionCallback {
 public:
  explicit LatchOpCompletionCallback(CountDownLatch* latch,
                                     ResponsePB* response)
    : latch_(DCHECK_NOTNULL(latch)),
      response_(DCHECK_NOTNULL(response)) {
  }

  virtual void OpCompleted() OVERRIDE {
    if (!status_.ok()) {
      StatusToPB(status_, response_->mutable_error()->mutable_status());
    }
    latch_->CountDown();
  }

 private:
  CountDownLatch* latch_;
  ResponsePB* response_;
};

}  // namespace tablet
}  // namespace kudu
