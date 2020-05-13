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

#include <memory>
#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/walltime.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

namespace kudu {
class ThreadPool;
class ThreadPoolToken;
class Timestamp;

namespace log {
class Log;
} // namespace log

namespace consensus {
class RaftConsensus;
}

namespace tablet {
class OpOrderVerifier;
class OpTracker;

// Base class for op drivers.
//
// OpDriver classes encapsulate the logic of coordinating the execution of
// an operation. The exact triggering of the methods differs based on whether the
// operation is being executed on a leader or replica, but the general flow is:
//
//  1 - Init() is called on a newly created driver object.
//      If the driver is instantiated from a REPLICA, then we know that
//      the operation is already "REPLICATING" (and thus we don't need to
//      trigger replication ourself later on). In this case the op has already
//      been serialized by the leader, so we also call op_->Start().
//
//  2 - ExecuteAsync() is called. This submits PrepareTask() to prepare_pool_
//      and returns immediately.
//
//  3 - PrepareTask() calls Prepare() on the op.
//
//      Once successfully prepared, if we have not yet replicated (i.e we are leader),
//      assign a timestamp and call op_->Start(). Finally call consensus->Replicate()
//      and change the replication state to REPLICATING.
//
//      On the other hand, if we have already successfully replicated (eg we are the
//      follower and ReplicationFinished() has already been called, then we can move
//      on to ApplyAsync().
//
//  4 - RaftConsensus calls ReplicationFinished()
//
//      This is triggered by consensus when the commit index moves past our own
//      OpId. On followers, this can happen before Prepare() finishes, and thus
//      we have to check whether we have already done step 3. On leaders, we
//      don't start the consensus round until after Prepare, so this check always
//      passes.
//
//      If Prepare() has already completed, then we trigger ApplyAsync().
//
//  5 - ApplyAsync() submits ApplyTask() to the apply_pool_.
//      ApplyTask() calls op_->Apply().
//
//      When Apply() is called, changes are made to the in-memory data structures. These
//      changes are not visible to clients yet. After Apply() completes, a CommitMsg
//      is enqueued to the WAL in order to store information about the operation result
//      and provide correct recovery.
//
//      After the commit message has been enqueued in the Log, the driver executes Finalize()
//      which, in turn, makes ops make their changes visible to other ops.
//      After this step the driver replies to the client if needed and the op
//      is completed.
//      In-mem data structures that contain the changes made by the op can now
//      be made durable.
//
// [1] - see 'Implementation Techniques for Main Memory Database Systems', DeWitt et. al.
//
// ===========================================================================================
//
// Ordering requirements for lock acquisition and write (mvcc) op start
//
// On the leader side, starting the mvcc op for writes
// (calling tablet_->StartOp()) must always be done _after_ any relevant row locks are
// acquired (using AcquireLockForOp). This ensures that, within each row, timestamps only move
// forward. If we took a timestamp before getting the row lock, we could have the following
// situation:
//
//   Thread 1         |  Thread 2
//   ----------------------
//   Start op 1       |
//                    |  Start op 2
//                    |  Obtain row lock
//                    |  Update row
//                    |  Commit op 2
//   Obtain row lock  |
//   Delete row       |
//   Commit op 1
//
// This would cause the mutation list to look like: @t1: DELETE, @t2: UPDATE which is invalid,
// since we expect to be able to be able to replay mutations in increasing timestamp order on a
// given row.
//
// This requirement is basically two-phase-locking: the order in which row locks are acquired for
// ops determines their serialization order. If/when we support multi-node serializable
// ops, we'll have to acquire _all_ row locks (across all nodes) before obtaining a
// timestamp.
//
// Note that on non-leader replicas this requirement is no longer relevant. The leader assigned
// the timestamps and serialized the ops properly, so calling tablet_->StartOp()
// before lock acquisition on non-leader replicas is inconsequential.
//
// ===========================================================================================
//
// Tracking op results for exactly once semantics
//
// Exactly once semantics for ops require that the results of previous executions
// of an op be cached and replayed to the client, when a duplicate request is received.
// For single server operations, this can be encapsulated on the rpc layer, but for replicated
// ones, like ops, it needs additional care, as multiple copies of an RPC can arrive
// from different sources. For instance a client might be retrying an operation on a different
// tablet server, while that tablet server actually received the same operation from a previous
// leader.
//
// The prepare phase of ops is single threaded, so it's an ideal place to register
// follower ops with the result tracker and deduplicate possible multiple attempts.
// However rpc's from clients are first registered as they first arrive, outside of the prepare
// phase, and requests from another replica might arrive just as we're becoming leader, so we
// need to account for all the possible interleavings.
//
// Relevant constraints:
//
// 1 - Before a replica becomes leader (i.e. accepts new client requests), it has already enqueued
//     all current requests on the prepare queue.
//
// 2 - If a replica is not leader it rejects client requests on the prepare phase.
//
// 3 - Replicated ops, i.e. ones received as a follower of another (leader) replica, are
//     registered with the result tracker on the prepare phase itself. This constrains the possible
//     interleavings because when a client-originated request prepares, it will either observe a
//     previous request and abort, or it won't observe it and know it is the first attempt at
//     executing that op.
//
// Given these constraints the following interleavings might happen, on a newly elected leader:
//
// CR - Client Request
// RR - Replica Request
//
//
//             a) ---------                                  b) ---------
//                    |                                             |
//       RR prepares->1                                 CR arrives->1
//                    |                                             |
//                    2<-CR arrives                                 2<-RR prepares
//                    |                                             |
//                    3<-CR attaches                                3<-CR prepares/aborts
//                    |                                             |
//      RR completes->4                                 CR retries->4
//                    |                                             |
// CR replies cached->5                 CR attaches/replies cached->5
//                    |                                             |
//                ---------                                     ---------
//
// Case a) is the simplest. The client retries a request (2) after the duplicate request from the
// previous leader has prepared (1). When trying to register the result in the ResultTracker (2)
// the client's request will be "attached" (3) to the execution of the previously running request.
// When it completes (4) the client will receive the cached response (5).
//
// Case b) is a slightly more complex. When the client request arrives (1) the request from the
// previous leader hasn't yet prepared, meaning that the client request will be marked as a NEW
// request and its handler will proceed to try and prepare. In the meanwhile the replica request
// prepares (2). Since the replica request must take precedence over the client's request, the
// replica request must be the one to actually execute the operations, i.e. it must force the client
// request's handler to abort (3).
// It does this in two ways:
//
// - It immediately calls ResultTracker::TrackRpcOrChangeDriver(), this will make sure that it is
//   registered as the handler driver and that it is the only one whose response will be stored.
//
// - Later on, when the handler for the client request finally tries to prepare (3), it will observe
//   that it is no longer the driver of the op (his attempt_no has been replaced by
//   the replica request's attempt_no) and will later call ResultTracker::FailAndRespond()
//   causing the client to retry later (4,5).
//
// After the client receives the error sent on (2) it retries the operation (4) which will then
// attach to the replica request's execution and receive the same response when it is done (5).
//
// Additional notes:
//
// The above diagrams don't distinguish between arrive/prepare for replica requests as registering
// with the result tracker and preparing in that case happen atomically.
//
// If the client request was to proceed to prepare _before_ the replica request prepared, it would
// still be ok, as it would get aborted because the replica wasn't a leader yet (constraints 1/2).
//
// This class is thread safe.
class OpDriver : public RefCountedThreadSafe<OpDriver> {

 public:
  // Construct OpDriver. OpDriver does not take ownership
  // of any of the objects pointed to in the constructor's arguments.
  OpDriver(OpTracker* op_tracker,
           consensus::RaftConsensus* consensus,
           log::Log* log,
           ThreadPoolToken* prepare_pool_token,
           ThreadPool* apply_pool,
           OpOrderVerifier* order_verifier);

  // Perform any non-constructor initialization. Sets the op that will be
  // executed.
  Status Init(std::unique_ptr<Op> op,
              consensus::DriverType type);

  // Returns the OpId of the op being executed or an uninitialized
  // OpId if none has been assigned. Returns a copy and thus should not
  // be used in tight loops.
  consensus::OpId GetOpId();

  // Submits the op for execution.
  // The returned status acknowledges any error on the submission process.
  // The op will be replied to asynchronously.
  Status ExecuteAsync();

  // Aborts the op, if possible. Since ops are executed in
  // multiple stages by multiple executors it might not be possible to stop
  // the op immediately, but this will make sure it is aborted
  // at the next synchronization point.
  void Abort(const Status& status);

  // Callback from RaftConsensus when replication is complete, and thus the operation
  // is considered "committed" from the consensus perspective (ie it will be
  // applied on every node, and not ever truncated from the state machine history).
  // If status is anything different from OK() we don't proceed with the apply.
  //
  // see comment in the interface for an important TODO.
  void ReplicationFinished(const Status& status);

  std::string ToString() const;

  std::string ToStringUnlocked() const;

  std::string LogPrefix() const;

  // Returns the type of the op being executed by this driver.
  Op::OpType op_type() const;

  // Returns the state of the op being executed by this driver.
  const OpState* state() const;

  const MonoTime& start_time() const { return start_time_; }

  Trace* trace() { return trace_.get(); }

 private:
  FRIEND_TEST(TabletReplicaTest, TestShuttingDownMVCC);
  friend class RefCountedThreadSafe<OpDriver>;
  enum ReplicationState {
    // The operation has not yet been sent to consensus for replication
    NOT_REPLICATING,

    // Replication has been triggered (either because we are the leader and triggered it,
    // or because we are a follower and we started this operation in response to a
    // leader's call)
    REPLICATING,

    // Replication has failed, and we are certain that no other may have received the
    // operation (ie we failed before even sending the request off of our node).
    REPLICATION_FAILED,

    // Replication has succeeded.
    REPLICATED
  };

  enum PrepareState {
    NOT_PREPARED,
    PREPARED
  };

  ~OpDriver() {}

  // The task submitted to the prepare threadpool to prepare the op. If Prepare() fails,
  // calls HandleFailure.
  void PrepareTask();

  // Actually prepare.
  Status Prepare();

  // Submits ApplyTask to the apply pool.
  Status ApplyAsync();

  // Calls Op::Apply() followed by RaftConsensus::Commit() with the
  // results from the Apply().
  void ApplyTask();

  // Sleeps until the op is allowed to commit based on the
  // requested consistency mode.
  Status CommitWait();

  // Handle a failure in any of the stages of the operation.
  // In cases where we can recover or where the op's Tablet has been
  // stopped, this will end the operation and call its callback.
  // In others, where we can't recover, this will FATAL.
  void HandleFailure(const Status& s);

  // Called on Op::Apply() after the CommitMsg has been successfully
  // appended to the WAL.
  void Finalize();

  // Returns the mutable state of the op being executed by
  // this driver.
  OpState* mutable_state();

  // Return a short string indicating where the op currently is in the
  // state machine.
  static std::string StateString(ReplicationState repl_state,
                                 PrepareState prep_state);

  // Sets the timestamp on the response PB, if there is one.
  void SetResponseTimestamp(OpState* op_state,
                            const Timestamp& timestamp);

  // If this driver is executing a follower op then it is possible
  // it never went through the rpc system so we have to register it with the
  // ResultTracker.
  void RegisterFollowerOpOnResultTracker();

  OpTracker* const op_tracker_;
  consensus::RaftConsensus* const consensus_;
  log::Log* const log_;
  ThreadPoolToken* const prepare_pool_token_;
  ThreadPool* const apply_pool_;
  OpOrderVerifier* const order_verifier_;

  Status op_status_;

  // Lock that synchronizes access to the op's state.
  mutable simple_spinlock lock_;

  // A copy of the op's OpId, set when the op first
  // receives one from RaftConsensus and uninitialized until then.
  // TODO(todd): we have three separate copies of this now -- in OpState,
  // CommitMsg, and here... we should be able to consolidate!
  consensus::OpId op_id_copy_;

  // Lock that protects access to the driver's copy of the op_id, specifically.
  // GetOpId() is the only method expected to be called by threads outside
  // of the control of the driver, so we use a special lock to control access
  // otherwise callers would block for a long time for long running ops.
  mutable simple_spinlock opid_lock_;

  // The op to be executed by this driver.
  std::unique_ptr<Op> op_;

  // Trace object for tracing any ops started by this driver.
  scoped_refptr<Trace> trace_;

  const MonoTime start_time_;
  MonoTime replication_start_time_;

  ReplicationState replication_state_;
  PrepareState prepare_state_;

  // The system monotonic time when the operation was prepared.
  // This is used for debugging only, not any actual operation ordering.
  MicrosecondsInt64 prepare_physical_timestamp_;

  DISALLOW_COPY_AND_ASSIGN(OpDriver);
};

}  // namespace tablet
}  // namespace kudu

