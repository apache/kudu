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

#include <map>
#include <string>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

class TimeManager;

// Tracks the pending consensus rounds being managed by a Raft replica (either leader
// or follower).
//
// This class is not thread-safe.
//
// TODO(todd): this class inconsistently uses the term "round", "op", and "transaction".
// We should consolidate to "round".
class PendingRounds {
 public:
  PendingRounds(std::string log_prefix, scoped_refptr<TimeManager> time_manager);
  ~PendingRounds();

  // Set the committed op during startup. This should be done after
  // appending any of the pending transactions, and will take care
  // of triggering any that are now considered committed.
  Status SetInitialCommittedOpId(const OpId& committed_op);

  // Returns the the ConsensusRound with the provided index, if there is any, or NULL
  // if there isn't.
  scoped_refptr<ConsensusRound> GetPendingOpByIndexOrNull(int64_t index);

  // Add 'round' to the set of rounds waiting to be committed.
  Status AddPendingOperation(const scoped_refptr<ConsensusRound>& round);

  // Advances the committed index.
  // This is a no-op if the committed index has not changed.
  Status AdvanceCommittedIndex(int64_t committed_index);

  // Aborts pending operations after, but not including 'index'. The OpId with 'index'
  // will become our new last received id. If there are pending operations with indexes
  // higher than 'index' those operations are aborted.
  void AbortOpsAfter(int64_t index);

  // Returns true if an operation is in this replica's log, namely:
  // - If the op's index is lower than or equal to our committed index
  // - If the op id matches an inflight op.
  // If an operation with the same index is in our log but the terms
  // are different 'term_mismatch' is set to true, it is false otherwise.
  bool IsOpCommittedOrPending(const OpId& op_id, bool* term_mismatch);

  // Returns the id of the latest pending transaction (i.e. the one with the
  // latest index). This must be called under the lock.
  OpId GetLastPendingTransactionOpId() const;

  // Used by replicas to cancel pending transactions. Pending transaction are those
  // that have completed prepare/replicate but are waiting on the LEADER's commit
  // to complete. This does not cancel transactions being applied.
  Status CancelPendingTransactions();

  // Returns the number of transactions that are currently in the pending state
  // i.e. transactions for which Prepare() is done or under way.
  int GetNumPendingTxns() const;

  // Returns the watermark below which all operations are known to
  // be committed according to consensus.
  // TODO(todd): these should probably be removed in favor of using the queue.
  int64_t GetCommittedIndex() const;
  int64_t GetTermWithLastCommittedOp() const;

  // Checks that 'current' correctly follows 'previous'. Specifically it checks
  // that the term is the same or higher and that the index is sequential.
  static Status CheckOpInSequence(const OpId& previous, const OpId& current);

 private:
  const std::string& LogPrefix() const { return log_prefix_; }

  const std::string log_prefix_;

  // Index=>Round map that manages pending ops, i.e. operations for which we've
  // received a replicate message from the leader but have yet to be committed.
  // The key is the index of the replicate operation.
  typedef std::map<int64_t, scoped_refptr<ConsensusRound> > IndexToRoundMap;
  IndexToRoundMap pending_txns_;

  // The OpId of the round that was last committed. Initialized to MinimumOpId().
  OpId last_committed_op_id_;

  scoped_refptr<TimeManager> time_manager_;

  DISALLOW_COPY_AND_ASSIGN(PendingRounds);
};

}  // namespace consensus
}  // namespace kudu
