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

#include <vector>

#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/ops/participant_op.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

constexpr const int64_t kDummyCommitTimestamp = 1337;
const std::vector<tserver::ParticipantOpPB::ParticipantOpType> kCommitSequence = {
  tserver::ParticipantOpPB::BEGIN_TXN,
  tserver::ParticipantOpPB::BEGIN_COMMIT,
  tserver::ParticipantOpPB::FINALIZE_COMMIT,
};

std::unique_ptr<ParticipantOpState> NewParticipantOp(
    TabletReplica* replica,
    int64_t txn_id,
    tserver::ParticipantOpPB::ParticipantOpType type,
    int64_t finalized_commit_timestamp,
    tserver::ParticipantRequestPB* req,
    tserver::ParticipantResponsePB* resp) {
  auto* op = req->mutable_op();
  op->set_txn_id(txn_id);
  op->set_type(type);
  if (type == tserver::ParticipantOpPB::FINALIZE_COMMIT) {
    op->set_finalized_commit_timestamp(finalized_commit_timestamp);
  }
  std::unique_ptr<ParticipantOpState> op_state(new ParticipantOpState(
      replica,
      replica->tablet()->txn_participant(),
      req,
      resp));
  return op_state;
}

Status CallParticipantOp(TabletReplica* replica,
                         int64_t txn_id,
                         tserver::ParticipantOpPB::ParticipantOpType type,
                         int64_t finalized_commit_timestamp,
                         tserver::ParticipantResponsePB* resp) {
  tserver::ParticipantRequestPB req;
  std::unique_ptr<ParticipantOpState> op_state =
    NewParticipantOp(replica, txn_id, type, finalized_commit_timestamp, &req, resp);
  CountDownLatch latch(1);
  op_state->set_completion_callback(std::unique_ptr<OpCompletionCallback>(
      new LatchOpCompletionCallback<tserver::ParticipantResponsePB>(&latch, resp)));
  RETURN_NOT_OK(replica->SubmitTxnParticipantOp(std::move(op_state)));
  latch.Wait();
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
