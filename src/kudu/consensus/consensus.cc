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

#include "kudu/consensus/consensus.h"

#include <set>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace consensus {

using std::shared_ptr;
using strings::Substitute;

ConsensusBootstrapInfo::ConsensusBootstrapInfo()
  : last_id(MinimumOpId()),
    last_committed_id(MinimumOpId()) {
}

ConsensusBootstrapInfo::~ConsensusBootstrapInfo() {
  STLDeleteElements(&orphaned_replicates);
}

ConsensusRound::ConsensusRound(Consensus* consensus,
                               gscoped_ptr<ReplicateMsg> replicate_msg,
                               ConsensusReplicatedCallback replicated_cb)
    : consensus_(consensus),
      replicate_msg_(new RefCountedReplicate(replicate_msg.release())),
      replicated_cb_(std::move(replicated_cb)),
      bound_term_(-1) {}

ConsensusRound::ConsensusRound(Consensus* consensus,
                               const ReplicateRefPtr& replicate_msg)
    : consensus_(consensus),
      replicate_msg_(replicate_msg),
      bound_term_(-1) {
  DCHECK(replicate_msg_);
}

void ConsensusRound::NotifyReplicationFinished(const Status& status) {
  if (PREDICT_FALSE(replicated_cb_.is_null())) return;
  replicated_cb_.Run(status);
}

Status ConsensusRound::CheckBoundTerm(int64_t current_term) const {
  if (PREDICT_FALSE(bound_term_ != -1 &&
                    bound_term_ != current_term)) {
    return Status::Aborted(
      strings::Substitute(
        "Transaction submitted in term $0 cannot be replicated in term $1",
        bound_term_, current_term));
  }
  return Status::OK();
}

scoped_refptr<ConsensusRound> Consensus::NewRound(
    gscoped_ptr<ReplicateMsg> replicate_msg,
    const ConsensusReplicatedCallback& replicated_cb) {
  return make_scoped_refptr(new ConsensusRound(this, std::move(replicate_msg), replicated_cb));
}

} // namespace consensus
} // namespace kudu
