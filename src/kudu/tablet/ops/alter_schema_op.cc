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

#include "kudu/tablet/ops/alter_schema_op.h"

#include <memory>
#include <ostream>
#include <utility>

#include <glog/logging.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tablet {

using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::ALTER_SCHEMA_OP;
using consensus::DriverType;
using pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using strings::Substitute;
using tserver::TabletServerErrorPB;

void AlterSchemaOpState::AcquireSchemaLock(rw_semaphore* l) {
  TRACE("Acquiring schema lock in exclusive mode");
  schema_lock_ = std::unique_lock<rw_semaphore>(*l);
  TRACE("Acquired schema lock");
}

void AlterSchemaOpState::ReleaseSchemaLock() {
  CHECK(schema_lock_.owns_lock());
  schema_lock_ = std::unique_lock<rw_semaphore>();
  TRACE("Released schema lock");
}

void AlterSchemaOpState::SetError(const Status& s) {
  CHECK(!s.ok()) << "Expected an error status";
  error_ = OperationResultPB();
  StatusToPB(s, error_->mutable_failed_status());
}

string AlterSchemaOpState::ToString() const {
  return Substitute("AlterSchemaOpState "
                    "[timestamp=$0, schema=$1, request=$2, error=$3]",
                    has_timestamp() ? timestamp().ToString() : "<unassigned>",
                    schema_ == nullptr ? "(none)" : schema_->ToString(),
                    request_ == nullptr ? "(none)" : SecureShortDebugString(*request_),
                    error_ ? SecureShortDebugString(error_->failed_status()) : "(none)");
}

AlterSchemaOp::AlterSchemaOp(unique_ptr<AlterSchemaOpState> state,
                             DriverType type)
    : Op(type, Op::ALTER_SCHEMA_OP),
      state_(std::move(state)) {
}

void AlterSchemaOp::NewReplicateMsg(unique_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(consensus::OperationType::ALTER_SCHEMA_OP);
  (*replicate_msg)->mutable_alter_schema_request()->CopyFrom(*state()->request());
}

Status AlterSchemaOp::Prepare() {
  TRACE("PREPARE ALTER-SCHEMA: Starting");

  // Decode schema
  unique_ptr<Schema> schema(new Schema);
  Status s = SchemaFromPB(state_->request()->schema(), schema.get());
  if (!s.ok()) {
    state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }

  Tablet* tablet = state_->tablet_replica()->tablet();
  RETURN_NOT_OK(tablet->CreatePreparedAlterSchema(state(), schema.get()));

  state_->AddToAutoReleasePool(schema.release());

  TRACE("PREPARE ALTER-SCHEMA: finished");
  return s;
}

Status AlterSchemaOp::Start() {
  DCHECK(!state_->has_timestamp());
  DCHECK(state_->consensus_round()->replicate_msg()->has_timestamp());
  state_->set_timestamp(Timestamp(state_->consensus_round()->replicate_msg()->timestamp()));
  TRACE("START. Timestamp: $0", clock::HybridClock::GetPhysicalValueMicros(state_->timestamp()));
  return Status::OK();
}

Status AlterSchemaOp::Apply(unique_ptr<CommitMsg>* commit_msg) {
  TRACE("APPLY ALTER-SCHEMA: Starting");

  Tablet* tablet = state_->tablet_replica()->tablet();
  RETURN_NOT_OK(tablet->AlterSchema(state()));

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(consensus::OperationType::ALTER_SCHEMA_OP);

  // If there was a logical error (e.g. bad schema version) with the alter,
  // record the error and exit.
  if (state_->error()) {
    TxResultPB* result = (*commit_msg)->mutable_result();
    *result->add_ops() = std::move(*state_->error());
    return Status::OK();
  }

  state_->tablet_replica()->log()
    ->SetSchemaForNextLogSegment(*DCHECK_NOTNULL(state_->schema()),
                                 state_->schema_version());

  // Altered tablets should be included in the next tserver heartbeat so that
  // clients waiting on IsAlterTableDone() are unblocked promptly.
  state_->tablet_replica()->MarkTabletDirty("Alter schema finished");
  return Status::OK();
}

void AlterSchemaOp::Finish(OpResult result) {
  if (PREDICT_FALSE(result == Op::ABORTED)) {
    TRACE("AlterSchemaCommitCallback: op aborted");
    state()->Finish();
    return;
  }

  // The schema lock was acquired by Tablet::CreatePreparedAlterSchema.
  // Normally, we would release it in tablet.cc after applying the operation,
  // but currently we need to wait until after the COMMIT message is logged
  // to release this lock as a workaround for KUDU-915. See the TODO in
  // Tablet::AlterSchema().
  state()->ReleaseSchemaLock();

  DCHECK_EQ(result, Op::COMMITTED);
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("AlterSchemaCommitCallback: making alter schema visible");
  state()->Finish();
}

string AlterSchemaOp::ToString() const {
  return Substitute("AlterSchemaOp [state=$0]", state_->ToString());
}

}  // namespace tablet
}  // namespace kudu
