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

#include <boost/optional/optional.hpp>

#include "kudu/common/common.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;
class rw_semaphore;

namespace tablet {

class TabletReplica;

// Op Context for the AlterSchema operation.
// Keeps track of the Op states (request, result, ...)
class AlterSchemaOpState : public OpState {
 public:
  ~AlterSchemaOpState() {
  }

  AlterSchemaOpState(TabletReplica* tablet_replica,
                     const tserver::AlterSchemaRequestPB* request,
                     tserver::AlterSchemaResponsePB* response)
      : OpState(tablet_replica),
        schema_(nullptr),
        request_(request),
        response_(response) {
  }

  const tserver::AlterSchemaRequestPB* request() const override { return request_; }
  tserver::AlterSchemaResponsePB* response() const override { return response_; }

  void set_schema(const Schema* schema) { schema_ = schema; }
  const Schema* schema() const { return schema_; }

  std::string new_table_name() const {
    return request_->new_table_name();
  }

  bool has_new_table_name() const {
    return request_->has_new_table_name();
  }

  TableExtraConfigPB new_extra_config() const {
    return request_->new_extra_config();
  }

  bool has_new_extra_config() const {
    return request_->has_new_extra_config();
  }

  uint32_t schema_version() const {
    return request_->schema_version();
  }

  void AcquireSchemaLock(rw_semaphore* l);

  // Release the acquired schema lock.
  // Crashes if the lock was not already acquired.
  void ReleaseSchemaLock();

  // Note: request_ and response_ are set to null after this method returns.
  void Finish() {
    // Make the request null since after this op commits
    // the request may be deleted at any moment.
    request_ = nullptr;
    response_ = nullptr;
  }

  // Sets the fact that the alter had an error.
  void SetError(const Status& s);

  boost::optional<OperationResultPB> error() const {
    return error_;
  }

  std::string ToString() const override;

 private:
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaOpState);

  // The new (target) Schema.
  const Schema* schema_;

  // The original RPC request and response.
  const tserver::AlterSchemaRequestPB *request_;
  tserver::AlterSchemaResponsePB *response_;

  // The lock held on the tablet's schema_lock_.
  std::unique_lock<rw_semaphore> schema_lock_;

  // The error result of this alter schema op. May be empty if the
  // op hasn't been applied or if the alter succeeded.
  boost::optional<OperationResultPB> error_;
};

// Executes the alter schema op.
class AlterSchemaOp : public Op {
 public:
  AlterSchemaOp(std::unique_ptr<AlterSchemaOpState> state,
                consensus::DriverType type);

  AlterSchemaOpState* state() override { return state_.get(); }
  const AlterSchemaOpState* state() const override { return state_.get(); }

  void NewReplicateMsg(std::unique_ptr<consensus::ReplicateMsg>* replicate_msg) override;

  // Executes a Prepare for the alter schema op.
  Status Prepare() override;

  // Starts the AlterSchemaOp by assigning it a timestamp.
  Status Start() override;

  // Executes an Apply for the alter schema op
  Status Apply(std::unique_ptr<consensus::CommitMsg>* commit_msg) override;

  // Actually commits the op.
  void Finish(OpResult result) override;

  std::string ToString() const override;

 private:
  std::unique_ptr<AlterSchemaOpState> state_;
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaOp);
};

}  // namespace tablet
}  // namespace kudu

