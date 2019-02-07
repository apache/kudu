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

#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
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

// Transaction Context for the AlterSchema operation.
// Keeps track of the Transaction states (request, result, ...)
class AlterSchemaTransactionState : public TransactionState {
 public:
  ~AlterSchemaTransactionState() {
  }

  AlterSchemaTransactionState(TabletReplica* tablet_replica,
                              const tserver::AlterSchemaRequestPB* request,
                              tserver::AlterSchemaResponsePB* response)
      : TransactionState(tablet_replica),
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

  uint32_t schema_version() const {
    return request_->schema_version();
  }

  void AcquireSchemaLock(rw_semaphore* l);

  // Release the acquired schema lock.
  // Crashes if the lock was not already acquired.
  void ReleaseSchemaLock();

  // Note: request_ and response_ are set to null after this method returns.
  void Finish() {
    // Make the request null since after this transaction commits
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
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaTransactionState);

  // The new (target) Schema.
  const Schema* schema_;

  // The original RPC request and response.
  const tserver::AlterSchemaRequestPB *request_;
  tserver::AlterSchemaResponsePB *response_;

  // The lock held on the tablet's schema_lock_.
  std::unique_lock<rw_semaphore> schema_lock_;

  // The error result of this alter schema transaction. May be empty if the
  // transaction hasn't been applied or if the alter succeeded.
  boost::optional<OperationResultPB> error_;
};

// Executes the alter schema transaction,.
class AlterSchemaTransaction : public Transaction {
 public:
  AlterSchemaTransaction(std::unique_ptr<AlterSchemaTransactionState> state,
                         consensus::DriverType type);

  AlterSchemaTransactionState* state() override { return state_.get(); }
  const AlterSchemaTransactionState* state() const override { return state_.get(); }

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg) override;

  // Executes a Prepare for the alter schema transaction.
  Status Prepare() override;

  // Starts the AlterSchemaTransaction by assigning it a timestamp.
  Status Start() override;

  // Executes an Apply for the alter schema transaction
  Status Apply(gscoped_ptr<consensus::CommitMsg>* commit_msg) override;

  // Actually commits the transaction.
  void Finish(TransactionResult result) override;

  std::string ToString() const override;

 private:
  std::unique_ptr<AlterSchemaTransactionState> state_;
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaTransaction);
};

}  // namespace tablet
}  // namespace kudu

