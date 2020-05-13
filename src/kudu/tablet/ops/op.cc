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

#include "kudu/tablet/ops/op.h"

#include "kudu/rpc/result_tracker.h"

namespace kudu {
namespace tablet {

using consensus::DriverType;

Op::Op(DriverType type, OpType op_type)
    : type_(type),
      op_type_(op_type) {
}

OpState::OpState(TabletReplica* tablet_replica)
    : tablet_replica_(tablet_replica),
      completion_clbk_(new OpCompletionCallback()),
      timestamp_error_(0),
      arena_(1024),
      external_consistency_mode_(CLIENT_PROPAGATED) {
}

OpState::~OpState() {
}

OpCompletionCallback::OpCompletionCallback()
    : code_(tserver::TabletServerErrorPB::UNKNOWN_ERROR) {
}

void OpCompletionCallback::set_error(const Status& status,
                                     tserver::TabletServerErrorPB::Code code) {
  status_ = status;
  code_ = code;
}

void OpCompletionCallback::set_error(const Status& status) {
  status_ = status;
}

bool OpCompletionCallback::has_error() const {
  return !status_.ok();
}

const Status& OpCompletionCallback::status() const {
  return status_;
}

const tserver::TabletServerErrorPB::Code OpCompletionCallback::error_code() const {
  return code_;
}

void OpCompletionCallback::OpCompleted() {}

OpCompletionCallback::~OpCompletionCallback() {}

OpMetrics::OpMetrics()
  : successful_inserts(0),
    insert_ignore_errors(0),
    successful_upserts(0),
    successful_updates(0),
    successful_deletes(0),
    commit_wait_duration_usec(0) {
}

void OpMetrics::Reset() {
  successful_inserts = 0;
  insert_ignore_errors = 0;
  successful_upserts = 0;
  successful_updates = 0;
  successful_deletes = 0;
  commit_wait_duration_usec = 0;
}


}  // namespace tablet
}  // namespace kudu
