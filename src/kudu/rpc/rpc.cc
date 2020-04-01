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

#include "kudu/rpc/rpc.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"

using std::string;
using strings::Substitute;
using strings::SubstituteAndAppend;

namespace kudu {

namespace rpc {

MonoDelta ComputeExponentialBackoff(int num_attempts) {
  return MonoDelta::FromMilliseconds(
      (10 + rand() % 10) * static_cast<int>(
          std::pow(2.0, std::min(8, num_attempts - 1))));
}

void RpcRetrier::DelayedRetry(Rpc* rpc, const Status& why_status) {
  if (!why_status.ok() && (last_error_.ok() || last_error_.IsTimedOut())) {
    last_error_ = why_status;
  }
  // If the delay causes us to miss our deadline, RetryCb will fail the
  // RPC on our behalf.
  MonoDelta backoff = ComputeBackoff(attempt_num_++);
  messenger_->ScheduleOnReactor(
      [this, rpc](const Status& s) { this->DelayedRetryCb(rpc, s); }, backoff);
}

MonoDelta RpcRetrier::ComputeBackoff(int num_attempts) const {
  if (backoff_ == BackoffType::LINEAR) {
    return MonoDelta::FromMilliseconds(num_attempts + ((rand() % 5)));
  }
  DCHECK(BackoffType::EXPONENTIAL == backoff_);
  return ComputeExponentialBackoff(num_attempts);
}

void RpcRetrier::DelayedRetryCb(Rpc* rpc, const Status& status) {
  Status new_status = status;
  if (new_status.ok()) {
    // Has this RPC timed out?
    if (deadline_.Initialized()) {
      if (MonoTime::Now() > deadline_) {
        string err_str = Substitute("$0 passed its deadline", rpc->ToString());
        if (!last_error_.ok()) {
          SubstituteAndAppend(&err_str, ": $0", last_error_.ToString());
        }
        new_status = Status::TimedOut(err_str);
      }
    }
  }
  if (new_status.ok()) {
    controller_.Reset();
    rpc->SendRpc();
  } else {
    rpc->SendRpcCb(new_status);
  }
}

} // namespace rpc
} // namespace kudu
