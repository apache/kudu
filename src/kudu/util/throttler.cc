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

#include "kudu/util/throttler.h"

#include <algorithm>

#include <glog/logging.h>

namespace kudu {

Throttler::Throttler(uint64_t op_rate_per_sec,
                     uint64_t byte_rate_per_sec,
                     double burst_factor)
    : Throttler(MonoTime::Now(), op_rate_per_sec, byte_rate_per_sec, burst_factor) {
}

Throttler::Throttler(MonoTime now,
                     uint64_t op_rate_per_sec,
                     uint64_t byte_rate_per_sec,
                     double burst_factor)
    : byte_refill_(byte_rate_per_sec / (MonoTime::kMicrosecondsPerSecond / kRefillPeriodMicros)),
      byte_token_max_(static_cast<uint64_t>(byte_refill_ * burst_factor)),
      op_refill_(op_rate_per_sec / (MonoTime::kMicrosecondsPerSecond / kRefillPeriodMicros)),
      op_token_max_(static_cast<uint64_t>(op_refill_ * burst_factor)),
      byte_token_(0),
      op_token_(0),
      next_refill_(now) {
}

bool Throttler::Take(uint64_t ops, uint64_t bytes) {
  return Take(MonoTime::Now(), ops, bytes);
}

bool Throttler::Take(MonoTime now, uint64_t ops, uint64_t bytes) {
  DCHECK(ops > 0 || bytes > 0);
  if (op_refill_ == kNoLimit && byte_refill_ == kNoLimit) {
    return true;
  }

  std::lock_guard lock(lock_);
  Refill(now);
  if ((op_refill_ == 0 || ops <= op_token_) &&
      (byte_refill_ == 0 || bytes <= byte_token_)) {
    if (op_refill_ > 0) {
      op_token_ -= ops;
    }
    if (byte_refill_ > 0) {
      byte_token_ -= bytes;
    }
    return true;
  }
  return false;
}

void Throttler::Refill(MonoTime now) {
  int64_t d = (now - next_refill_).ToMicroseconds();
  if (d < 0) {
    return;
  }
  uint64_t num_period = d / kRefillPeriodMicros + 1;
  next_refill_ += MonoDelta::FromMicroseconds(num_period * kRefillPeriodMicros);
  op_token_ += num_period * op_refill_;
  op_token_ = std::min(op_token_, op_token_max_);
  byte_token_ += num_period * byte_refill_;
  byte_token_ = std::min(byte_token_, byte_token_max_);
}

} // namespace kudu
