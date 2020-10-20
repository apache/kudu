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

#include "kudu/common/txn_id.h"

#include <ostream> // IWYU pragma: keep

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"

namespace kudu {

const TxnId TxnId::kInvalidTxnId;

TxnId::TxnId(int64_t id) noexcept
    : id_(id) {
  DCHECK_GE(id, 0) << "negative value is not allowed for TxnId";
}

int64_t TxnId::value() const {
  DCHECK(IsValid()) << "TxnId contains an invalid value";
  return id_;
}

std::string TxnId::ToString() const {
  return IsValid() ? strings::Substitute("$0", id_) : "InvalidTxnId";
}

std::ostream& operator<<(std::ostream& o, const TxnId& id) {
  return o << id.ToString();
}

} // namespace kudu
