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

#include "kudu/consensus/opid_util.h"

#include <limits>
#include <mutex>
#include <ostream>

#include <glog/logging.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace consensus {

const int64_t kMinimumTerm = 0;
const int64_t kMinimumOpIdIndex = 0;
const int64_t kInvalidOpIdIndex = -1;

const OpId& MinimumOpId() {
  static std::once_flag once;
  static OpId kMinOpId;
  std::call_once(once, [&]() {
    kMinOpId.set_term(0);
    kMinOpId.set_index(0);
  });

  return kMinOpId;
}

const OpId& MaximumOpId() {
  static std::once_flag once;
  static OpId kMaxOpId;
  std::call_once(once, [&] {
    kMaxOpId.set_term(std::numeric_limits<int64_t>::max());
    kMaxOpId.set_index(std::numeric_limits<int64_t>::max());
  });

  return kMaxOpId;
}

std::ostream& operator<<(std::ostream& os, const consensus::OpId& op_id) {
  os << OpIdToString(op_id);
  return os;
}

std::string OpIdToString(const OpId& id) {
  if (!id.IsInitialized()) {
    return "<uninitialized op>";
  }
  return strings::Substitute("$0.$1", id.term(), id.index());
}

std::string OpsRangeString(const ConsensusRequestPB& req) {
  std::string ret;
  ret.reserve(82);  // 4 * 19 + 3 + 2 + 1
  ret.push_back('[');
  if (req.ops_size() > 0) {
    const OpId& first_op = req.ops(0).id();
    const OpId& last_op = req.ops(req.ops_size() - 1).id();
    strings::SubstituteAndAppend(&ret, "$0.$1-$2.$3",
                                 first_op.term(), first_op.index(),
                                 last_op.term(), last_op.index());
  }
  ret.push_back(']');
  return ret;
}

OpId MakeOpId(int64_t term, int64_t index) {
  CHECK_GE(term, 0);
  CHECK_GE(index, 0);
  OpId ret;
  ret.set_index(index);
  ret.set_term(term);
  return ret;
}

} // namespace consensus
} // namespace kudu
