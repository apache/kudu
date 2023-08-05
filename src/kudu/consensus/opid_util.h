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
#include <iosfwd>
#include <string>

#include "kudu/consensus/opid.pb.h"

namespace kudu {
namespace consensus {

class ConsensusRequestPB;

// Minimum possible term.
extern const int64_t kMinimumTerm;

// Minimum possible log index.
extern const int64_t kMinimumOpIdIndex;

// Log index that is lower than the minimum index (and so will never occur).
extern const int64_t kInvalidOpIdIndex;

inline bool operator==(const OpId& lhs, const OpId& rhs) {
  return lhs.term() == rhs.term() && lhs.index() == rhs.index();
}

inline bool operator!=(const OpId& lhs, const OpId& rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const OpId& lhs, const OpId& rhs) {
  if (lhs.term() < rhs.term()) {
    return true;
  }
  if (lhs.term() > rhs.term()) {
    return false;
  }
  return lhs.index() < rhs.index();
}

inline bool operator<=(const OpId& lhs, const OpId& rhs) {
  return lhs < rhs || lhs == rhs;
}

inline bool operator>(const OpId& lhs, const OpId& rhs) {
  return !(lhs <= rhs);
}

inline bool operator>=(const OpId& lhs, const OpId& rhs) {
  return !(lhs < rhs);
}

std::ostream& operator<<(std::ostream& os, const consensus::OpId& op_id);

// Return the minimum possible OpId.
const OpId& MinimumOpId();

// Return the maximum possible OpId.
const OpId& MaximumOpId();

std::string OpIdToString(const OpId& id);

std::string OpsRangeString(const ConsensusRequestPB& req);

OpId MakeOpId(int64_t term, int64_t index);

}  // namespace consensus
}  // namespace kudu
