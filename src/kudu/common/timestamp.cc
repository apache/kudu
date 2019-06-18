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

#include "kudu/common/timestamp.h"

#include <ostream> // IWYU pragma: keep

#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/substitute.h"

namespace kudu {

const Timestamp Timestamp::kMin(MathLimits<Timestamp::val_type>::kMin);
const Timestamp Timestamp::kMax(MathLimits<Timestamp::val_type>::kMax);
const Timestamp Timestamp::kInitialTimestamp(MathLimits<Timestamp::val_type>::kMin + 1);
const Timestamp Timestamp::kInvalidTimestamp(MathLimits<Timestamp::val_type>::kMax - 1);

void Timestamp::EncodeTo(faststring* dst) const {
  PutMemcmpableVarint64(dst, v);
}

std::string Timestamp::ToString() const {
  return strings::Substitute("$0", v);
}

uint64_t Timestamp::ToUint64() const {
  return v;
}

void Timestamp::FromUint64(uint64_t value) {
  v = value;
}

std::ostream& operator<<(std::ostream& o, const Timestamp& timestamp) {
  return o << timestamp.ToString();
}

}  // namespace kudu
