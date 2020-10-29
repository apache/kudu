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

namespace kudu {

// A representation of transaction identifier.
class TxnId {
 public:
  // An invalid transaction identifier: the default constructed instances are
  // initialized to this value.
  static const TxnId kInvalidTxnId;

  TxnId() noexcept : id_(-1) {}
  TxnId(int64_t id) noexcept; // NOLINT

  int64_t value() const;
  bool IsValid() const { return id_ >= 0; }
  std::string ToString() const;

  // Cast operator: returns TxnId's int64_t value.
  operator int64_t () const { return value(); }
  bool operator!() const { return !IsValid(); }

 private:
  friend bool operator==(const TxnId& lhs, const TxnId& rhs);
  friend bool operator!=(const TxnId& lhs, const TxnId& rhs);
  friend bool operator<(const TxnId& lhs, const TxnId& rhs);
  friend bool operator<=(const TxnId& lhs, const TxnId& rhs);
  friend bool operator>(const TxnId& lhs, const TxnId& rhs);
  friend bool operator>=(const TxnId& lhs, const TxnId& rhs);

  int64_t id_;
};

std::ostream& operator<<(std::ostream& o, const TxnId& txn_id);

inline bool operator==(const TxnId& lhs, const TxnId& rhs) {
  return lhs.id_ == rhs.id_;
}

inline bool operator!=(const TxnId& lhs, const TxnId& rhs) {
  return !(lhs.id_ == rhs.id_);
}

inline bool operator<(const TxnId& lhs, const TxnId& rhs) {
  return lhs.id_ < rhs.id_;
}

inline bool operator>(const TxnId& lhs, const TxnId& rhs) {
  return rhs < lhs;
}

inline bool operator<=(const TxnId& lhs, const TxnId& rhs) {
  return !(lhs > rhs);
}

inline bool operator>=(const TxnId& lhs, const TxnId& rhs) {
  return !(lhs < rhs);
}

} // namespace kudu
