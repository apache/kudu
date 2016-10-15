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
#ifndef KUDU_CLIENT_SCAN_PREDICATE_H
#define KUDU_CLIENT_SCAN_PREDICATE_H

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#else
#include "kudu/client/stubs.h"
#endif

#include "kudu/util/kudu_export.h"

namespace kudu {
namespace client {

/// @brief A representation of comparison predicate for Kudu queries.
///
/// Call KuduTable::NewComparisonPredicate() to create a predicate object.
class KUDU_EXPORT KuduPredicate {
 public:
  /// @brief Supported comparison operators.
  enum ComparisonOp {
    LESS_EQUAL,
    GREATER_EQUAL,
    EQUAL,
    LESS,
    GREATER,
  };

  ~KuduPredicate();

  /// @return A new, identical, KuduPredicate object.
  KuduPredicate* Clone() const;

  /// @brief Forward declaration for the embedded PIMPL class.
  ///
  /// The PIMPL class has to be public since it's actually just an interface,
  /// and gcc gives an error trying to derive from a private nested class.
  class KUDU_NO_EXPORT Data;
 private:
  friend class ComparisonPredicateData;
  friend class ErrorPredicateData;
  friend class InListPredicateData;
  friend class IsNotNullPredicateData;
  friend class IsNullPredicateData;
  friend class KuduTable;
  friend class ScanConfiguration;

  explicit KuduPredicate(Data* d);

  Data* data_;
  DISALLOW_COPY_AND_ASSIGN(KuduPredicate);
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCAN_PREDICATE_H
