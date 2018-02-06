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
#ifndef KUDU_CLIENT_VALUE_H
#define KUDU_CLIENT_VALUE_H

#include <stdint.h>

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#else
#include "kudu/client/stubs.h"
#endif
#include "kudu/util/int128.h"
#include "kudu/util/kudu_export.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace client {

/// @brief A constant cell value with a specific type.
class KUDU_EXPORT KuduValue {
 public:
  /// @return A new identical KuduValue object.
  KuduValue* Clone() const;

  /// @name Builders from integral types.
  ///
  /// Construct a KuduValue object from the given value of integral type.
  ///
  /// @param [in] val
  ///   The value to build the KuduValue from.
  /// @return A new KuduValue object.
  ///
  ///@{
  static KuduValue* FromInt(int64_t val);
  static KuduValue* FromFloat(float f);
  static KuduValue* FromDouble(double d);
  static KuduValue* FromBool(bool b);
  ///@}

#if KUDU_INT128_SUPPORTED
  /// Construct a decimal KuduValue from the raw value and scale.
  ///
  /// The validity of the decimal value is not checked until the
  /// KuduValue is used by Kudu.
  ///
  /// @param [in] dv
  ///   The raw decimal value to build the KuduValue from.
  /// @param [in] scale
  ///   The decimal value's scale. (Must match the column's scale exactly)
  /// @return A new KuduValue object.
  ///@{
  static KuduValue* FromDecimal(int128_t dv, int8_t scale);
///@}
#endif

  /// Construct a KuduValue by copying the value of the given Slice.
  ///
  /// @param [in] s
  ///   The slice to copy value from.
  /// @return A new KuduValue object.
  static KuduValue* CopyString(Slice s);

  ~KuduValue();
 private:
  friend class ComparisonPredicateData;
  friend class InListPredicateData;
  friend class KuduColumnSpec;

  class KUDU_NO_EXPORT Data;
  explicit KuduValue(Data* d);

  // Owned.
  Data* data_;

 private:
  DISALLOW_COPY_AND_ASSIGN(KuduValue);
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_VALUE_H */
