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

// This file is the central location for defining the int128 type
// used by Kudu. Though this file is small it ensures flexibility
// as choices and standards around int128 change.
#pragma once

#include <iostream>

#include "kudu/gutil/mathlimits.h"

namespace kudu {

typedef unsigned __int128 uint128_t;
typedef signed __int128 int128_t;

// Note: We don't use numeric_limits because it can give incorrect
// values for __int128 and unsigned __int128.
static const uint128_t UINT128_MIN = MathLimits<uint128_t>::kMin;
static const uint128_t UINT128_MAX = MathLimits<uint128_t>::kMax;
static const int128_t INT128_MIN = MathLimits<int128_t>::kMin;
static const int128_t INT128_MAX = MathLimits<int128_t>::kMax;

namespace int128Suffix {

// Convert the characters 0 through 9 to their int value.
constexpr uint128_t CharValue(char c) {
  return c - '0';
}

// Terminate the recursion.
template <uint128_t VALUE>
constexpr uint128_t ValidateU128Helper() {
  return true;
}

// Recurse the literal from left to right and validate the input.
// Return true if the input is valid.
template <uint128_t VALUE, char C, char... CS>
constexpr bool ValidateU128Helper() {
  return (VALUE <= UINT128_MAX / 10) &&
         VALUE * 10 <= UINT128_MAX - CharValue(C) &&
         ValidateU128Helper<VALUE * 10 + CharValue(C), CS...>();
}

template <char... CS>
constexpr bool ValidateU128() {
  return ValidateU128Helper<0, CS...>();
}

// Terminate the recursion.
template <uint128_t VALUE>
constexpr uint128_t MakeU128Helper() {
  return VALUE;
}

// Recurse the literal from left to right calculating the resulting value.
// C is the current left most character, CS is the rest.
template <uint128_t VALUE, char C, char... CS>
constexpr uint128_t MakeU128Helper() {
  return MakeU128Helper<VALUE * 10 + CharValue(C), CS...>();
}

template <char... CS>
constexpr uint128_t MakeU128() {
  return MakeU128Helper<0, CS...>();
}

template <char... CS>
constexpr bool ValidateI128() {
  return ValidateU128Helper<0, CS...>() && MakeU128<CS...>() <= INT128_MAX;
}

}  // namespace int128Suffix

// User-defined literal "_u128" to support basic integer constants
// for uint128_t until officially supported by compilers.
template <char... CS>
constexpr uint128_t operator"" _u128() {
  static_assert(int128Suffix::ValidateU128<CS...>(),
                "integer literal is too large to be represented in "
                    "uint128_t integer type");
  return int128Suffix::MakeU128<CS...>();
}

// User-defined literal "_u128" to support basic integer constants
// for int128_t until officially supported by compilers.
template <char... CS>
constexpr int128_t operator"" _i128() {
  static_assert(int128Suffix::ValidateI128<CS...>(),
                "integer literal is too large to be represented in "
                    "int128_t integer type");
  return static_cast<int128_t>(int128Suffix::MakeU128<CS...>());
}

} // namespace kudu

namespace std {

  // Support the << operator on int128_t and uint128_t types.
  // We use __int128 here because these are not in the Kudu namespace.
  std::ostream& operator<<(std::ostream& os, const __int128& val);
  std::ostream& operator<<(std::ostream& os, const unsigned __int128& val);

} // namespace std
