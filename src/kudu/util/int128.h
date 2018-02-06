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

// __int128 is not supported before gcc 4.6
#if defined(__clang__) || \
  (defined(__GNUC__) && \
  (__GNUC__ * 10000 + __GNUC_MINOR__ * 100) >= 40600)
#define KUDU_INT128_SUPPORTED 1
#else
#define KUDU_INT128_SUPPORTED 0
#endif

#if KUDU_INT128_SUPPORTED
namespace kudu {

typedef unsigned __int128 uint128_t;
typedef signed __int128 int128_t;

// Note: We don't use numeric_limits because it can give incorrect
// values for __int128 and unsigned __int128.
static const uint128_t UINT128_MIN = (uint128_t) 0;
static const uint128_t UINT128_MAX = ((uint128_t) -1);
static const int128_t INT128_MAX = ((int128_t)(UINT128_MAX >> 1));
static const int128_t INT128_MIN = (-INT128_MAX - 1);

} // namespace kudu
#endif
