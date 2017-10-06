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

// Utility functions for working with the primary key portion of a row.

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/port.h"

namespace kudu {

class Arena;
class ColumnPredicate;
class ColumnSchema;
class ContiguousRow;

namespace key_util {

// Increments the primary key with the provided column indices to the smallest
// key which is greater than the current key.
//
// For example, for a composite key with types (INT8, INT8), incrementing
// the row (1, 1) will result in (1, 2). Incrementing (1, 127) will result
// in (2, -128).
//
// Note that not all keys may be incremented without overflow. For example,
// if the key is an INT8, and the key is already set to '127', incrementing
// would overflow. In this case, the value is incremented and overflowed, but
// the function returns 'false' to indicate the overflow condition. Otherwise,
// returns 'true'.
//
// String and binary types are incremented by appending a '\0' byte to the end.
// Since our string and binary types have unbounded length, this implies that if
// a key has a string or binary component, it will always be incremented.
//
// For the case of incrementing string or binary types, we allocate a new copy
// from 'arena', which must be non-NULL.
//
// REQUIRES: all primary key columns must be initialized.
bool IncrementPrimaryKey(ContiguousRow* row, Arena* arena) WARN_UNUSED_RESULT;

// Like 'IncrementPrimaryKey(ContiguousRow*, Arena*)', but only increments up to
// 'num_columns' columns of the primary key prefix.
bool IncrementPrimaryKey(ContiguousRow* row, int32_t num_columns, Arena* arena) WARN_UNUSED_RESULT;

// Increments the provided cell in place.
bool IncrementCell(const ColumnSchema& col, void* cell_ptr, Arena* arena);

// Decrements the provided cell. If fail, the result will not be updated.
bool TryDecrementCell(const ColumnSchema &col, void *cell_ptr);

// Pushes lower bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushLowerBoundKeyPredicates(
    const std::vector<int32_t>& col_idxs,
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

// Pushes upper bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushUpperBoundKeyPredicates(
    const std::vector<int32_t>& col_idxs,
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

// Pushes lower bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushLowerBoundPrimaryKeyPredicates(
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

// Pushes upper bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushUpperBoundPrimaryKeyPredicates(
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

// Appends the encoded key into the buffer.
void EncodeKey(const std::vector<int32_t>& col_idxs,
               const ContiguousRow& row,
               std::string* buffer);

} // namespace key_util
} // namespace kudu
