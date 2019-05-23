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

#include "kudu/tablet/mutation.h"

#include <string>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/strings/strcat.h"

namespace kudu {

class Schema;

namespace tablet {

std::string Mutation::StringifyMutationList(const Schema &schema, const Mutation *head) {
  std::string ret;

  ret.append("[");

  bool first = true;
  while (head != nullptr) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    StrAppend(&ret, "@", head->timestamp().ToString(), "(");
    ret.append(head->changelist().ToString(schema));
    ret.append(")");

    head = head->acquire_next();
  }

  ret.append("]");
  return ret;
}

void Mutation::AppendToListAtomic(Mutation** redo_head, Mutation** redo_tail) {
  next_ = nullptr;
  if (*redo_tail == nullptr) {
    Release_Store(reinterpret_cast<AtomicWord*>(redo_head),
                  reinterpret_cast<AtomicWord>(this));
    Release_Store(reinterpret_cast<AtomicWord*>(redo_tail),
                  reinterpret_cast<AtomicWord>(this));
  } else {
    Release_Store(reinterpret_cast<AtomicWord*>(&(*redo_tail)->next_),
                  reinterpret_cast<AtomicWord>(this));
    Release_Store(reinterpret_cast<AtomicWord*>(redo_tail),
                  reinterpret_cast<AtomicWord>((*redo_tail)->next_));
  }
}

} // namespace tablet
} // namespace kudu
