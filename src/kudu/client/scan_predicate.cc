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

#include "kudu/client/scan_predicate.h"

#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/value-internal.h"
#include "kudu/client/value.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using boost::optional;
using std::move;
using std::vector;
using strings::Substitute;

namespace kudu {

class Arena;

namespace client {

KuduPredicate::KuduPredicate(Data* d)
  : data_(d) {
}

KuduPredicate::~KuduPredicate() {
  delete data_;
}

KuduPredicate::Data::Data() {
}

KuduPredicate::Data::~Data() {
}

KuduPredicate* KuduPredicate::Clone() const {
  return new KuduPredicate(data_->Clone());
}

ComparisonPredicateData::ComparisonPredicateData(ColumnSchema col,
                                                 KuduPredicate::ComparisonOp op,
                                                 KuduValue* val)
    : col_(move(col)),
      op_(op),
      val_(val) {
}

ComparisonPredicateData::~ComparisonPredicateData() {
}

Status ComparisonPredicateData::AddToScanSpec(ScanSpec* spec, Arena* arena) {
  void* val_void;
  RETURN_NOT_OK(val_->data_->CheckTypeAndGetPointer(col_.name(),
                                                    col_.type_info()->type(),
                                                    col_.type_attributes(),
                                                    &val_void));
  switch (op_) {
    case KuduPredicate::LESS_EQUAL: {
      optional<ColumnPredicate> pred =
        ColumnPredicate::InclusiveRange(col_, nullptr, val_void, arena);
      if (pred) {
        spec->AddPredicate(*pred);
      }
      break;
    };
    case KuduPredicate::GREATER_EQUAL: {
      spec->AddPredicate(ColumnPredicate::Range(col_, val_void, nullptr));
      break;
    };
    case KuduPredicate::EQUAL: {
      spec->AddPredicate(ColumnPredicate::Equality(col_, val_void));
      break;
    };
    case KuduPredicate::LESS: {
      spec->AddPredicate(ColumnPredicate::Range(col_, nullptr, val_void));
      break;
    };
    case KuduPredicate::GREATER: {
      optional<ColumnPredicate> pred =
        ColumnPredicate::ExclusiveRange(col_, val_void, nullptr, arena);
      if (pred) {
        spec->AddPredicate(*pred);
      }
      break;
    };
    default:
      return Status::InvalidArgument(Substitute("invalid comparison op: $0", op_));
  }
  return Status::OK();
}

InListPredicateData::InListPredicateData(ColumnSchema col,
                                         vector<KuduValue*>* values)
    : col_(move(col)) {
  vals_.swap(*values);
}

InListPredicateData::~InListPredicateData() {
  STLDeleteElements(&vals_);
}

Status InListPredicateData::AddToScanSpec(ScanSpec* spec, Arena* /*arena*/) {
  vector<const void*> vals_list;
  vals_list.reserve(vals_.size());
  for (auto value : vals_) {
    void* val_void;
    // The local vals_ list consists of KuduValue pointers that make up the IN
    // list predicate. For every value in the vals_ list a call to
    // CheckTypeAndGetPointer is made to get a local pointer (void*) to the
    // underlying value. The local list (vals_list) of all the void* pointers is
    // passed to the ColumnPredicate::InList constructor. The constructor for
    // ColumnPredicate::InList will assume ownership of the pointers via a swap.
    RETURN_NOT_OK(value->data_->CheckTypeAndGetPointer(col_.name(),
                                                       col_.type_info()->type(),
                                                       col_.type_attributes(),
                                                       &val_void));
    vals_list.push_back(val_void);
  }

  spec->AddPredicate(ColumnPredicate::InList(col_, &vals_list));

  return Status::OK();
}

} // namespace client
} // namespace kudu
