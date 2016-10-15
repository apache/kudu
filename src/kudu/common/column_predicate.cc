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

#include "kudu/common/column_predicate.h"

#include <algorithm>
#include <cstring>

#include <boost/optional/optional.hpp>

#include "kudu/common/columnblock.h"
#include "kudu/common/key_util.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/memory/arena.h"

using std::move;
using std::string;
using std::vector;

namespace kudu {

ColumnPredicate::ColumnPredicate(PredicateType predicate_type,
                                 ColumnSchema column,
                                 const void* lower,
                                 const void* upper)
    : predicate_type_(predicate_type),
      column_(move(column)),
      lower_(lower),
      upper_(upper) {
}

ColumnPredicate::ColumnPredicate(PredicateType predicate_type,
                                 ColumnSchema column,
                                 vector<const void*>* values)
    : predicate_type_(predicate_type),
      column_(move(column)),
      lower_(nullptr),
      upper_(nullptr) {
  values_.swap(*values);
}

ColumnPredicate ColumnPredicate::Equality(ColumnSchema column, const void* value) {
  CHECK(value != nullptr);
  return ColumnPredicate(PredicateType::Equality, move(column), value, nullptr);
}

ColumnPredicate ColumnPredicate::Range(ColumnSchema column,
                                       const void* lower,
                                       const void* upper) {
  CHECK(lower != nullptr || upper != nullptr);
  ColumnPredicate pred(PredicateType::Range, move(column), lower, upper);
  pred.Simplify();
  return pred;
}

ColumnPredicate ColumnPredicate::InList(ColumnSchema column,
                                        vector<const void*>* values) {
  CHECK(values != nullptr);

  // Sort values and remove duplicates.
  std::sort(values->begin(), values->end(),
            [&] (const void* a, const void* b) {
              return column.type_info()->Compare(a, b) < 0;
            });
  values->erase(std::unique(values->begin(), values->end(),
                            [&] (const void* a, const void* b) {
                              return column.type_info()->Compare(a, b) == 0;
                            }),
                values->end());

  ColumnPredicate pred(PredicateType::InList, move(column), values);
  pred.Simplify();
  return pred;
}

boost::optional<ColumnPredicate> ColumnPredicate::InclusiveRange(ColumnSchema column,
                                                                 const void* lower,
                                                                 const void* upper,
                                                                 Arena* arena) {
  CHECK(lower != nullptr || upper != nullptr);

  if (upper != nullptr) {
    // Transform the upper bound to exclusive by incrementing it.
    // Make a copy of the value before incrementing in case it's aliased.
    size_t size = column.type_info()->size();
    void*  buf = CHECK_NOTNULL(arena->AllocateBytes(size));
    memcpy(buf, upper, size);
    if (!key_util::IncrementCell(column, buf, arena)) {
      if (lower == nullptr) {
        if (column.is_nullable()) {
          // If incrementing the upper bound fails and the column is nullable,
          // then return an IS NOT NULL predicate, so that null values will be
          // filtered.
          return ColumnPredicate::IsNotNull(move(column));
        } else {
          return boost::none;
        }
      } else {
        upper = nullptr;
      }
    } else {
      upper = buf;
    }
  }
  return ColumnPredicate::Range(move(column), lower, upper);
}

ColumnPredicate ColumnPredicate::ExclusiveRange(ColumnSchema column,
                                                const void* lower,
                                                const void* upper,
                                                Arena* arena) {
  CHECK(lower != nullptr || upper != nullptr);

  if (lower != nullptr) {
    // Transform the lower bound to inclusive by incrementing it.
    // Make a copy of the value before incrementing in case it's aliased.
    size_t size = column.type_info()->size();
    void* buf = CHECK_NOTNULL(arena->AllocateBytes(size));
    memcpy(buf, lower, size);
    if (!key_util::IncrementCell(column, buf, arena)) {
      // If incrementing the lower bound fails then the predicate can match no values.
      return ColumnPredicate::None(move(column));
    } else {
      lower = buf;
    }
  }
  return ColumnPredicate::Range(move(column), lower, upper);
}

ColumnPredicate ColumnPredicate::IsNotNull(ColumnSchema column) {
  return ColumnPredicate(PredicateType::IsNotNull, move(column), nullptr, nullptr);
}

ColumnPredicate ColumnPredicate::IsNull(ColumnSchema column) {
  return column.is_nullable() ?
         ColumnPredicate(PredicateType::IsNull, move(column), nullptr, nullptr) :
         None(move(column));
}

ColumnPredicate ColumnPredicate::None(ColumnSchema column) {
  return ColumnPredicate(PredicateType::None, move(column), nullptr, nullptr);
}

void ColumnPredicate::SetToNone() {
  predicate_type_ = PredicateType::None;
  lower_ = nullptr;
  upper_ = nullptr;
}

void ColumnPredicate::Simplify() {
  auto type_info = column_.type_info();
  switch (predicate_type_) {
    case PredicateType::None:
    case PredicateType::Equality:
    case PredicateType::IsNotNull: return;
    case PredicateType::IsNull: return;
    case PredicateType::Range: {
      DCHECK(lower_ != nullptr || upper_ != nullptr);
      if (lower_ != nullptr && upper_ != nullptr) {
        // _ <= VALUE < _
        if (type_info->Compare(lower_, upper_) >= 0) {
          // If the range bounds are empty then no results can be returned.
          SetToNone();
        } else if (type_info->AreConsecutive(lower_, upper_)) {
          // If the values are consecutive, then it is an equality bound.
          predicate_type_ = PredicateType::Equality;
          upper_ = nullptr;
        }
      } else if (lower_ != nullptr) {
        // VALUE >= _
        if (type_info->IsMinValue(lower_)) {
          predicate_type_ = PredicateType::IsNotNull;
          lower_ = nullptr;
          upper_ = nullptr;
        } else if (type_info->IsMaxValue(lower_)) {
          predicate_type_ = PredicateType::Equality;
          upper_ = nullptr;
        }
      } else if (upper_ != nullptr) {
        // VALUE < _
        if (type_info->IsMinValue(upper_)) {
          predicate_type_ = PredicateType::None;
          lower_ = nullptr;
          upper_ = nullptr;
        }
      }
      return;
    };
    case PredicateType::InList: {
      if (values_.empty()) {
        // If the list is empty, no results can be returned.
        SetToNone();
      } else if (values_.size() == 1) {
        // List has only one value, so convert to Equality
        predicate_type_ = PredicateType::Equality;
        lower_ = values_[0];
        values_.clear();
      } else if (type_info->type() == BOOL) {
        // If this is a boolean IN list with both true and false in the list,
        // then we can just convert it to IS NOT NULL. This same simplification
        // could be done for other integer types, but it's probably not as
        // common (and hard to test).
        predicate_type_ = PredicateType::IsNotNull;
        lower_ = nullptr;
        upper_ = nullptr;
        values_.clear();
      }
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::Merge(const ColumnPredicate& other) {
  CHECK(column_.Equals(other.column_, false));
  switch (predicate_type_) {
    case PredicateType::None: return;
    case PredicateType::Range: {
      MergeIntoRange(other);
      return;
    };
    case PredicateType::Equality: {
      MergeIntoEquality(other);
      return;
    };
    case PredicateType::IsNotNull: {
      MergeIntoIsNotNull(other);
      return;
    };
    case PredicateType::IsNull: {
      MergeIntoIsNull(other);
      return;
    }
    case PredicateType::InList: {
      MergeIntoInList(other);
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::MergeIntoRange(const ColumnPredicate& other) {
  CHECK(predicate_type_ == PredicateType::Range);

  switch (other.predicate_type()) {
    case PredicateType::None: {
      SetToNone();
      return;
    };

    case PredicateType::Range: {
      // Set the lower bound to the larger of the two.
      if (other.lower_ != nullptr &&
          (lower_ == nullptr || column_.type_info()->Compare(lower_, other.lower_) < 0)) {
        lower_ = other.lower_;
      }

      // Set the upper bound to the smaller of the two.
      if (other.upper_ != nullptr &&
          (upper_ == nullptr || column_.type_info()->Compare(upper_, other.upper_) > 0)) {
        upper_ = other.upper_;
      }

      Simplify();
      return;
    };

    case PredicateType::Equality: {
      if ((lower_ != nullptr && column_.type_info()->Compare(lower_, other.lower_) > 0) ||
          (upper_ != nullptr && column_.type_info()->Compare(upper_, other.lower_) <= 0)) {
        // The equality value does not fall in this range.
        SetToNone();
      } else {
        predicate_type_ = PredicateType::Equality;
        lower_ = other.lower_;
        upper_ = nullptr;
      }
      return;
    };
    case PredicateType::IsNotNull: return;
    case PredicateType::IsNull: {
      SetToNone();
      return;
    }
    case PredicateType::InList : {
      // The InList predicate values are examined to check whether
      // they lie in the range.
      // The current predicate is then converted from a range predicate to
      // an InList predicate (since it is more selective).
      // The number of values in the InList will depend on how many
      // values were within the range.
      // The call to Simplify() will then convert the InList if appropriate:
      // i.e an InList with zero entries gets converted to a NONE
      //     and InList with 1 entry gets converted into an Equality.
      values_ = other.values_;
      values_.erase(std::remove_if(values_.begin(), values_.end(),
                                   [this] (const void* v) {
                                     return !CheckValueInRange(v);
                                   }), values_.end());
      predicate_type_ = PredicateType::InList;
      Simplify();
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::MergeIntoEquality(const ColumnPredicate& other) {
  CHECK(predicate_type_ == PredicateType::Equality);

  switch (other.predicate_type()) {
    case PredicateType::None: {
      SetToNone();
      return;
    }
    case PredicateType::Range: {
      if (!other.CheckValueInRange(lower_)) {
        // This equality value does not fall in the other range.
        SetToNone();
      }
      return;
    };
    case PredicateType::Equality: {
      if (column_.type_info()->Compare(lower_, other.lower_) != 0) {
        SetToNone();
      }
      return;
    };
    case PredicateType::IsNotNull: return;
    case PredicateType::IsNull: {
      SetToNone();
      return;
    }
    case PredicateType::InList : {
      // The equality value needs to be a member of the InList
      if (!other.CheckValueInList(lower_)) {
        SetToNone();
      }
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::MergeIntoIsNotNull(const ColumnPredicate &other) {
  CHECK(predicate_type_ == PredicateType::IsNotNull);
  switch (other.predicate_type()) {
    // The intersection of NULL and IS NOT NULL is None.
    case PredicateType::IsNull: {
      SetToNone();
      return;
    }
    default: {
      // Otherwise, the intersection is the other predicate.
      predicate_type_ = other.predicate_type_;
      lower_ = other.lower_;
      upper_ = other.upper_;
      values_ = other.values_;
      return;
    }
  }
}

void ColumnPredicate::MergeIntoIsNull(const ColumnPredicate &other) {
  CHECK(predicate_type_ == PredicateType::IsNull);
  switch (other.predicate_type()) {
    // The intersection of IS NULL and IS NULL is IS NULL.
    case PredicateType::IsNull: {
      return;
    }
    default: {
      // Otherwise, the intersection is None.
      // NB: This will not be true if NULL is allowed in an InList predicate.
      SetToNone();
      return;
    }
  }
}

void ColumnPredicate::MergeIntoInList(const ColumnPredicate &other) {
  CHECK(predicate_type_ == PredicateType::InList);
  DCHECK(values_.size() > 1);

  switch (other.predicate_type()) {
    case PredicateType::None: {
      SetToNone();
      return;
    };
    case PredicateType::Range: {
      // Only values within the range should be retained.
      auto search_by = [&] (const void* lhs, const void* rhs) {
        return this->column_.type_info()->Compare(lhs, rhs) < 0;
      };

      // Remove all values greater than the range.
      if (other.upper_ != nullptr) {
        // lower_bound is used here instead of upper_bound, since the upper
        // bound of the range is exclusive, and the in list is inclusive.
        auto upper = std::lower_bound(values_.begin(), values_.end(), other.upper_, search_by);
        values_.erase(upper, values_.end());
      }

      // Remove all values less than the range.
      if (other.lower_ != nullptr) {
        auto lower = std::lower_bound(values_.begin(), values_.end(), other.lower_, search_by);
        values_.erase(values_.begin(), lower);
      }

      Simplify();
      return;
    }
    case PredicateType::Equality: {
      if (CheckValueInList(other.lower_)) {
        // value falls in list so change to Equality predicate
        predicate_type_ = PredicateType::Equality;
        lower_ = other.lower_;
        upper_ = nullptr;
      } else {
        SetToNone(); // Value does not fall in list
      }
      return;
    }
    case PredicateType::IsNotNull: return;
    case PredicateType::IsNull: {
      SetToNone();
      return;
    }
    case PredicateType::InList: {
      // Merge the 'other' IN list into this IN list. The basic idea is to loop
      // through this predicate list, retaining only the values which are also
      // contained in the other predicate list. We apply an optimization first:
      // all values from this in list which fall outside the range of the other
      // IN list are removed, and the remaining values in this IN list are only
      // checked against values in the other list which are in this list's
      // range. This doesn't reduce the worst-case complexity, but can really
      // speed up the merge for big lists in certain cases. This optimization
      // relies on the lists being sorted.
      DCHECK(other.values_.size() > 1);

      auto search_by = [&] (const void* lhs, const void* rhs) {
        return this->column_.type_info()->Compare(lhs, rhs) < 0;
      };

      // Remove all values in this IN list which are greater than the largest
      // value in the other list.
      values_.erase(std::upper_bound(values_.begin(), values_.end(),
                                     other.values_.back(), search_by),
                    values_.end());

      // Remove all values in this IN list which are less than the smallest
      // value in the other list.
      values_.erase(values_.begin(),
                    std::lower_bound(values_.begin(), values_.end(),
                                     other.values_.front(), search_by));

      if (values_.empty()) {
        SetToNone();
        return;
      }

      // Find the sublist in the other IN list which overlaps with this IN list.
      auto other_start = std::lower_bound(other.values_.begin(), other.values_.end(),
                                          values_.front(), search_by);
      auto other_end = std::upper_bound(other_start, other.values_.end(),
                                        values_.back(), search_by);

      // Returns true if the value is *not* present in the other list.
      // Modifies other_start to point at the position of v in the other list.
      auto other_absent = [&] (const void* v) {
        other_start = std::lower_bound(other_start, other_end, v, search_by);
        return this->column_.type_info()->Compare(v, *other_start) != 0;
      };

      // Iterate through the values_ list and remove elements that do not exist
      // in the other list.
      values_.erase(std::remove_if(values_.begin(), values_.end(), other_absent), values_.end());
      Simplify();
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

namespace {
template <typename P>
void ApplyPredicate(const ColumnBlock& block, SelectionVector* sel, P p) {
  if (block.is_nullable()) {
    for (size_t i = 0; i < block.nrows(); i++) {
      if (!sel->IsRowSelected(i)) continue;
      const void* cell = block.nullable_cell_ptr(i);
      if (cell == nullptr || !p(cell)) {
        BitmapClear(sel->mutable_bitmap(), i);
      }
    }
  } else {
    for (size_t i = 0; i < block.nrows(); i++) {
      if (!sel->IsRowSelected(i)) continue;
      const void* cell = block.cell_ptr(i);
      if (!p(cell)) {
        BitmapClear(sel->mutable_bitmap(), i);
      }
    }
  }
}
} // anonymous namespace

template <DataType PhysicalType>
void ColumnPredicate::EvaluateForPhysicalType(const ColumnBlock& block,
                                              SelectionVector* sel) const {
  switch (predicate_type()) {
    case PredicateType::Range: {
      if (lower_ == nullptr) {
        ApplyPredicate(block, sel, [this] (const void* cell) {
          return DataTypeTraits<PhysicalType>::Compare(cell, this->upper_) < 0;
        });
      } else if (upper_ == nullptr) {
        ApplyPredicate(block, sel, [this] (const void* cell) {
          return DataTypeTraits<PhysicalType>::Compare(cell, this->lower_) >= 0;
        });
      } else {
        ApplyPredicate(block, sel, [this] (const void* cell) {
          return DataTypeTraits<PhysicalType>::Compare(cell, this->upper_) < 0 &&
                 DataTypeTraits<PhysicalType>::Compare(cell, this->lower_) >= 0;
        });
      }
      return;
    };
    case PredicateType::Equality: {
      ApplyPredicate(block, sel, [this] (const void* cell) {
        return DataTypeTraits<PhysicalType>::Compare(cell, this->lower_) == 0;
      });
      return;
    };
    case PredicateType::IsNotNull: {
      if (!block.is_nullable()) return;
      // TODO: make this more efficient by using bitwise operations on the
      // null and selection vectors.
      for (size_t i = 0; i < block.nrows(); i++) {
        if (sel->IsRowSelected(i) && block.is_null(i)) {
          BitmapClear(sel->mutable_bitmap(), i);
        }
      }
      return;
    };
    case PredicateType::IsNull: {
      if (!block.is_nullable()) {
        BitmapChangeBits(sel->mutable_bitmap(), 0, block.nrows(), false);
        return;
      }
      // TODO(wdberkeley): make this more efficient by using bitwise operations on the
      // null and selection vectors.
      for (size_t i = 0; i < block.nrows(); i++) {
        if (sel->IsRowSelected(i) && !block.is_null(i)) {
          BitmapClear(sel->mutable_bitmap(), i);
        }
      }
      return;
    }
    case PredicateType::InList: {
      ApplyPredicate(block, sel, [this] (const void* cell) {
        return std::binary_search(values_.begin(), values_.end(), cell,
                                  [] (const void* lhs, const void* rhs) {
                                    return DataTypeTraits<PhysicalType>::Compare(lhs, rhs) < 0;
                                  });
      });
      return;
    };
    case PredicateType::None: LOG(FATAL) << "NONE predicate evaluation";
  }
  LOG(FATAL) << "unknown predicate type";
}

bool ColumnPredicate::EvaluateCell(DataType type, const void* cell) const {
  switch (type) {
    case BOOL: return EvaluateCell<BOOL>(cell);
    case INT8: return EvaluateCell<INT8>(cell);
    case INT16: return EvaluateCell<INT16>(cell);
    case INT32: return EvaluateCell<INT32>(cell);
    case INT64: return EvaluateCell<INT64>(cell);
    case UINT8: return EvaluateCell<UINT8>(cell);
    case UINT16: return EvaluateCell<UINT16>(cell);
    case UINT32: return EvaluateCell<UINT32>(cell);
    case UINT64: return EvaluateCell<UINT64>(cell);
    case FLOAT: return EvaluateCell<FLOAT>(cell);
    case DOUBLE: return EvaluateCell<DOUBLE>(cell);
    case BINARY: return EvaluateCell<BINARY>(cell);
    default: LOG(FATAL) << "unknown physical type: " << GetTypeInfo(type)->name();
  }
}

void ColumnPredicate::Evaluate(const ColumnBlock& block, SelectionVector* sel) const {
  DCHECK(sel);
  switch (block.type_info()->physical_type()) {
    case BOOL: return EvaluateForPhysicalType<BOOL>(block, sel);
    case INT8: return EvaluateForPhysicalType<INT8>(block, sel);
    case INT16: return EvaluateForPhysicalType<INT16>(block, sel);
    case INT32: return EvaluateForPhysicalType<INT32>(block, sel);
    case INT64: return EvaluateForPhysicalType<INT64>(block, sel);
    case UINT8: return EvaluateForPhysicalType<UINT8>(block, sel);
    case UINT16: return EvaluateForPhysicalType<UINT16>(block, sel);
    case UINT32: return EvaluateForPhysicalType<UINT32>(block, sel);
    case UINT64: return EvaluateForPhysicalType<UINT64>(block, sel);
    case FLOAT: return EvaluateForPhysicalType<FLOAT>(block, sel);
    case DOUBLE: return EvaluateForPhysicalType<DOUBLE>(block, sel);
    case BINARY: return EvaluateForPhysicalType<BINARY>(block, sel);
    default: LOG(FATAL) << "unknown physical type: " << block.type_info()->name();
  }
}

string ColumnPredicate::ToString() const {
  switch (predicate_type()) {
    case PredicateType::None: return strings::Substitute("`$0` NONE", column_.name());
    case PredicateType::Range: {
      if (lower_ == nullptr) {
        return strings::Substitute("`$0` < $1", column_.name(), column_.Stringify(upper_));
      } else if (upper_ == nullptr) {
        return strings::Substitute("`$0` >= $1", column_.name(), column_.Stringify(lower_));
      } else {
        return strings::Substitute("`$0` >= $1 AND `$0` < $2",
                                   column_.name(),
                                   column_.Stringify(lower_),
                                   column_.Stringify(upper_));
      }
    };
    case PredicateType::Equality: {
      return strings::Substitute("`$0` = $1", column_.name(), column_.Stringify(lower_));
    };
    case PredicateType::IsNotNull: {
      return strings::Substitute("`$0` IS NOT NULL", column_.name());
    };
    case PredicateType::IsNull: {
      return strings::Substitute("`$0` IS NULL", column_.name());
    };
    case PredicateType::InList: {
      string ss = "`";
      ss.append(column_.name());
      ss.append("` IN (");
      bool is_first = true;
      for (auto* value : values_) {
        if (is_first) {
          is_first = false;
        } else {
          ss.append(", ");
        }
        ss.append(column_.Stringify(value));
      }
      ss.append(")");
      return ss;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

bool ColumnPredicate::operator==(const ColumnPredicate& other) const {
  if (!column_.Equals(other.column_, false)) { return false; }
  if (predicate_type_ != other.predicate_type_) {
    return false;
  }
  switch (predicate_type_) {
    case PredicateType::Equality: return column_.type_info()->Compare(lower_, other.lower_) == 0;
    case PredicateType::Range: {
      return (lower_ == other.lower_ ||
              (lower_ != nullptr && other.lower_ != nullptr &&
               column_.type_info()->Compare(lower_, other.lower_) == 0)) &&
             (upper_ == other.upper_ ||
              (upper_ != nullptr && other.upper_ != nullptr &&
               column_.type_info()->Compare(upper_, other.upper_) == 0));
    };
    case PredicateType::InList: {
      if (values_.size() != other.values_.size()) return false;
      for (int i = 0; i < values_.size(); i++) {
        if (column_.type_info()->Compare(values_[i], other.values_[i]) != 0) return false;
      }
      return true;
    };
    case PredicateType::None:
    case PredicateType::IsNotNull:
    case PredicateType::IsNull: return true;
  }
  LOG(FATAL) << "unknown predicate type";
}

bool ColumnPredicate::CheckValueInRange(const void* value) const {
  CHECK(predicate_type_ == PredicateType::Range);
  return ((lower_ == nullptr || column_.type_info()->Compare(lower_, value) <= 0) &&
          (upper_ == nullptr || column_.type_info()->Compare(upper_, value) > 0));
}

bool ColumnPredicate::CheckValueInList(const void* value) const {
  return std::binary_search(values_.begin(), values_.end(), value,
                            [this](const void* lhs, const void* rhs) {
                              return this->column_.type_info()->Compare(lhs, rhs) < 0;
                            });
}

namespace {
int SelectivityRank(const ColumnPredicate& predicate) {
  int rank;
  switch (predicate.predicate_type()) {
    case PredicateType::None: rank = 0; break;
    case PredicateType::IsNull: rank = 1; break;
    case PredicateType::Equality: rank = 2; break;
    case PredicateType::InList: rank = 3; break;
    case PredicateType::Range: rank = 4; break;
    case PredicateType::IsNotNull: rank = 5; break;
    default: LOG(FATAL) << "unknown predicate type";
  }
  return rank * (kLargestTypeSize + 1) + predicate.column().type_info()->size();
}
} // anonymous namespace

int SelectivityComparator(const ColumnPredicate& left, const ColumnPredicate& right) {
  return SelectivityRank(left) - SelectivityRank(right);
}

} // namespace kudu
