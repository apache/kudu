// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>

#include "common/types.h"
#include "common/scan_predicate.h"
#include "util/bitmap.h"

namespace kudu {

using std::string;


ValueRange::ValueRange(const TypeInfo &type,
                       boost::optional<const void *> lower_bound,
                       boost::optional<const void *> upper_bound)
  : type_info_(&type),
    lower_bound_(lower_bound),
    upper_bound_(upper_bound) {
  CHECK(has_lower_bound() || has_upper_bound())
    << "range predicate has no bounds";
}

bool ValueRange::IsEquality() const {
  if (has_lower_bound() && has_upper_bound()) {
    return type_info_->Compare(upper_bound_.get(), lower_bound_.get()) == 0;
  }
  return false;
}

bool ValueRange::ContainsCell(const void *cell) const {
  if (has_lower_bound() && type_info_->Compare(cell, lower_bound_.get()) < 0) {
    return false;
  }
  if (has_upper_bound() && type_info_->Compare(cell, upper_bound_.get()) > 0) {
    return false;
  }
  return true;
}

////////////////////////////////////////////////////////////

ColumnRangePredicate::ColumnRangePredicate(const ColumnSchema &col,
                                           boost::optional<const void *> lower_bound,
                                           boost::optional<const void *> upper_bound) :
  col_(col),
  range_(col_.type_info(), lower_bound, upper_bound) {
}


void ColumnRangePredicate::Evaluate(RowBlock *block, SelectionVector *vec) const {
  // TODO: this evaluates on every row, whereas we only need to evaluate on
  // rows where the selection vector is currently true.
  // Perhaps we should swap implementations here based on the current selectivity:
  // - if only a small number of bits are set, only evaluate on the set bits
  // - if most bits are set, evaluate on all the bits

  int col_idx = block->schema().find_column(col_.name());
  CHECK_GE(col_idx, 0) << "bad col: " << col_.ToString();

  ColumnBlock cblock(block->column_block(col_idx, block->nrows()));

  // TODO: this is all rather slow, could probably push down all the way
  // to the TypeInfo so we only make one virtual call, or use codegen.
  // Not concerned for now -- plan of record is to eventually embed Impala
  // expression evaluation somewhere here, so this is just a stub.
  if (cblock.is_nullable()) {
    for (size_t i = 0; i < block->nrows(); i++) {
      const void *cell = cblock.nullable_cell_ptr(i);
      if (cell == NULL || !range_.ContainsCell(cell)) {
        BitmapClear(vec->mutable_bitmap(), i);
      }
    }
  } else {
    for (size_t i = 0; i < block->nrows(); i++) {
      const void *cell = cblock.cell_ptr(i);
      if (!range_.ContainsCell(cell)) {
        BitmapClear(vec->mutable_bitmap(), i);
      }
    }
  }
}

string ColumnRangePredicate::ToString() const {
  if (range_.has_lower_bound() && range_.has_upper_bound()) {
    return StringPrintf("(`%s` BETWEEN %s AND %s)", col_.name().c_str(),
                        col_.Stringify(range_.lower_bound()).c_str(),
                        col_.Stringify(range_.upper_bound()).c_str());
  } else if (range_.has_lower_bound()) {
    return StringPrintf("(`%s` >= %s)", col_.name().c_str(),
                        col_.Stringify(range_.lower_bound()).c_str());
  } else if (range_.has_upper_bound()) {
    return StringPrintf("(`%s` <= %s)", col_.name().c_str(),
                        col_.Stringify(range_.upper_bound()).c_str());
  } else {
    LOG(FATAL) << "Cannot reach here";
    return string("Does not reach here");
  }
}

} // namespace kudu
