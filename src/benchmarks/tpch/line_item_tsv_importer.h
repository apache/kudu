// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_LINE_ITEM_TSV_IMPORTER_H
#define KUDU_TPCH_LINE_ITEM_TSV_IMPORTER_H

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

#include "common/schema.h"
#include "common/row.h"
#include "common/partial_row.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"


namespace kudu {

static const char* const kPipeSeparator = "|";

// Utility class used to parse the lineitem tsv file
class LineItemTsvImporter {
 public:
  explicit LineItemTsvImporter(const string &path) : in_(path.c_str()) {
    CHECK(in_.is_open()) << "not able to open input file: " << path;
    done_ = !getline(in_, line_);
  }

  bool done() const { return done_; }

  // Fills the row builder with a single line item from the file.
  // It returns 0 if it's done or the order number if it got a line
  int GetNextLine(PartialRow* row) {
    if (done_) return 0;

    columns_.clear();

    // grab all the columns_ individually
    columns_ = strings::Split(line_, kPipeSeparator);

    int i = 0;
    int order_number = ConvertToIntAndPopulate(columns_[i++], row, tpch::kOrderKeyColIdx);
    ConvertToIntAndPopulate(columns_[i++], row, tpch::kPartKeyColIdx);
    ConvertToIntAndPopulate(columns_[i++], row, tpch::kSuppKeyColIdx);
    ConvertToIntAndPopulate(columns_[i++], row, tpch::kLineNumberColIdx);
    ConvertToIntAndPopulate(columns_[i++], row, tpch::kQuantityColIdx);
    ConvertDoubleToIntAndPopulate(columns_[i++], row, tpch::kExtendedPriceColIdx);
    ConvertDoubleToIntAndPopulate(columns_[i++], row, tpch::kDiscountColIdx);
    ConvertDoubleToIntAndPopulate(columns_[i++], row, tpch::kTaxColIdx);
    CHECK_OK(row->SetString(tpch::kReturnFlagColIdx, columns_[i++]));
    CHECK_OK(row->SetString(tpch::kLineStatusColIdx, columns_[i++]));
    CHECK_OK(row->SetString(tpch::kShipDateColIdx, columns_[i++]));
    CHECK_OK(row->SetString(tpch::kCommitDateColIdx, columns_[i++]));
    CHECK_OK(row->SetString(tpch::kReceiptDateColIdx, columns_[i++]));
    CHECK_OK(row->SetString(tpch::kShipInstructColIdx, columns_[i++]));
    CHECK_OK(row->SetString(tpch::kShipModeColIdx, columns_[i++]));
    CHECK_OK(row->SetString(tpch::kCommentColIdx, columns_[i++]));

    // Get next line
    done_ = !getline(in_, line_);

    return order_number;
  }

  int ConvertToIntAndPopulate(const StringPiece &chars, PartialRow* row,
                              int col_idx) {
    // TODO: extra copy here, since we don't have a way to parse StringPiece
    // into ints.
    chars.CopyToString(&tmp_);
    int number;
    bool ok_parse = SimpleAtoi(tmp_.c_str(), &number);
    CHECK(ok_parse);
    CHECK_OK(row->SetUInt32(col_idx, number));
    return number;
  }

  void ConvertDoubleToIntAndPopulate(const StringPiece &chars, PartialRow* row,
                                     int col_idx) {
    // TODO: extra copy here, since we don't have a way to parse StringPiece
    // into ints.
    chars.CopyToString(&tmp_);
    char *error = NULL;
    errno = 0;
    const char *cstr = tmp_.c_str();
    double number = strtod(cstr, &error);
    CHECK(errno == 0 &&  // overflow/underflow happened
        error != cstr);
    int new_num = number * 100;
    CHECK_OK(row->SetUInt32(col_idx, new_num));
  }

 private:
  std::ifstream in_;
  vector<StringPiece> columns_;
  string line_, tmp_;
  bool done_;
};
} // namespace kudu
#endif
