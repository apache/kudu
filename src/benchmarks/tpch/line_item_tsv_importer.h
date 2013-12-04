// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_LINE_ITEM_TSV_IMPORTER_H
#define KUDU_TPCH_LINE_ITEM_TSV_IMPORTER_H

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

#include "common/schema.h"
#include "common/row.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"


namespace kudu {

static const char* const kPipeSeparator = "|";

// Utility class used to parse the lineitem tsv file
class LineItemTsvImporter {
 public:
  explicit LineItemTsvImporter(const string &path) : in_(path.c_str()) {
    CHECK(in_.is_open()) << "not able to open input file: " << path;
  }

  // Fills the row builder with a single line item from the file.
  // It returns 0 if it's done or the order number if it got a line
  int GetNextLine(RowBuilder &rb) {
    if (!getline(in_, line_)) {
      return 0;
    }
    columns_.clear();
    rb.Reset();

    // grab all the columns_ individually
    columns_ = strings::Split(line_, kPipeSeparator);

    int order_number = ConvertToIntAndPopulate(columns_[0], &rb);
    ConvertToIntAndPopulate(columns_[3], &rb);
    ConvertToIntAndPopulate(columns_[1], &rb);
    ConvertToIntAndPopulate(columns_[2], &rb);
    ConvertToIntAndPopulate(columns_[4], &rb);
    ConvertDoubleToIntAndPopulate(columns_[5], &rb);
    ConvertDoubleToIntAndPopulate(columns_[6], &rb);
    ConvertDoubleToIntAndPopulate(columns_[7], &rb);
    for (int i = 8; i < 16; i++)  {
      rb.AddString(Slice(columns_[i].data(), columns_[i].size()));
    }
    return order_number;
  }

  int ConvertToIntAndPopulate(const StringPiece &chars, RowBuilder *rb) {
    // TODO: extra copy here, since we don't have a way to parse StringPiece
    // into ints.
    chars.CopyToString(&tmp_);
    int number;
    bool ok_parse = SimpleAtoi(tmp_.c_str(), &number);
    CHECK(ok_parse);
    rb->AddUint32(number);
    return number;
  }

  void ConvertDoubleToIntAndPopulate(const StringPiece &chars, RowBuilder *rb) {
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
    rb->AddUint32(new_num);
  }

 private:
  std::ifstream in_;
  vector<StringPiece> columns_;
  string line_, tmp_;
};
} // namespace kudu
#endif
