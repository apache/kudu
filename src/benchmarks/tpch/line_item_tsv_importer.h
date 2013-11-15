// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_LINE_ITEM_TSV_IMPORTER_H
#define KUDU_TPCH_LINE_ITEM_TSV_IMPORTER_H

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

#include <boost/tokenizer.hpp>

#include "common/schema.h"
#include "common/row.h"

namespace kudu {

static const boost::char_separator<char> kPipeSeparator("|");
typedef boost::tokenizer<boost::char_separator<char> > Tokenizer;

// Utility class used to parse the lineitem tsv file
class LineItemTsvImporter {
 public:
  explicit LineItemTsvImporter(const string &path) : in_(path.c_str()) {
    CHECK(in_.is_open()) << "not able to open input file: " << path;
  }

  // Fills the row builder with a single line item from the file.
  // It returns 0 if it's done or the order number if it got a line
  int GetNextLine(RowBuilder &rb) {
    if (!getline(in_,line_)) {
      return 0;
    }
    columns_.clear();
    rb.Reset();
    Tokenizer tokens(line_, kPipeSeparator);

    // grab all the columns_ individually
    columns_.assign(tokens.begin(), tokens.end());

    int order_number = ConvertToIntAndPopulate(columns_[0], &rb);
    ConvertToIntAndPopulate(columns_[3], &rb);
    ConvertToIntAndPopulate(columns_[1], &rb);
    ConvertToIntAndPopulate(columns_[2], &rb);
    ConvertToIntAndPopulate(columns_[4], &rb);
    ConvertDoubleToIntAndPopulate(columns_[5], &rb);
    ConvertDoubleToIntAndPopulate(columns_[6], &rb);
    ConvertDoubleToIntAndPopulate(columns_[7], &rb);
    for (int i = 8; i < 16; i++)  {
      rb.AddString(Slice(columns_[i]));
    }
    return order_number;
  }

  int ConvertToIntAndPopulate(const string &chars, RowBuilder *rb) {
    int number;
    bool ok_parse = SimpleAtoi(chars.c_str(), &number);
    CHECK(ok_parse);
    rb->AddUint32(number);
    return number;
  }

  void ConvertDoubleToIntAndPopulate(const string &chars, RowBuilder *rb) {
    char *error = NULL;
    errno = 0;
    const char *cstr = chars.c_str();
    double number = strtod(cstr, &error);
    CHECK(errno == 0 &&  // overflow/underflow happened
        error != cstr);
    int new_num = number * 100;
    rb->AddUint32(new_num);
  }

 private:
  std::ifstream in_;
  vector<string> columns_;
  string line_;
};
} // namespace kudu
#endif
