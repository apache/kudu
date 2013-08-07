// Copyright (c) 2013, Cloudera, inc.

#include "common/schema.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/join.h"
#include "gutil/strings/strcat.h"
#include "util/status.h"

namespace kudu {

string ColumnSchema::ToString() const {
  return StringPrintf("%s[type='%s']",
                      name_.c_str(),
                      type_info_->name().c_str());
}

Status Schema::Reset(const vector<ColumnSchema> &cols,
                     int key_columns) {
  cols_ = cols;
  num_key_columns_ = key_columns;

  if (PREDICT_FALSE(key_columns > cols_.size())) {
    return Status::InvalidArgument(
      "Bad schema", "More key columns than columns");
  }

  // Verify that the key columns are not nullable
  for (int i = 0; i < key_columns; ++i) {
    if (PREDICT_FALSE(cols_[i].is_nullable())) {
      return Status::InvalidArgument(
        "Bad schema", "Nullable key columns not supported");
    }
  }

  // Calculate the offset of each column in the row format.
  col_offsets_.reserve(cols_.size());
  size_t off = 0;
  size_t i = 0;
  BOOST_FOREACH(const ColumnSchema &col, cols) {
    name_to_index_[col.name()] = i++;
    col_offsets_.push_back(off);
    off += col.type_info().size();
  }

  if (PREDICT_FALSE(cols.size() != name_to_index_.size())) {
    return Status::InvalidArgument(
      "Bad schema", "Duplicate name present in schema!");
  }

  // Add an extra element on the end for the total
  // byte size
  col_offsets_.push_back(off);

  return Status::OK();
}

Status Schema::GetProjectionFrom(const Schema &from_schema,
                                 vector<size_t> *indexes) const {
  indexes->clear();
  indexes->reserve(num_columns());
  BOOST_FOREACH(const ColumnSchema &col, cols_) {
    NameToIndexMap::const_iterator iter =
      from_schema.name_to_index_.find(col.name());
    if (iter == from_schema.name_to_index_.end()) {
      return Status::InvalidArgument(
        string("Cannot map from schema ") +
        from_schema.ToString() + " to " + ToString() +
        ": column '" + col.name() + "' not present in source");
    }

    size_t idx = (*iter).second;
    const ColumnSchema &from_col = from_schema.column(idx);
    if (!from_col.EqualsType(col)) {
      return Status::InvalidArgument(
        string("Cannot map from schema ") +
        from_schema.ToString() + " to " + ToString() +
        ": type mismatch for column '" + col.name() + "'");
    }

    indexes->push_back((*iter).second);
  }
  return Status::OK();
}

string Schema::ToString() const {
  vector<string> col_strs;
  BOOST_FOREACH(const ColumnSchema &col, cols_) {
    col_strs.push_back(col.ToString());
  }

  return StrCat("Schema [",
                JoinStrings(col_strs, ", "),
                "]");
}

} // namespace kudu
