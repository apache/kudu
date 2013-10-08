// Copyright (c) 2013, Cloudera, inc.

#include "common/schema.h"

#include <set>
#include <algorithm>

#include "gutil/stringprintf.h"
#include "gutil/strings/join.h"
#include "gutil/strings/strcat.h"
#include "gutil/strings/substitute.h"
#include "util/status.h"

namespace kudu {

using std::set;
using std::tr1::unordered_set;
using std::tr1::unordered_map;

string ColumnSchema::ToString() const {
  return strings::Substitute("$0[type='$1' $2]",
                             name_,
                             type_info_->name(),
                             is_nullable_ ? "NULLABLE" : "NOT NULL");
}

Status Schema::Reset(const vector<ColumnSchema>& cols,
                     const vector<size_t>& ids,
                     int key_columns) {
  cols_ = cols;
  num_key_columns_ = key_columns;

  if (PREDICT_FALSE(key_columns > cols_.size())) {
    return Status::InvalidArgument(
      "Bad schema", "More key columns than columns");
  }

  if (PREDICT_FALSE(!ids.empty() && ids.size() != cols_.size())) {
    return Status::InvalidArgument("Bad schema",
      "The number of ids does not match with the number of columns");
  }

  // Verify that the key columns are not nullable
  for (int i = 0; i < key_columns; ++i) {
    if (PREDICT_FALSE(cols_[i].is_nullable())) {
      return Status::InvalidArgument(
        "Bad schema", strings::Substitute("Nullable key columns are not supported: $0", cols_[i].name()));
    }
  }

  // Calculate the offset of each column in the row format.
  col_offsets_.reserve(cols_.size() + 1);  // Include space for total byte size at the end.
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

  // Initialize IDs mapping
  col_ids_ = ids;
  id_to_index_.clear();
  for (int i = 0; i < ids.size(); ++i) {
    id_to_index_[col_ids_[i]] = i;
  }

  return Status::OK();
}

Status Schema::CreatePartialSchema(const vector<size_t>& col_indexes,
                                   unordered_map<size_t, size_t>* old_to_new,
                                   Schema* out) const {
  int keys_included = 0;
  for (int i = 0; i < num_key_columns(); i++) {
    if (std::binary_search(col_indexes.begin(), col_indexes.end(), i)) {
      keys_included++;
    }
  }
  bool has_all_key_columns = (keys_included == num_key_columns());
  if (keys_included > 0 && !has_all_key_columns) {
    return Status::InvalidArgument("Partial Schema must either include all key columns or none!");
  }
  size_t new_idx = 0;
  vector<ColumnSchema> cols_to_include;
  BOOST_FOREACH(size_t col_idx, col_indexes) {
    cols_to_include.push_back(column(col_idx));
    if (old_to_new != NULL) {
      (*old_to_new)[col_idx] = new_idx++;
    }
  }
  RETURN_NOT_OK(out->Reset(cols_to_include, has_all_key_columns ? num_key_columns() : 0));
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

// ============================================================================
//  Schema Builder
// ============================================================================
void SchemaBuilder::Reset() {
  cols_.clear();
  col_ids_.clear();
  col_names_.clear();
  num_key_columns_ = 0;
  next_id_ = 0;
}

void SchemaBuilder::Reset(const Schema& schema) {
  cols_ = schema.cols_;
  col_ids_ = schema.col_ids_;
  num_key_columns_ = schema.num_key_columns_;
  for (int i = 0; i < cols_.size(); ++i) {
    col_names_.insert(cols_[i].name());
  }

  if (col_ids_.empty()) {
    for (int i = 0; i < cols_.size(); ++i) {
      col_ids_.push_back(i);
    }
    next_id_ = cols_.size();
  } else {
    next_id_ = *std::max_element(col_ids_.begin(), col_ids_.end()) + 1;
  }
}

Status SchemaBuilder::AddKeyColumn(const string& name, DataType type) {
  return AddColumn(ColumnSchema(name, type), true);
}

Status SchemaBuilder::AddColumn(const string& name,
                                DataType type,
                                bool is_nullable,
                                const void *read_default,
                                const void *write_default) {
  return AddColumn(ColumnSchema(name, type, is_nullable, read_default, write_default), false);
}

Status SchemaBuilder::RemoveColumn(const string& name) {
  unordered_set<string>::const_iterator it_names;
  if ((it_names = col_names_.find(name)) == col_names_.end()) {
    return Status::NotFound("The specified column does not exist", name);
  }

  col_names_.erase(it_names);
  for (int i = 0; i < cols_.size(); ++i) {
    if (name == cols_[i].name()) {
      cols_.erase(cols_.begin() + i);
      col_ids_.erase(col_ids_.begin() + i);
      if (i < num_key_columns_) {
        num_key_columns_--;
      }
      return Status::OK();
    }
  }

  LOG(FATAL) << "Should not reach here";
  return Status::Corruption("Unable to remove existing column");
}

Status SchemaBuilder::RenameColumn(const string& old_name, const string& new_name) {
  unordered_set<string>::const_iterator it_names;

  // check if 'new_name' is already in use
  if ((it_names = col_names_.find(new_name)) != col_names_.end()) {
    return Status::AlreadyPresent("The column already exists", new_name);
  }

  // check if the 'old_name' column exists
  if ((it_names = col_names_.find(old_name)) == col_names_.end()) {
    return Status::NotFound("The specified column does not exist", old_name);
  }

  col_names_.erase(it_names);   // TODO: Should this one stay and marked as alias?
  col_names_.insert(new_name);

  BOOST_FOREACH(ColumnSchema& col_schema, cols_) {
    if (old_name == col_schema.name()) {
      col_schema.set_name(new_name);
      return Status::OK();
    }
  }

  LOG(FATAL) << "Should not reach here";
  return Status::IllegalState("Unable to rename existing column");
}

Status SchemaBuilder::AddColumn(const ColumnSchema& column, bool is_key) {
  if (ContainsKey(col_names_, column.name())) {
    return Status::AlreadyPresent("The column already exists", column.name());
  }

  col_names_.insert(column.name());
  if (is_key) {
    cols_.insert(cols_.begin() + num_key_columns_, column);
    col_ids_.insert(col_ids_.begin() + num_key_columns_, next_id_);
    num_key_columns_++;
  } else {
    cols_.push_back(column);
    col_ids_.push_back(next_id_);
  }

  next_id_++;
  return Status::OK();
}

} // namespace kudu
