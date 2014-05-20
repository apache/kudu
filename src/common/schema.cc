// Copyright (c) 2013, Cloudera, inc.

#include "common/schema.h"

#include <set>
#include <algorithm>

#include "gutil/stringprintf.h"
#include "gutil/strings/join.h"
#include "gutil/strings/strcat.h"
#include "gutil/strings/substitute.h"
#include "util/status.h"
#include "common/row.h"

namespace kudu {

using std::set;
using std::tr1::unordered_set;
using std::tr1::unordered_map;

string ColumnStorageAttributes::ToString() const {
  return strings::Substitute("encoding=$0,compression=$1",
                             EncodingType_Name(encoding_),
                             CompressionType_Name(compression_));
}

// TODO: include attributes_.ToString() -- need to fix unit tests
// first
string ColumnSchema::ToString() const {
  return strings::Substitute("$0[$1]",
                             name_,
                             TypeToString());
}

string ColumnSchema::TypeToString() const {
  return strings::Substitute("$0 $1",
                             type_info_->name(),
                             is_nullable_ ? "NULLABLE" : "NOT NULL");
}

Schema::Schema(const Schema& other) {
  CopyFrom(other);
}

Schema& Schema::operator=(const Schema& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void Schema::CopyFrom(const Schema& other) {
  num_key_columns_ = other.num_key_columns_;
  cols_ = other.cols_;
  col_ids_ = other.col_ids_;
  col_offsets_ = other.col_offsets_;
  id_to_index_ = other.id_to_index_;

  // We can't simply copy name_to_index_ since the StringPiece keys
  // reference the other Schema's ColumnSchema objects.
  name_to_index_.clear();
  int i = 0;
  BOOST_FOREACH(const ColumnSchema &col, cols_) {
    // The map uses the 'name' string from within the ColumnSchema object.
    name_to_index_[col.name()] = i++;
  }
}

void Schema::swap(Schema& other) { // NOLINT(build/include_what_you_use)
  int tmp = other.num_key_columns_;
  other.num_key_columns_ = num_key_columns_;
  num_key_columns_ = tmp;
  cols_.swap(other.cols_);
  col_ids_.swap(other.col_ids_);
  col_offsets_.swap(other.col_offsets_);
  name_to_index_.swap(other.name_to_index_);
  id_to_index_.swap(other.id_to_index_);
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
        "Bad schema", strings::Substitute("Nullable key columns are not "
                                          "supported: $0", cols_[i].name()));
    }
  }

  // Calculate the offset of each column in the row format.
  col_offsets_.reserve(cols_.size() + 1);  // Include space for total byte size at the end.
  size_t off = 0;
  size_t i = 0;
  name_to_index_.clear();
  BOOST_FOREACH(const ColumnSchema &col, cols_) {
    // The map uses the 'name' string from within the ColumnSchema object.
    name_to_index_[col.name()] = i++;
    col_offsets_.push_back(off);
    off += col.type_info()->size();
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

Status Schema::VerifyProjectionCompatibility(const Schema& projection) const {
  DCHECK(has_column_ids()) "The server schema must have IDs";

  if (projection.has_column_ids()) {
    return Status::InvalidArgument("User requests should not have Column IDs");
  }

  vector<string> missing_columns;
  BOOST_FOREACH(const ColumnSchema& pcol, projection.columns()) {
    int index = find_column(pcol.name());
    if (index < 0) {
      missing_columns.push_back(pcol.name());
    } else if (!pcol.EqualsType(cols_[index])) {
      // TODO: We don't support query with type adaptors yet
      return Status::InvalidArgument("The column '" + pcol.name() + "' must have type " +
                                     cols_[index].TypeToString() + " found " + pcol.TypeToString());
    }
  }

  if (!missing_columns.empty()) {
    return Status::InvalidArgument("Some columns are not present in the current schema",
                                   JoinStrings(missing_columns, ", "));
  }
  return Status::OK();
}


Status Schema::GetMappedReadProjection(const Schema& projection,
                                       Schema *mapped_projection) const {
  // - The user projection may have different columns from the ones on the tablet
  // - User columns non present in the tablet are considered errors
  // - The user projection is not supposed to have the defaults or the nullable
  //   information on each field. The current tablet schema is supposed to.
  // - Each CFileSet may have a different schema and each CFileSet::Iterator
  //   must use projection from the CFileSet schema to the mapped user schema.
  RETURN_NOT_OK(VerifyProjectionCompatibility(projection));

  // Get the Projection Mapping
  vector<ColumnSchema> mapped_cols;
  vector<size_t> mapped_ids;

  mapped_cols.reserve(projection.num_columns());
  mapped_ids.reserve(projection.num_columns());

  BOOST_FOREACH(const ColumnSchema& col, projection.columns()) {
    int index = find_column(col.name());
    DCHECK_GE(index, 0) << col.name();
    mapped_cols.push_back(cols_[index]);
    mapped_ids.push_back(col_ids_[index]);
  }

  CHECK_OK(mapped_projection->Reset(mapped_cols, mapped_ids, projection.num_key_columns()));
  return Status::OK();
}

string Schema::ToString() const {
  vector<string> col_strs;
  if (has_column_ids()) {
    for (int i = 0; i < cols_.size(); ++i) {
      col_strs.push_back(strings::Substitute("$0:$1", col_ids_[i], cols_[i].ToString()));
    }
  } else {
    BOOST_FOREACH(const ColumnSchema &col, cols_) {
      col_strs.push_back(col.ToString());
    }
  }

  return StrCat("Schema [",
                JoinStrings(col_strs, ", "),
                "]");
}

void Schema::DecodeRowKey(const string& encoded_key,
                          uint8_t* buffer,
                          Arena* arena,
                          gscoped_ptr<ContiguousRow>* row) const {
  row->reset(new ContiguousRow(*this, buffer));

  size_t offset = 0;
  size_t remaining = encoded_key.length();
  for (size_t col_idx = 0; col_idx < num_key_columns(); ++col_idx) {
    const ColumnSchema& col = column(col_idx);
    const KeyEncoder& key_encoder = GetKeyEncoder(col.type_info()->type());
    if (num_key_columns() == 1) {
      key_encoder.Decode(encoded_key,
                         arena,
                         (*row)->mutable_cell_ptr(col_idx));
      break;
    }
    const char* str_data = encoded_key.c_str() + offset;
    size_t key_slice_size;
    if (col.type_info()->type() == STRING) {
      bool is_last = col_idx == (num_key_columns() - 1);
      Slice key_slice(str_data, remaining);
      key_encoder.Decode(key_slice,
                         is_last,
                         arena,
                         (*row)->mutable_cell_ptr(col_idx),
                         &key_slice_size);
    } else {
      key_slice_size = col.type_info()->size();
      Slice key_slice(str_data, key_slice_size);
      key_encoder.Decode(key_slice,
                         arena,
                         (*row)->mutable_cell_ptr(col_idx));
    }
    remaining -= key_slice_size;
    offset += key_slice_size;
  }

}

string Schema::DebugEncodedRowKey(const string& encoded_key) const {
  gscoped_ptr<uint8_t[]> data(new uint8_t[key_byte_size()]);
  Arena arena(1024, 128 * 1024);
  gscoped_ptr<ContiguousRow> row;
  DecodeRowKey(encoded_key, data.get(), &arena, &row);
  return DebugRowKey(*row);
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
