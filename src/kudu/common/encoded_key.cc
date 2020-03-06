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

#include <cstring>
#include <memory>
#include <ostream>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/encoded_key.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/key_util.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

EncodedKey::EncodedKey(faststring* data,
                       vector<const void *> *raw_keys,
                       size_t num_key_cols)
  : num_key_cols_(num_key_cols) {
  int len = data->size();
  data_.reset(data->release());
  encoded_key_ = Slice(data_.get(), len);

  DCHECK_LE(raw_keys->size(), num_key_cols);

  raw_keys_.swap(*raw_keys);
}

unique_ptr<EncodedKey> EncodedKey::FromContiguousRow(const ConstContiguousRow& row) {
  EncodedKeyBuilder kb(row.schema());
  for (int i = 0; i < row.schema()->num_key_columns(); i++) {
    kb.AddColumnKey(row.cell_ptr(i));
  }
  return unique_ptr<EncodedKey>(kb.BuildEncodedKey());

}

Status EncodedKey::DecodeEncodedString(const Schema& schema,
                                       Arena* arena,
                                       const Slice& encoded,
                                       unique_ptr<EncodedKey>* result) {
  uint8_t* raw_key_buf = static_cast<uint8_t*>(arena->AllocateBytes(schema.key_byte_size()));
  if (PREDICT_FALSE(!raw_key_buf)) {
    return Status::RuntimeError("OOM");
  }

  ContiguousRow row(&schema, raw_key_buf);
  RETURN_NOT_OK(schema.DecodeRowKey(encoded, &row, arena));

  vector<const void*> raw_keys(schema.num_key_columns());
  for (int i = 0; i < schema.num_key_columns(); i++) {
    raw_keys[i] = row.cell_ptr(i);
  }

  faststring data_copy;
  data_copy.assign_copy(encoded.data(), encoded.size());

  result->reset(new EncodedKey(&data_copy, &raw_keys, schema.num_key_columns()));
  return Status::OK();
}

Status EncodedKey::IncrementEncodedKey(const Schema& tablet_schema,
                                       unique_ptr<EncodedKey> *key,
                                       Arena* arena) {
  // Copy the row itself to the Arena.
  uint8_t* new_row_key = static_cast<uint8_t*>(
      arena->AllocateBytes(tablet_schema.key_byte_size()));
  if (PREDICT_FALSE(!new_row_key)) {
    return Status::RuntimeError("Out of memory allocating row key");
  }

  vector<const void*> new_raw_keys(tablet_schema.num_key_columns());
  for (int i = 0; i < tablet_schema.num_key_columns(); i++) {
    int size = tablet_schema.column(i).type_info()->size();

    void* dst = new_row_key + tablet_schema.column_offset(i);
    new_raw_keys[i] = dst;
    memcpy(dst,
           (*key)->raw_keys()[i],
           size);
  }

  // Increment the new key
  ContiguousRow new_row(&tablet_schema, new_row_key);
  if (!key_util::IncrementPrimaryKey(&new_row, arena)) {
    return Status::IllegalState("No lexicographically greater key exists");
  }

  // Re-encode it.
  faststring buf;
  tablet_schema.EncodeComparableKey(new_row, &buf);

  key->reset(new EncodedKey(&buf, &new_raw_keys, tablet_schema.num_key_columns()));
  return Status::OK();
}

string EncodedKey::Stringify(const Schema &schema) const {
  DCHECK_EQ(schema.num_key_columns(), num_key_cols_);
  DCHECK_EQ(schema.num_key_columns(), raw_keys_.size());

  faststring s;
  s.append("(");
  for (int i = 0; i < raw_keys_.size(); i++) {
    if (i > 0) {
      if (schema.column(i).type_info()->IsMinValue(raw_keys_[i])) {
        // If the value is the minimum, short-circuit to avoid printing keys such as
        // '(2, -9223372036854775808, -9223372036854775808, -9223372036854775808)',
        // and instead print '(2)'. The minimum values are usually filled in
        // automatically upon decoding, so it makes sense to omit them.
        break;
      }
      s.append(", ");
    }
    s.append(schema.column(i).Stringify(raw_keys_[i]));
  }
  s.append(")");
  return s.ToString();
}

////////////////////////////////////////////////////////////

EncodedKeyBuilder::EncodedKeyBuilder(const Schema* schema)
 : schema_(schema),
   encoded_key_(schema->key_byte_size()),
   num_key_cols_(schema->num_key_columns()),
   idx_(0) {
}

void EncodedKeyBuilder::Reset() {
  encoded_key_.clear();
  idx_ = 0;
  raw_keys_.clear();
  encoded_key_.reserve(schema_->key_byte_size());
}

void EncodedKeyBuilder::AddColumnKey(const void *raw_key) {
  DCHECK_LT(idx_, num_key_cols_);

  const ColumnSchema &col = schema_->column(idx_);
  DCHECK(!col.is_nullable());

  const TypeInfo* ti = col.type_info();
  bool is_last = idx_ == num_key_cols_ - 1;
  GetKeyEncoder<faststring>(ti).Encode(raw_key, is_last, &encoded_key_);
  raw_keys_.push_back(raw_key);

  ++idx_;
}

EncodedKey *EncodedKeyBuilder::BuildEncodedKey() {
  if (idx_ == 0) {
    return nullptr;
  }
  auto ret = new EncodedKey(&encoded_key_, &raw_keys_, num_key_cols_);
  idx_ = 0;
  return ret;
}

void EncodedKeyBuilder::AssignCopy(const EncodedKeyBuilder &other) {
  DCHECK_SCHEMA_EQ(*schema_, *other.schema_);

  encoded_key_.assign_copy(other.encoded_key_.data(),
                           other.encoded_key_.length());
  idx_ = other.idx_;
  raw_keys_.assign(other.raw_keys_.begin(), other.raw_keys_.end());
}

string EncodedKey::RangeToString(const EncodedKey* lower, const EncodedKey* upper) {
  string ret;
  if (lower && upper) {
    ret.append("encoded key BETWEEN ");
    ret.append(KUDU_REDACT(lower->encoded_key().ToDebugString()));
    ret.append(" AND ");
    ret.append(KUDU_REDACT(upper->encoded_key().ToDebugString()));
    return ret;
  }
  if (lower) {
    ret.append("encoded key >= ");
    ret.append(KUDU_REDACT(lower->encoded_key().ToDebugString()));
    return ret;
  }
  if (upper) {
    ret.append("encoded key <= ");
    ret.append(KUDU_REDACT(upper->encoded_key().ToDebugString()));
  } else {
    LOG(DFATAL) << "Invalid key!";
    ret = "invalid key range";
  }
  return ret;
}

string EncodedKey::RangeToStringWithSchema(const EncodedKey* lower, const EncodedKey* upper,
                                           const Schema& s) {
  string ret;
  if (lower) {
    ret.append("PK >= ");
    ret.append(s.DebugEncodedRowKey(lower->encoded_key(), Schema::START_KEY));
  }
  if (lower && upper) {
    ret.append(" AND ");
  }
  if (upper) {
    ret.append("PK < ");
    ret.append(s.DebugEncodedRowKey(upper->encoded_key(), Schema::END_KEY));
  }
  return ret;
}

} // namespace kudu
