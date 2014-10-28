// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include <vector>

#include "kudu/common/encoded_key.h"
#include "kudu/common/key_encoder.h"

namespace kudu {

using std::string;


EncodedKey::EncodedKey(faststring *data,
                       vector<const void *> *raw_keys,
                       size_t num_key_cols)
  : num_key_cols_(num_key_cols) {
  int len = data->size();
  data_.reset(data->release());
  encoded_key_ = Slice(data_.get(), len);

  DCHECK_LE(raw_keys->size(), num_key_cols);

  raw_keys_.swap(*raw_keys);
}

string EncodedKey::Stringify(const Schema &schema) const {
  if (num_key_cols_ == 1) {
    return schema.column(0).Stringify(raw_keys_.front());
  }

  faststring s;
  s.append("(");
  for (int i = 0; i < num_key_cols_; i++) {
    if (i > 0) {
      s.append(",");
    }
    if (i < raw_keys_.size()) {
      s.append(schema.column(i).Stringify(raw_keys_[i]));
    } else {
      s.append("*");
    }
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
  GetKeyEncoder(ti->type()).Encode(raw_key, is_last, &encoded_key_);
  raw_keys_.push_back(raw_key);

  ++idx_;
}

EncodedKey* EncodedKeyBuilder::BuildSuccessorEncodedKey() {
  return encoded_key_.AdvanceToSuccessor() ? BuildEncodedKey() : NULL;
}

EncodedKey *EncodedKeyBuilder::BuildEncodedKey() {
  if (idx_ == 0) {
    return NULL;
  }
  EncodedKey *ret = new EncodedKey(&encoded_key_, &raw_keys_, num_key_cols_);
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

////////////////////////////////////////////////////////////

EncodedKeyRange::EncodedKeyRange(EncodedKey *lower_bound,
                                 EncodedKey *upper_bound) :
    lower_bound_(lower_bound),
    upper_bound_(upper_bound) {
}

EncodedKeyRange::~EncodedKeyRange() {
  if (upper_bound_ != lower_bound_) {
    delete upper_bound_;
  }
  delete lower_bound_;
}

bool EncodedKeyRange::ContainsKey(const Slice &key) const {
  if (has_lower_bound() && key.compare(lower_bound_->encoded_key()) < 0) {
    return false;
  }
  if (has_upper_bound() && key.compare(upper_bound_->encoded_key()) > 0) {
    return false;
  }
  return true;
}

string EncodedKeyRange::ToString() const {
  string ret;
  if (has_lower_bound() && has_upper_bound()) {
    ret.append("encoded key BETWEEN ");
    ret.append(lower_bound_->encoded_key().ToDebugString());
    ret.append(" AND ");
    ret.append(upper_bound_->encoded_key().ToDebugString());
    return ret;
  } else if (has_lower_bound()) {
    ret.append("encoded key >= ");
    ret.append(lower_bound_->encoded_key().ToDebugString());
    return ret;
  } else if (has_upper_bound()) {
    ret.append("encoded key <= ");
    ret.append(upper_bound_->encoded_key().ToDebugString());
  } else {
    LOG(DFATAL) << "Invalid key!";
    ret = "invalid key range";
  }
  return ret;
}

} // namespace kudu
