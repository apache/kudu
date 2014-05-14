// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_COMMON_ENCODED_KEY_H
#define KUDU_COMMON_ENCODED_KEY_H

#include <string>
#include <vector>

#include "common/schema.h"
#include "util/faststring.h"

namespace kudu {

using std::string;


class EncodedKey {
 public:
  // Constructs a new EncodedKey.
  // This class takes over the value of 'data' and contents of
  // raw_keys. Note that num_key_cols is the number of key columns for
  // the schema, but this may be different from the size of raw_keys
  // in which case raw_keys represents the supplied prefix of a
  // composite key.
  EncodedKey(faststring *data,
             vector<const void *> *raw_keys,
             size_t num_key_cols);

  const Slice &encoded_key() const { return encoded_key_; }

  const vector<const void *> &raw_keys() const { return raw_keys_; }

  size_t num_key_columns() const { return num_key_cols_; }

  string Stringify(const Schema &schema) const;

  // Tests whether this EncodedKey is within the bounds given by 'start'
  // and 'end'.
  //
  // The empty bound has special significance: it's both the lowest value
  // (if in 'start') and the highest (if in 'end').
  bool InRange(const Slice& start, const Slice& end) const {
    return (start.compare(encoded_key_) <= 0 &&
            (end.empty() || encoded_key_.compare(end) < 0));
  }

 private:
  const int num_key_cols_;
  Slice encoded_key_;
  gscoped_ptr<uint8_t[]> data_;
  vector<const void *> raw_keys_;
};

// A builder for encoded key: creates an encoded key from
// one or more key columns specified as raw pointers.
class EncodedKeyBuilder {
 public:
  explicit EncodedKeyBuilder(const Schema &schema);

  void Reset();

  void AddColumnKey(const void *raw_key);

  // Returns the successor of the current encoded key or NULL if
  // there is no successor (i.e., if the key is empty or is composed
  // entirely of 0xff bytes).
  //
  // Calling this mutates the current value of the builder such that
  // another call to BuildEncodedKey() with the same value will not
  // return the same resullt as before.
  EncodedKey *BuildSuccessorEncodedKey();

  EncodedKey *BuildEncodedKey();

  void AssignCopy(const EncodedKeyBuilder &other);

 private:
  DISALLOW_COPY_AND_ASSIGN(EncodedKeyBuilder);

  const Schema &schema_;
  faststring encoded_key_;
  const size_t num_key_cols_;
  size_t idx_;
  vector<const void *> raw_keys_;
};

// Specifies upper and lower bound using encoded keys
class EncodedKeyRange {
 public:

  // Constructs a new EncodedKeyRange.
  // This class takes ownership of the lower_bound and upper_bound
  // pointers.
  EncodedKeyRange(EncodedKey *lower_bound,
                  EncodedKey *upper_bound);

  ~EncodedKeyRange();

  const EncodedKey &lower_bound() const {
    return *lower_bound_;
  }

  const EncodedKey &upper_bound() const {
    return *upper_bound_;
  }

  bool has_lower_bound() const {
    return lower_bound_ != NULL;
  }

  bool has_upper_bound() const {
    return upper_bound_ != NULL;
  }

  bool ContainsKey(const Slice &key) const;

  // Return a string for debugging. This outputs the encoded key format,
  // rather than a user-interpretable string, so shouldn't be used in
  // user-facing contexts.
  std::string ToString() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(EncodedKeyRange);

  EncodedKey *lower_bound_;
  EncodedKey *upper_bound_;
};

} // namespace kudu
#endif
