// Copyright (c) 2013, Cloudera, inc.
#ifndef CFILE_UTIL_H_
#define CFILE_UTIL_H_

#include "common/schema.h"
#include "common/row.h"

#include "util/bloom_filter.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace cfile {

// TODO move WriterOptions and ReaderOptions here when adding new types
// as that will also require those to be top level dependency wise

// An analogous, simpler version of tablet::RowSetKeyProbe for CFiles
// that does not know how to encode a key and instead receives it
// already encoded
class CFileKeyProbe {
 public:

  // Usually not used directly but from RowSetKeyProbe. Public
  // constructor for testing purposes only.
  //
  // raw_key is not copied and must be valid for the lifetime
  // of this object.
  //
  // encoded_key is only kept as a reference as it is usually managed by
  // RowSetKeyProbe when generating a CFileKeyProbe from it and lifetime
  // of RSKP > lifetime of CFKP
  CFileKeyProbe(const void *raw_key,
                const faststring &encoded_key,
                int num_key_columns)
      : raw_key_(raw_key),
        encoded_key_(encoded_key),
        num_key_columns_(num_key_columns) {
  }

  // Pointer to the raw pointer for the key in memory.
  const void *raw_key() const {
    return raw_key_;
  }

  // Pointer to the key which has been encoded to be contiguous
  // and lexicographically comparable
  const Slice encoded_key() const {
    return Slice(encoded_key_);
  }

  const int num_key_columns() const {
    return num_key_columns_;
  }

 private:
  const void *raw_key_;
  const faststring &encoded_key_;
  int num_key_columns_;
};

// inline method to encode a key
inline void EncodeKey(const Schema &schema,
                      const void *raw_key,
                      faststring *encoded_key) {
  ConstContiguousRow row_slice(schema, raw_key);
  schema.EncodeComparableKey(row_slice, encoded_key);
}

}  // namespace cfile
}  // namespace kudu

#endif /* CFILE_UTIL_H_ */
