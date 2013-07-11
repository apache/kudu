// Copyright (c) 2013, Cloudera, inc.
#ifndef CFILE_UTIL_H_
#define CFILE_UTIL_H_

#include <algorithm>

#include "cfile/cfile.pb.h"

#include "common/schema.h"
#include "common/row.h"

#include "util/bloom_filter.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace cfile {


struct WriterOptions {
  // Approximate size of user data packed per block.  Note that the
  // block size specified here corresponds to uncompressed data.  The
  // actual size of the unit read from disk may be smaller if
  // compression is enabled.  This parameter can be changed dynamically.
  //
  // Default: 256K
  size_t block_size;

  // Approximate size of index blocks.
  //
  // Default: 32KB.
  size_t index_block_size;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically.  Most clients should
  // leave this parameter alone.
  //
  // This is currently only used by StringPrefixBlockBuilder
  //
  // Default: 16
  int block_restart_interval;

  // Whether the file needs a positional index.
  bool write_posidx;

  // Whether the file needs a value index
  bool write_validx;

  // Block compression codec type
  //
  // Default: specified by --cfile_default_compression_codec
  CompressionType compression;

  WriterOptions();
};

struct ReaderOptions {
};


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
inline void EncodeKey(const ConstContiguousRow& row_slice,
                      faststring *encoded_key) {
  row_slice.schema().EncodeComparableKey(row_slice, encoded_key);
}

}  // namespace cfile
}  // namespace kudu

#endif /* CFILE_UTIL_H_ */
