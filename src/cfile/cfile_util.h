// Copyright (c) 2013, Cloudera, inc.
#ifndef CFILE_UTIL_H_
#define CFILE_UTIL_H_

#include <algorithm>

#include "cfile/cfile.pb.h"

#include "common/schema.h"
#include "common/row.h"
#include "common/scan_predicate.h"
#include "common/encoded_key.h"
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

// inline method to encode a key
inline void EncodeKey(const ConstContiguousRow& row_slice,
                      gscoped_ptr<EncodedKey> *encoded_key) {
  const Schema &schema = row_slice.schema();
  EncodedKeyBuilder kb(schema);
  for (int i = 0; i < schema.num_key_columns(); i++) {
    kb.AddColumnKey(row_slice.cell_ptr(schema, i));
  }
  encoded_key->reset(kb.BuildEncodedKey());
}

}  // namespace cfile
}  // namespace kudu

#endif /* CFILE_UTIL_H_ */
