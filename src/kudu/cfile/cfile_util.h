// Copyright (c) 2013, Cloudera, inc.
#ifndef CFILE_UTIL_H_
#define CFILE_UTIL_H_

#include <algorithm>
#include <iostream>

#include "kudu/cfile/cfile.pb.h"

#include "kudu/common/schema.h"
#include "kudu/common/row.h"
#include "kudu/common/scan_predicate.h"
#include "kudu/common/encoded_key.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

class CFileReader;
class CFileIterator;

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

  // Column storage attributes.
  //
  // Default: all default values as specified in the constructor in
  // schema.h
  ColumnStorageAttributes storage_attributes;

  WriterOptions();
};

struct ReaderOptions {
};

// inline method to encode a key
inline void EncodeKey(const ConstContiguousRow& row_slice,
                      gscoped_ptr<EncodedKey> *encoded_key) {
  const Schema* schema = row_slice.schema();
  EncodedKeyBuilder kb(schema);
  for (int i = 0; i < schema->num_key_columns(); i++) {
    kb.AddColumnKey(row_slice.cell_ptr(i));
  }
  encoded_key->reset(kb.BuildEncodedKey());
}

struct DumpIteratorOptions {
  // If true, print values of rows, otherwise only print aggregate
  // information.
  bool print_rows;

  // Number of rows to iterate over. If 0, will iterate over all rows.
  size_t nrows;

  DumpIteratorOptions()
      : print_rows(false), nrows(0) {
  }
};

// Dumps the contents of a cfile to 'out'; 'reader' and 'iterator'
// must be initialized. See cfile/cfile-dump.cc and tools/fs_tool.cc
// for sample usage.
//
// See also: DumpIteratorOptions
Status DumpIterator(const CFileReader& reader,
                    CFileIterator* it,
                    std::ostream* out,
                    const DumpIteratorOptions& opts);

}  // namespace cfile
}  // namespace kudu

#endif /* CFILE_UTIL_H_ */
