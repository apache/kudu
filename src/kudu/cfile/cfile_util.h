// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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
  ReaderOptions();

  // The MemTracker that should account for this reader's memory consumption.
  //
  // Default: the root tracker.
  std::tr1::shared_ptr<MemTracker> parent_mem_tracker;
};

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
                    const DumpIteratorOptions& opts,
                    int indent);

}  // namespace cfile
}  // namespace kudu

#endif /* CFILE_UTIL_H_ */
