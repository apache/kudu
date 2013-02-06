// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CFILE_BLOOMFILE_H
#define KUDU_CFILE_BLOOMFILE_H

#include <boost/noncopyable.hpp>
#include <tr1/memory>

#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "util/bloom_filter.h"
#include "util/env.h"
#include "util/status.h"

namespace kudu {
namespace cfile {

using std::tr1::shared_ptr;

class BloomFileWriter : boost::noncopyable {
public:
  BloomFileWriter(const shared_ptr<WritableFile> &file);

  Status Start();
  Status Finish();

  Status AppendBloom(const BloomFilterBuilder &bloom,
                     const Slice &start_key);
private:
  scoped_ptr<cfile::Writer> writer_;
};

// Reader for a bloom file.
// NB: this is not currently thread-safe.
// When making it thread-safe, should make sure that the threads
// share a single CFileReader, or else the cache keys won't end up
// shared!
class BloomFileReader : boost::noncopyable {
public:
  static Status Open(Env *env, const string &path,
                     BloomFileReader **reader);

  // Check if the given key may be present in the file.
  //
  // Sets *maybe_present to false if the key is definitely not
  // present, otherwise sets it to true to indicate maybe present.
  Status CheckKeyPresent(const BloomKeyProbe &probe,
                         bool *maybe_present);

private:
  BloomFileReader(const shared_ptr<RandomAccessFile> &file,
                  uint64_t file_size);
  Status Init();

  // Parse the header present in the given block.
  //
  // Returns the parsed header inside *hdr, and returns
  // a Slice to the true bloom filter data inside
  // *bloom_data.
  Status ParseBlockHeader(const Slice &block,
                          BloomBlockHeaderPB *hdr,
                          Slice *bloom_data) const;

  scoped_ptr<cfile::IndexTreeIterator> index_iter_;
  scoped_ptr<CFileReader> reader_;
};


} // namespace tablet
} // namespace kudu

#endif
