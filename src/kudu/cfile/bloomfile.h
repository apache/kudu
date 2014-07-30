// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CFILE_BLOOMFILE_H
#define KUDU_CFILE_BLOOMFILE_H

#include <boost/ptr_container/ptr_vector.hpp>
#include <tr1/memory>
#include <string>
#include <vector>

#include "kudu/cfile/cfile.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/pthread_spinlock.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

using std::tr1::shared_ptr;

class BloomFileWriter {
 public:
  BloomFileWriter(const shared_ptr<WritableFile> &file,
                  const BloomFilterSizing &sizing);

  Status Start();
  Status AppendKeys(const Slice *keys, size_t n_keys);
  Status Finish();

  // Estimate the amount of data already written to this file.
  size_t written_size() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFileWriter);

  Status FinishCurrentBloomBlock();

  gscoped_ptr<cfile::Writer> writer_;

  BloomFilterBuilder bloom_builder_;

  // first key inserted in the current block.
  faststring first_key_;
};

// Reader for a bloom file.
// NB: this is not currently thread-safe.
// When making it thread-safe, should make sure that the threads
// share a single CFileReader, or else the cache keys won't end up
// shared!
class BloomFileReader {
 public:
  static Status Open(Env *env, const string &path,
                     gscoped_ptr<BloomFileReader> *reader);

  static Status Open(const shared_ptr<RandomAccessFile> &file,
                     uint64_t file_size,
                     gscoped_ptr<BloomFileReader> *reader);

  // Check if the given key may be present in the file.
  //
  // Sets *maybe_present to false if the key is definitely not
  // present, otherwise sets it to true to indicate maybe present.
  Status CheckKeyPresent(const BloomKeyProbe &probe,
                         bool *maybe_present);

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFileReader);

  // Constructor. Takes ownership of 'reader'
  //
  // 'reader' should already have had CFileReader::Init() called.
  explicit BloomFileReader(CFileReader *reader);
  Status Init();

  // Parse the header present in the given block.
  //
  // Returns the parsed header inside *hdr, and returns
  // a Slice to the true bloom filter data inside
  // *bloom_data.
  Status ParseBlockHeader(const Slice &block,
                          BloomBlockHeaderPB *hdr,
                          Slice *bloom_data) const;

  gscoped_ptr<CFileReader> reader_;

  // TODO: temporary workaround for the fact that
  // the index tree iterator is a member of the Reader object.
  // We need a big per-thread object which gets passed around so as
  // to avoid this... Instead we'll use a per-CPU iterator as a
  // lame hack.
  boost::ptr_vector<cfile::IndexTreeIterator> index_iters_;
  std::vector<PThreadSpinLock> iter_locks_;
};

} // namespace cfile
} // namespace kudu

#endif
