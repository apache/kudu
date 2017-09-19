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
#ifndef KUDU_CFILE_BLOOMFILE_H
#define KUDU_CFILE_BLOOMFILE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/once.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

namespace fs {
class BlockCreationTransaction;
class ReadableBlock;
class WritableBlock;
}

namespace cfile {

class BloomBlockHeaderPB;
struct ReaderOptions;

class BloomFileWriter {
 public:
  BloomFileWriter(std::unique_ptr<fs::WritableBlock> block,
                  const BloomFilterSizing &sizing);

  Status Start();
  Status AppendKeys(const Slice *keys, size_t n_keys);

  // Close the bloom's CFile, closing the underlying writable block.
  Status Finish();

  // Close the bloom's CFile, finalizing the underlying block and
  // releasing it to 'transaction'.
  Status FinishAndReleaseBlock(fs::BlockCreationTransaction* transaction);

  // Estimate the amount of data already written to this file.
  size_t written_size() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFileWriter);

  Status FinishCurrentBloomBlock();

  gscoped_ptr<cfile::CFileWriter> writer_;

  BloomFilterBuilder bloom_builder_;

  // first key inserted in the current block.
  faststring first_key_;

  // last key inserted in the previous block
  faststring last_key_;
};

// Reader for a bloom file.
// NB: this is not currently thread-safe.
// When making it thread-safe, should make sure that the threads
// share a single CFileReader, or else the cache keys won't end up
// shared!
class BloomFileReader {
 public:

  // Fully open a bloom file using a previously opened block.
  //
  // After this call, the bloom reader is safe for use.
  static Status Open(std::unique_ptr<fs::ReadableBlock> block,
                     ReaderOptions options,
                     gscoped_ptr<BloomFileReader> *reader);

  // Lazily opens a bloom file using a previously opened block. A lazy open
  // does not incur additional I/O, nor does it validate the contents of
  // the bloom file.
  //
  // Init() must be called before using CheckKeyPresent().
  static Status OpenNoInit(std::unique_ptr<fs::ReadableBlock> block,
                           ReaderOptions options,
                           gscoped_ptr<BloomFileReader> *reader);

  // Fully opens a previously lazily opened bloom file, parsing and
  // validating its contents.
  //
  // May be called multiple times; subsequent calls will no-op.
  Status Init();

  // Check if the given key may be present in the file.
  //
  // Sets *maybe_present to false if the key is definitely not
  // present, otherwise sets it to true to indicate maybe present.
  Status CheckKeyPresent(const BloomKeyProbe &probe,
                         bool *maybe_present);

  // Can be called before Init().
  uint64_t FileSize() const {
    return reader_->file_size();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFileReader);

  BloomFileReader(std::unique_ptr<CFileReader> reader, ReaderOptions options);

  // Parse the header present in the given block.
  //
  // Returns the parsed header inside *hdr, and returns
  // a Slice to the true bloom filter data inside
  // *bloom_data.
  Status ParseBlockHeader(const Slice &block,
                          BloomBlockHeaderPB *hdr,
                          Slice *bloom_data) const;

  // Callback used in 'init_once_' to initialize this bloom file.
  Status InitOnce();

  // Returns the memory usage of this object including the object itself but
  // excluding the CFileReader, which is tracked independently.
  size_t memory_footprint_excluding_reader() const;

  // Sequence number for the instance, generated from a global counter.
  // Used for a ThreadLocalCache key.
  // TODO(todd): if we want to conserve a bit of memory we could try to
  // collapse this into the init_once_ object or some-such.
  const uint64_t instance_nonce_;

  std::unique_ptr<CFileReader> reader_;

  KuduOnceDynamic init_once_;

  ScopedTrackedConsumption mem_consumption_;
};

} // namespace cfile
} // namespace kudu

#endif
