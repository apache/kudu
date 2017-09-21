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

#include <cstdint>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/cfile/block_handle.h"
#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/bloomfile.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/move.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/coding.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/logging.h"
#include "kudu/util/malloc.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/threadlocal_cache.h"

DECLARE_bool(cfile_lazy_open);

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace cfile {

using fs::BlockCreationTransaction;
using fs::ReadableBlock;
using fs::WritableBlock;

// Generator for BloomFileReader::instance_nonce_.
static Atomic64 g_next_nonce = 0;

namespace {

// Frequently, a thread processing a batch of operations will consult the same BloomFile
// many times in a row. So, we keep a thread-local cache of the state for recently-accessed
// BloomFileReaders so that we can avoid doing repetitive work.
class BloomCacheItem {
 public:
  explicit BloomCacheItem(CFileReader* reader)
      : index_iter(reader, reader->validx_root()),
        cur_block_pointer(0, 0) {
  }

  // The IndexTreeIterator used to seek the BloomFileReader last time it was accessed.
  IndexTreeIterator index_iter;

  // The block pointer to the specific block we read last time we used this bloom reader.
  BlockPointer cur_block_pointer;
  // The block handle and parsed BloomFilter corresponding to cur_block_pointer.
  BlockHandle cur_block_handle;
  BloomFilter cur_bloom;

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomCacheItem);
};
using BloomCacheTLC = ThreadLocalCache<uint64_t, BloomCacheItem>;
} // anonymous namespace

////////////////////////////////////////////////////////////
// Writer
////////////////////////////////////////////////////////////

BloomFileWriter::BloomFileWriter(unique_ptr<WritableBlock> block,
                                 const BloomFilterSizing &sizing)
  : bloom_builder_(sizing) {
  cfile::WriterOptions opts;
  opts.write_posidx = false;
  opts.write_validx = true;
  // Never use compression, regardless of the default settings, since
  // bloom filters are high-entropy data structures by their nature.
  opts.storage_attributes.encoding  = PLAIN_ENCODING;
  opts.storage_attributes.compression = NO_COMPRESSION;
  writer_.reset(new cfile::CFileWriter(opts, GetTypeInfo(BINARY), false, std::move(block)));
}

Status BloomFileWriter::Start() {
  return writer_->Start();
}

Status BloomFileWriter::Finish() {
  BlockCreationTransaction transaction(writer_->block()->block_manager());
  RETURN_NOT_OK(FinishAndReleaseBlock(&transaction));
  return transaction.CommitCreatedBlocks();
}

Status BloomFileWriter::FinishAndReleaseBlock(BlockCreationTransaction* transaction) {
  if (bloom_builder_.count() > 0) {
    RETURN_NOT_OK(FinishCurrentBloomBlock());
  }
  return writer_->FinishAndReleaseBlock(transaction);
}

size_t BloomFileWriter::written_size() const {
  return writer_->written_size();
}

Status BloomFileWriter::AppendKeys(
  const Slice *keys, size_t n_keys) {

  // If this is the call on a new bloom, copy the first key.
  if (bloom_builder_.count() == 0 && n_keys > 0) {
    first_key_.assign_copy(keys[0].data(), keys[0].size());
  }

  for (size_t i = 0; i < n_keys; i++) {

    bloom_builder_.AddKey(BloomKeyProbe(keys[i]));

    // Bloom has reached optimal occupancy: flush it to the file
    if (PREDICT_FALSE(bloom_builder_.count() >= bloom_builder_.expected_count())) {
      RETURN_NOT_OK(FinishCurrentBloomBlock());

      // Update the last key and set the next key as the first key of the next block.
      // Setting the first key here avoids having to do it in normal code path of the loop.
      last_key_.assign_copy(keys[i].data(), keys[i].size());
      if (i < n_keys - 1) {
        first_key_.assign_copy(keys[i + 1].data(), keys[i + 1].size());
      }

    }
  }

  return Status::OK();
}

Status BloomFileWriter::FinishCurrentBloomBlock() {
  VLOG(1) << "Appending a new bloom block, first_key="
          << KUDU_REDACT(Slice(first_key_).ToDebugString());

  // Encode the header.
  BloomBlockHeaderPB hdr;
  hdr.set_num_hash_functions(bloom_builder_.n_hashes());
  faststring hdr_str;
  PutFixed32(&hdr_str, hdr.ByteSize());
  pb_util::AppendToString(hdr, &hdr_str);

  // The data is the concatenation of the header and the bloom itself.
  vector<Slice> slices;
  slices.emplace_back(hdr_str);
  slices.push_back(bloom_builder_.slice());

  // Append to the file.
  Slice start_key(first_key_);
  Slice last_key(last_key_);
  RETURN_NOT_OK(writer_->AppendRawBlock(slices, 0, &start_key, last_key, "bloom block"));

  bloom_builder_.Clear();

  #ifndef NDEBUG
  first_key_.assign_copy("POST_RESET");
  #endif

  return Status::OK();
}

////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status BloomFileReader::Open(unique_ptr<ReadableBlock> block,
                             ReaderOptions options,
                             gscoped_ptr<BloomFileReader> *reader) {
  gscoped_ptr<BloomFileReader> bf_reader;
  RETURN_NOT_OK(OpenNoInit(std::move(block),
                           std::move(options), &bf_reader));
  RETURN_NOT_OK(bf_reader->Init());

  *reader = std::move(bf_reader);
  return Status::OK();
}

Status BloomFileReader::OpenNoInit(unique_ptr<ReadableBlock> block,
                                   ReaderOptions options,
                                   gscoped_ptr<BloomFileReader> *reader) {
  unique_ptr<CFileReader> cf_reader;
  RETURN_NOT_OK(CFileReader::OpenNoInit(std::move(block),
                                        options, &cf_reader));
  gscoped_ptr<BloomFileReader> bf_reader(new BloomFileReader(
      std::move(cf_reader), std::move(options)));
  if (!FLAGS_cfile_lazy_open) {
    RETURN_NOT_OK(bf_reader->Init());
  }

  *reader = std::move(bf_reader);
  return Status::OK();
}

BloomFileReader::BloomFileReader(unique_ptr<CFileReader> reader,
                                 ReaderOptions options)

    : instance_nonce_(base::subtle::NoBarrier_AtomicIncrement(&g_next_nonce, 1)),
      reader_(std::move(reader)),
      mem_consumption_(std::move(options.parent_mem_tracker),
                       memory_footprint_excluding_reader()) {
}

Status BloomFileReader::Init() {
  return init_once_.Init(&BloomFileReader::InitOnce, this);
}

Status BloomFileReader::InitOnce() {
  // Fully open the CFileReader if it was lazily opened earlier.
  //
  // If it's already initialized, this is a no-op.
  RETURN_NOT_OK(reader_->Init());

  if (reader_->is_compressed()) {
    return Status::Corruption("bloom file is compressed (compression not supported)",
                              reader_->ToString());
  }
  if (!reader_->has_validx()) {
    return Status::Corruption("bloom file missing value index",
                              reader_->ToString());
  }
  return Status::OK();
}

Status BloomFileReader::ParseBlockHeader(const Slice &block,
                                         BloomBlockHeaderPB *hdr,
                                         Slice *bloom_data) const {
  Slice data(block);
  if (PREDICT_FALSE(data.size() < 4)) {
    return Status::Corruption("Invalid bloom block header: not enough bytes");
  }

  uint32_t header_len = DecodeFixed32(data.data());
  data.remove_prefix(sizeof(header_len));

  if (header_len > data.size()) {
    return Status::Corruption(
      StringPrintf("Header length %d doesn't fit in buffer of size %ld",
                   header_len, data.size()));
  }

  if (!hdr->ParseFromArray(data.data(), header_len)) {
    return Status::Corruption(
      string("Invalid bloom block header: ") +
      hdr->InitializationErrorString() +
      "\nHeader:" + HexDump(Slice(data.data(), header_len)));
  }

  data.remove_prefix(header_len);
  *bloom_data = data;
  return Status::OK();
}

Status BloomFileReader::CheckKeyPresent(const BloomKeyProbe &probe,
                                        bool *maybe_present) {
  DCHECK(init_once_.initted());

  // Since we frequently will access the same BloomFile many times in a row
  // when processing a batch of operations, we put our state in a small thread-local
  // cache, keyed by the BloomFileReader's nonce. We use this nonce rather than
  // the BlockID because it's possible that a BloomFile could be closed and
  // re-opened, in which case we don't want to use our previous cache entry,
  // which now points to a destructed CFileReader.
  auto* tlc = BloomCacheTLC::GetInstance();
  BloomCacheItem* bci = tlc->Lookup(instance_nonce_);
  // If we didn't hit in the cache, make a new cache entry and instantiate a reader.
  if (!bci) {
    bci = tlc->EmplaceNew(instance_nonce_, reader_.get());
  }
  DCHECK_EQ(reader_.get(), bci->index_iter.cfile_reader())
      << "Cached index reader does not match expected instance";

  IndexTreeIterator* index_iter = &bci->index_iter;
  Status s = index_iter->SeekAtOrBefore(probe.key());
  if (PREDICT_FALSE(s.IsNotFound())) {
    // Seek to before the first entry in the file.
    *maybe_present = false;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  // Successfully found the pointer to the bloom block.
  BlockPointer bblk_ptr = index_iter->GetCurrentBlockPointer();

  // If the previous lookup from this bloom on this thread seeked to a different
  // block in the BloomFile, we need to read the correct block and re-hydrate the
  // BloomFilter instance.
  if (!bci->cur_block_pointer.Equals(bblk_ptr)) {
    BlockHandle dblk_data;
    RETURN_NOT_OK(reader_->ReadBlock(bblk_ptr, CFileReader::CACHE_BLOCK, &dblk_data));

    // Parse the header in the block.
    BloomBlockHeaderPB hdr;
    Slice bloom_data;
    RETURN_NOT_OK(ParseBlockHeader(dblk_data.data(), &hdr, &bloom_data));

    // Save the data back into our threadlocal cache.
    bci->cur_bloom = BloomFilter(bloom_data, hdr.num_hash_functions());
    bci->cur_block_pointer = bblk_ptr;
    bci->cur_block_handle = std::move(dblk_data);
  }

  // Actually check the bloom filter.
  *maybe_present = bci->cur_bloom.MayContainKey(probe);
  return Status::OK();
}

size_t BloomFileReader::memory_footprint_excluding_reader() const {
  return kudu_malloc_usable_size(this) + init_once_.memory_footprint_excluding_this();
}

} // namespace cfile
} // namespace kudu
