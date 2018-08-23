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

#include "kudu/cfile/cfile_reader.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cfile/binary_plain_block.h"
#include "kudu/cfile/block_cache.h"
#include "kudu/cfile/block_compression.h"
#include "kudu/cfile/block_handle.h"
#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h" // for kMagicString
#include "kudu/cfile/index_btree.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/column_materialization_context.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/types.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/cache.h"
#include "kudu/util/coding.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/malloc.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/memory/overwrite.h"
#include "kudu/util/object_pool.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/rle-encoding.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

DEFINE_bool(cfile_lazy_open, true,
            "Allow lazily opening of cfiles");
TAG_FLAG(cfile_lazy_open, hidden);

DEFINE_bool(cfile_verify_checksums, true,
            "Verify the checksum for each block on read if one exists");
TAG_FLAG(cfile_verify_checksums, evolving);

using kudu::fs::ReadableBlock;
using kudu::pb_util::SecureDebugString;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cfile {

const char* CFILE_CACHE_MISS_BYTES_METRIC_NAME = "cfile_cache_miss_bytes";
const char* CFILE_CACHE_HIT_BYTES_METRIC_NAME = "cfile_cache_hit_bytes";

// Magic+Length: 8-byte magic, followed by 4-byte header size
static const size_t kMagicAndLengthSize = 12;
static const size_t kMaxHeaderFooterPBSize = 64*1024;

static Status ParseMagicAndLength(const Slice &data,
                                  uint8_t* cfile_version,
                                  uint32_t *parsed_len) {
  if (data.size() != kMagicAndLengthSize) {
    return Status::Corruption("Bad size data");
  }

  uint8_t version;
  if (memcmp(kMagicStringV1, data.data(), kMagicLength) == 0) {
    version = 1;
  } else if (memcmp(kMagicStringV2, data.data(), kMagicLength) == 0) {
    version = 2;
  } else {
    return Status::Corruption("bad CFile header magic", data.ToDebugString());
  }

  uint32_t len = DecodeFixed32(data.data() + kMagicLength);
  if (len > kMaxHeaderFooterPBSize) {
    return Status::Corruption("invalid data size for header",
                              std::to_string(len));
  }

  *cfile_version = version;
  *parsed_len = len;

  return Status::OK();
}

CFileReader::CFileReader(ReaderOptions options,
                         uint64_t file_size,
                         unique_ptr<ReadableBlock> block) :
  block_(std::move(block)),
  file_size_(file_size),
  codec_(nullptr),
  mem_consumption_(std::move(options.parent_mem_tracker),
                   memory_footprint()) {
}

Status CFileReader::Open(unique_ptr<ReadableBlock> block,
                         ReaderOptions options,
                         unique_ptr<CFileReader>* reader) {
  unique_ptr<CFileReader> reader_local;
  RETURN_NOT_OK(OpenNoInit(std::move(block),
                           std::move(options),
                           &reader_local));
  RETURN_NOT_OK(reader_local->Init());

  *reader = std::move(reader_local);
  return Status::OK();
}

Status CFileReader::OpenNoInit(unique_ptr<ReadableBlock> block,
                               ReaderOptions options,
                               unique_ptr<CFileReader>* reader) {
  uint64_t block_size;
  RETURN_NOT_OK(block->Size(&block_size));
  unique_ptr<CFileReader> reader_local(
      new CFileReader(std::move(options), block_size, std::move(block)));
  if (!FLAGS_cfile_lazy_open) {
    RETURN_NOT_OK(reader_local->Init());
  }

  *reader = std::move(reader_local);
  return Status::OK();
}

Status CFileReader::InitOnce() {
  VLOG(1) << "Initializing CFile with ID " << block_->id().ToString();
  TRACE_COUNTER_INCREMENT("cfile_init", 1);

  // Parse Footer first to find unsupported features.
  RETURN_NOT_OK(ReadAndParseFooter());

  RETURN_NOT_OK(ReadAndParseHeader());

  if (PREDICT_FALSE(footer_->incompatible_features() & ~IncompatibleFeatures::SUPPORTED)) {
    return Status::NotSupported(Substitute(
        "cfile uses features from an incompatible bitset value $0 vs supported $1 ",
        footer_->incompatible_features(),
        IncompatibleFeatures::SUPPORTED));
  }

  type_info_ = GetTypeInfo(footer_->data_type());

  RETURN_NOT_OK(TypeEncodingInfo::Get(type_info_,
                                      footer_->encoding(),
                                      &type_encoding_info_));

  VLOG(2) << "Initialized CFile reader. "
          << "Header: " << SecureDebugString(*header_)
          << " Footer: " << SecureDebugString(*footer_)
          << " Type: " << type_info_->name();

  // The header/footer have been allocated; memory consumption has changed.
  mem_consumption_.Reset(memory_footprint());

  return Status::OK();
}

Status CFileReader::Init() {
  RETURN_NOT_OK_PREPEND(init_once_.Init([this] { return InitOnce(); }),
                        Substitute("failed to init CFileReader for block $0",
                                   block_id().ToString()));
  return Status::OK();
}

Status CFileReader::ReadAndParseHeader() {
  TRACE_EVENT1("io", "CFileReader::ReadAndParseHeader",
               "cfile", ToString());
  DCHECK(!init_once_.init_succeeded());

  // First read and parse the "pre-header", which lets us know
  // that it is indeed a CFile and tells us the length of the
  // proper protobuf header.
  uint8_t mal_scratch[kMagicAndLengthSize];
  Slice mal(mal_scratch, kMagicAndLengthSize);
  RETURN_NOT_OK_PREPEND(block_->Read(0, mal),
                        "failed to read CFile pre-header");
  uint32_t header_size;
  RETURN_NOT_OK_PREPEND(ParseMagicAndLength(mal, &cfile_version_, &header_size),
                        "failed to parse CFile pre-header");

  // Quick check to ensure the header size is reasonable.
  if (header_size >= file_size_ - kMagicAndLengthSize) {
    return Status::Corruption("invalid CFile header size", std::to_string(header_size));
  }

  // Setup the data slices.
  uint64_t off = kMagicAndLengthSize;
  uint8_t header_scratch[header_size];
  Slice header(header_scratch, header_size);
  uint8_t checksum_scratch[kChecksumSize];
  Slice checksum(checksum_scratch, kChecksumSize);

  // Read the header and checksum if needed.
  vector<Slice> results = { header };
  if (has_checksums() && FLAGS_cfile_verify_checksums) {
    results.push_back(checksum);
  }
  RETURN_NOT_OK(block_->ReadV(off, results));

  if (has_checksums() && FLAGS_cfile_verify_checksums) {
    Slice slices[] = { mal, header };
    RETURN_NOT_OK(VerifyChecksum(slices, checksum));
  }

  // Parse the protobuf header.
  header_.reset(new CFileHeaderPB());
  if (!header_->ParseFromArray(header.data(), header.size())) {
    return Status::Corruption("invalid cfile pb header",
                              header.ToDebugString());
  }

  VLOG(2) << "Read header: " << SecureDebugString(*header_);

  return Status::OK();
}

Status CFileReader::ReadAndParseFooter() {
  TRACE_EVENT1("io", "CFileReader::ReadAndParseFooter",
               "cfile", ToString());
  DCHECK(!init_once_.init_succeeded());
  CHECK_GT(file_size_, kMagicAndLengthSize) <<
    "file too short: " << file_size_;

  // First read and parse the "post-footer", which has magic
  // and the length of the actual protobuf footer.
  uint8_t mal_scratch[kMagicAndLengthSize];
  Slice mal(mal_scratch, kMagicAndLengthSize);
  RETURN_NOT_OK(block_->Read(file_size_ - kMagicAndLengthSize, mal));
  uint32_t footer_size;
  RETURN_NOT_OK(ParseMagicAndLength(mal, &cfile_version_, &footer_size));

  // Quick check to ensure the footer size is reasonable.
  if (footer_size >= file_size_ - kMagicAndLengthSize) {
    return Status::Corruption(Substitute(
        "invalid CFile footer size $0 in block of size $1",
        footer_size, file_size_));
  }

  uint8_t footer_scratch[footer_size];
  Slice footer(footer_scratch, footer_size);

  uint8_t checksum_scratch[kChecksumSize];
  Slice checksum(checksum_scratch, kChecksumSize);

  // Read both the header and checksum in one call.
  // We read the checksum position in case one exists.
  // This is done to avoid the need for a follow up read call.
  Slice results[2] = {checksum, footer};
  uint64_t off = file_size_ - kMagicAndLengthSize - footer_size - kChecksumSize;
  RETURN_NOT_OK(block_->ReadV(off, results));

  // Parse the protobuf footer.
  // This needs to be done before validating the checksum since the
  // incompatible_features flag tells us if a checksum exists at all.
  footer_.reset(new CFileFooterPB());
  if (!footer_->ParseFromArray(footer.data(), footer.size())) {
    return Status::Corruption("invalid cfile pb footer", footer.ToDebugString());
  }

  // Verify the footer checksum if needed.
  if (has_checksums() && FLAGS_cfile_verify_checksums) {
    // If a checksum exists it was pre-read.
    Slice slices[2] = {footer, mal};
    RETURN_NOT_OK(VerifyChecksum(slices, checksum));
  }

  // Verify if the compression codec is available.
  if (footer_->compression() != NO_COMPRESSION) {
    RETURN_NOT_OK_PREPEND(GetCompressionCodec(footer_->compression(), &codec_),
                          "failed to load CFile compression codec");
  }

  VLOG(2) << "Read footer: " << SecureDebugString(*footer_);

  return Status::OK();
}

bool CFileReader::has_checksums() const {
  return footer_->incompatible_features() & IncompatibleFeatures::CHECKSUM;
}

Status CFileReader::VerifyChecksum(ArrayView<const Slice> data, const Slice& checksum) const {
  uint32_t expected_checksum = DecodeFixed32(checksum.data());
  uint32_t checksum_value = 0;
  for (auto& d : data) {
    checksum_value = crc::Crc32c(d.data(), d.size(), checksum_value);
  }
  if (PREDICT_FALSE(checksum_value != expected_checksum)) {
    return Status::Corruption(
        Substitute("Checksum does not match: $0 vs expected $1",
                   checksum_value, expected_checksum));
  }
  return Status::OK();
}

namespace {

// ScratchMemory acts as a holder for the destination buffer for a block read.
// The buffer itself could either be allocated on the heap or be the value of
// a pending block cache entry.
//
// In the case of the default DRAM-based cache, these two are equivalent, but we still
// make a distinction between "cache-managed" memory and "on-heap" memory. In the case of
// the NVM-based cache, this is a more important distinction: we would like to read (or
// decompress) blocks directly into NVM.
//
// This class tracks the block of memory, its size, and whether it came from the heap
// or the cache. In its destructor, the memory is freed, either via 'delete[]', if
// it's heap memory, or via Cache::Free(), if it came from the cache. Alternatively,
// the memory can be released using 'release()'.
class ScratchMemory {
 public:
  ScratchMemory() : ptr_(nullptr), size_(-1) {}
  ~ScratchMemory() {
    if (!ptr_) return;
    if (!from_cache_.valid()) {
      delete[] ptr_;
    }
  }

  // Try to allocate 'size' bytes from the cache. If the cache has
  // no capacity and cannot evict to make room, this will fall back
  // to allocating from the heap. In that case, IsFromCache() will
  // return false.
  void TryAllocateFromCache(BlockCache* cache, const BlockCache::CacheKey& key, int size) {
    DCHECK(!ptr_);
    from_cache_ = cache->Allocate(key, size);
    if (!from_cache_.valid()) {
      AllocateFromHeap(size);
      return;
    } else {
      ptr_ = from_cache_.val_ptr();
    }
    size_ = size;
  }

  void AllocateFromHeap(int size) {
    DCHECK(!ptr_);
    from_cache_.reset();
    ptr_ = new uint8_t[size];
    size_ = size;
  }

  // Return true if the current scratch memory was allocated from the cache.
  bool IsFromCache() const {
    return from_cache_.valid();
  }

  BlockCache::PendingEntry* mutable_pending_entry() {
    return &from_cache_;
  }

  uint8_t* get() {
    return DCHECK_NOTNULL(ptr_);
  }

  Slice as_slice() {
    return Slice(ptr_, size_);
  }

  uint8_t* release() {
    uint8_t* ret = ptr_;
    ptr_ = nullptr;
    size_ = -1;
    return ret;
  }

  // Swap the contents of this instance with another.
  void Swap(ScratchMemory* other) {
    std::swap(from_cache_, other->from_cache_);
    std::swap(ptr_, other->ptr_);
    std::swap(size_, other->size_);
  }

 private:
  BlockCache::PendingEntry from_cache_;
  uint8_t* ptr_;
  int size_;
  DISALLOW_COPY_AND_ASSIGN(ScratchMemory);
};
} // anonymous namespace

Status CFileReader::ReadBlock(const BlockPointer &ptr, CacheControl cache_control,
                              BlockHandle *ret) const {
  DCHECK(init_once_.init_succeeded());
  CHECK(ptr.offset() > 0 &&
        ptr.offset() + ptr.size() < file_size_) <<
    "bad offset " << ptr.ToString() << " in file of size "
                  << file_size_;
  BlockCacheHandle bc_handle;
  Cache::CacheBehavior cache_behavior = cache_control == CACHE_BLOCK ?
      Cache::EXPECT_IN_CACHE : Cache::NO_EXPECT_IN_CACHE;
  BlockCache* cache = BlockCache::GetSingleton();
  BlockCache::CacheKey key(block_->id(), ptr.offset());
  if (cache->Lookup(key, cache_behavior, &bc_handle)) {
    TRACE_COUNTER_INCREMENT("cfile_cache_hit", 1);
    TRACE_COUNTER_INCREMENT(CFILE_CACHE_HIT_BYTES_METRIC_NAME, ptr.size());
    *ret = BlockHandle::WithDataFromCache(&bc_handle);
    // Cache hit
    return Status::OK();
  }

  // Cache miss: need to read ourselves.
  // We issue trace events only in the cache miss case since we expect the
  // tracing overhead to be small compared to the IO (even if it's a memcpy
  // from the Linux cache).
  TRACE_EVENT1("io", "CFileReader::ReadBlock(cache miss)",
               "cfile", ToString());
  TRACE_COUNTER_INCREMENT("cfile_cache_miss", 1);
  TRACE_COUNTER_INCREMENT(CFILE_CACHE_MISS_BYTES_METRIC_NAME, ptr.size());

  uint32_t data_size = ptr.size();
  if (has_checksums()) {
    if (PREDICT_FALSE(kChecksumSize > data_size)) {
      return Status::Corruption("invalid data size for block pointer",
                                ptr.ToString());
    }
    data_size -= kChecksumSize;
  }

  ScratchMemory scratch;
  // If we are reading uncompressed data and plan to cache the result,
  // then we should allocate our scratch memory directly from the cache.
  // This avoids an extra memory copy in the case of an NVM cache.
  if (codec_ == nullptr && cache_control == CACHE_BLOCK) {
    scratch.TryAllocateFromCache(cache, key, data_size);
  } else {
    scratch.AllocateFromHeap(data_size);
  }
  uint8_t* buf = scratch.get();
  Slice block(buf, data_size);
  uint8_t checksum_scratch[kChecksumSize];
  Slice checksum(checksum_scratch, kChecksumSize);

  // Read the data and checksum if needed.
  Slice results_backing[] = { block, checksum };
  bool read_checksum = has_checksums() && FLAGS_cfile_verify_checksums;
  ArrayView<Slice> results(results_backing, read_checksum ? 2 : 1);
  RETURN_NOT_OK_PREPEND(block_->ReadV(ptr.offset(), results),
                        Substitute("failed to read CFile block $0 at $1",
                                   block_id().ToString(), ptr.ToString()));

  if (has_checksums() && FLAGS_cfile_verify_checksums) {
    RETURN_NOT_OK_PREPEND(VerifyChecksum(ArrayView<const Slice>(&block, 1), checksum),
                          Substitute("checksum error on CFile block $0 at $1",
                                     block_id().ToString(), ptr.ToString()));
  }

  // Decompress the block
  if (codec_ != nullptr) {
    // Init the decompressor and get the size required for the uncompressed buffer.
    CompressedBlockDecoder uncompressor(codec_, cfile_version_, block);
    Status s = uncompressor.Init();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to validate compressed block " << block_id().ToString()
                   << " at " << ptr.offset() << " of size " << block.size() << ": "
                   << s.ToString();
      return s;
    }
    int uncompressed_size = uncompressor.uncompressed_size();

    // If we plan to put the uncompressed block in the cache, we should
    // decompress directly into the cache's memory (to avoid a memcpy for NVM).
    ScratchMemory decompressed_scratch;
    if (cache_control == CACHE_BLOCK) {
      decompressed_scratch.TryAllocateFromCache(cache, key, uncompressed_size);
    } else {
      decompressed_scratch.AllocateFromHeap(uncompressed_size);
    }
    s = uncompressor.UncompressIntoBuffer(decompressed_scratch.get());
    if (!s.ok()) {
      LOG(WARNING) << "Unable to uncompress block " << block_id().ToString()
                   << " at " << ptr.offset()
                   << " of size " <<  block.size() << ": " << s.ToString();
      return s;
    }

    // Now that we've decompressed, we don't need to keep holding onto the original
    // scratch buffer. Instead, we have to start holding onto our decompression
    // output buffer.
    scratch.Swap(&decompressed_scratch);

    // Set the result block to our decompressed data.
    block = Slice(buf, uncompressed_size);
  } else {
    // Some of the File implementations from LevelDB attempt to be tricky
    // and just return a Slice into an mmapped region (or in-memory region).
    // But, this is hard to program against in terms of cache management, etc,
    // so we memcpy into our scratch buffer if necessary.
    block.relocate(scratch.get());
  }

  // It's possible that one of the TryAllocateFromCache() calls above
  // failed, in which case we don't insert it into the cache regardless
  // of what the user requested. The scratch memory includes both the
  // generated key and the data read from disk.
  if (cache_control == CACHE_BLOCK && scratch.IsFromCache()) {
    cache->Insert(scratch.mutable_pending_entry(), &bc_handle);
    *ret = BlockHandle::WithDataFromCache(&bc_handle);
  } else {
    // We get here by either not intending to cache the block or
    // if the entry could not be allocated from the block cache.
    // Since we allocate memory to include the key for the cache entry
    // we must reset the block.
    DCHECK_EQ(block.data(), buf);
    DCHECK(!scratch.IsFromCache());
    *ret = BlockHandle::WithOwnedData(scratch.as_slice());
  }

  // The cache or the BlockHandle now has ownership over the memory, so release
  // the scoped pointer.
  ignore_result(scratch.release());

  return Status::OK();
}

Status CFileReader::CountRows(rowid_t *count) const {
  *count = footer().num_values();
  return Status::OK();
}

bool CFileReader::GetMetadataEntry(const string &key, string *val) const {
  for (const FileMetadataPairPB &pair : header().metadata()) {
    if (pair.key() == key) {
      *val = pair.value();
      return true;
    }
  }
  for (const FileMetadataPairPB &pair : footer().metadata()) {
    if (pair.key() == key) {
      *val = pair.value();
      return true;
    }
  }
  return false;
}

Status CFileReader::NewIterator(CFileIterator **iter, CacheControl cache_control) {
  *iter = new CFileIterator(this, cache_control);
  return Status::OK();
}

size_t CFileReader::memory_footprint() const {
  size_t size = kudu_malloc_usable_size(this);
  size += block_->memory_footprint();
  size += init_once_.memory_footprint_excluding_this();

  // SpaceUsed() uses sizeof() instead of malloc_usable_size() to account for
  // the size of base objects (recursively too), thus not accounting for
  // malloc "slop".
  if (header_) {
    size += header_->SpaceUsed();
  }
  if (footer_) {
    size += footer_->SpaceUsed();
  }
  return size;
}

////////////////////////////////////////////////////////////
// Default Column Value Iterator
////////////////////////////////////////////////////////////

Status DefaultColumnValueIterator::SeekToOrdinal(rowid_t ord_idx) {
  ordinal_ = ord_idx;
  return Status::OK();
}

Status DefaultColumnValueIterator::PrepareBatch(size_t *n) {
  batch_ = *n;
  return Status::OK();
}

Status DefaultColumnValueIterator::Scan(ColumnMaterializationContext* ctx) {
  ColumnBlock* dst = ctx->block();
  if (dst->is_nullable()) {
    ColumnDataView dst_view(dst);
    dst_view.SetNullBits(dst->nrows(), value_ != nullptr);
  }
  // Cases where the selection vector can be cleared:
  // 1. There is a read_default and it does not satisfy the predicate
  // 2. There is no read_default and the predicate is searching for NULLs
  if (value_ != nullptr) {
    if (ctx->DecoderEvalNotDisabled() &&
        !ctx->pred()->EvaluateCell(typeinfo_->physical_type(), value_)) {
      SelectionVectorView sel_view(ctx->sel());
      sel_view.ClearBits(dst->nrows());
      return Status::OK();
    }
    if (typeinfo_->physical_type() == BINARY) {
      const Slice *src_slice = reinterpret_cast<const Slice *>(value_);
      Slice dst_slice;
      if (PREDICT_FALSE(!dst->arena()->RelocateSlice(*src_slice, &dst_slice))) {
        return Status::IOError("out of memory copying slice", src_slice->ToString());
      }
      for (size_t i = 0; i < dst->nrows(); ++i) {
        dst->SetCellValue(i, &dst_slice);
      }
    } else {
      for (size_t i = 0; i < dst->nrows(); ++i) {
        dst->SetCellValue(i, value_);
      }
    }
  } else {
    if (ctx->DecoderEvalNotDisabled() && !ctx->EvaluatingIsNull()) {
      SelectionVectorView sel_view(ctx->sel());
      sel_view.ClearBits(dst->nrows());
    }
  }
  return Status::OK();
}

Status DefaultColumnValueIterator::FinishBatch() {
  ordinal_ += batch_;
  return Status::OK();
}

////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////
CFileIterator::CFileIterator(CFileReader* reader,
                             CFileReader::CacheControl cache_control)
  : reader_(reader),
    seeked_(nullptr),
    prepared_(false),
    cache_control_(cache_control),
    last_prepare_idx_(-1),
    last_prepare_count_(-1) {
}

CFileIterator::~CFileIterator() {
}

Status CFileIterator::SeekToOrdinal(rowid_t ord_idx) {
  // Check to see if we already have the required block prepared. Typically
  // (when seeking forward during a scan), only the final block might be
  // suitable, since all previous blocks will have been scanned in the prior
  // batch. Only reusing the final block also ensures posidx_iter_ is already
  // seeked to the correct block.
  if (!prepared_blocks_.empty() &&
      prepared_blocks_.back()->last_row_idx() >= ord_idx &&
      prepared_blocks_.back()->first_row_idx() <= ord_idx) {
    PreparedBlock* b = prepared_blocks_.back();
    prepared_blocks_.pop_back();
    for (PreparedBlock* pb : prepared_blocks_) {
      prepared_block_pool_.Destroy(pb);
    }
    prepared_blocks_.clear();

    prepared_blocks_.push_back(b);
  } else {
    // Otherwise, prepare a new block to scan.
    RETURN_NOT_OK(PrepareForNewSeek());
    if (PREDICT_FALSE(posidx_iter_ == nullptr)) {
      return Status::NotSupported("no positional index in file");
    }

    tmp_buf_.clear();
    KeyEncoderTraits<UINT32, faststring>::Encode(ord_idx, &tmp_buf_);
    RETURN_NOT_OK(posidx_iter_->SeekAtOrBefore(Slice(tmp_buf_)));

    pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
      prepared_block_pool_.Construct());
    RETURN_NOT_OK(ReadCurrentDataBlock(*posidx_iter_, b.get()));

    // If the data block doesn't actually contain the data
    // we're looking for, then we're probably in the last
    // block in the file.
    // TODO(unknown): could assert that each of the index layers is
    // at its last entry (ie HasNext() is false for each)
    if (PREDICT_FALSE(ord_idx > b->last_row_idx())) {
      return Status::NotFound("trying to seek past highest ordinal in file");
    }
    prepared_blocks_.push_back(b.release());
  }

  PreparedBlock* b = prepared_blocks_.back();

  // Seek data block to correct index
  DCHECK(ord_idx >= b->first_row_idx() &&
         ord_idx <= b->last_row_idx())
    << "got wrong data block. looking for ord_idx=" << ord_idx
    << " but got dblk " << b->ToString();
  SeekToPositionInBlock(b, ord_idx - b->first_row_idx());

  last_prepare_idx_ = ord_idx;
  last_prepare_count_ = 0;
  seeked_ = posidx_iter_.get();

  CHECK_EQ(ord_idx, GetCurrentOrdinal());
  return Status::OK();
}

void CFileIterator::SeekToPositionInBlock(PreparedBlock *pb, uint32_t idx_in_block) {
  // Since the data block only holds the non-null values,
  // we need to translate from 'ord_idx' (the absolute row id)
  // to the index within the non-null entries.
  uint32_t index_within_nonnulls;
  if (reader_->is_nullable()) {
    if (PREDICT_TRUE(pb->idx_in_block_ <= idx_in_block)) {
      // We are seeking forward. Skip from the current position in the RLE decoder
      // instead of going back to the beginning of the block.
      uint32_t nskip = idx_in_block - pb->idx_in_block_;
      size_t cur_blk_idx = pb->dblk_->GetCurrentIndex();
      index_within_nonnulls = cur_blk_idx + pb->rle_decoder_.Skip(nskip);
    } else {
      // Seek backward - have to start from the start of the block.
      pb->rle_decoder_ = RleDecoder<bool>(pb->rle_bitmap.data(), pb->rle_bitmap.size(), 1);
      index_within_nonnulls = pb->rle_decoder_.Skip(idx_in_block);
    }
  } else {
    index_within_nonnulls = idx_in_block;
  }

  pb->dblk_->SeekToPositionInBlock(index_within_nonnulls);
  DCHECK_EQ(index_within_nonnulls, pb->dblk_->GetCurrentIndex()) << "failed seek";
  pb->idx_in_block_ = idx_in_block;
}

Status CFileIterator::SeekToFirst() {
  RETURN_NOT_OK(PrepareForNewSeek());
  IndexTreeIterator *idx_iter;
  if (PREDICT_TRUE(posidx_iter_ != nullptr)) {
    RETURN_NOT_OK(posidx_iter_->SeekToFirst());
    idx_iter = posidx_iter_.get();
  } else if (PREDICT_TRUE(validx_iter_ != nullptr)) {
    RETURN_NOT_OK(validx_iter_->SeekToFirst());
    idx_iter = validx_iter_.get();
  } else {
    return Status::NotSupported("no value or positional index present");
  }

  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(*idx_iter, b.get()));
  b->dblk_->SeekToPositionInBlock(0);
  last_prepare_idx_ = 0;
  last_prepare_count_ = 0;

  prepared_blocks_.push_back(b.release());

  seeked_ = idx_iter;
  return Status::OK();
}

Status CFileIterator::SeekAtOrAfter(const EncodedKey &key,
                                    bool *exact_match) {
  RETURN_NOT_OK(PrepareForNewSeek());
  DCHECK_EQ(reader_->is_nullable(), false);

  if (PREDICT_FALSE(validx_iter_ == nullptr)) {
    return Status::NotSupported("no value index present");
  }

  Status s = validx_iter_->SeekAtOrBefore(key.encoded_key());
  if (PREDICT_FALSE(s.IsNotFound())) {
    // Seeking to a value before the first value in the file
    // will return NotFound, due to the way the index seek
    // works. We need to special-case this and have the
    // iterator seek all the way down its leftmost branches
    // to get the correct reslt.
    s = validx_iter_->SeekToFirst();
  }
  RETURN_NOT_OK(s);

  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(*validx_iter_, b.get()));

  Status dblk_seek_status;
  if (key.num_key_columns() > 1) {
    Slice slice = key.encoded_key();
    dblk_seek_status = b->dblk_->SeekAtOrAfterValue(&slice, exact_match);
  } else {
    dblk_seek_status = b->dblk_->SeekAtOrAfterValue(key.raw_keys()[0],
                                                    exact_match);
  }

  // If seeking within the data block results in NotFound, then that indicates that the
  // value we're looking for fell after all the data in that block.
  // If this is not the last block, then the search key was 'in the cracks' between
  // two consecutive blocks, so we need to advance to the next one. If it was the
  // last block in the file, then we just return NotFound(), since there is no
  // value "at or after".
  if (PREDICT_FALSE(dblk_seek_status.IsNotFound())) {
    *exact_match = false;
    if (PREDICT_FALSE(!validx_iter_->HasNext())) {
      return Status::NotFound("key after last block in file",
                              KUDU_REDACT(key.encoded_key().ToDebugString()));
    }
    RETURN_NOT_OK(validx_iter_->Next());
    RETURN_NOT_OK(ReadCurrentDataBlock(*validx_iter_, b.get()));
    SeekToPositionInBlock(b.get(), 0);
  } else {
    // It's possible we got some other error seeking in our data block --
    // still need to propagate those.
    RETURN_NOT_OK(dblk_seek_status);
  }

  last_prepare_idx_ = b->first_row_idx() + b->dblk_->GetCurrentIndex();
  last_prepare_count_ = 0;

  prepared_blocks_.push_back(b.release());

  seeked_ = validx_iter_.get();
  return Status::OK();
}

Status CFileIterator::PrepareForNewSeek() {
  // Fully open the CFileReader if it was lazily opened earlier.
  //
  // If it's already initialized, this is a no-op.
  RETURN_NOT_OK(reader_->Init());

  // Create the index tree iterators if we haven't already done so.
  if (!posidx_iter_ && reader_->footer().has_posidx_info()) {
    BlockPointer bp(reader_->footer().posidx_info().root_block());
    posidx_iter_.reset(IndexTreeIterator::Create(reader_, bp));
  }
  if (!validx_iter_ && reader_->footer().has_validx_info()) {
    BlockPointer bp(reader_->footer().validx_info().root_block());
    validx_iter_.reset(IndexTreeIterator::Create(reader_, bp));
  }

  // Initialize the decoder for the dictionary block
  // in dictionary encoding mode.
  if (!dict_decoder_ && reader_->footer().has_dict_block_ptr()) {
    BlockPointer bp(reader_->footer().dict_block_ptr());

    // Cache the dictionary for performance
    RETURN_NOT_OK_PREPEND(reader_->ReadBlock(bp, CFileReader::CACHE_BLOCK, &dict_block_handle_),
                          "couldn't read dictionary block");

    dict_decoder_.reset(new BinaryPlainBlockDecoder(dict_block_handle_.data()));
    RETURN_NOT_OK_PREPEND(dict_decoder_->ParseHeader(),
                          Substitute("couldn't parse dictionary block header in block $0 ($1)",
                                     reader_->block_id().ToString(),
                                     bp.ToString()));
  }

  seeked_ = nullptr;
  for (PreparedBlock *pb : prepared_blocks_) {
    prepared_block_pool_.Destroy(pb);
  }
  prepared_blocks_.clear();

  return Status::OK();
}

rowid_t CFileIterator::GetCurrentOrdinal() const {
  CHECK(seeked_) << "not seeked";
  return last_prepare_idx_;
}

string CFileIterator::PreparedBlock::ToString() const {
  return StringPrintf("dblk(%s, rows=%d-%d)",
                      dblk_ptr_.ToString().c_str(),
                      first_row_idx(),
                      last_row_idx());
}

// Decode the null header in the beginning of the data block
Status DecodeNullInfo(Slice *data_block, uint32_t *num_rows_in_block, Slice *null_bitmap) {
  if (!GetVarint32(data_block, num_rows_in_block)) {
    return Status::Corruption("bad null header, num elements in block");
  }

  uint32_t null_bitmap_size;
  if (!GetVarint32(data_block, &null_bitmap_size)) {
    return Status::Corruption("bad null header, bitmap size");
  }

  *null_bitmap = Slice(data_block->data(), null_bitmap_size);
  data_block->remove_prefix(null_bitmap_size);
  return Status::OK();
}

Status CFileIterator::ReadCurrentDataBlock(const IndexTreeIterator &idx_iter,
                                           PreparedBlock *prep_block) {
  prep_block->dblk_ptr_ = idx_iter.GetCurrentBlockPointer();
  RETURN_NOT_OK(reader_->ReadBlock(prep_block->dblk_ptr_, cache_control_, &prep_block->dblk_data_));

  uint32_t num_rows_in_block = 0;
  Slice data_block = prep_block->dblk_data_.data();
  if (reader_->is_nullable()) {
    RETURN_NOT_OK(DecodeNullInfo(&data_block, &num_rows_in_block, &(prep_block->rle_bitmap)));
    prep_block->rle_decoder_ = RleDecoder<bool>(prep_block->rle_bitmap.data(),
                                                prep_block->rle_bitmap.size(), 1);
  }

  BlockDecoder *bd;
  RETURN_NOT_OK(reader_->type_encoding_info()->CreateBlockDecoder(&bd, data_block, this));
  prep_block->dblk_.reset(bd);
  RETURN_NOT_OK_PREPEND(prep_block->dblk_->ParseHeader(),
                        Substitute("unable to decode data block header in block $0 ($1)",
                                   reader_->block_id().ToString(),
                                   prep_block->dblk_ptr_.ToString()));

  // For nullable blocks, we filled in the row count from the null information above,
  // since the data block decoder only knows about the non-null values.
  // For non-nullable ones, we use the information from the block decoder.
  if (!reader_->is_nullable()) {
    num_rows_in_block = bd->Count();
  }

  io_stats_.cells_read += num_rows_in_block;
  io_stats_.blocks_read++;
  io_stats_.bytes_read += data_block.size();

  prep_block->idx_in_block_ = 0;
  prep_block->num_rows_in_block_ = num_rows_in_block;
  prep_block->needs_rewind_ = false;
  prep_block->rewind_idx_ = 0;

  DVLOG(2) << "Read dblk " << prep_block->ToString();
  return Status::OK();
}

Status CFileIterator::QueueCurrentDataBlock(const IndexTreeIterator &idx_iter) {
  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(idx_iter, b.get()));
  prepared_blocks_.push_back(b.release());
  return Status::OK();
}

bool CFileIterator::HasNext() const {
  CHECK(seeked_) << "not seeked";
  CHECK(!prepared_) << "Cannot call HasNext() mid-batch";

  return !prepared_blocks_.empty() || seeked_->HasNext();
}

Status CFileIterator::PrepareBatch(size_t *n) {
  CHECK(!prepared_) << "Should call FinishBatch() first";
  CHECK(seeked_ != nullptr) << "must be seeked";

  CHECK(!prepared_blocks_.empty());

  rowid_t start_idx = last_prepare_idx_;
  rowid_t end_idx = start_idx + *n;

  // Read blocks until all blocks covering the requested range are in the
  // prepared_blocks_ queue.
  while (prepared_blocks_.back()->last_row_idx() < end_idx) {
    Status s = seeked_->Next();
    if (PREDICT_FALSE(s.IsNotFound())) {
      VLOG(1) << "Reached EOF";
      break;
    } else if (!s.ok()) {
      return s;
    }
    RETURN_NOT_OK(QueueCurrentDataBlock(*seeked_));
  }

  // Seek the first block in the queue such that the first value to be read
  // corresponds to start_idx
  {
    PreparedBlock *front = prepared_blocks_.front();
    front->rewind_idx_ = start_idx - front->first_row_idx();
    front->needs_rewind_ = true;
  }

  uint32_t size_covered_by_prep_blocks = prepared_blocks_.back()->last_row_idx() - start_idx + 1;
  if (PREDICT_FALSE(size_covered_by_prep_blocks < *n)) {
    *n = size_covered_by_prep_blocks;
  }

  last_prepare_idx_ = start_idx;
  last_prepare_count_ = *n;
  prepared_ = true;

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    VLOG(1) << "Prepared for " << (*n) << " rows"
            << " (" << start_idx << "-" << (start_idx + *n - 1) << ")";
    for (PreparedBlock *b : prepared_blocks_) {
      VLOG(1) << "  " << b->ToString();
    }
    VLOG(1) << "-------------";
  }

  return Status::OK();
}

Status CFileIterator::FinishBatch() {
  CHECK(prepared_) << "no batch prepared";
  prepared_ = false;

  DVLOG(1) << "Finishing batch " << last_prepare_idx_ << "-"
           << (last_prepare_idx_ + last_prepare_count_ - 1);

  // Release all blocks except for the last one, which may still contain
  // relevent data for the next batch.
  for (int i = 0; i < prepared_blocks_.size() - 1; i++) {
    PreparedBlock *b = prepared_blocks_[i];
    prepared_block_pool_.Destroy(b);
  }

  PreparedBlock *back = prepared_blocks_.back();
  DVLOG(1) << "checking last block " << back->ToString() << " vs "
           << last_prepare_idx_ << " + " << last_prepare_count_
           << " (" << (last_prepare_idx_ + last_prepare_count_) << ")";
  if (back->last_row_idx() < last_prepare_idx_ + last_prepare_count_) {
    // Last block is irrelevant
    prepared_block_pool_.Destroy(back);
    prepared_blocks_.clear();
  } else {
    prepared_blocks_[0] = back;
    prepared_blocks_.resize(1);
  }

  #ifndef NDEBUG
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Left around following blocks:";
    for (PreparedBlock *b : prepared_blocks_) {
      VLOG(1) << "  " << b->ToString();
    }
    VLOG(1) << "-------------";
  }
  #endif

  last_prepare_idx_ += last_prepare_count_;
  last_prepare_count_ = 0;
  return Status::OK();
}

Status CFileIterator::Scan(ColumnMaterializationContext* ctx) {
  CHECK(seeked_) << "not seeked";

  // Use views to advance the block and selection vector as we read into them.
  ColumnDataView remaining_dst(ctx->block());
  SelectionVectorView remaining_sel(ctx->sel());
  uint32_t rem = last_prepare_count_;
  DCHECK_LE(rem, ctx->block()->nrows());

  // Determine the matching codewords for dictionary encoding if they haven't
  // yet been determined for this CFile.
  if (dict_decoder_ && ctx->DecoderEvalNotDisabled() && !codewords_matching_pred_) {
    size_t nwords = dict_decoder_->Count();
    if (nwords > 0) {
      codewords_matching_pred_.reset(new SelectionVector(nwords));
      codewords_matching_pred_->SetAllFalse();
      for (size_t i = 0; i < nwords; i++) {
        Slice cur_string = dict_decoder_->string_at_index(i);
        if (ctx->pred()->EvaluateCell<BINARY>(static_cast<const void *>(&cur_string))) {
          BitmapSet(codewords_matching_pred_->mutable_bitmap(), i);
        }
      }
    }
  }
  for (PreparedBlock *pb : prepared_blocks_) {
    if (pb->needs_rewind_) {
      // Seek back to the saved position.
      SeekToPositionInBlock(pb, pb->rewind_idx_);
      // TODO: we could add a mark/reset like interface in BlockDecoder interface
      // that might be more efficient (allowing the decoder to save internal state
      // instead of having to reconstruct it)
    }
    if (reader_->is_nullable()) {
      DCHECK(ctx->block()->is_nullable());

      size_t nrows = std::min(rem, pb->num_rows_in_block_ - pb->idx_in_block_);
      // Fill column bitmap
      size_t count = nrows;
      while (count > 0) {
        bool not_null = false;
        size_t nblock = pb->rle_decoder_.GetNextRun(&not_null, count);
        DCHECK_LE(nblock, count);
        if (PREDICT_FALSE(nblock == 0)) {
          return Status::Corruption(
            Substitute("Unexpected EOF on NULL bitmap read. Expected at least $0 more rows",
                       count));
        }
        size_t this_batch = nblock;
        if (not_null) {
          if (ctx->DecoderEvalNotDisabled()) {
            RETURN_NOT_OK(pb->dblk_->CopyNextAndEval(&this_batch,
                                                     ctx,
                                                     &remaining_sel,
                                                     &remaining_dst));
          } else {
            RETURN_NOT_OK(pb->dblk_->CopyNextValues(&this_batch, &remaining_dst));
          }
          DCHECK_EQ(nblock, this_batch);
          pb->needs_rewind_ = true;
        } else {
#ifndef NDEBUG
          kudu::OverwriteWithPattern(reinterpret_cast<char *>(remaining_dst.data()),
                                     remaining_dst.stride() * nblock,
                                     "NULLNULLNULLNULLNULL");
#endif
          if (ctx->DecoderEvalNotDisabled() && !ctx->EvaluatingIsNull()) {
            remaining_sel.ClearBits(this_batch);
          }
        }

        // Set the ColumnBlock bitmap
        remaining_dst.SetNullBits(this_batch, not_null);

        rem -= this_batch;
        count -= this_batch;
        pb->idx_in_block_ += this_batch;
        remaining_dst.Advance(this_batch);
        remaining_sel.Advance(this_batch);
      }
    } else {
      // Fetch as many as we can from the current datablock.
      size_t this_batch = rem;

      if (ctx->DecoderEvalNotDisabled()) {
        RETURN_NOT_OK(pb->dblk_->CopyNextAndEval(&this_batch, ctx, &remaining_sel, &remaining_dst));
      } else {
        RETURN_NOT_OK(pb->dblk_->CopyNextValues(&this_batch, &remaining_dst));
      }
      pb->needs_rewind_ = true;
      DCHECK_LE(this_batch, rem);

      // If the column is nullable, set all bits to true
      if (ctx->block()->is_nullable()) {
        remaining_dst.SetNullBits(this_batch, true);
      }

      rem -= this_batch;
      pb->idx_in_block_ += this_batch;
      remaining_dst.Advance(this_batch);
      remaining_sel.Advance(this_batch);
    }

    // If we didn't fetch as many as requested, then it should
    // be because the current data block ran out.
    if (rem > 0) {
      DCHECK_EQ(pb->dblk_->Count(), pb->dblk_->GetCurrentIndex()) <<
        "dblk stopped yielding values before it was empty.";
    } else {
      break;
    }
  }

  DCHECK_EQ(rem, 0) << "Should have fetched exactly the number of prepared rows";
  return Status::OK();
}

Status CFileIterator::CopyNextValues(size_t* n, ColumnMaterializationContext* ctx) {
  RETURN_NOT_OK(PrepareBatch(n));
  RETURN_NOT_OK(Scan(ctx));
  RETURN_NOT_OK(FinishBatch());
  return Status::OK();
}

} // namespace cfile
} // namespace kudu
