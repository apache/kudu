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

#include "kudu/cfile/cfile_writer.h"

#include <functional>
#include <iterator>
#include <numeric>
#include <ostream>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cfile/block_compression.h"
#include "kudu/cfile/block_encodings.h"
#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/util/array_view.h" // IWYU pragma: keep
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"

DEFINE_int32(cfile_default_block_size, 256*1024, "The default block size to use in cfiles");
TAG_FLAG(cfile_default_block_size, advanced);

DEFINE_string(cfile_default_compression_codec, "no_compression",
              "Default cfile block compression codec.");
TAG_FLAG(cfile_default_compression_codec, advanced);

DEFINE_bool(cfile_write_checksums, true,
            "Write CRC32 checksums for each block");
TAG_FLAG(cfile_write_checksums, evolving);

using google::protobuf::RepeatedPtrField;
using kudu::fs::BlockCreationTransaction;
using kudu::fs::BlockManager;
using kudu::fs::WritableBlock;
using std::accumulate;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace cfile {

const char kMagicStringV1[] = "kuducfil";
const char kMagicStringV2[] = "kuducfl2";
const int kMagicLength = 8;
const size_t kChecksumSize = sizeof(uint32_t);

static const size_t kMinBlockSize = 512;

////////////////////////////////////////////////////////////
// CFileWriter
////////////////////////////////////////////////////////////


CFileWriter::CFileWriter(WriterOptions options,
                         const TypeInfo* typeinfo,
                         bool is_nullable,
                         unique_ptr<WritableBlock> block)
  : block_(std::move(block)),
    off_(0),
    value_count_(0),
    options_(std::move(options)),
    is_nullable_(is_nullable),
    typeinfo_(typeinfo),
    state_(kWriterInitialized) {
  EncodingType encoding = options_.storage_attributes.encoding;
  Status s = TypeEncodingInfo::Get(typeinfo_, encoding, &type_encoding_info_);
  if (!s.ok()) {
    // TODO: we should somehow pass some contextual info about the
    // tablet here.
    WARN_NOT_OK(s, "Falling back to default encoding");
    s = TypeEncodingInfo::Get(typeinfo,
                              TypeEncodingInfo::GetDefaultEncoding(typeinfo_),
                              &type_encoding_info_);
    CHECK_OK(s);
  }

  compression_ = options_.storage_attributes.compression;
  if (compression_ == DEFAULT_COMPRESSION) {
    compression_ = GetCompressionCodecType(FLAGS_cfile_default_compression_codec);
  }

  if (options_.storage_attributes.cfile_block_size <= 0) {
    options_.storage_attributes.cfile_block_size = FLAGS_cfile_default_block_size;
  }
  if (options_.storage_attributes.cfile_block_size < kMinBlockSize) {
    LOG(WARNING) << "Configured block size " << options_.storage_attributes.cfile_block_size
                 << " smaller than minimum allowed value " << kMinBlockSize
                 << ": using minimum.";
    options_.storage_attributes.cfile_block_size = kMinBlockSize;
  }

  if (options_.write_posidx) {
    posidx_builder_.reset(new IndexTreeBuilder(&options_, this));
  }

  if (options_.write_validx) {
    if (!options_.validx_key_encoder) {
      auto key_encoder = &GetKeyEncoder<faststring>(typeinfo_);
      options_.validx_key_encoder = [key_encoder] (const void* value, faststring* buffer) {
        key_encoder->ResetAndEncode(value, buffer);
      };
    }

    validx_builder_.reset(new IndexTreeBuilder(&options_, this));
  }
}

CFileWriter::~CFileWriter() {
}

Status CFileWriter::Start() {
  TRACE_EVENT0("cfile", "CFileWriter::Start");
  CHECK(state_ == kWriterInitialized) <<
    "bad state for Start(): " << state_;

  if (compression_ != NO_COMPRESSION) {
    const CompressionCodec* codec;
    RETURN_NOT_OK(GetCompressionCodec(compression_, &codec));
    block_compressor_.reset(new CompressedBlockBuilder(codec));
  }

  CFileHeaderPB header;
  FlushMetadataToPB(header.mutable_metadata());

  uint32_t pb_size = header.ByteSize();

  faststring header_str;
  // First the magic.
  header_str.append(kMagicStringV2);
  // Then Length-prefixed header.
  PutFixed32(&header_str, pb_size);
  pb_util::AppendToString(header, &header_str);

  vector<Slice> header_slices;
  header_slices.emplace_back(header_str);

  // Append header checksum.
  uint8_t checksum_buf[kChecksumSize];
  if (FLAGS_cfile_write_checksums) {
    uint32_t header_checksum = crc::Crc32c(header_str.data(), header_str.size());
    InlineEncodeFixed32(checksum_buf, header_checksum);
    header_slices.emplace_back(checksum_buf, kChecksumSize);
  }

  RETURN_NOT_OK_PREPEND(WriteRawData(header_slices), "Couldn't write header");

  BlockBuilder *bb;
  RETURN_NOT_OK(type_encoding_info_->CreateBlockBuilder(&bb, &options_));
  data_block_.reset(bb);

  if (is_nullable_) {
    size_t nrows = ((options_.storage_attributes.cfile_block_size + typeinfo_->size() - 1) /
                    typeinfo_->size());
    non_null_bitmap_builder_.reset(new NullBitmapBuilder(nrows * 8));
  }

  state_ = kWriterWriting;

  return Status::OK();
}

Status CFileWriter::Finish() {
  TRACE_EVENT0("cfile", "CFileWriter::Finish");
  BlockManager* bm = block_->block_manager();
  unique_ptr<BlockCreationTransaction> transaction = bm->NewCreationTransaction();
  RETURN_NOT_OK(FinishAndReleaseBlock(transaction.get()));
  return transaction->CommitCreatedBlocks();
}

Status CFileWriter::FinishAndReleaseBlock(BlockCreationTransaction* transaction) {
  TRACE_EVENT0("cfile", "CFileWriter::FinishAndReleaseBlock");
  CHECK(state_ == kWriterWriting) <<
    "Bad state for Finish(): " << state_;

  // Write out any pending values as the last data block.
  RETURN_NOT_OK(FinishCurDataBlock());

  state_ = kWriterFinished;

  uint32_t incompatible_features = 0;
  if (FLAGS_cfile_write_checksums) {
    incompatible_features |= IncompatibleFeatures::CHECKSUM;
  }

  // Start preparing the footer.
  CFileFooterPB footer;
  footer.set_data_type(typeinfo_->type());
  footer.set_is_type_nullable(is_nullable_);
  footer.set_encoding(type_encoding_info_->encoding_type());
  footer.set_num_values(value_count_);
  footer.set_compression(compression_);
  footer.set_incompatible_features(incompatible_features);

  // Write out any pending positional index blocks.
  if (options_.write_posidx) {
    BTreeInfoPB posidx_info;
    RETURN_NOT_OK_PREPEND(posidx_builder_->Finish(&posidx_info),
                          "Couldn't write positional index");
    footer.mutable_posidx_info()->CopyFrom(posidx_info);
  }

  if (options_.write_validx) {
    BTreeInfoPB validx_info;
    RETURN_NOT_OK_PREPEND(validx_builder_->Finish(&validx_info), "Couldn't write value index");
    footer.mutable_validx_info()->CopyFrom(validx_info);
  }

  // Optionally append extra information to the end of cfile.
  // Example: dictionary block for dictionary encoding
  RETURN_NOT_OK(data_block_->AppendExtraInfo(this, &footer));

  // Flush metadata.
  FlushMetadataToPB(footer.mutable_metadata());

  faststring footer_str;
  pb_util::SerializeToString(footer, &footer_str);

  footer_str.append(kMagicStringV2);
  PutFixed32(&footer_str, footer.GetCachedSize());

  // Prepend the footer checksum.
  vector<Slice> footer_slices;
  uint8_t checksum_buf[kChecksumSize];
  if (FLAGS_cfile_write_checksums) {
    uint32_t footer_checksum = crc::Crc32c(footer_str.data(), footer_str.size());
    InlineEncodeFixed32(checksum_buf, footer_checksum);
    footer_slices.emplace_back(checksum_buf, kChecksumSize);
  }

  footer_slices.emplace_back(footer_str);
  RETURN_NOT_OK_PREPEND(WriteRawData(footer_slices), "Couldn't write footer");

  // Done with this block.
  RETURN_NOT_OK(block_->Finalize());
  transaction->AddCreatedBlock(std::move(block_));
  return Status::OK();
}

void CFileWriter::AddMetadataPair(const Slice &key, const Slice &value) {
  CHECK_NE(state_, kWriterFinished);

  unflushed_metadata_.emplace_back(key.ToString(), value.ToString());
}

string CFileWriter::GetMetaValueOrDie(Slice key) const {
  typedef pair<string, string> ss_pair;
  for (const ss_pair& entry : unflushed_metadata_) {
    if (Slice(entry.first) == key) {
      return entry.second;
    }
  }
  LOG(FATAL) << "Missing metadata entry: " << KUDU_REDACT(key.ToDebugString());
}

void CFileWriter::FlushMetadataToPB(RepeatedPtrField<FileMetadataPairPB> *field) {
  typedef pair<string, string> ss_pair;
  for (const ss_pair &entry : unflushed_metadata_) {
    FileMetadataPairPB *pb = field->Add();
    pb->set_key(entry.first);
    pb->set_value(entry.second);
  }
  unflushed_metadata_.clear();
}

Status CFileWriter::AppendEntries(const void *entries, size_t count) {
  DCHECK(!is_nullable_);

  int rem = count;

  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(entries);

  while (rem > 0) {
    int n = data_block_->Add(ptr, rem);
    DCHECK_GE(n, 0);

    ptr += typeinfo_->size() * n;
    rem -= n;
    value_count_ += n;

    if (data_block_->IsBlockFull()) {
      RETURN_NOT_OK(FinishCurDataBlock());
    }
  }

  DCHECK_EQ(rem, 0);
  return Status::OK();
}

Status CFileWriter::AppendNullableEntries(const uint8_t *bitmap,
                                          const void *entries,
                                          size_t count) {
  DCHECK(is_nullable_ && bitmap != nullptr);

  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(entries);

  size_t nitems;
  bool is_non_null = false;
  BitmapIterator bmap_iter(bitmap, count);
  while ((nitems = bmap_iter.Next(&is_non_null)) > 0) {
    if (is_non_null) {
      size_t rem = nitems;
      do {
        int n = data_block_->Add(ptr, rem);
        DCHECK_GE(n, 0);

        non_null_bitmap_builder_->AddRun(true, n);
        ptr += n * typeinfo_->size();
        value_count_ += n;
        rem -= n;

        if (data_block_->IsBlockFull()) {
          RETURN_NOT_OK(FinishCurDataBlock());
        }

      } while (rem > 0);
    } else {
      non_null_bitmap_builder_->AddRun(false, nitems);
      ptr += nitems * typeinfo_->size();
      value_count_ += nitems;
    }
  }

  return Status::OK();
}

Status CFileWriter::FinishCurDataBlock() {
  uint32_t num_elems_in_block = data_block_->Count();
  if (is_nullable_) {
    num_elems_in_block = non_null_bitmap_builder_->nitems();
  }

  if (PREDICT_FALSE(num_elems_in_block == 0)) {
    return Status::OK();
  }

  rowid_t first_elem_ord = value_count_ - num_elems_in_block;
  VLOG(1) << "Appending data block for values " <<
    first_elem_ord << "-" << value_count_;

  // The current data block is full, need to push it
  // into the file, and add to index
  vector<Slice> data_slices;
  data_block_->Finish(first_elem_ord, &data_slices);

  uint8_t key_tmp_space[typeinfo_->size()];
  if (validx_builder_ != nullptr) {
    // If we're building an index, we need to copy the first
    // key from the block locally, so we can write it into that index.
    RETURN_NOT_OK(data_block_->GetFirstKey(key_tmp_space));
    VLOG(1) << "Appending validx entry\n" <<
      kudu::HexDump(Slice(key_tmp_space, typeinfo_->size()));
  }

  vector<Slice> v;
  faststring null_headers;
  if (is_nullable_) {
    Slice non_null_bitmap = non_null_bitmap_builder_->Finish();
    PutVarint32(&null_headers, num_elems_in_block);
    PutVarint32(&null_headers, non_null_bitmap.size());
    v.emplace_back(null_headers.data(), null_headers.size());
    v.push_back(non_null_bitmap);
  }
  std::move(data_slices.begin(), data_slices.end(), std::back_inserter(v));
  Status s = AppendRawBlock(v, first_elem_ord,
                            reinterpret_cast<const void *>(key_tmp_space),
                            Slice(last_key_),
                            "data block");

  if (is_nullable_) {
    non_null_bitmap_builder_->Reset();
  }

  if (validx_builder_ != nullptr) {
    RETURN_NOT_OK(data_block_->GetLastKey(key_tmp_space));
    (*options_.validx_key_encoder)(key_tmp_space, &last_key_);
  }
  data_block_->Reset();

  return s;
}

Status CFileWriter::AppendRawBlock(const vector<Slice>& data_slices,
                                   size_t ordinal_pos,
                                   const void *validx_curr,
                                   const Slice& validx_prev,
                                   const char *name_for_log) {
  CHECK_EQ(state_, kWriterWriting);

  BlockPointer ptr;
  Status s = AddBlock(data_slices, &ptr, name_for_log);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to append block to file: " << s.ToString();
    return s;
  }

  // Now add to the index blocks
  if (posidx_builder_ != nullptr) {
    tmp_buf_.clear();
    KeyEncoderTraits<UINT32, faststring>::Encode(ordinal_pos, &tmp_buf_);
    RETURN_NOT_OK(posidx_builder_->Append(Slice(tmp_buf_), ptr));
  }

  if (validx_builder_ != nullptr) {
    CHECK(validx_curr != nullptr) <<
      "must pass a key for raw block if validx is configured";

    (*options_.validx_key_encoder)(validx_curr, &tmp_buf_);
    Slice idx_key = Slice(tmp_buf_);
    if (options_.optimize_index_keys) {
      GetSeparatingKey(validx_prev, &idx_key);
    }
    VLOG(1) << "Appending validx entry\n" <<
            kudu::HexDump(idx_key);
    s = validx_builder_->Append(idx_key, ptr);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to append to value index: " << s.ToString();
      return s;
    }
  }

  return s;
}

Status CFileWriter::AddBlock(const vector<Slice> &data_slices,
                             BlockPointer *block_ptr,
                             const char *name_for_log) {
  uint64_t start_offset = off_;
  vector<Slice> out_slices;

  if (block_compressor_ != nullptr) {
    // Write compressed block
    Status s = block_compressor_->Compress(data_slices, &out_slices);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to compress block at offset " << off_
                   << ": " << s.ToString();
      return s;
    }
  } else {
    out_slices = data_slices;
  }

  // Calculate and append a data checksum.
  uint8_t checksum_buf[kChecksumSize];
  if (FLAGS_cfile_write_checksums) {
    uint32_t checksum = 0;
    for (const Slice &data : out_slices) {
      checksum = crc::Crc32c(data.data(), data.size(), checksum);
    }
    InlineEncodeFixed32(checksum_buf, checksum);
    out_slices.emplace_back(checksum_buf, kChecksumSize);
  }

  RETURN_NOT_OK(WriteRawData(out_slices));

  uint64_t total_size = off_ - start_offset;

  *block_ptr = BlockPointer(start_offset, total_size);
  VLOG(1) << "Appended " << name_for_log
          << " with " << total_size << " bytes at " << start_offset;
  return Status::OK();
}

Status CFileWriter::WriteRawData(const vector<Slice>& data) {
  size_t data_size = accumulate(data.begin(), data.end(), static_cast<size_t>(0),
                                [&](int sum, const Slice& curr) {
                                  return sum + curr.size();
                                });
  Status s = block_->AppendV(data);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to append data of size "
                 << data_size << " at offset " << off_
                 << ": " << s.ToString();
  }
  off_ += data_size;
  return s;
}

} // namespace cfile
} // namespace kudu
