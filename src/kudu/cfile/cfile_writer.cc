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

#include <sys/types.h>

#include <functional>
#include <iterator>
#include <numeric>
#include <optional>
#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cfile/block_compression.h"
#include "kudu/cfile/block_encodings.h"
#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/array_cell_view.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/util/array_view.h" // IWYU pragma: keep
#include "kudu/util/bitmap.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/rle-encoding.h"

DEFINE_int32(cfile_default_block_size, 256*1024, "The default block size to use in cfiles");
TAG_FLAG(cfile_default_block_size, advanced);

DEFINE_string(cfile_default_compression_codec, "no_compression",
              "Default cfile block compression codec.");
TAG_FLAG(cfile_default_compression_codec, advanced);

DEFINE_bool(cfile_write_checksums, true,
            "Write CRC32 checksums for each block");
TAG_FLAG(cfile_write_checksums, evolving);

DEFINE_bool(cfile_support_arrays, true,
            "Support encoding/decoding of arrays in CFile data blocks");
TAG_FLAG(cfile_support_arrays, experimental);

using google::protobuf::RepeatedPtrField;
using kudu::fs::BlockCreationTransaction;
using kudu::fs::BlockManager;
using kudu::fs::WritableBlock;
using std::accumulate;
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

class NonNullBitmapBuilder {
 public:
  explicit NonNullBitmapBuilder(size_t initial_row_capacity)
      : nitems_(0),
        bitmap_(BitmapSize(initial_row_capacity)),
        rle_encoder_(&bitmap_) {
  }

  size_t nitems() const {
    return nitems_;
  }

  // If value parameter is true, it means that all values in this run are null
  void AddRun(bool value, size_t run_length = 1) {
    nitems_ += run_length;
    rle_encoder_.Put(value, run_length);
  }

  // the returned Slice is only valid until this Builder is destroyed or Reset
  Slice Finish() {
    int len = rle_encoder_.Flush();
    return Slice(bitmap_.data(), len);
  }

  void Reset() {
    nitems_ = 0;
    rle_encoder_.Clear();
  }

 private:
  size_t nitems_;
  faststring bitmap_;
  RleEncoder<bool, 1> rle_encoder_;
};

class ArrayElemNumBuilder {
 public:
  explicit ArrayElemNumBuilder(size_t initial_row_capacity)
      : nitems_(0),
        // TODO(aserbin): improve or remove the estimate for the buffer size
        buffer_(initial_row_capacity * sizeof(uint32_t)),
        rle_encoder_(&buffer_) {
  }

  size_t nitems() const {
    return nitems_;
  }

  // If value parameter is true, it means that all values in this run are null
  void Add(uint32_t elem_num) {
    ++nitems_;
    rle_encoder_.Put(elem_num, 1);
  }

  // NOTE: the returned Slice is only valid until this Builder is destroyed or Reset
  Slice Finish() {
    int len = rle_encoder_.Flush();
    return Slice(buffer_.data(), len);
  }

  void Reset() {
    nitems_ = 0;
    rle_encoder_.Clear();
  }

 private:
  size_t nitems_;
  faststring buffer_;
  RleEncoder<uint16_t, 16> rle_encoder_;
};

////////////////////////////////////////////////////////////
// CFileWriter
////////////////////////////////////////////////////////////


CFileWriter::CFileWriter(WriterOptions options,
                         const TypeInfo* typeinfo,
                         bool is_nullable,
                         unique_ptr<WritableBlock> block)
    : options_(std::move(options)),
      block_(std::move(block)),
      off_(0),
      value_count_(0),
      is_nullable_(is_nullable),
      typeinfo_(typeinfo),
      is_array_(typeinfo->is_array()),
      state_(kWriterInitialized) {
  const EncodingType encoding = options_.storage_attributes.encoding;
  if (auto s = TypeEncodingInfo::Get(typeinfo_, encoding, &type_encoding_info_);
      PREDICT_FALSE(!s.ok())) {
    // TODO(af): we should somehow pass some contextual info about the
    // tablet here.
    WARN_NOT_OK(s, "Falling back to default encoding");
    s = TypeEncodingInfo::Get(typeinfo_,
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
  if (PREDICT_FALSE(options_.storage_attributes.cfile_block_size < kMinBlockSize)) {
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
  DCHECK(state_ == kWriterInitialized) << "bad state for Start(): " << state_;

  if (PREDICT_FALSE(is_array_ && !FLAGS_cfile_support_arrays)) {
    static const auto kErrStatus = Status::ConfigurationError(
        "support for array data blocks is disabled");
    LOG(DFATAL) << kErrStatus.ToString();
    return kErrStatus;
  }

  if (compression_ != NO_COMPRESSION) {
    const CompressionCodec* codec;
    RETURN_NOT_OK(GetCompressionCodec(compression_, &codec));
    block_compressor_.reset(new CompressedBlockBuilder(codec));
  }

  CFileHeaderPB header;
  FlushMetadataToPB(header.mutable_metadata());

  const uint32_t pb_size = header.ByteSizeLong();

  faststring header_str;
  // First the magic.
  header_str.append(kMagicStringV2);
  // Then Length-prefixed header.
  PutFixed32(&header_str, pb_size);
  pb_util::AppendToString(header, &header_str);

  vector<Slice> header_slices { header_str };

  // Append header checksum.
  uint8_t checksum_buf[kChecksumSize];
  if (FLAGS_cfile_write_checksums) {
    uint32_t header_checksum = crc::Crc32c(header_str.data(), header_str.size());
    InlineEncodeFixed32(checksum_buf, header_checksum);
    header_slices.emplace_back(checksum_buf, kChecksumSize);
  }

  RETURN_NOT_OK_PREPEND(WriteRawData(header_slices), "Couldn't write header");
  data_block_ = type_encoding_info_->CreateBlockBuilder(&options_);

  if (is_array_) {
    // Array data blocks allows for nullable elements in array cells and
    // the cells themselves, so both non_null_bitmap_builder_ and
    // array_non_null_bitmap_builder_ are needed to maintain non-nullness
    // (a.k.a. validity) metadata.
    // The initial estimate for the encoders' memory buffers assumes the worst
    // case of all cells being single element arrays, and also takes into
    // account the possibility of going over the configured size for the
    // CFile block (that's why factor 2 appeared below).
    const size_t elem_size = GetArrayElementTypeInfo(*typeinfo_)->size();
    const size_t nrows =
        (2 * options_.storage_attributes.cfile_block_size + elem_size - 1) / elem_size;
    array_non_null_bitmap_builder_.reset(new NonNullBitmapBuilder((nrows + 1) / 2));
    non_null_bitmap_builder_.reset(new NonNullBitmapBuilder(nrows));
    array_elem_num_builder_.reset(new ArrayElemNumBuilder(nrows));
  } else if (is_nullable_) {
    size_t nrows =
        (options_.storage_attributes.cfile_block_size + typeinfo_->size() - 1) /
        typeinfo_->size();
    non_null_bitmap_builder_.reset(new NonNullBitmapBuilder(nrows * 8));
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
  DCHECK(state_ == kWriterWriting) << "Bad state for Finish(): " << state_;

  uint32_t incompatible_features = IncompatibleFeatures::NONE;
  if (FLAGS_cfile_write_checksums) {
    incompatible_features |= IncompatibleFeatures::CHECKSUM;
  }
  if (is_array_) {
    incompatible_features |= IncompatibleFeatures::ARRAY_DATA_BLOCK;
  }

  // Write out any pending values as the last data block.
  if (is_array_) {
    RETURN_NOT_OK(FinishCurArrayDataBlock());
  } else {
    RETURN_NOT_OK(FinishCurDataBlock());
  }

  state_ = kWriterFinished;

  // Start preparing the footer.
  CFileFooterPB footer;
  if (is_array_) {
    // For 1D arrays, the 'data_type' field is set to reflect the type
    // of the array's elements. Elements of an array can be nullable.
    const auto* desc = typeinfo_->nested_type_info();
    DCHECK(desc);
    DCHECK(desc->is_array());
    DCHECK(desc->array().elem_type_info());
    footer.set_data_type(desc->array().elem_type_info()->type());
    footer.set_is_type_array(true);
  } else {
    footer.set_data_type(typeinfo_->type());
    // NOTE: leaving the 'is_type_array' field as unset: semantically it's the
    //       same as if it were set to 'false', but the result CFile footer
    //       would be a few bytes larger if explicitly setting the field
  }
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

void CFileWriter::AddMetadataPair(const Slice& key, const Slice& value) {
  DCHECK_NE(state_, kWriterFinished);

  unflushed_metadata_.emplace_back(key.ToString(), value.ToString());
}

string CFileWriter::GetMetaValueOrDie(Slice key) const {
  for (const auto& [m_key, m_val] : unflushed_metadata_) {
    if (Slice(m_key) == key) {
      return m_val;
    }
  }
  LOG(FATAL) << "Missing metadata entry: " << KUDU_REDACT(key.ToDebugString());
}

void CFileWriter::FlushMetadataToPB(RepeatedPtrField<FileMetadataPairPB>* field) {
  for (const auto& [key, val] : unflushed_metadata_) {
    FileMetadataPairPB* pb = field->Add();
    pb->set_key(key);
    pb->set_value(val);
  }
  unflushed_metadata_.clear();
}

Status CFileWriter::AppendEntries(const void* entries, size_t count) {
  DCHECK(!is_nullable_);
  DCHECK(!is_array_);

  int rem = count;

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(entries);

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

Status CFileWriter::AppendNullableEntries(const uint8_t* bitmap,
                                          const void* entries,
                                          size_t count) {
  DCHECK(is_nullable_ && bitmap != nullptr);
  DCHECK(!is_array_);

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(entries);

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

Status CFileWriter::AppendNullableArrayEntries(const uint8_t* bitmap,
                                               const void* entries,
                                               size_t count) {
  DCHECK_EQ(DataType::NESTED, typeinfo_->type());
  DCHECK_EQ(sizeof(Slice), typeinfo_->size());

  // For 1D arrays, get information on the elements.
  const auto* desc = typeinfo_->nested_type_info();
  DCHECK(desc);
  DCHECK(desc->is_array());
  // For 1D arrays, the encoder is chosen based on the type of the array's
  // elements.
  const auto* const elem_type_info = desc->array().elem_type_info();
  DCHECK(elem_type_info);
  const size_t elem_size = elem_type_info->size();

  const Slice* cells_ptr = reinterpret_cast<const Slice*>(entries);
  BitmapIterator cell_bitmap_it(bitmap, count);
  size_t cur_cell_idx = 0;

  size_t cells_num = 0;
  bool cell_is_not_null = false;
  while ((cells_num = cell_bitmap_it.Next(&cell_is_not_null)) > 0) {
    if (!cell_is_not_null) {
      // This is a run of null array-type cells.

      value_count_ += cells_num;
      array_non_null_bitmap_builder_->AddRun(false, cells_num);
      for (size_t i = 0; i < cells_num; ++i) {
        array_elem_num_builder_->Add(0);
      }

      cells_ptr += cells_num;
      cur_cell_idx += cells_num;
    } else {
      // This is a run of non-null array-type cells.
      for (size_t i = 0; i < cells_num; ++i, ++cur_cell_idx, ++cells_ptr) {
        ++value_count_;
        array_non_null_bitmap_builder_->AddRun(true, 1);

        // Information on validity of the elements in the in-cell array.
        const Slice* cell = cells_ptr;
        DCHECK(cell);
        ArrayCellMetadataView view(cell->data(), cell->size());
        RETURN_NOT_OK(view.Init());

        // Add information on the array's elements boundary
        // in the flattened sequence.
        const size_t cell_elem_num = view.elem_num();
        array_elem_num_builder_->Add(cell_elem_num);
        if (cell_elem_num == 0) {
          // Current cell contains an empty array.
          //DCHECK(!cell->data_);
          //DCHECK(!cell->non_null_bitmap_);
          continue;
        }

        const uint8_t* cell_non_null_bitmap = view.not_null_bitmap();
        DCHECK(cell_non_null_bitmap || !view.has_nulls());
        BitmapIterator elem_bitmap_iter(cell_non_null_bitmap,
                                        cell_elem_num);
        const uint8_t* data = view.data_as(elem_type_info->type());
        DCHECK(data);

        // Mask the 'block is full' while writing a single array.
        data_block_->SetBlockFullMasked(true);
        size_t elem_num = 0;
        bool elem_is_non_null = false;
        while ((elem_num = elem_bitmap_iter.Next(&elem_is_non_null)) > 0) {
          if (!elem_is_non_null) {
            // Add info on the run of 'elem_num' null elements in the array.
            non_null_bitmap_builder_->AddRun(false, elem_num);
            // Skip over the null elements in the input data.
            data += elem_num * elem_size;
            continue;
          }

          // A run of 'elem_num' non-null elements in the array.
          ssize_t elem_rem = elem_num;
          do {
            int n = data_block_->Add(data, elem_rem);
            DCHECK_GT(n, 0);

            non_null_bitmap_builder_->AddRun(true, n);
            data += n * elem_size;
            elem_rem -= n;
          } while (elem_rem > 0);
        }
        // Unmask the 'block is full' logic, so the logic below works
        // as necessary.
        data_block_->SetBlockFullMasked(false);

        // If the current block is full, switch to a new one.
        if (data_block_->IsBlockFull()) {
          // NOTE: with long arrays the block may get quite beyond the size
          // threshold before switching to the next one.
          RETURN_NOT_OK(FinishCurArrayDataBlock());
        }
      }
    }
  }

  DCHECK_EQ(count, cur_cell_idx);

  return Status::OK();
}

Status CFileWriter::FinishCurDataBlock() {
  DCHECK(!is_array_);
  const uint32_t num_elems_in_block =
      is_nullable_ ? non_null_bitmap_builder_->nitems() : data_block_->Count();
  if (PREDICT_FALSE(num_elems_in_block == 0)) {
    return Status::OK();
  }

  DCHECK_GE(value_count_, num_elems_in_block);
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

  Status s;
  {
    vector<Slice> v;
    v.reserve(data_slices.size() + (is_nullable_ ? 2 : 0));
    faststring null_headers;
    if (is_nullable_) {
      const Slice non_null_bitmap = non_null_bitmap_builder_->Finish();
      PutVarint32(&null_headers, num_elems_in_block);
      PutVarint32(&null_headers, non_null_bitmap.size());
      v.emplace_back(null_headers.data(), null_headers.size());
      v.emplace_back(non_null_bitmap);
    }
    std::move(data_slices.begin(), data_slices.end(), std::back_inserter(v));
    s = AppendRawBlock(std::move(v), first_elem_ord,
                       reinterpret_cast<const void*>(key_tmp_space),
                       Slice(last_key_),
                       "data block");
  }

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

Status CFileWriter::FinishCurArrayDataBlock() {
  DCHECK(!validx_builder_); // array-type column cannot be a part of primary key

  // Number of array cells in the block.
  const uint32_t num_arrays_in_block = array_non_null_bitmap_builder_->nitems();
  const uint32_t num_elems_in_block = non_null_bitmap_builder_->nitems();
  if (PREDICT_FALSE(num_arrays_in_block == 0)) {
    DCHECK_EQ(0, num_elems_in_block);
    return Status::OK();
  }

  DCHECK_GE(value_count_, num_arrays_in_block);
  rowid_t first_array_ord = value_count_ - num_arrays_in_block;
  VLOG(1) << "Appending nullable array data block for values "
          << first_array_ord << "-" << value_count_;

  // The current data block is full, need to push it into the file.
  Status s;
  {
    vector<Slice> data_slices;
    data_block_->Finish(first_array_ord, &data_slices);

    // A nullable array data block has the following layout
    // (see docs/design-docs/cfile.md for more details):
    //
    // flattened value count          : unsigned [LEB128] encoded count of values
    //
    // array count                    : unsigned [LEB128] encoded count of arrays
    //
    // flattened non-null bitmap size : unsigned [LEB128] encoded size
    //                                  of the following non-null bitmap
    //
    // flattened non-null bitmap      : [RLE] encoded bitmap
    //
    // array element numbers size     : unsigned [LEB128] encoded size of the
    //                                  following field
    //
    // array element numbers          : [RLE] encoded sequence of 16-bit
    //                                  unsigned integers
    //
    // array non-null bitmap size     : unsigned [LEB128] encoded size of the
    //                                  following non-null bitmap
    //
    // array non-null bitmap          : [RLE] encoded bitmap on non-nullness
    //                                  of array cells
    //
    // data                           : encoded non-null data values
    //                                  (a.k.a. 'flattened sequence of elements')
    vector<Slice> v;
    v.reserve(data_slices.size() + 6);

    const Slice flattened_non_null_bitmap = non_null_bitmap_builder_->Finish();
    faststring array_headers_0;
    PutVarint32(&array_headers_0, num_elems_in_block);
    PutVarint32(&array_headers_0, array_elem_num_builder_->nitems());
    PutVarint32(&array_headers_0, static_cast<uint32_t>(flattened_non_null_bitmap.size()));
    v.emplace_back(array_headers_0.data(), array_headers_0.size());
    if (!flattened_non_null_bitmap.empty()) {
      v.emplace_back(flattened_non_null_bitmap);
    }

    const Slice array_elem_num_encoded = array_elem_num_builder_->Finish();
    faststring array_headers_1;
    PutVarint32(&array_headers_1, static_cast<uint32_t>(array_elem_num_encoded.size()));
    v.emplace_back(array_headers_1.data(), array_headers_1.size());
    if (!array_elem_num_encoded.empty()) {
      v.emplace_back(array_elem_num_encoded);
    }

    const Slice array_non_null_bitmap = array_non_null_bitmap_builder_->Finish();
    DCHECK(!array_non_null_bitmap.empty());
    faststring array_headers_2;
    PutVarint32(&array_headers_2, static_cast<uint32_t>(array_non_null_bitmap.size()));
    v.emplace_back(array_headers_2.data(), array_headers_2.size());
    v.emplace_back(array_non_null_bitmap);

    std::move(data_slices.begin(), data_slices.end(), std::back_inserter(v));
    s = AppendRawBlock(std::move(v),
                       first_array_ord,
                       nullptr,
                       Slice(last_key_),
                       "array data block");
  }

  // Reset per-block state.
  non_null_bitmap_builder_->Reset();
  array_non_null_bitmap_builder_->Reset();
  array_elem_num_builder_->Reset();

  data_block_->Reset();

  return s;
}

Status CFileWriter::AppendRawBlock(vector<Slice> data_slices,
                                   size_t ordinal_pos,
                                   const void* validx_curr,
                                   const Slice& validx_prev,
                                   const char* name_for_log) {
  DCHECK_EQ(state_, kWriterWriting);

  BlockPointer ptr;
  Status s = AddBlock(std::move(data_slices), &ptr, name_for_log);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(ERROR) << "Unable to append block to file: " << s.ToString();
    return s;
  }

  // Now add to the index blocks
  if (posidx_builder_ != nullptr) {
    tmp_buf_.clear();
    KeyEncoderTraits<UINT32, faststring>::Encode(ordinal_pos, &tmp_buf_);
    RETURN_NOT_OK(posidx_builder_->Append(Slice(tmp_buf_), ptr));
  }

  if (validx_builder_ != nullptr) {
    DCHECK(validx_curr != nullptr) <<
      "must pass a key for raw block if validx is configured";

    (*options_.validx_key_encoder)(validx_curr, &tmp_buf_);
    Slice idx_key = Slice(tmp_buf_);
    if (options_.optimize_index_keys) {
      GetSeparatingKey(validx_prev, &idx_key);
    }
    VLOG(1) << "Appending validx entry\n" << kudu::HexDump(idx_key);
    Status s = validx_builder_->Append(idx_key, ptr);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Unable to append to value index: " << s.ToString();
      return s;
    }
  }

  return Status::OK();
}

Status CFileWriter::AddBlock(vector<Slice> data_slices,
                             BlockPointer* block_ptr,
                             const char* name_for_log) {
  const uint64_t start_offset = off_;
  vector<Slice> out_slices;

  if (block_compressor_ != nullptr) {
    // Write compressed block
    Status s = block_compressor_->Compress(std::move(data_slices), &out_slices);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Unable to compress block at offset " << start_offset
                 << ": " << s.ToString();
      return s;
    }
  } else {
    out_slices = std::move(data_slices);
  }

  // Calculate and append a data checksum.
  uint8_t checksum_buf[kChecksumSize];
  if (FLAGS_cfile_write_checksums) {
    uint32_t checksum = 0;
    for (const Slice& data : out_slices) {
      checksum = crc::Crc32c(data.data(), data.size(), checksum);
    }
    InlineEncodeFixed32(checksum_buf, checksum);
    out_slices.emplace_back(checksum_buf, kChecksumSize);
  }

  RETURN_NOT_OK(WriteRawData(out_slices));

  DCHECK_GT(off_, start_offset);
  const uint64_t total_size = off_ - start_offset;

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
  if (PREDICT_FALSE(!s.ok())) {
    LOG(ERROR) << "Unable to append data of size "
               << data_size << " at offset " << off_
               << ": " << s.ToString();
  }
  off_ += data_size;
  return s;
}

} // namespace cfile
} // namespace kudu
