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

#include <glog/logging.h>
#include <algorithm>
#include <gflags/gflags.h>

#include "kudu/cfile/block_compression.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/coding.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"

DEFINE_int64(max_cfile_block_size, 16 * 1024 * 1024,
             "The maximum size of an uncompressed CFile block when using compression. "
             "Blocks larger than this will prevent flushing.");
// Mark this flag unsafe since we're likely to hit other downstream issues with
// cells that are this large, and we haven't tested these scenarios. The purpose
// of this flag is just to provide an 'escape hatch' if we somehow insert a too-large
// value.
TAG_FLAG(max_cfile_block_size, unsafe);

DEFINE_double(min_compression_ratio, 0.9,
              "If a column compression codec is configured, but the codec is unable "
              "to achieve a compression ratio at least as good as the configured "
              "value, then the data will be written uncompressed. This will reduce "
              "CPU overhead on the read side at the expense of a small amount of "
              "extra space if the codec encounters portions of data that are "
              "not easily compressible.");
TAG_FLAG(min_compression_ratio, experimental);

namespace kudu {
namespace cfile {

using std::vector;
using strings::Substitute;

CompressedBlockBuilder::CompressedBlockBuilder(const CompressionCodec* codec)
  : codec_(DCHECK_NOTNULL(codec)) {
}

Status CompressedBlockBuilder::Compress(const vector<Slice>& data_slices, vector<Slice>* result) {
  size_t data_size = 0;
  for (const Slice& data : data_slices) {
    data_size += data.size();
  }

  // On the read side, we won't read any data which uncompresses larger than the
  // configured maximum. So, we should prevent writing any data which would later
  // be unreadable.
  if (data_size > FLAGS_max_cfile_block_size) {
    return Status::InvalidArgument(Substitute(
        "uncompressed block size $0 is greater than the configured maximum "
        "size $1", data_size, FLAGS_max_cfile_block_size));
  }

  // Ensure that the buffer for header + compressed data is large enough
  // for the upper bound compressed size reported by the codec.
  size_t ub_compressed_size = codec_->MaxCompressedLength(data_size);
  buffer_.resize(kHeaderLength + ub_compressed_size);

  // Compress
  size_t compressed_size;
  RETURN_NOT_OK(codec_->Compress(data_slices,
                                 buffer_.data() + kHeaderLength, &compressed_size));

  // If the compression was not effective, then store the uncompressed data, so
  // that at read time we don't need to waste CPU executing the codec.
  // We use a user-provided threshold, but also guarantee that the compression saves
  // at least one byte using integer math. This way on the read side we can assume
  // that the compressed size can never be >= the uncompressed.
  double ratio = static_cast<double>(compressed_size) / data_size;
  if (compressed_size >= data_size || // use integer comparison to be 100% sure.
      ratio > FLAGS_min_compression_ratio) {
    buffer_.resize(kHeaderLength);
    InlineEncodeFixed32(&buffer_[0], data_size);
    result->clear();
    result->reserve(data_slices.size() + 1);
    result->push_back(Slice(buffer_.data(), kHeaderLength));
    for (const Slice& orig_data : data_slices) {
      result->push_back(orig_data);
    }
    return Status::OK();
  }

  // Set up the header
  InlineEncodeFixed32(&buffer_[0], data_size);
  *result = { Slice(buffer_.data(), compressed_size + kHeaderLength) };

  return Status::OK();
}

CompressedBlockDecoder::CompressedBlockDecoder(const CompressionCodec* codec,
                                               int cfile_version,
                                               const Slice& block_data)
    : codec_(DCHECK_NOTNULL(codec)),
      cfile_version_(cfile_version),
      data_(block_data) {
}

Status CompressedBlockDecoder::Init() {
  // Check that the on-disk size is at least as big as the expected header.
  if (PREDICT_FALSE(data_.size() < header_length())) {
    return Status::Corruption(
        Substitute("data size $0 is not enough to contains the header. "
                   "required $1, buffer: $2",
                   data_.size(), header_length(),
                   KUDU_REDACT(data_.ToDebugString(50))));
  }

  const uint8_t* p = data_.data();
  // Decode the header
  uint32_t compressed_size;
  if (cfile_version_ == 1) {
    // CFile v1 stores the compressed size in the compressed block header.
    // This is redundant, since we already know the block length, but it's
    // an opportunity for extra verification.
    compressed_size = DecodeFixed32(p);
    p += 4;

    // Check that the on-disk data size matches with the buffer.
    if (data_.size() != header_length() + compressed_size) {
      return Status::Corruption(
          Substitute("compressed size $0 does not match remaining length in buffer $1, buffer: $2",
                     compressed_size, data_.size() - header_length(),
                     KUDU_REDACT(data_.ToDebugString(50))));
    }
  } else {
    // CFile v2 doesn't store the compressed size. Just use the remaining length.
    compressed_size = data_.size() - header_length();
  }

  uncompressed_size_ = DecodeFixed32(p);
  p += 4;

  // In CFile v2, we ensure that compressed_size <= uncompressed_size,
  // though, as per the file format, if compressed_size == uncompressed_size,
  // this indicates that the data was not compressed.
  if (PREDICT_FALSE(compressed_size > uncompressed_size_ &&
                    cfile_version_ > 1)) {
    return Status::Corruption(
        Substitute("compressed size $0 must be <= uncompressed size $1, buffer",
                   compressed_size, uncompressed_size_),
        KUDU_REDACT(data_.ToDebugString(50)));
  }

  // Check if uncompressed size seems to be reasonable.
  if (uncompressed_size_ > FLAGS_max_cfile_block_size) {
    return Status::Corruption(
      Substitute("uncompressed size $0 overflows the maximum length $1, buffer",
                 compressed_size, FLAGS_max_cfile_block_size),
      KUDU_REDACT(data_.ToDebugString(50)));
  }

  return Status::OK();
}

Status CompressedBlockDecoder::UncompressIntoBuffer(uint8_t* dst) {
  DCHECK_GE(uncompressed_size_, 0);

  Slice compressed = data_;
  compressed.remove_prefix(header_length());
  if (uncompressed_size_ == compressed.size() && cfile_version_ > 1) {
    // TODO(perf): we could potentially avoid this memcpy and instead
    // just use the data in place. However, it's a bit tricky, since the
    // block cache expects that the stored pointer for the block is at
    // the beginning of block data, not the compression header. Copying
    // is simple to implement and at least several times faster than
    // executing a codec, so this optimization is still worthwhile.
    memcpy(dst, compressed.data(), uncompressed_size_);
  } else {
    RETURN_NOT_OK(codec_->Uncompress(compressed, dst, uncompressed_size_));
  }

  return Status::OK();
}

} // namespace cfile
} // namespace kudu
