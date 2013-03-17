// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <glog/logging.h>

#include "cfile/block_compression.h"
#include "util/coding.h"
#include "util/coding-inl.h"
#include "gutil/gscoped_ptr.h"

namespace kudu {
namespace cfile {

CompressedBlockBuilder::CompressedBlockBuilder(const shared_ptr<CompressionCodec> &codec,
                                               size_t size_limit) :
  codec_(codec),
  compressed_size_limit_(size_limit)
{
  CHECK_NOTNULL(codec_.get());
}

Status CompressedBlockBuilder::Compress(const Slice& data, Slice *result) {
  // Ensure that the buffer for header + compressed data is large enough
  size_t max_compressed_size = codec_->MaxCompressedLength(data.size());
  if (max_compressed_size > compressed_size_limit_) {
    return Status::InvalidArgument(
      StringPrintf("estimated max size %lu is greater than the expected %lu",
        max_compressed_size, compressed_size_limit_));
  }

  buffer_.reserve(kHeaderReservedLength + max_compressed_size);

  // Compress
  size_t compressed_size;
  RETURN_NOT_OK(codec_->Compress(data, buffer_.data() + kHeaderReservedLength, &compressed_size));

  // Set up the header
  InlineEncodeFixed32(&buffer_[0], compressed_size);
  InlineEncodeFixed32(&buffer_[4], data.size());
  *result = Slice(buffer_.data(), compressed_size + kHeaderReservedLength);

  return Status::OK();
}

CompressedBlockDecoder::CompressedBlockDecoder(const shared_ptr<CompressionCodec> &codec,
                                               size_t size_limit) :
  codec_(codec),
  uncompressed_size_limit_(size_limit)
{
  CHECK_NOTNULL(codec_.get());
}

Status CompressedBlockDecoder::Uncompress(const Slice& data, Slice *result) {
  // Check if the on-dosk
  if (data.size() < CompressedBlockBuilder::kHeaderReservedLength) {
    return Status::Corruption(
      StringPrintf("data size %lu is not enough to contains the header. required %lu, buffer",
        data.size(), CompressedBlockBuilder::kHeaderReservedLength),
        data.ToDebugString(50));
  }

  // Decode the header
  uint32_t compressed_size = DecodeFixed32(data.data());
  uint32_t uncompressed_size = DecodeFixed32(data.data() + 4);

  // Check if the on-disk data size matches with the buffer
  if (data.size() != (CompressedBlockBuilder::kHeaderReservedLength + compressed_size)) {
    return Status::Corruption(
      StringPrintf("compressed size %u does not match remaining length in buffer %lu, buffer",
        compressed_size, data.size() - CompressedBlockBuilder::kHeaderReservedLength),
        data.ToDebugString(50));
  }

  // Check if uncompressed size seems to be reasonable
  if (uncompressed_size > uncompressed_size_limit_) {
    return Status::Corruption(
      StringPrintf("uncompressed size %u overflows the maximum length %lu, buffer",
        compressed_size, uncompressed_size_limit_), data.ToDebugString(50));
  }

  Slice compressed(data.data() + CompressedBlockBuilder::kHeaderReservedLength, compressed_size);

  // Allocate the buffer for the uncompressed data and uncompress
  ::gscoped_array<uint8_t> buffer(new uint8_t[uncompressed_size]);
  RETURN_NOT_OK(codec_->Uncompress(compressed, buffer.get(), uncompressed_size));
  *result = Slice(buffer.release(), uncompressed_size);

  return Status::OK();
}

} // namespace cfile
} // namespace kudu
