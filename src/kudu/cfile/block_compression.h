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
#ifndef KUDU_CFILE_BLOCK_COMPRESSION_H
#define KUDU_CFILE_BLOCK_COMPRESSION_H

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class CompressionCodec;

namespace cfile {

// A compressed block has the following format:
//
// CFile version 1
// -----------------
// 4-byte little-endian: compressed_size
//    the size of the compressed data, not including the 8-byte header.
// 4-byte little-endian: uncompressed_size
//    the expected size of the data after decompression
// <compressed data>
//
// CFile version 2
// -----------------
// 4-byte little-endian: uncompressed_size
//    The size of the data after decompression.
//
//    NOTE: if uncompressed_size is equal to the remaining size of the
//    block (i.e. the uncompressed and compressed sizes are equal)
//    then the block is assumed to be uncompressed, and the codec should
//    not be executed.
// <compressed data>

// Builder for writing compressed blocks.
// Always writes v2 format.
class CompressedBlockBuilder {
 public:
  // 'codec' is expected to remain alive for the lifetime of this object.
  explicit CompressedBlockBuilder(const CompressionCodec* codec);

  // Sets "*data_slices" to the compressed version of the given input data.
  // The input data is formed by concatenating the elements of 'data_slices'.
  //
  // The slices inside the result may either refer to data owned by this instance,
  // or to slices of the input data. In the former case, the slices remain valid
  // until the class is destructed or until Compress() is called again. In the latter
  // case, it's up to the user to ensure that the original input data is not
  // modified while the elements of 'result' are still being used.
  //
  // If an error was encountered, returns a non-OK status.
  Status Compress(const std::vector<Slice>& data_slices,
                  std::vector<Slice>* result);

  // See format information above.
  static const size_t kHeaderLength = 4;

 private:
  DISALLOW_COPY_AND_ASSIGN(CompressedBlockBuilder);
  const CompressionCodec* codec_;
  faststring buffer_;
};

// Builder for reading compressed blocks.
// Can read v1 or v2 format based on 'cfile_version' constructor parameter.
class CompressedBlockDecoder {
 public:
  // 'codec' is expected to remain alive for the lifetime of this object.
  CompressedBlockDecoder(const CompressionCodec* codec,
                         int cfile_version,
                         const Slice& block_data);

  // Parses and validates the header in the data block.
  // After calling this, the accessors below as well as UncompressIntoBuffer()
  // may be safely called.
  //
  // Returns Corruption if the data block header indicates a compressed size
  // that is different than the amount of remaining data in the block, or if the
  // uncompressed size is greater than the configured maximum uncompressed block size.
  Status Init();

  int uncompressed_size() const {
    DCHECK_GE(uncompressed_size_, 0) << "must Init()";
    return uncompressed_size_;
  }

  // Uncompress into the provided 'dst' buffer, which must be at least as
  // large as the 'uncompressed_size()'.
  //
  // REQUIRES: Init() has been called and returned successfully.
  // REQUIRES: !block_skipped()
  Status UncompressIntoBuffer(uint8_t* dst);
 private:
  DISALLOW_COPY_AND_ASSIGN(CompressedBlockDecoder);

  static const size_t kHeaderLengthV1 = 8;
  static const size_t kHeaderLengthV2 = 4;

  size_t header_length() const {
    return cfile_version_ == 1 ? kHeaderLengthV1 : kHeaderLengthV2;
  }

  const CompressionCodec* const codec_;
  const int cfile_version_;
  const Slice data_;

  int uncompressed_size_ = -1;
};

} // namespace cfile
} // namespace kudu
#endif
