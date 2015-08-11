// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CFILE_BLOCK_COMPRESSION_H
#define KUDU_CFILE_BLOCK_COMPRESSION_H

#include <tr1/memory>
#include <vector>

#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/compression_codec.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

class CompressedBlockBuilder {
 public:
  // 'codec' is expected to remain alive for the lifetime of this object.
  CompressedBlockBuilder(const CompressionCodec* codec, size_t size_limit);

  // Sets "*result" to the compressed version of the "data".
  // The data inside the result is owned by the CompressedBlockBuilder class
  // and valid until the class is destructed or until Compress() is called again.
  //
  // If an error was encountered, returns a non-OK status.
  Status Compress(const Slice& data, Slice *result);
  Status Compress(const std::vector<Slice> &data_slices, Slice *result);

  // header includes a 32-bit compressed length, 32-bit uncompressed length
  static const size_t kHeaderReservedLength = (2 * sizeof(uint32_t));

 private:
  DISALLOW_COPY_AND_ASSIGN(CompressedBlockBuilder);
  const CompressionCodec* codec_;
  faststring buffer_;
  size_t compressed_size_limit_;
};

class CompressedBlockDecoder {
 public:
  // 'codec' is expected to remain alive for the lifetime of this object.
  CompressedBlockDecoder(const CompressionCodec* codec, size_t size_limit);

  // Sets "*result" to the uncompressed version of the "data".
  // It is the caller's responsibility to free the result data.
  //
  // If an error was encountered, returns a non-OK status.
  Status Uncompress(const Slice& data, Slice *result);

  // Validates the header in the data block 'data'.
  // Sets '*uncompressed_size' to the uncompressed size of the data block
  // (i.e. the size of buffer that's required for a later call for UncompressIntoBuffer()).
  //
  // Returns Corruption if the data block header indicates a compressed size
  // that is different than the amount of remaining data in the block, or if the
  // uncompressed size is greater than the 'size_limit' provided in this class's constructor.
  //
  // In the case that this doesn't return OK, the output parameter may still
  // be modified.
  Status ValidateHeader(const Slice& data, uint32_t *uncompressed_size);

  // Uncompress into the provided 'dst' buffer, which must be at least as
  // large as 'uncompressed_size'. It's assumed that this length has already
  // been determined by calling Uncompress_Validate().
  Status UncompressIntoBuffer(const Slice& data, uint8_t* dst,
                              uint32_t uncompressed_size);
 private:
  DISALLOW_COPY_AND_ASSIGN(CompressedBlockDecoder);
  const CompressionCodec* codec_;
  size_t uncompressed_size_limit_;
};

} // namespace cfile
} // namespace kudu
#endif
