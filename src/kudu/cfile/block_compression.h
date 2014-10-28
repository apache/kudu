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
  explicit CompressedBlockBuilder(const std::tr1::shared_ptr<CompressionCodec> &codec,
                                  size_t size_limit);

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
  std::tr1::shared_ptr<CompressionCodec> codec_;
  faststring buffer_;
  size_t compressed_size_limit_;
};

class CompressedBlockDecoder {
 public:
  explicit CompressedBlockDecoder(const std::tr1::shared_ptr<CompressionCodec> &codec,
                                  size_t size_limit);

  // Sets "*result" to the uncompressed version of the "data".
  // It is the caller's responsibility to free the result data.
  //
  // If an error was encountered, returns a non-OK status.
  Status Uncompress(const Slice& data, Slice *result);

 private:
  DISALLOW_COPY_AND_ASSIGN(CompressedBlockDecoder);
  std::tr1::shared_ptr<CompressionCodec> codec_;
  size_t uncompressed_size_limit_;
};

} // namespace cfile
} // namespace kudu
#endif
