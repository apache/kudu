// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <snappy.h>
#include <zlib.h>
#include <lz4.h>

#include "cfile/compression_codec.h"
#include "gutil/singleton.h"

namespace kudu {
namespace cfile {

class SnappyCodec : public CompressionCodec {
public:
  static SnappyCodec *GetSingleton() {
    return Singleton<SnappyCodec>::get();
  }

  Status Compress(const Slice& input, uint8_t *compressed, size_t *compressed_length) {
    snappy::RawCompress(reinterpret_cast<const char *>(input.data()), input.size(),
                        reinterpret_cast<char *>(compressed), compressed_length);
    return Status::OK();
  }

  Status Uncompress(const Slice& compressed, uint8_t *uncompressed, size_t uncompressed_length) {
    bool success = snappy::RawUncompress(reinterpret_cast<const char *>(compressed.data()),
                                         compressed.size(), reinterpret_cast<char *>(uncompressed));
    return success ? Status::OK() : Status::Corruption("unable to uncompress the buffer");
  }

  size_t MaxCompressedLength(size_t source_bytes) {
    return snappy::MaxCompressedLength(source_bytes);
  }
};

class Lz4Codec : public CompressionCodec {
public:
  static Lz4Codec *GetSingleton() {
    return Singleton<Lz4Codec>::get();
  }

  Status Compress(const Slice& input, uint8_t *compressed, size_t *compressed_length) {
    int n = LZ4_compress(reinterpret_cast<const char *>(input.data()),
                         reinterpret_cast<char *>(compressed), input.size());
    *compressed_length = n;
    return Status::OK();
  }

  Status Uncompress(const Slice& compressed, uint8_t *uncompressed, size_t uncompressed_length) {
    int n = LZ4_uncompress(reinterpret_cast<const char *>(compressed.data()),
                           reinterpret_cast<char *>(uncompressed), uncompressed_length);
    if (n != compressed.size()) {
      return Status::Corruption(
        StringPrintf("unable to uncompress the buffer. error near %d, buffer", -n),
          compressed.ToDebugString(100));
    }
    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) {
    return LZ4_compressBound(source_bytes);
  }
};

/**
 * TODO: use a instance-local Arena and pass alloc/free into zlib
 * so that it allocates from the arena.
 */
class ZlibCodec : public CompressionCodec {
public:
  static ZlibCodec *GetSingleton() {
    return Singleton<ZlibCodec>::get();
  }

  Status Compress(const Slice& input, uint8_t *compressed, size_t *compressed_length) {
    *compressed_length = MaxCompressedLength(input.size());
    int err = ::compress(compressed, compressed_length, input.data(), input.size());
    return err == Z_OK ? Status::OK() : Status::IOError("unable to compress the buffer");;
  }

  Status Uncompress(const Slice& compressed, uint8_t *uncompressed, size_t uncompressed_length) {
    int err = ::uncompress(uncompressed, &uncompressed_length, compressed.data(), compressed.size());
    return err == Z_OK ? Status::OK() : Status::Corruption("unable to uncompress the buffer");
  }

  size_t MaxCompressedLength(size_t source_bytes) {
    // one-time overhead of six bytes for the entire stream plus five bytes per 16 KB block
    return source_bytes + (6 + (5 * ((source_bytes + 16383) >> 14)));
  }
};

Status GetCompressionCodec (CompressionType compression, shared_ptr<CompressionCodec> *codec) {
  switch (compression) {
    case NO_COMPRESSION:
      codec->reset();
      return Status::OK();
    case SNAPPY:
      codec->reset(new SnappyCodec());
      break;
    case LZ4:
      codec->reset(new Lz4Codec());
      break;
    case ZLIB:
      codec->reset(new ZlibCodec());
      break;
    default:
      return Status::NotFound("bad compression type");
  }

  CHECK(codec->get() != NULL); // sanity check postcondition
  return Status::OK();
}

} // namespace cfile
} // namespace kudu
