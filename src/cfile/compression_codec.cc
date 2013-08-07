// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <snappy-sinksource.h>
#include <snappy.h>
#include <zlib.h>
#include <lz4.h>
#include <string>
#include <vector>

#include "cfile/compression_codec.h"
#include "gutil/singleton.h"

namespace kudu {
namespace cfile {

CompressionCodec::CompressionCodec() {
}
CompressionCodec::~CompressionCodec() {
}

class SlicesSource : public snappy::Source {
 public:
  explicit SlicesSource(const std::vector<Slice>& slices)
    : slice_index_(0),
      slice_offset_(0),
      slices_(slices) {
    available_ = TotalSize();
  }

  size_t Available() const {
    return available_;
  }

  const char* Peek(size_t* len) {
    if (available_ == 0) {
      *len = 0;
      return NULL;
    }

    const Slice& data = slices_[slice_index_];
    *len = data.size() - slice_offset_;
    return reinterpret_cast<const char *>(data.data()) + slice_offset_;
  }

  void Skip(size_t n) {
    DCHECK_LE(n, Available());
    if (n == 0) return;

    available_ -= n;
    if ((n + slice_offset_) < slices_[slice_index_].size()) {
      slice_offset_ += n;
    } else {
      n -= slices_[slice_index_].size() - slice_offset_;
      slice_index_++;
      while (n > 0 && n >= slices_[slice_index_].size()) {
        n -= slices_[slice_index_].size();
        slice_index_++;
      }
      slice_offset_ = n;
    }
  }

  void Dump(faststring *buffer) {
    buffer->reserve(buffer->size() + TotalSize());
    BOOST_FOREACH(const Slice& block, slices_) {
      buffer->append(block.data(), block.size());
    }
  }

 private:
  size_t TotalSize(void) const {
    size_t size = 0;
    BOOST_FOREACH(const Slice& data, slices_) {
      size += data.size();
    }
    return size;
  }

 private:
  size_t available_;
  size_t slice_index_;
  size_t slice_offset_;
  const vector<Slice>& slices_;
};

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

  Status Compress(const vector<Slice>& input_slices, uint8_t *compressed, size_t *compressed_length) {
    SlicesSource source(input_slices);
    snappy::UncheckedByteArraySink sink(reinterpret_cast<char *>(compressed));
    if ((*compressed_length = snappy::Compress(&source, &sink)) <= 0) {
      return Status::Corruption("unable to compress the buffer");
    }
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

  Status Compress(const vector<Slice>& input_slices, uint8_t *compressed, size_t *compressed_length) {
    if (input_slices.size() == 1) {
      return Compress(input_slices[0], compressed, compressed_length);
    }

    SlicesSource source(input_slices);
    faststring buffer;
    source.Dump(&buffer);
    return Compress(Slice(buffer.data(), buffer.size()), compressed, compressed_length);
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
    return err == Z_OK ? Status::OK() : Status::IOError("unable to compress the buffer");
  }

  Status Compress(const vector<Slice>& input_slices, uint8_t *compressed, size_t *compressed_length) {
    if (input_slices.size() == 1) {
      return Compress(input_slices[0], compressed, compressed_length);
    }

    // TODO: use z_stream
    SlicesSource source(input_slices);
    faststring buffer;
    source.Dump(&buffer);
    return Compress(Slice(buffer.data(), buffer.size()), compressed, compressed_length);
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

Status GetCompressionCodec(CompressionType compression, shared_ptr<CompressionCodec> *codec) {
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

CompressionType GetCompressionCodecType(const std::string& name) {
  if (name.compare("snappy") == 0)
    return SNAPPY;
  if (name.compare("lz4") == 0)
    return LZ4;
  if (name.compare("zlib") == 0)
    return ZLIB;
  if (name.compare("none") == 0)
    return NO_COMPRESSION;

  LOG(WARNING) << "Unable to recognize the compression codec '" << name
               << "' using no compression as default.";
  return NO_COMPRESSION;
}

} // namespace cfile
} // namespace kudu
