// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Classes used internally by pb_util.h.
// This header should not be included by anything but pb_util and its tests.
#ifndef KUDU_UTIL_PB_UTIL_INTERNAL_H
#define KUDU_UTIL_PB_UTIL_INTERNAL_H

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include "kudu/util/env.h"

namespace kudu {
namespace pb_util {
namespace internal {

// Input Stream used by ParseFromSequentialFile()
class SequentialFileFileInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  SequentialFileFileInputStream(SequentialFile *rfile, size_t buffer_size = kDefaultBufferSize)
    : buffer_used_(0), buffer_offset_(0),
      buffer_size_(buffer_size), buffer_(new uint8[buffer_size_]),
      total_read_(0), rfile_(rfile) {
    CHECK_GT(buffer_size, 0);
  }

  ~SequentialFileFileInputStream() {
  }

  bool Next(const void **data, int *size) OVERRIDE;
  bool Skip(int count) OVERRIDE;

  void BackUp(int count) OVERRIDE {
    CHECK_GE(count, 0);
    CHECK_LE(count, buffer_offset_);
    buffer_offset_ -= count;
    total_read_ -= count;
  }

  long ByteCount() const OVERRIDE { // NOLINT(runtime/int)
    return total_read_;
  }

 private:
  static const size_t kDefaultBufferSize = 8192;

  Status status_;

  size_t buffer_used_;
  size_t buffer_offset_;
  const size_t buffer_size_;
  gscoped_ptr<uint8_t[]> buffer_;

  size_t total_read_;
  SequentialFile *rfile_;
};

// Output Stream used by SerializeToWritableFile()
class WritableFileOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  WritableFileOutputStream(WritableFile *wfile, size_t buffer_size = kDefaultBufferSize)
    : buffer_offset_(0), buffer_size_(buffer_size), buffer_(new uint8[buffer_size_]),
      flushed_(0), wfile_(wfile) {
    CHECK_GT(buffer_size, 0);
  }

  ~WritableFileOutputStream() {
  }

  bool Flush() {
    if (buffer_offset_ > 0) {
      Slice data(buffer_.get(), buffer_offset_);
      status_ = wfile_->Append(data);
      flushed_ += buffer_offset_;
      buffer_offset_ = 0;
    }
    return status_.ok();
  }

  bool Next(void **data, int *size) OVERRIDE;

  void BackUp(int count) OVERRIDE {
    CHECK_GE(count, 0);
    CHECK_LE(count, buffer_offset_);
    buffer_offset_ -= count;
  }

  long ByteCount() const OVERRIDE { // NOLINT(runtime/int)
    return flushed_ + buffer_offset_;
  }

 private:
  static const size_t kDefaultBufferSize = 8192;

  Status status_;

  size_t buffer_offset_;
  const size_t buffer_size_;
  gscoped_ptr<uint8_t[]> buffer_;

  size_t flushed_;
  WritableFile *wfile_;
};

} // namespace internal
} // namespace pb_util
} // namespace kudu
#endif
