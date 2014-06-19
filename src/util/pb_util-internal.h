// Copyright (c) 2013, Cloudera, inc.
//
// Classes used internally by pb_util.h.
// This header should not be included by anything but pb_util and its tests.
#ifndef KUDU_UTIL_PB_UTIL_INTERNAL_H
#define KUDU_UTIL_PB_UTIL_INTERNAL_H

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include "util/env.h"

namespace kudu {

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

bool SequentialFileFileInputStream::Next(const void **data, int *size) {
  if (PREDICT_FALSE(!status_.ok())) {
    LOG(WARNING) << "Already failed on a previous read: " << status_.ToString();
    return false;
  }

  size_t available = (buffer_used_ - buffer_offset_);
  if (available > 0) {
    *data = buffer_.get() + buffer_offset_;
    *size = available;
    buffer_offset_ += available;
    total_read_ += available;
    return true;
  }

  Slice result;
  status_ = rfile_->Read(buffer_size_, &result, buffer_.get());
  if (!status_.ok()) {
    LOG(WARNING) << "Read at " << buffer_offset_ << " failed: " << status_.ToString();
    return false;
  }

  if (result.data() != buffer_.get()) {
    memcpy(buffer_.get(), result.data(), result.size());
  }

  buffer_used_ = result.size();
  buffer_offset_ = buffer_used_;
  total_read_ += buffer_used_;
  *data = buffer_.get();
  *size = buffer_used_;
  return buffer_used_ > 0;
}

bool SequentialFileFileInputStream::Skip(int count) {
  CHECK_GT(count, 0);
  int avail = (buffer_used_ - buffer_offset_);
  if (avail > count) {
    buffer_offset_ += count;
    total_read_ += count;
  } else {
    buffer_used_ = 0;
    buffer_offset_ = 0;
    status_ = rfile_->Skip(count - avail);
    total_read_ += count - avail;
  }
  return status_.ok();
}

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

bool WritableFileOutputStream::Next(void **data, int *size) {
  if (PREDICT_FALSE(!status_.ok())) {
    LOG(WARNING) << "Already failed on a previous write: " << status_.ToString();
    return false;
  }

  size_t available = (buffer_size_ - buffer_offset_);
  if (available > 0) {
    *data = buffer_.get() + buffer_offset_;
    *size = available;
    buffer_offset_ += available;
    return true;
  }

  if (!Flush()) {
    return false;
  }

  buffer_offset_ = buffer_size_;
  *data = buffer_.get();
  *size = buffer_size_;
  return true;
}

} // namespace kudu
#endif
