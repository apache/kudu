// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
//
// Some portions copyright (C) 2008, Google, inc.
//
// Utilities for working with protobufs.
// Some of this code is cribbed from the protobuf source,
// but modified to work with kudu's 'faststring' instead of STL strings.

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/message_lite.h>

#include "util/pb_util.h"
#include "util/status.h"
#include "util/env.h"

namespace kudu {
namespace pb_util {

namespace {

using std::string;

// When serializing, we first compute the byte size, then serialize the message.
// If serialization produces a different number of bytes than expected, we
// call this function, which crashes.  The problem could be due to a bug in the
// protobuf implementation but is more likely caused by concurrent modification
// of the message.  This function attempts to distinguish between the two and
// provide a useful error message.
void ByteSizeConsistencyError(int byte_size_before_serialization,
                              int byte_size_after_serialization,
                              int bytes_produced_by_serialization) {
  CHECK_EQ(byte_size_before_serialization, byte_size_after_serialization)
      << "Protocol message was modified concurrently during serialization.";
  CHECK_EQ(bytes_produced_by_serialization, byte_size_before_serialization)
      << "Byte size calculation and serialization were inconsistent.  This "
         "may indicate a bug in protocol buffers or it may be caused by "
         "concurrent modification of the message.";
  LOG(FATAL) << "This shouldn't be called if all the sizes are equal.";
}

string InitializationErrorMessage(const char* action,
                                  const MessageLite& message) {
  // Note:  We want to avoid depending on strutil in the lite library, otherwise
  //   we'd use:
  //
  // return strings::Substitute(
  //   "Can't $0 message of type \"$1\" because it is missing required "
  //   "fields: $2",
  //   action, message.GetTypeName(),
  //   message.InitializationErrorString());

  string result;
  result += "Can't ";
  result += action;
  result += " message of type \"";
  result += message.GetTypeName();
  result += "\" because it is missing required fields: ";
  result += message.InitializationErrorString();
  return result;
}

} // anonymous

bool AppendToString(const MessageLite &msg, faststring *output) {
  DCHECK(msg.IsInitialized()) << InitializationErrorMessage("serialize", msg);
  return AppendPartialToString(msg, output);
}

bool AppendPartialToString(const MessageLite &msg, faststring* output) {
  int old_size = output->size();
  int byte_size = msg.ByteSize();

  output->resize(old_size + byte_size);

  uint8* start = &((*output)[old_size]);
  uint8* end = msg.SerializeWithCachedSizesToArray(start);
  if (end - start != byte_size) {
    ByteSizeConsistencyError(byte_size, msg.ByteSize(), end - start);
  }
  return true;
}

bool SerializeToString(const MessageLite &msg, faststring *output) {
  output->clear();
  return AppendToString(msg, output);
}

// Input Stream used by ParseFromSequentialFile()
class SequentialFileFileInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  SequentialFileFileInputStream(SequentialFile *rfile, size_t buffer_size = kDefaultBufferSize)
    : buffer_used_(0), buffer_offset_(0),
      buffer_size_(buffer_size), buffer_(new uint8[buffer_size_]),
      total_read_(0), rfile_(rfile)
  {
    CHECK_GT(buffer_size, 0);
  }

  ~SequentialFileFileInputStream() {
  }

  bool Next(const void **data, int *size);
  bool Skip(int count);

  void BackUp(int count) {
    CHECK_GE(count, 0);
    CHECK_LE(count, buffer_offset_);
    buffer_offset_ -= count;
    total_read_ -= count;
  }

  long ByteCount() const {
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
      written_(0), wfile_(wfile)
  {
    CHECK_GT(buffer_size, 0);
  }

  ~WritableFileOutputStream() {
  }

  bool Flush() {
    if (buffer_offset_ > 0) {
      Slice data(buffer_.get(), buffer_offset_);
      status_ = wfile_->Append(data);
      buffer_offset_ = 0;
    }
    return status_.ok();
  }

  bool Next(void **data, int *size);

  void BackUp(int count) {
    CHECK_GE(count, 0);
    CHECK_LE(count, buffer_offset_);
    buffer_offset_ -= count;
    written_ -= count;
  }

  long ByteCount() const {
    return written_;
  }

 private:
  static const size_t kDefaultBufferSize = 8192;

  Status status_;

  size_t buffer_offset_;
  const size_t buffer_size_;
  gscoped_ptr<uint8_t[]> buffer_;

  size_t written_;
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
    written_ += available;
    return true;
  }

  if (!Flush()) {
    return false;
  }

  buffer_offset_ = 0;
  written_ += buffer_size_;
  *data = buffer_.get();
  *size = buffer_size_;
  return true;
}

bool ParseFromSequentialFile(MessageLite *msg, SequentialFile *rfile) {
  SequentialFileFileInputStream istream(rfile);
  return msg->ParseFromZeroCopyStream(&istream);
}

bool SerializeToWritableFile(const MessageLite& msg, WritableFile *wfile) {
  WritableFileOutputStream ostream(wfile);
  bool res = msg.SerializeToZeroCopyStream(&ostream);
  return res && ostream.Flush();
}

} // namespace pb_util
} // namespace kudu
