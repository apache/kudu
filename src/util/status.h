// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef KUDU_UTIL_STATUS_H_
#define KUDU_UTIL_STATUS_H_

#include <string>
#include "util/slice.h"

// Return the given status if it is not OK.
#define RETURN_NOT_OK(s) do { \
    Status _s = (s); \
    if (PREDICT_FALSE(!_s.ok())) return _s;     \
  } while (0);

// Return 'to_return' if 'to_call' returns a bad status.
// The substitution for 'to_return' may reference the variable
// 's' for the bad status.
#define RETURN_NOT_OK_RET(to_call, to_return) do { \
    Status s = (to_call); \
    if (PREDICT_FALSE(!s.ok())) return (to_return);  \
  } while (0);


#define CHECK_OK(s) do { \
  Status _s = (s); \
  CHECK(_s.ok()) << "Bad status: " << _s.ToString(); \
  } while (0);

namespace kudu {

class Status {
 public:
  // Create a success status.
  Status() : state_(NULL) { }
  ~Status() { delete[] state_; }

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kNotFound, msg, msg2, posix_code);
  }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kCorruption, msg, msg2, posix_code);
  }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kNotSupported, msg, msg2, posix_code);
  }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kInvalidArgument, msg, msg2, posix_code);
  }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kIOError, msg, msg2, posix_code);
  }
  static Status AlreadyPresent(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kAlreadyPresent, msg, msg2, posix_code);
  }
  static Status RuntimeError(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kRuntimeError, msg, msg2, posix_code);
  }
  static Status NetworkError(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kNetworkError, msg, msg2, posix_code);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates a NotSupported error.
  bool IsNotSupported() const { return code() == kNotSupported; }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  // Returns true iff the status indicates an InvalidArgument error
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // Returns true iff the status indicates an AlreadyPresent error
  bool IsAlreadyPresent() const { return code() == kAlreadyPresent; }

  // Returns true iff the status indicates a RuntimeError.
  bool IsRuntimeError() const { return code() == kRuntimeError; }

  // Returns true iff the status indicates a NetworkError.
  bool IsNetworkError() const { return code() == kNetworkError; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  // Get the POSIX code associated with this Status, or -1 if there is none.
  int16_t posix_code() const;

 private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..6] == posix_code
  //    state_[7..]  == message
  const char* state_;

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kAlreadyPresent = 6,
    kRuntimeError = 7,
    kNetworkError = 8
  };

  Code code() const {
    return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const Slice& msg, const Slice& msg2, int16_t posix_code);
  static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
}
inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    delete[] state_;
    state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
  }
}

}  // namespace kudu

#endif  // KUDU_UTIL_STATUS_H_
