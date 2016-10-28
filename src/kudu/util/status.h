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

#include <stdint.h>
#include <string>

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif

#include "kudu/util/kudu_export.h"
#include "kudu/util/slice.h"

/// @brief Return the given status if it is not @c OK.
#define KUDU_RETURN_NOT_OK(s) do { \
    const ::kudu::Status& _s = (s);             \
    if (PREDICT_FALSE(!_s.ok())) return _s;     \
  } while (0);

/// @brief Return the given status if it is not OK, but first clone it and
///   prepend the given message.
#define KUDU_RETURN_NOT_OK_PREPEND(s, msg) do { \
    const ::kudu::Status& _s = (s);                              \
    if (PREDICT_FALSE(!_s.ok())) return _s.CloneAndPrepend(msg); \
  } while (0);

/// @brief Return @c to_return if @c to_call returns a bad status.
///   The substitution for 'to_return' may reference the variable
///   @c s for the bad status.
#define KUDU_RETURN_NOT_OK_RET(to_call, to_return) do { \
    const ::kudu::Status& s = (to_call);                \
    if (PREDICT_FALSE(!s.ok())) return (to_return);  \
  } while (0);

/// @brief Emit a warning if @c to_call returns a bad status.
#define KUDU_WARN_NOT_OK(to_call, warning_prefix) do { \
    const ::kudu::Status& _s = (to_call);              \
    if (PREDICT_FALSE(!_s.ok())) { \
      KUDU_LOG(WARNING) << (warning_prefix) << ": " << _s.ToString();  \
    } \
  } while (0);

/// @brief Log the given status and return immediately.
#define KUDU_LOG_AND_RETURN(level, status) do { \
    const ::kudu::Status& _s = (status);        \
    KUDU_LOG(level) << _s.ToString(); \
    return _s; \
  } while (0);

/// @brief If the given status is not OK, log it and 'msg' at 'level' and return the status.
#define KUDU_RETURN_NOT_OK_LOG(s, level, msg) do { \
    const ::kudu::Status& _s = (s);             \
    if (PREDICT_FALSE(!_s.ok())) { \
      KUDU_LOG(level) << "Status: " << _s.ToString() << " " << (msg); \
      return _s;     \
    } \
  } while (0);

/// @brief If @c to_call returns a bad status, CHECK immediately with
///   a logged message of @c msg followed by the status.
#define KUDU_CHECK_OK_PREPEND(to_call, msg) do { \
    const ::kudu::Status& _s = (to_call);                   \
    KUDU_CHECK(_s.ok()) << (msg) << ": " << _s.ToString();  \
  } while (0);

/// @brief If the status is bad, CHECK immediately, appending the status to the
///   logged message.
#define KUDU_CHECK_OK(s) KUDU_CHECK_OK_PREPEND(s, "Bad status")

/// @file status.h
///
/// This header is used in both the Kudu build as well as in builds of
/// applications that use the Kudu C++ client. In the latter we need to be
/// careful to "namespace" our macros, to avoid colliding or overriding with
/// similarly named macros belonging to the application.
///
/// KUDU_HEADERS_USE_SHORT_STATUS_MACROS handles this behavioral change. When
/// defined, we're building Kudu and:
/// @li Non-namespaced macros are allowed and mapped to the namespaced versions
///   defined above.
/// @li Namespaced versions of glog macros are mapped to the real glog macros
///   (otherwise the macros are defined in the C++ client stubs).
#ifdef KUDU_HEADERS_USE_SHORT_STATUS_MACROS
#define RETURN_NOT_OK         KUDU_RETURN_NOT_OK
#define RETURN_NOT_OK_PREPEND KUDU_RETURN_NOT_OK_PREPEND
#define RETURN_NOT_OK_RET     KUDU_RETURN_NOT_OK_RET
#define WARN_NOT_OK           KUDU_WARN_NOT_OK
#define LOG_AND_RETURN        KUDU_LOG_AND_RETURN
#define RETURN_NOT_OK_LOG     KUDU_RETURN_NOT_OK_LOG
#define CHECK_OK_PREPEND      KUDU_CHECK_OK_PREPEND
#define CHECK_OK              KUDU_CHECK_OK

// These are standard glog macros.
#define KUDU_LOG              LOG
#define KUDU_CHECK            CHECK
#endif

namespace kudu {

/// @brief A representation of an operation's outcome.
class KUDU_EXPORT Status {
 public:
  /// Create an object representing success status.
  Status() : state_(NULL) { }

  ~Status() { delete[] state_; }

  /// Copy the specified status.
  ///
  /// @param [in] s
  ///   The status object to copy from.
  Status(const Status& s);

  /// Assign the specified status.
  ///
  /// @param [in] s
  ///   The status object to assign from.
  void operator=(const Status& s);

#if __cplusplus >= 201103L
  /// Move the specified status (C++11).
  ///
  /// @param [in] s
  ///   rvalue reference to a Status object.
  Status(Status&& s);

  /// Assign the specified status using move semantics (C++11).
  ///
  /// @param [in] s
  ///   rvalue reference to a Status object.
  void operator=(Status&& s);
#endif

  /// @return A success status.
  static Status OK() { return Status(); }


  /// @name Methods to build status objects for various types of errors.
  ///
  /// @param [in] msg
  ///   The informational message on the error.
  /// @param [in] msg2
  ///   Additional information on the error (optional).
  /// @param [in] posix_code
  ///   POSIX error code, if applicable (optional).
  /// @return The error status of an appropriate type.
  ///
  ///@{
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
  static Status IllegalState(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kIllegalState, msg, msg2, posix_code);
  }
  static Status NotAuthorized(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kNotAuthorized, msg, msg2, posix_code);
  }
  static Status Aborted(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kAborted, msg, msg2, posix_code);
  }
  static Status RemoteError(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kRemoteError, msg, msg2, posix_code);
  }
  static Status ServiceUnavailable(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kServiceUnavailable, msg, msg2, posix_code);
  }
  static Status TimedOut(const Slice& msg, const Slice& msg2 = Slice(),
                         int16_t posix_code = -1) {
    return Status(kTimedOut, msg, msg2, posix_code);
  }
  static Status Uninitialized(const Slice& msg, const Slice& msg2 = Slice(),
                              int16_t posix_code = -1) {
    return Status(kUninitialized, msg, msg2, posix_code);
  }
  static Status ConfigurationError(const Slice& msg, const Slice& msg2 = Slice(),
                                   int16_t posix_code = -1) {
    return Status(kConfigurationError, msg, msg2, posix_code);
  }
  static Status Incomplete(const Slice& msg, const Slice& msg2 = Slice(),
                           int64_t posix_code = -1) {
    return Status(kIncomplete, msg, msg2, posix_code);
  }
  static Status EndOfFile(const Slice& msg, const Slice& msg2 = Slice(),
                          int64_t posix_code = -1) {
    return Status(kEndOfFile, msg, msg2, posix_code);
  }
  ///@}

  /// @return @c true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

  /// @return @c true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  /// @return @c true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  /// @return @c true iff the status indicates a NotSupported error.
  bool IsNotSupported() const { return code() == kNotSupported; }

  /// @return @c true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  /// @return @c true iff the status indicates an InvalidArgument error.
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  /// @return @c true iff the status indicates an AlreadyPresent error.
  bool IsAlreadyPresent() const { return code() == kAlreadyPresent; }

  /// @return @c true iff the status indicates a RuntimeError.
  bool IsRuntimeError() const { return code() == kRuntimeError; }

  /// @return @c true iff the status indicates a NetworkError.
  bool IsNetworkError() const { return code() == kNetworkError; }

  /// @return @c true iff the status indicates an IllegalState error.
  bool IsIllegalState() const { return code() == kIllegalState; }

  /// @return @c true iff the status indicates a NotAuthorized error.
  bool IsNotAuthorized() const { return code() == kNotAuthorized; }

  /// @return @c true iff the status indicates an Aborted error.
  bool IsAborted() const { return code() == kAborted; }

  /// @return @c true iff the status indicates a RemoteError.
  bool IsRemoteError() const { return code() == kRemoteError; }

  /// @return @c true iff the status indicates ServiceUnavailable.
  bool IsServiceUnavailable() const { return code() == kServiceUnavailable; }

  /// @return @c true iff the status indicates TimedOut.
  bool IsTimedOut() const { return code() == kTimedOut; }

  /// @return @c true iff the status indicates Uninitialized.
  bool IsUninitialized() const { return code() == kUninitialized; }

  /// @return @c true iff the status indicates ConfigurationError.
  bool IsConfigurationError() const { return code() == kConfigurationError; }

  /// @return @c true iff the status indicates Incomplete.
  bool IsIncomplete() const { return code() == kIncomplete; }

  /// @return @c true iff the status indicates end of file.
  bool IsEndOfFile() const { return code() == kEndOfFile; }

  /// @return A string representation of this status suitable for printing.
  ///   Returns the string "OK" for success.
  std::string ToString() const;

  /// @return A string representation of the status code, without the message
  ///   text or posix code information.
  std::string CodeAsString() const;

  /// This is similar to ToString, except that it does not include
  /// the stringified error code or posix code.
  ///
  /// @note The returned Slice is only valid as long as this Status object
  ///   remains live and unchanged.
  ///
  /// @return The message portion of the Status. For @c OK statuses,
  ///   this returns an empty string.
  Slice message() const;

  /// @return The POSIX code associated with this Status object,
  ///   or @c -1 if there is none.
  int16_t posix_code() const;

  /// Clone the object and add the specified prefix to the clone's message.
  ///
  /// @param [in] msg
  ///   The message to prepend.
  /// @return A new Status object with the same state plus an additional
  ///   leading message.
  Status CloneAndPrepend(const Slice& msg) const;

  /// Clone the object and add the specified suffix to the clone's message.
  ///
  /// @param [in] msg
  ///   The message to append.
  /// @return A new Status object with the same state plus an additional
  ///   trailing message.
  Status CloneAndAppend(const Slice& msg) const;

  /// @return The memory usage of this object without the object itself.
  ///   Should be used when embedded inside another object.
  size_t memory_footprint_excluding_this() const;

  /// @return The memory usage of this object including the object itself.
  ///   Should be used when allocated on the heap.
  size_t memory_footprint_including_this() const;

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
    kNetworkError = 8,
    kIllegalState = 9,
    kNotAuthorized = 10,
    kAborted = 11,
    kRemoteError = 12,
    kServiceUnavailable = 13,
    kTimedOut = 14,
    kUninitialized = 15,
    kConfigurationError = 16,
    kIncomplete = 17,
    kEndOfFile = 18,
    // NOTE: Remember to duplicate these constants into wire_protocol.proto and
    // and to add StatusTo/FromPB ser/deser cases in wire_protocol.cc !
    // Also remember to make the same changes to the java client in Status.java.
    //
    // TODO: Move error codes into an error_code.proto or something similar.
  };
  COMPILE_ASSERT(sizeof(Code) == 4, code_enum_size_is_part_of_abi);

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

#if __cplusplus >= 201103L
inline Status::Status(Status&& s) : state_(s.state_) {
  s.state_ = nullptr;
}

inline void Status::operator=(Status&& s) {
  if (state_ != s.state_) {
    delete[] state_;
    state_ = s.state_;
    s.state_ = nullptr;
  }
}
#endif

}  // namespace kudu

#endif  // KUDU_UTIL_STATUS_H_
