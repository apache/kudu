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
#ifndef KUDU_CLIENT_CALLBACKS_H
#define KUDU_CLIENT_CALLBACKS_H

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif
#include "kudu/util/kudu_export.h"

namespace kudu {

class Status;

namespace client {


/// @brief All possible log levels.
enum KuduLogSeverity {
  SEVERITY_INFO,
  SEVERITY_WARNING,
  SEVERITY_ERROR,
  SEVERITY_FATAL
};

/// @brief The interface for all logging callbacks.
class KUDU_EXPORT KuduLoggingCallback {
 public:
  KuduLoggingCallback() {
  }

  virtual ~KuduLoggingCallback() {
  }

  /// Log the message.
  ///
  /// @note The @c message is NOT terminated with an endline.
  ///
  /// @param [in] severity
  ///   Severity of the log message.
  /// @param [in] filename
  ///   The name of the source file the message is originated from.
  /// @param [in] line_number
  ///   The line of the source file the message is originated from.
  /// @param [in] time
  ///   The absolute time when the log event was generated.
  /// @param [in] message
  ///   The message to log. It's not terminated with an endline.
  /// @param [in] message_len
  ///   Number of characters in the @c message.
  virtual void Run(KuduLogSeverity severity,
                   const char* filename,
                   int line_number,
                   const struct ::tm* time,
                   const char* message,
                   size_t message_len) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(KuduLoggingCallback);
};

/// @brief The logging callback that invokes a member function of an object.
template <typename T>
class KUDU_EXPORT KuduLoggingMemberCallback : public KuduLoggingCallback {
 public:
  /// @brief A handy typedef for the member function with appropriate signature.
  typedef void (T::*MemberType)(
      KuduLogSeverity severity,
      const char* filename,
      int line_number,
      const struct ::tm* time,
      const char* message,
      size_t message_len);

  /// Build an instance of KuduLoggingMemberCallback.
  ///
  /// @param [in] object
  ///   A pointer to the object.
  /// @param [in] member
  ///   A pointer to the member function of the @c object to invoke.
  KuduLoggingMemberCallback(T* object, MemberType member)
    : object_(object),
      member_(member) {
  }

  /// @copydoc KuduLoggingCallback::Run()
  virtual void Run(KuduLogSeverity severity,
                   const char* filename,
                   int line_number,
                   const struct ::tm* time,
                   const char* message,
                   size_t message_len) OVERRIDE {
    (object_->*member_)(severity, filename, line_number, time,
        message, message_len);
  }

 private:
  T* object_;
  MemberType member_;
};

/// @brief The logging callback that invokes a function by pointer
///   with a single argument.
template <typename T>
class KUDU_EXPORT KuduLoggingFunctionCallback : public KuduLoggingCallback {
 public:
  /// @brief A handy typedef for the function with appropriate signature.
  typedef void (*FunctionType)(T arg,
      KuduLogSeverity severity,
      const char* filename,
      int line_number,
      const struct ::tm* time,
      const char* message,
      size_t message_len);

  /// Build an instance of KuduLoggingFunctionCallback.
  ///
  /// @param [in] function
  ///   A pointer to the logging function to invoke with the @c arg argument.
  /// @param [in] arg
  ///   An argument for the function invocation.
  KuduLoggingFunctionCallback(FunctionType function, T arg)
    : function_(function),
      arg_(arg) {
  }

  /// @copydoc KuduLoggingCallback::Run()
  virtual void Run(KuduLogSeverity severity,
                   const char* filename,
                   int line_number,
                   const struct ::tm* time,
                   const char* message,
                   size_t message_len) OVERRIDE {
    function_(arg_, severity, filename, line_number, time,
              message, message_len);
  }

 private:
  FunctionType function_;
  T arg_;
};

/// @brief The interface for all status callbacks.
class KUDU_EXPORT KuduStatusCallback {
 public:
  KuduStatusCallback() {
  }

  virtual ~KuduStatusCallback() {
  }

  /// Notify/report on the status.
  ///
  /// @param [in] s
  ///   The status to report.
  virtual void Run(const Status& s) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(KuduStatusCallback);
};

/// @brief The status callback that invokes a member function of an object.
template <typename T>
class KUDU_EXPORT KuduStatusMemberCallback : public KuduStatusCallback {
 public:
  /// @brief A handy typedef for the member with appropriate signature.
  typedef void (T::*MemberType)(const Status& s);

  /// Build an instance of the KuduStatusMemberCallback class.
  ///
  /// @param [in] object
  ///   A pointer to the object.
  /// @param [in] member
  ///   A pointer to the member function of the @c object to invoke.
  KuduStatusMemberCallback(T* object, MemberType member)
    : object_(object),
      member_(member) {
  }

  /// @copydoc KuduStatusCallback::Run()
  virtual void Run(const Status& s) OVERRIDE {
    (object_->*member_)(s);
  }

 private:
  T* object_;
  MemberType member_;
};

/// @brief The status callback that invokes a function by pointer
///   with a single argument.
template <typename T>
class KUDU_EXPORT KuduStatusFunctionCallback : public KuduStatusCallback {
 public:
  /// @brief A handy typedef for the function with appropriate signature.
  typedef void (*FunctionType)(T arg, const Status& s);

  /// Build an instance of KuduStatusFunctionCallback.
  ///
  /// @param [in] function
  ///   A pointer to the status report function to invoke
  ///   with the @c arg argument.
  /// @param [in] arg
  ///   An argument for the function invocation.
  KuduStatusFunctionCallback(FunctionType function, T arg)
    : function_(function),
      arg_(arg) {
  }

  /// @copydoc KuduStatusCallback::Run()
  virtual void Run(const Status& s) OVERRIDE {
    function_(arg_, s);
  }

 private:
  FunctionType function_;
  T arg_;
};

} // namespace client
} // namespace kudu

#endif
