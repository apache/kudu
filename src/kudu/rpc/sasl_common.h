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

#ifndef KUDU_RPC_SASL_COMMON_H
#define KUDU_RPC_SASL_COMMON_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <set>
#include <string>

#include <sasl/sasl.h>

#include "kudu/gutil/port.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

#if defined(__APPLE__)
// Almost all functions in the SASL API are marked as deprecated
// since macOS 10.11.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif // #if defined(__APPLE__)

namespace kudu {

class Sockaddr;

namespace rpc {

// Constants
extern const char* const kSaslMechPlain;
extern const char* const kSaslMechGSSAPI;
extern const size_t kSaslMaxBufSize;

struct SaslMechanism {
  enum Type {
    INVALID,
    PLAIN,
    GSSAPI
  };
  static Type value_of(const std::string& mech);
  static const char* name_of(Type val);
};

struct SaslProtection {
  enum Type {
    // SASL authentication without integrity or privacy.
    kAuthentication = 0,
    // Integrity protection, i.e. messages are HMAC'd.
    kIntegrity = 1,
    // Privacy protection, i.e. messages are encrypted.
    kPrivacy = 2,
  };
  static const char* name_of(Type val);
};

// Initialize the SASL library.
// appname: Name of the application for logging messages & sasl plugin configuration.
//          Note that this string must remain allocated for the lifetime of the program.
// This function must be called before using SASL.
// If the library initializes without error, calling more than once has no effect.
//
// Some SASL plugins take time to initialize random number generators and other things,
// so the first time this function is invoked it may execute for several seconds.
// After that, it should be very fast. This function should be invoked as early as possible
// in the application lifetime to avoid SASL initialization taking place in a
// performance-critical section.
//
// This function is thread safe and uses a static lock.
// This function should NOT be called during static initialization.
Status SaslInit(bool kerberos_keytab_provided = false) WARN_UNUSED_RESULT;

// Disable Kudu's initialization of SASL. See equivalent method in client.h.
Status DisableSaslInitialization() WARN_UNUSED_RESULT;

// Wrap a call into the SASL library. 'call' should be a lambda which
// returns a SASL error code.
//
// The result is translated into a Status as follows:
//
//  SASL_OK:       Status::OK()
//  SASL_CONTINUE: Status::Incomplete()
//  otherwise:     Status::NotAuthorized()
//
// The Status message is beautified to be more user-friendly compared
// to the underlying sasl_errdetails() call.
Status WrapSaslCall(sasl_conn_t* conn, const std::function<int()>& call) WARN_UNUSED_RESULT;

// Return <ip>;<port> string formatted for SASL library use.
std::string SaslIpPortString(const Sockaddr& addr);

// Return available plugin mechanisms for the given connection.
std::set<SaslMechanism::Type> SaslListAvailableMechs();

// Initialize and return a libsasl2 callback data structure based on the passed args.
// id: A SASL callback identifier (e.g., SASL_CB_GETOPT).
// proc: A C-style callback with appropriate signature based on the callback id, or NULL.
// context: An object to pass to the callback as the context pointer, or NULL.
sasl_callback_t SaslBuildCallback(int id, int (*proc)(void), void* context);

// Enable SASL integrity and privacy protection on the connection. Also allows
// setting the minimum required protection level, and the maximum receive buffer
// size.
Status EnableProtection(sasl_conn_t* sasl_conn,
                        SaslProtection::Type minimum_protection = SaslProtection::kAuthentication,
                        size_t max_recv_buf_size = kSaslMaxBufSize) WARN_UNUSED_RESULT;

// Returns true if the SASL connection has been negotiated with auth-int or
// auth-conf. 'sasl_conn' must already be negotiated.
bool NeedsWrap(sasl_conn_t* sasl_conn);

// Retrieves the negotiated maximum send buffer size for auth-int or auth-conf
// protected channels.
uint32_t GetMaxSendBufferSize(sasl_conn_t* sasl_conn) WARN_UNUSED_RESULT;

// Encode the provided data.
//
// The plaintext data must not be longer than the negotiated maximum buffer size.
//
// The output 'ciphertext' slice is only valid until the next use of the SASL connection.
Status SaslEncode(sasl_conn_t* conn,
                  Slice plaintext,
                  Slice* ciphertext) WARN_UNUSED_RESULT;

// Decode the provided SASL-encoded data.
//
// The decoded plaintext must not be longer than the negotiated maximum buffer size.
//
// The output 'plaintext' slice is only valid until the next use of the SASL connection.
Status SaslDecode(sasl_conn_t* conn,
                  Slice ciphertext,
                  Slice* plaintext) WARN_UNUSED_RESULT;

// Deleter for sasl_conn_t instances, for use with gscoped_ptr after calling sasl_*_new()
struct SaslDeleter {
  inline void operator()(sasl_conn_t* conn) {
    sasl_dispose(&conn);
  }
};

// Internals exposed in the header for test purposes.
namespace internal {
void SaslSetMutex();
} // namespace internal

} // namespace rpc
} // namespace kudu

#if defined(__APPLE__)
#pragma GCC diagnostic pop
#endif // #if defined(__APPLE__)

#endif
