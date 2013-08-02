// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_RPC_SASL_COMMON_H
#define KUDU_RPC_SASL_COMMON_H

#include <sasl/sasl.h>
#include <string>

#include "util/status.h"

namespace kudu {
namespace rpc {

// Returns a space-delimited list of the sasl mechanisms we attempt to support
const char* RpcSaslMechanisms();

// Initialize the SASL library.
// appname: Name of the application for logging messages & sasl plugin configuration.
//          Note that this string must remain allocated for the lifetime of the program.
// This function must be called before using SASL.
// If the library initializes without error, calling more than once has no effect.
// This function is thread safe and uses a static lock.
Status InitSASL(const char* const app_name);

// Deleter for sasl_conn_t instances, for use with gscoped_ptr after calling sasl_*_new()
struct SaslDeleter {
  inline void operator()(sasl_conn_t* conn) {
    sasl_dispose(&conn);
  }
};

}
}

#endif
