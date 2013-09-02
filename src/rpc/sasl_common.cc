// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "rpc/sasl_common.h"

#include <string>
#include <boost/algorithm/string/predicate.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sasl/sasl.h>

#include "gutil/macros.h"
#include "gutil/once.h"
#include "gutil/stringprintf.h"
#include "util/net/sockaddr.h"

// Use this to search for plugins. Should work at least on RHEL & Ubuntu.
DEFINE_string(sasl_path, "/usr/lib/sasl2:/usr/lib64/sasl2:/usr/lib/x86_64-linux-gnu/sasl2",
    "Colon separated list of paths to look for SASL security library plugins.");

namespace kudu {
namespace rpc {

const char* const kSaslMechAnonymous = "ANONYMOUS";
const char* const kSaslMechPlain = "PLAIN";

// Output Sasl messages.
// context: not used.
// level: logging level.
// message: message to output;
static int SaslLogCallback(void* context, int level, const char* message) {

  if (message == NULL) return SASL_BADPARAM;

  switch (level) {
    case SASL_LOG_NONE:
      break;

    case SASL_LOG_ERR:
    case SASL_LOG_FAIL:
      LOG(ERROR) << "SASL: " << message;
      break;

    case SASL_LOG_WARN:
      LOG(WARNING) << "SASL: " << message;
      break;

    case SASL_LOG_NOTE:
      LOG(INFO) << "SASL: " << message;
      break;

    case SASL_LOG_DEBUG:
      VLOG(1) << "SASL: " << message;
      break;

    case SASL_LOG_TRACE:
    case SASL_LOG_PASS:
      VLOG(3) << "SASL: " << message;
      break;
  }

  return SASL_OK;
}

// Get Sasl option.
// context: not used
// plugin_name: name of plugin for which an option is being requested.
// option: option requested
// result: value for option
// len: length of the result
// Return SASL_FAIL if the option is not handled, this does not fail the handshake.
static int SaslGetOption(void* context, const char* plugin_name, const char* option,
                         const char** result, unsigned* len) {
  // Handle Sasl Library options
  if (plugin_name == NULL) {
    // Return the logging level that we want the sasl library to use.
    if (strcmp("log_level", option) == 0) {
      int level = SASL_LOG_NOTE;
      if (VLOG_IS_ON(1)) {
        level = SASL_LOG_DEBUG;
      } else if (VLOG_IS_ON(3)) {
        level = SASL_LOG_TRACE;
      }
      static char buf[4];
      snprintf(buf, arraysize(buf), "%d", level);
      *result = buf;
      if (len != NULL) *len = strlen(buf);
      return SASL_OK;
    }
    // Options can default so don't complain.
    VLOG(4) << "SaslGetOption: Unknown library option: " << option;
    return SASL_FAIL;
  }
  VLOG(4) << "SaslGetOption: Unknown plugin: " << plugin_name;
  return SASL_FAIL;
}

// Sasl Get Path callback.
// Returns the list of possible places for the plugins might be.
// Places we know they might be:
// UBUNTU:          /usr/lib/sasl2 or /usr/lib/x86_64-linux-gnu/sasl2
// CENTOS:          /usr/lib64/sasl2
static int SaslGetPath(void* context, const char** path) {
  *path = FLAGS_sasl_path.c_str();
  VLOG(3) << "SASL path: " << FLAGS_sasl_path;
  return SASL_OK;
}

// Array of callbacks for the sasl library.
static sasl_callback_t callbacks[] = {
  { SASL_CB_LOG, reinterpret_cast<int (*)()>(&SaslLogCallback), NULL },
  { SASL_CB_GETOPT, reinterpret_cast<int (*)()>(&SaslGetOption), NULL },
  { SASL_CB_GETPATH, reinterpret_cast<int (*)()>(&SaslGetPath), NULL },
  { SASL_CB_LIST_END, NULL, NULL }
};

// Determine whether initialization was ever called
struct InitializationData {
  Status status;
  string app_name;
};
static struct InitializationData* sasl_init_data;

static void DoSaslInit(const char* app_name) {
  VLOG(3) << "Initializing SASL library";

  sasl_init_data = new InitializationData();
  sasl_init_data->app_name = app_name;

  int result = sasl_client_init(&callbacks[0]);
  if (result != SASL_OK) {
    sasl_init_data->status = Status::RuntimeError("Could not initialize SASL client",
        sasl_errstring(result, NULL, NULL));
    return;
  }

  result = sasl_server_init(&callbacks[0], sasl_init_data->app_name.c_str());
  if (result != SASL_OK) {
    sasl_init_data->status = Status::RuntimeError("Could not initialize SASL server",
        sasl_errstring(result, NULL, NULL));
    return;
  }

  sasl_init_data->status = Status::OK();
}

// Only execute SASL initialization once
static GoogleOnceType once = GOOGLE_ONCE_INIT;

Status SaslInit(const char* const app_name) {
  GoogleOnceInitArg(&once, &DoSaslInit, app_name);
  if (sasl_init_data->app_name != app_name) {
    return Status::InvalidArgument("SaslInit called successively with different arguments",
        StringPrintf("Previous: %s, current: %s", sasl_init_data->app_name.c_str(), app_name));
  }
  return sasl_init_data->status;
}

string SaslErrDesc(int status, sasl_conn_t* conn) {
  if (conn != NULL) {
    return StringPrintf("SASL result code: %s, error: %s",
        sasl_errstring(status, NULL, NULL),
        sasl_errdetail(conn));
  }
  return StringPrintf("SASL result code: %s", sasl_errstring(status, NULL, NULL));
}

string SaslIpPortString(const Sockaddr& addr) {
  string addr_str = addr.ToString();
  size_t colon_pos = addr_str.find(':');
  if (colon_pos != string::npos) {
    addr_str[colon_pos] = ';';
  }
  return addr_str;
}

set<string> SaslListAvailableMechs() {
  set<string> mechs;

  // Array of NULL-terminated strings. Array terminated with NULL.
  const char** mech_strings = sasl_global_listmech();
  while (mech_strings != NULL && *mech_strings != NULL) {
    mechs.insert(*mech_strings);
    mech_strings++;
  }
  return mechs;
}

sasl_callback_t SaslBuildCallback(int id, int (*proc)(void), void* context) {
  sasl_callback_t callback;
  callback.id = id;
  callback.proc = proc;
  callback.context = context;
  return callback;
}

SaslMechanism::Type SaslMechanism::value_of(const string& mech) {
  if (boost::iequals(mech, "ANONYMOUS")) {
    return ANONYMOUS;
  } else if (boost::iequals(mech, "PLAIN")) {
    return PLAIN;
  }
  return INVALID;
}

} // namespace rpc
} // namespace kudu
