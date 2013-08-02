// Copyright (c) 2013, Cloudera, inc.

#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gutil/macros.h>
#include <gutil/once.h>
#include <gutil/stringprintf.h>
#include <sasl/sasl.h>

#include "rpc/sasl_common.h"

// Use this to search for plugins. Should work at least on RHEL & Ubuntu.
DEFINE_string(sasl_path, "/usr/lib/sasl2:/usr/lib64/sasl2:/usr/lib/x86_64-linux-gnu/sasl2",
    "Colon separated list of paths to look for SASL security library plugins.");

namespace kudu {
namespace rpc {

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

    if (strcmp("client_mech_list", option) == 0) {
      const char* mechs = RpcSaslMechanisms();
      *result = mechs;
      if (len != NULL) *len = strlen(mechs);
      return SASL_OK;
    }
    // Options can default so don't complain.
    VLOG(3) << "SaslGetOption: Unknown option: " << option;
    return SASL_FAIL;
  }

  VLOG(3) << "SaslGetOption: Unknown plugin: " << plugin_name;
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

// space-delimited, since we feed it to the client_mech_list callback
const char* RpcSaslMechanisms() {
  return "ANONYMOUS PLAIN";
}

// Array of callbacks for the sasl library.
static sasl_callback_t callbacks[] = {
  { SASL_CB_LOG, reinterpret_cast<int (*)()>(&SaslLogCallback), NULL },
  { SASL_CB_GETOPT, reinterpret_cast<int (*)()>(&SaslGetOption), NULL },
  { SASL_CB_GETPATH, reinterpret_cast<int (*)()>(&SaslGetPath), NULL },
  { SASL_CB_LIST_END, NULL, NULL }
};

// Determine whether initialization was ever called
static Status sasl_init_status_;
static const char* app_name_;

static void DoInitSASL(const char* const app_name) {
  VLOG(3) << "Initializing SASL library";
  app_name_ = app_name;

  int result = sasl_client_init(&callbacks[0]);
  if (result != SASL_OK) {
    sasl_init_status_ = Status::RuntimeError("Could not initialize SASL client",
        sasl_errstring(result, NULL, NULL));
    return;
  }

  result = sasl_server_init(&callbacks[0], app_name);
  if (result != SASL_OK) {
    sasl_init_status_ = Status::RuntimeError("Could not initialize SASL server",
        sasl_errstring(result, NULL, NULL));
    return;
  }

  sasl_init_status_ = Status::OK();
}

// Only execute SASL initialization once
static GoogleOnceType once = GOOGLE_ONCE_INIT;

Status InitSASL(const char* const app_name) {
  GoogleOnceInitArg(&once, &DoInitSASL, app_name);
  if (strcmp(app_name, app_name_) != 0) {
    return Status::InvalidArgument("InitSASL called successively with different arguments",
        StringPrintf("Previous: %s, current: %s", app_name_, app_name));
  }
  return sasl_init_status_;
}

}
}
