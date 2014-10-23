// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_SERVER_TRACING_PATH_HANDLERS_H
#define KUDU_SERVER_TRACING_PATH_HANDLERS_H

#include "kudu/gutil/macros.h"
#include "kudu/server/webserver.h"
#include "kudu/util/status.h"

#include <sstream>

namespace kudu {
namespace server {

// Web handlers for Chromium tracing.
// These handlers provide AJAX endpoints for /tracing.html provided by
// the trace-viewer package.
class TracingPathHandlers {
 public:
  static void RegisterHandlers(Webserver* server);

  DISALLOW_IMPLICIT_CONSTRUCTORS(TracingPathHandlers);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_TRACING_PATH_HANDLERS_H */
