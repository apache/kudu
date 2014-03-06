// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_BOOTSTRAP_H_
#define KUDU_TABLET_TABLET_BOOTSTRAP_H_

#include <tr1/memory>
#include <string>

#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "util/status.h"

namespace kudu {

class MetricContext;

namespace log {
class Log;
}

namespace metadata {
class TabletMetadata;
}

namespace server {
class Clock;
}

namespace tablet {
class Tablet;

// An API for monitoring the tablet bootstrap process.
class TabletBootstrapListener {
 public:
  virtual ~TabletBootstrapListener() {
  }

  virtual void StatusMessage(const std::string& status) = 0;

  // Creates an instance of the default implementation of
  // TabletBootstrapListener for tablet described by 'meta'.
  static void GetDefaultBootstrapListener(
      const metadata::TabletMetadata* meta,
      gscoped_ptr<TabletBootstrapListener>* listener);
};

extern const char* kLogRecoveryDir;

// Bootstraps a tablet, initializing it with the provided metadata. If the tablet
// has blocks and log segments, this method rebuilds the soft state by replaying
// the Log.
// TODO add functionality to fetch blocks and log segments from other TabletServers.
// TODO make this async and allow the caller to check on the status of recovery
// for monitoring purposes.
Status BootstrapTablet(gscoped_ptr<metadata::TabletMetadata> meta,
                       const scoped_refptr<server::Clock>& clock,
                       MetricContext* metric_context,
                       gscoped_ptr<TabletBootstrapListener> boostrap_listener,
                       std::tr1::shared_ptr<tablet::Tablet>* rebuilt_tablet,
                       gscoped_ptr<log::Log>* rebuilt_log);

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TABLET_BOOTSTRAP_H_ */
