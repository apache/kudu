// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_BOOTSTRAP_H_
#define KUDU_TABLET_TABLET_BOOTSTRAP_H_

#include <boost/thread/shared_mutex.hpp>
#include <tr1/memory>
#include <string>

#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "util/status.h"

namespace kudu {

class MetricContext;

namespace log {
class Log;
class OpIdAnchorRegistry;
}

namespace metadata {
class TabletMetadata;
}

namespace server {
class Clock;
}

namespace tablet {
class Tablet;

// A listener for logging the tablet related statuses as well as
// piping it into the web UI.
class TabletStatusListener {
 public:
  explicit TabletStatusListener(const scoped_refptr<metadata::TabletMetadata>& meta);

  ~TabletStatusListener();

  void StatusMessage(const std::string& status);

  const std::string tablet_id() const;

  const std::string table_name() const;

  const std::string start_key() const;

  const std::string end_key() const;

  const Schema schema() const;

  std::string last_status() const {
    boost::shared_lock<boost::shared_mutex> l(lock_);
    return last_status_;
  }

 private:
  mutable boost::shared_mutex lock_;

  scoped_refptr<metadata::TabletMetadata> meta_;
  std::string last_status_;

  DISALLOW_COPY_AND_ASSIGN(TabletStatusListener);
};

extern const char* kLogRecoveryDir;

// Bootstraps a tablet, initializing it with the provided metadata. If the tablet
// has blocks and log segments, this method rebuilds the soft state by replaying
// the Log.
// TODO add functionality to fetch blocks and log segments from other TabletServers.
// TODO make this async and allow the caller to check on the status of recovery
// for monitoring purposes.
Status BootstrapTablet(const scoped_refptr<metadata::TabletMetadata>& meta,
                       const scoped_refptr<server::Clock>& clock,
                       MetricContext* metric_context,
                       TabletStatusListener* status_listener,
                       std::tr1::shared_ptr<tablet::Tablet>* rebuilt_tablet,
                       gscoped_ptr<log::Log>* rebuilt_log,
                       scoped_refptr<log::OpIdAnchorRegistry>* opid_anchor_registry);

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TABLET_BOOTSTRAP_H_ */
