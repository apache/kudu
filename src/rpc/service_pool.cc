// Copyright (c) 2013, Cloudera, inc.

#include "rpc/service_pool.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <tr1/memory>

#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "util/metrics.h"
#include "util/status.h"
#include "util/thread_util.h"
#include "util/trace.h"

using std::tr1::shared_ptr;

METRIC_DEFINE_histogram(incoming_queue_time, kudu::MetricUnit::kMicroseconds,
    "Number of microseconds incoming RPC requests spend in the worker queue",
    60000000LU, 3);

namespace kudu {
namespace rpc {

ServicePool::ServicePool(const std::tr1::shared_ptr<Messenger> &messenger,
                         gscoped_ptr<ServiceIf> service)
  : messenger_(messenger),
    service_(service.Pass()),
    incoming_queue_time_(METRIC_incoming_queue_time.Instantiate(
        *(CHECK_NOTNULL(messenger_->metric_context())))) {
}

ServicePool::~ServicePool() {
  // We can't join all of our threads unless the Messenger is closing.
  CHECK(messenger_->closing());
  BOOST_FOREACH(shared_ptr<boost::thread>& thread, threads_) {
    CHECK_OK(ThreadJoiner(thread.get(), "service thread").Join());
  }
}

Status ServicePool::Init(int num_threads) {
  try {
    for (int i = 0; i < num_threads; i++) {
      threads_.push_back(shared_ptr<boost::thread>(
          new boost::thread(boost::bind(&ServicePool::RunThread, this))));
    }
  } catch(const boost::thread_resource_error &exception) {
    CHECK_EQ(string(), exception.what());
  }
  return Status::OK();
}

void ServicePool::RunThread() {
  SetThreadName("rpc worker");
  while (true) {
    gscoped_ptr<InboundCall> incoming;
    if (!messenger_->service_queue().BlockingGet(&incoming)) {
      VLOG(1) << "ServicePool: messenger shutting down.";
      return;
    }

    incoming->RecordHandlingStarted(incoming_queue_time_);
    incoming->trace()->Message("Handling call");
    ADOPT_TRACE(incoming->trace());

    // Release the InboundCall pointer -- when the call is responded to,
    // it will get deleted at that point.
    service_->Handle(incoming.release());
  }
}

} // namespace rpc
} // namespace kudu
