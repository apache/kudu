// Copyright (c) 2013, Cloudera, inc.

#include "rpc/service_pool.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <tr1/memory>

#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "rpc/inbound_call.h"
#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "gutil/strings/substitute.h"
#include "util/metrics.h"
#include "util/status.h"
#include "util/thread.h"
#include "util/trace.h"

using std::tr1::shared_ptr;
using strings::Substitute;

METRIC_DEFINE_histogram(incoming_queue_time, kudu::MetricUnit::kMicroseconds,
    "Number of microseconds incoming RPC requests spend in the worker queue",
    60000000LU, 3);

METRIC_DEFINE_counter(rpcs_timed_out_in_queue, kudu::MetricUnit::kRequests,
                      "Number of RPCs whose timeout elapsed while waiting "
                      "in the service queue, and thus were not processed.");

METRIC_DEFINE_counter(rpcs_queue_overflow, kudu::MetricUnit::kRequests,
                      "Number of RPCs dropped because the service queue "
                      "was full.");

namespace kudu {
namespace rpc {

ServicePool::ServicePool(gscoped_ptr<ServiceIf> service,
                         const MetricContext& metric_ctx,
                         size_t service_queue_length)
  : service_(service.Pass()),
    service_queue_(service_queue_length),
    incoming_queue_time_(METRIC_incoming_queue_time.Instantiate(metric_ctx)),
    rpcs_timed_out_in_queue_(METRIC_rpcs_timed_out_in_queue.Instantiate(metric_ctx)),
    rpcs_queue_overflow_(METRIC_rpcs_queue_overflow.Instantiate(metric_ctx)),
    closing_(false) {
}

ServicePool::~ServicePool() {
  Shutdown();
}

Status ServicePool::Init(int num_threads) {
  for (int i = 0; i < num_threads; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("service pool", "rpc worker",
        &ServicePool::RunThread, this, &new_thread));
    threads_.push_back(new_thread);
  }
  return Status::OK();
}

void ServicePool::Shutdown() {
  service_queue_.Shutdown();

  boost::lock_guard<boost::mutex> lock(shutdown_lock_);
  if (closing_) return;
  closing_ = true;
  // TODO: Use a proper thread pool implementation.
  BOOST_FOREACH(scoped_refptr<kudu::Thread>& thread, threads_) {
    CHECK_OK(ThreadJoiner(thread.get()).Join());
  }

  // Now we must drain the service queue.
  Status status = Status::ServiceUnavailable("Service is shutting down");
  gscoped_ptr<InboundCall> incoming;
  while (service_queue_.BlockingGet(&incoming)) {
    incoming.release()->RespondFailure(ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  }

  service_->Shutdown();
}

Status ServicePool::QueueInboundCall(gscoped_ptr<InboundCall> call) {
  InboundCall* c = call.release();

  TRACE_TO(c->trace(), "Inserting onto call queue");
  // Queue message on service queue
  QueueStatus queue_status = service_queue_.Put(c);
  if (PREDICT_TRUE(queue_status == QUEUE_SUCCESS)) {
    // NB: do not do anything with 'c' after it is successfully queued --
    // a service thread may have already dequeued it, processed it, and
    // responded by this point, in which case the pointer would be invalid.
    return Status::OK();
  }

  Status status = Status::OK();
  if (queue_status == QUEUE_FULL) {
    status = Status::ServiceUnavailable(Substitute(
        "$0 request on $1 from $2 dropped due to backpressure. "
        "The service queue is full; it has $3 items.",
        c->method_name(),
        service_->service_name(),
        c->remote_address().ToString(),
        service_queue_.max_size()));
    rpcs_queue_overflow_->Increment();
    c->RespondFailure(ErrorStatusPB::ERROR_SERVER_TOO_BUSY, status);
  } else if (queue_status == QUEUE_SHUTDOWN) {
    status = Status::ServiceUnavailable("Service is shutting down");
    c->RespondFailure(ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  } else {
    status = Status::RuntimeError(Substitute("Unknown error from BlockingQueue: $0", queue_status));
    c->RespondFailure(ErrorStatusPB::FATAL_UNKNOWN, status);
  }
  return status;
}

void ServicePool::RunThread() {
  while (true) {
    gscoped_ptr<InboundCall> incoming;
    if (!service_queue_.BlockingGet(&incoming)) {
      VLOG(1) << "ServicePool: messenger shutting down.";
      return;
    }

    incoming->RecordHandlingStarted(incoming_queue_time_);
    ADOPT_TRACE(incoming->trace());

    if (PREDICT_FALSE(incoming->ClientTimedOut())) {
      TRACE_TO(incoming->trace(), "Skipping call since client already timed out");
      rpcs_timed_out_in_queue_->Increment();

      // Respond as a failure, even though the client will probably ignore
      // the response anyway.
      incoming->RespondFailure(
        ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
        Status::TimedOut("Call waited in the queue past client deadline"));

      // Must release since RespondFailure above ends up taking ownership
      // of the object.
      ignore_result(incoming.release());
      continue;
    }

    TRACE_TO(incoming->trace(), "Handling call");

    // Release the InboundCall pointer -- when the call is responded to,
    // it will get deleted at that point.
    service_->Handle(incoming.release());
  }
}

} // namespace rpc
} // namespace kudu
