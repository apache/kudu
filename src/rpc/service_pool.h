// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_SERVICE_POOL_H
#define KUDU_SERVICE_POOL_H

#include <tr1/memory>
#include <vector>

#include "gutil/macros.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "rpc/rpc_service.h"
#include "util/blocking_queue.h"
#include "util/thread.h"
#include "util/status.h"

namespace kudu {

class Counter;
class Histogram;
class MetricContext;
class Socket;

namespace rpc {

class Messenger;
class ServiceIf;

// A pool of threads that handle new incoming RPC calls.
// Also includes a queue that calls get pushed onto for handling by the pool.
class ServicePool : public RpcService {
 public:
  ServicePool(gscoped_ptr<ServiceIf> service,
              const MetricContext& metric_ctx,
              size_t service_queue_length);
  virtual ~ServicePool();

  // Start up the thread pool.
  virtual Status Init(int num_threads);

  // Shut down the queue and the thread pool.
  virtual void Shutdown();

  virtual Status QueueInboundCall(gscoped_ptr<InboundCall> call) OVERRIDE;

  const Counter* RpcsTimedOutInQueueMetricForTests() const {
    return rpcs_timed_out_in_queue_;
  }

 private:
  void RunThread();
  gscoped_ptr<ServiceIf> service_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;
  BlockingQueue<InboundCall*> service_queue_;
  Histogram* incoming_queue_time_;
  Counter* rpcs_timed_out_in_queue_;

  mutable boost::mutex shutdown_lock_;
  bool closing_;

  DISALLOW_COPY_AND_ASSIGN(ServicePool);
};

} // namespace rpc
} // namespace kudu

#endif
