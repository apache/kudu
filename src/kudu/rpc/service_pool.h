// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_SERVICE_POOL_H
#define KUDU_SERVICE_POOL_H

#include <tr1/memory>
#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/mutex.h"
#include "kudu/util/thread.h"
#include "kudu/util/status.h"

namespace kudu {

class Counter;
class Histogram;
class MetricEntity;
class Socket;

namespace rpc {

class Messenger;
class ServiceIf;

// A pool of threads that handle new incoming RPC calls.
// Also includes a queue that calls get pushed onto for handling by the pool.
class ServicePool : public RpcService {
 public:
  ServicePool(gscoped_ptr<ServiceIf> service,
              const scoped_refptr<MetricEntity>& metric_entity,
              size_t service_queue_length);
  virtual ~ServicePool();

  // Start up the thread pool.
  virtual Status Init(int num_threads);

  // Shut down the queue and the thread pool.
  virtual void Shutdown();

  virtual Status QueueInboundCall(gscoped_ptr<InboundCall> call) OVERRIDE;

  const Counter* RpcsTimedOutInQueueMetricForTests() const {
    return rpcs_timed_out_in_queue_.get();
  }

  const Counter* RpcsQueueOverflowMetric() const {
    return rpcs_queue_overflow_.get();
  }

  const std::string service_name() const;

 private:
  void RunThread();
  gscoped_ptr<ServiceIf> service_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;
  BlockingQueue<InboundCall*> service_queue_;
  scoped_refptr<Histogram> incoming_queue_time_;
  scoped_refptr<Counter> rpcs_timed_out_in_queue_;
  scoped_refptr<Counter> rpcs_queue_overflow_;

  mutable Mutex shutdown_lock_;
  bool closing_;

  DISALLOW_COPY_AND_ASSIGN(ServicePool);
};

} // namespace rpc
} // namespace kudu

#endif
