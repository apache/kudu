// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class Counter;
class Histogram;
class MetricEntity;
class Thread;

namespace rpc {

class InboundCall;
class RemoteMethod;
class ServiceIf;

struct RpcMethodInfo;

// A pool of threads that handle new incoming RPC calls.
// Also includes a queue that calls get pushed onto for handling by the pool.
class ServicePool : public RpcService {
 public:
  ServicePool(std::unique_ptr<ServiceIf> service,
              const scoped_refptr<MetricEntity>& metric_entity,
              size_t service_queue_length);
  virtual ~ServicePool();

  // Set a hook function to be called when any RPC gets rejected because
  // the service queue is full.
  //
  // NOTE: This hook runs on a reactor thread so must execute quickly.
  // Additionally, if a service queue is overflowing, the server is likely
  // under a lot of load, so hooks should be careful to throttle their own
  // execution.
  void set_too_busy_hook(std::function<void(void)> hook) {
    too_busy_hook_ = std::move(hook);
  }

  // Start up the thread pool.
  virtual Status Init(int num_threads);

  // Shut down the queue and the thread pool.
  virtual void Shutdown();

  RpcMethodInfo* LookupMethod(const RemoteMethod& method) override;

  virtual Status QueueInboundCall(std::unique_ptr<InboundCall> call) OVERRIDE;

  const Counter* RpcsTimedOutInQueueMetricForTests() const {
    return rpcs_timed_out_in_queue_.get();
  }

  const Histogram* IncomingQueueTimeMetricForTests() const {
    return incoming_queue_time_.get();
  }

  const Counter* RpcsQueueOverflowMetric() const {
    return rpcs_queue_overflow_.get();
  }

  const std::string service_name() const;

 private:
  void RunThread();
  void RejectTooBusy(InboundCall* c);

  std::unique_ptr<ServiceIf> service_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;
  LifoServiceQueue service_queue_;
  scoped_refptr<Histogram> incoming_queue_time_;
  scoped_refptr<Counter> rpcs_timed_out_in_queue_;
  scoped_refptr<Counter> rpcs_queue_overflow_;

  mutable Mutex shutdown_lock_;
  bool closing_;

  std::function<void(void)> too_busy_hook_;

  DISALLOW_COPY_AND_ASSIGN(ServicePool);
};

} // namespace rpc
} // namespace kudu
