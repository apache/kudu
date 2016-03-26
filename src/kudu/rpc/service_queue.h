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
#ifndef KUDU_UTIL_SERVICE_QUEUE_H
#define KUDU_UTIL_SERVICE_QUEUE_H

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <set>

#include "kudu/rpc/inbound_call.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/mutex.h"

namespace kudu {
namespace rpc {

// Blocking queue used for passing inbound RPC calls to the service handler pool.
// Calls are dequeued in 'earliest-deadline first' order. The queue also maintains a
// bounded number of calls. If the queue overflows, then calls with deadlines farthest
// in the future are evicted.
//
// When calls do not provide deadlines, the RPC layer considers their deadline to
// be infinitely in the future. This means that any call that does have a deadline
// can evict any call that does not have a deadline. This incentivizes clients to
// provide accurate deadlines for their calls.
class ServiceQueue {
 public:
  // Return values for ServiceQueue::Put()
  enum QueueStatus {
    QUEUE_SUCCESS = 0,
    QUEUE_SHUTDOWN = 1,
    QUEUE_FULL = 2
  };

  explicit ServiceQueue(int max_size)
      : shutdown_(false),
        max_queue_size_(max_size),
        not_empty_(&lock_) {
  }

  ~ServiceQueue() {
    DCHECK(queue_.empty())
        << "ServiceQueue holds bare pointers at destruction time";
  }


  // Get an element from the queue.  Returns false if we were shut down prior to
  // getting the element.
  bool BlockingGet(std::unique_ptr<InboundCall> *out) {
    MutexLock l(lock_);
    while (true) {
      if (!queue_.empty()) {
        auto it = queue_.begin();
        out->reset(*it);
        queue_.erase(it);
        return true;
      }
      if (shutdown_) {
        return false;
      }
      not_empty_.Wait();
    }
  }

  // Add a new call to the queue.
  // Returns:
  // - QUEUE_SHUTDOWN if Shutdown() has already been called.
  // - QUEUE_FULL if the queue is full and 'call' has a later deadline than any
  //   RPC already in the queue.
  // - QUEUE_SUCCESS if 'call' was enqueued.
  //
  // In the case of a 'QUEUE_SUCCESS' response, the new element may have bumped
  // another call out of the queue. In that case, *evicted will be set to the
  // call that was bumped.
  QueueStatus Put(InboundCall* call, boost::optional<InboundCall*>* evicted) {
    MutexLock l(lock_);
    if (shutdown_) {
      return QUEUE_SHUTDOWN;
    }

    if (queue_.size() >= max_queue_size_) {
      DCHECK_EQ(queue_.size(), max_queue_size_);
      auto it = queue_.end();
      --it;
      if (DeadlineLess(*it, call)) {
        return QUEUE_FULL;
      }

      *evicted = *it;
      queue_.erase(it);
    }

    queue_.insert(call);

    l.Unlock();
    not_empty_.Signal();
    return QUEUE_SUCCESS;
  }

  // Shut down the queue.
  // When a blocking queue is shut down, no more elements can be added to it,
  // and Put() will return QUEUE_SHUTDOWN.
  // Existing elements will drain out of it, and then BlockingGet will start
  // returning false.
  void Shutdown() {
    MutexLock l(lock_);
    shutdown_ = true;
    not_empty_.Broadcast();
  }

  bool empty() const {
    MutexLock l(lock_);
    return queue_.empty();
  }

  int max_size() const {
    return max_queue_size_;
  }

  std::string ToString() const {
    std::string ret;

    MutexLock l(lock_);
    for (const auto* t : queue_) {
      ret.append(t->ToString());
      ret.append("\n");
    }
    return ret;
  }

 private:
  // Comparison function which orders calls by their deadlines.
  static bool DeadlineLess(const InboundCall* a,
                           const InboundCall* b) {
    auto time_a = a->GetClientDeadline();
    auto time_b = b->GetClientDeadline();
    if (time_a.Equals(time_b)) {
      // If two calls have the same deadline (most likely because neither one specified
      // one) then we should order them by arrival order.
      time_a = a->GetTimeReceived();
      time_b = b->GetTimeReceived();
    }
    return time_a.ComesBefore(time_b);
  }

  // Struct functor wrapper for DeadlineLess.
  struct DeadlineLessStruct {
    bool operator()(const InboundCall* a, const InboundCall* b) const {
      return DeadlineLess(a, b);
    }
  };

  bool shutdown_;
  int max_queue_size_;
  mutable Mutex lock_;
  ConditionVariable not_empty_;
  std::multiset<InboundCall*, DeadlineLessStruct> queue_;
};

} // namespace rpc
} // namespace kudu

#endif
