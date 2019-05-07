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
//
// Utility functions which are handy when doing async/callback-based programming.

#pragma once

#include <functional>
#include <memory>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {

// Simple class which can be used to make async methods synchronous.
// For example:
//   Synchronizer s;
//   SomeAsyncMethod(s.AsStatusCallback());
//   CHECK_OK(s.Wait());
//
// The lifetime of the synchronizer is decoupled from the callback it produces.
// If the callback outlives the synchronizer then executing it will be a no-op.
// Callers must be careful not to allow the callback to be destructed without
// completing it, otherwise the thread waiting on the synchronizer will block
// indefinitely.
class Synchronizer {
 public:
  Synchronizer()
      : data_(std::make_shared<Data>()) {
  }

  void StatusCB(const Status& status) {
    Data::Callback(std::weak_ptr<Data>(data_), status);
  }

  StatusCallback AsStatusCallback() {
    return Bind(Data::Callback, std::weak_ptr<Data>(data_));
  }

  StdStatusCallback AsStdStatusCallback() {
    return std::bind(Data::Callback, std::weak_ptr<Data>(data_), std::placeholders::_1);
  }

  Status Wait() const {
    data_->latch.Wait();
    return data_->status;
  }

  Status WaitFor(const MonoDelta& delta) const {
    if (PREDICT_FALSE(!data_->latch.WaitFor(delta))) {
      return Status::TimedOut("timed out while waiting for the callback to be called");
    }
    return data_->status;
  }

  Status WaitUntil(const MonoTime& deadline) const {
    if (PREDICT_FALSE(!data_->latch.WaitUntil(deadline))) {
      return Status::TimedOut("timed out while waiting for the callback to be called");
    }
    return data_->status;
  }

  void Reset() {
    data_->latch.Reset(1);
  }

 private:

  struct Data {
    Data() : latch(1) {
    }

    static void Callback(std::weak_ptr<Data> weak, const Status& status) {
      auto ptr = weak.lock();
      if (ptr) {
        ptr->status = status;
        ptr->latch.CountDown();
      }
    }

    Status status;
    CountDownLatch latch;
    DISALLOW_COPY_AND_ASSIGN(Data);
  };

  std::shared_ptr<Data> data_;
};
} // namespace kudu
