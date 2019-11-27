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

#include "kudu/util/cloud/instance_detector.h"

#include <algorithm>
#include <limits>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/cloud/instance_metadata.h"
#include "kudu/util/monotime.h"
#include "kudu/util/thread.h"

using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace cloud {

const size_t InstanceDetector::kNoIdx = std::numeric_limits<size_t>::max();

InstanceDetector::InstanceDetector(MonoDelta timeout)
    : timeout_(timeout),
      cv_(&mutex_),
      num_running_detectors_(0),
      result_detector_idx_(kNoIdx) {
  detectors_.push_back(
    { unique_ptr<InstanceMetadata>(new AwsInstanceMetadata), nullptr });
  detectors_.push_back(
    { unique_ptr<InstanceMetadata>(new AzureInstanceMetadata), nullptr });
  detectors_.push_back(
    { unique_ptr<InstanceMetadata>(new GceInstanceMetadata), nullptr });
}

InstanceDetector::~InstanceDetector() {
  for (auto& d : detectors_) {
    if (d.runner) {
      CHECK_OK(ThreadJoiner(d.runner.get()).Join());
    }
  }
}

Status InstanceDetector::Detect(unique_ptr<InstanceMetadata>* metadata) {
  const auto deadline = MonoTime::Now() + timeout_;
  {
    // An extra sanity check.
    MutexLock lock(mutex_);
    CHECK_EQ(0, num_running_detectors_);
    CHECK_EQ(kNoIdx, result_detector_idx_);
    num_running_detectors_ = detectors_.size();
  }

  // Spawn multiple threads: one thread per known type of cloud instance.
  for (auto idx = 0; idx < detectors_.size(); ++idx) {
    auto& d = detectors_[idx];
    CHECK(d.metadata);
    CHECK(!d.runner);
    scoped_refptr<Thread> runner;
    RETURN_NOT_OK(Thread::Create("cloud detector", TypeToString(d.metadata->type()),
        &InstanceDetector::GetInstanceInfo, this, d.metadata.get(), idx, &runner));
    d.runner = std::move(runner);
  }

  // A cloud instance cannot be of many types: the first detector to update
  // the 'result_detector_idx_' field wins the race and breaks the loop.
  // Spurious wakeups are ignored by the virtue of checking the value of the
  // 'result_detector_idx_' field.
  {
    MutexLock lock(mutex_);
    while (result_detector_idx_ == kNoIdx && num_running_detectors_ > 0) {
      if (!cv_.WaitUntil(deadline)) {
        break;
      }
    }
    if (deadline < MonoTime::Now()) {
      return Status::TimedOut(
          "could not retrieve instance metadata before the deadline");
    }
    if (result_detector_idx_ != kNoIdx) {
      CHECK_LT(result_detector_idx_, detectors_.size());
      *metadata = std::move(detectors_[result_detector_idx_].metadata);
      return Status::OK();
    }
  }

  return Status::NotFound("could not retrieve instance metadata");
}

void InstanceDetector::GetInstanceInfo(InstanceMetadata* imd, size_t idx) {
  DCHECK(imd);
  const auto s = imd->Init();
  {
    MutexLock lock(mutex_);
    --num_running_detectors_;
    if (s.ok()) {
      CHECK_EQ(kNoIdx, result_detector_idx_)
          << "conflicting cloud instance types";
      result_detector_idx_ = idx;
    }
  }
  cv_.Signal();
  if (!s.ok()) {
    LOG(WARNING) << Substitute("could not retrieve $0 instance metadata: $1",
                               TypeToString(imd->type()), s.ToString());
  }
}

} // namespace cloud
} // namespace kudu
