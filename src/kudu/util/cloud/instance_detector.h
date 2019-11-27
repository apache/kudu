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
#include <memory>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/cloud/instance_metadata.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

namespace kudu {

namespace cloud {

// Cloud instance detector. Provides an interface to retieve metadata about
// machines/instances run by public cloud vendors.
class InstanceDetector {
 public:
  // Instantiate the detector using the specified timeout for auto-detection of
  // the type of the cloud environment it's run at.
  explicit InstanceDetector(MonoDelta timeout = MonoDelta::FromSeconds(1));

  // The destructor awaits for the detector threads to join (if any spawn).
  virtual ~InstanceDetector();

  // Perform the auto-detection of a cloud instance this object is running at.
  // This method must not be invoked more than once.
  // It's a synchronous call and it can take some time to complete (see
  // the parameters of the constructor to specify timeout for the operation).
  // On success, returns Status::OK() and provides the instance's metadata
  // object via the 'metadata' output parameter. In case of a failure, no
  // valid metadata is provided in the 'metadata' output parameter, and this
  // method returns
  //   * Status::NotFound() if the auto-detection didn't recognize any known
  //                        instance types: the auto-detection was run in a
  //                        non-cloud environment (most likely) or at a VM
  //                        of unknown/not-yet-supported cloud type
  //   * Status::TimedOut() if the specified auto-detection timeout was too
  //                        short to identify at least one known cloud type;
  //                        the auto-detection results should not be trusted
  //
  // TODO(aserbin): do we need async version of this method?
  Status Detect(std::unique_ptr<InstanceMetadata>* metadata) WARN_UNUSED_RESULT;

 private:
  static const size_t kNoIdx;

  // Get metadata for the specified instance. In case of success, store the
  // specified index 'idx' in 'result_detector_idx_' field.
  void GetInstanceInfo(InstanceMetadata* imd, size_t idx);

  const MonoDelta timeout_;

  // Mutex and associated condition variable.
  Mutex mutex_;
  ConditionVariable cv_;

  struct DetectorInfo {
    std::unique_ptr<InstanceMetadata> metadata;
    scoped_refptr<Thread> runner;
  };
  std::vector<DetectorInfo> detectors_;

  // Number of detector threads starting/running.
  size_t num_running_detectors_;

  // The index of the matched detector thread in the detectors_ container;
  // access to the field is guarded by the 'mutex_'.
  size_t result_detector_idx_;
};

} // namespace cloud
} // namespace kudu
