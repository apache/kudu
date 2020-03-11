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

#include <memory>
#include <ostream>
#include <string>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/cloud/instance_metadata.h"
#include "kudu/util/status.h"

DECLARE_uint32(cloud_metadata_server_request_timeout_ms);

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace cloud {

// Test the destruction of the object if not running the auto-detection.
TEST(InstanceDetectorTest, NoDetectionRun) {
  InstanceDetector detector;
}

// Basic scenario to verify the functionality of the InstanceDetector::Detect()
// method.
TEST(InstanceDetectorTest, Basic) {
#ifdef THREAD_SANITIZER
  // In case of TSAN builds, it takes longer to spawn threads and overall work
  // with the sanitized version of libcurl while having a lot of concurrent
  // activity. It doesn't make the metadata server taking more time to respond,
  // but it takes longer to process a response: it's often turns into a time-out
  // error even if the server responds in a timely manner.
  FLAGS_cloud_metadata_server_request_timeout_ms = 10000;
#endif
  InstanceDetector detector;
  unique_ptr<InstanceMetadata> metadata;
  const auto s = detector.Detect(&metadata);

  const auto s_aws = AwsInstanceMetadata().Init();
  const auto s_azure = AzureInstanceMetadata().Init();
  const auto s_gce = GceInstanceMetadata().Init();

  if (s_aws.ok() || s_azure.ok() || s_gce.ok()) {
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_NE(nullptr, metadata.get());
    LOG(INFO) << Substitute("detected $0 environment",
                            TypeToString(metadata->type()));
  } else {
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_EQ(nullptr, metadata.get());
    LOG(INFO) << "detected non-cloud environment";
  }

  if (s_aws.ok()) {
    ASSERT_FALSE(s_azure.ok());
    ASSERT_FALSE(s_gce.ok());
    ASSERT_EQ(CloudType::AWS, metadata->type());
  } else if (s_azure.ok()) {
    ASSERT_FALSE(s_aws.ok());
    ASSERT_FALSE(s_gce.ok());
    ASSERT_EQ(CloudType::AZURE, metadata->type());
  } else if (s_gce.ok()) {
    ASSERT_FALSE(s_aws.ok());
    ASSERT_FALSE(s_azure.ok());
    ASSERT_EQ(CloudType::GCE, metadata->type());
  }
}

// If the timeout for auto-detection is set too low, the detector should return
// Status::NotFound(). Even if the timeout is set too low to get a response
// from the metadata servers, the detector has no other choice but to rely
// on the results from the metadata fetchers.
TEST(InstanceDetectorTest, Timeout) {
  // Set very short interval for the timeout.
  FLAGS_cloud_metadata_server_request_timeout_ms = 1;
  InstanceDetector detector;
  unique_ptr<InstanceMetadata> metadata;
  const auto s = detector.Detect(&metadata);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_EQ(nullptr, metadata.get());
}

}  // namespace cloud
}  // namespace kudu
