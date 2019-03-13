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

#include "kudu/master/ts_descriptor.h"

#include <memory>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_macros.h"

using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

// Configures 'instance' and 'registration' with basic info that can be
// used to register a tablet server.
void SetupBasicRegistrationInfo(const string& uuid,
                                NodeInstancePB* instance,
                                ServerRegistrationPB* registration) {
  CHECK_NOTNULL(instance);
  CHECK_NOTNULL(registration);

  instance->set_permanent_uuid(uuid);
  instance->set_instance_seqno(0);

  registration->clear_rpc_addresses();
  for (const auto port : { 12345, 67890 }) {
    auto* rpc_hostport = registration->add_rpc_addresses();
    rpc_hostport->set_host("localhost");
    rpc_hostport->set_port(port);
  }
  registration->clear_http_addresses();
  auto* http_hostport = registration->add_http_addresses();
  http_hostport->set_host("localhost");
  http_hostport->set_port(54321);
  registration->set_software_version("1.0.0");
  registration->set_https_enabled(false);
  registration->set_start_time(10000);
}

TEST(TSDescriptorTest, TestRegistration) {
  const string uuid = "test";
  NodeInstancePB instance;
  ServerRegistrationPB registration;
  SetupBasicRegistrationInfo(uuid, &instance, &registration);
  shared_ptr<TSDescriptor> desc;
  ASSERT_OK(TSDescriptor::RegisterNew(instance, registration, {}, &desc));

  // Spot check some fields and the ToString value.
  ASSERT_EQ(uuid, desc->permanent_uuid());
  ASSERT_EQ(0, desc->latest_seqno());
  // There is no location as --location_mapping_cmd is unset by default.
  ASSERT_EQ(boost::none, desc->location());
  ASSERT_EQ("test (localhost:12345)", desc->ToString());
}

TEST(TSDescriptorTest, TestLocationCmd) {
  // A happy case, using all allowed special characters.
  const string location = "/foo-bar0/BAAZ._9-quux";
  const string uuid = "test";
  NodeInstancePB instance;
  ServerRegistrationPB registration;
  SetupBasicRegistrationInfo(uuid, &instance, &registration);
  shared_ptr<TSDescriptor> desc;
  ASSERT_OK(TSDescriptor::RegisterNew(instance, registration, location, &desc));
  ASSERT_EQ(location, desc->location());
}
} // namespace master
} // namespace kudu
