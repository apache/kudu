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

#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/thrift/client.h"
#include "kudu/util/test_macros.h"

namespace kudu {

using client::KuduColumnSchema;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduTableCreator;
using cluster::ExternalMiniClusterOptions;
using hms::HmsClient;
using std::string;
using std::unique_ptr;
using strings::Substitute;

// Test Master authorization enforcement with Sentry and HMS
// integration enabled.
class MasterSentryTest : public ExternalMiniClusterITestBase,
                         public ::testing::WithParamInterface<bool> {
 public:

  void SetUp() override {
    ExternalMiniClusterITestBase::SetUp();
    ExternalMiniClusterOptions opts;
    opts.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
    opts.enable_sentry = true;
    opts.enable_kerberos = enable_kerberos_;
    StartClusterWithOpts(std::move(opts));
  }

 protected:
    const bool enable_kerberos_ = GetParam();
};
INSTANTIATE_TEST_CASE_P(KerberosEnabled, MasterSentryTest, ::testing::Bool());

// TODO(hao): Write a proper test when master authorization enforcement is done.
TEST_P(MasterSentryTest, TestFoo) {
  const string kDatabaseName = "default";
  const string kTableName = "foo";
  const string kTableIdentifier = Substitute("$0.$1", kDatabaseName, kTableName);

  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("val")->Type(KuduColumnSchema::INT32);
  ASSERT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableIdentifier)
                           .schema(&schema)
                           .num_replicas(1)
                           .set_range_partition_columns({ "key" })
                           .Create());

  // Verify the table exists in Kudu and the HMS.

  bool exists;
  ASSERT_OK(client_->TableExists(kTableIdentifier, &exists));
  ASSERT_TRUE(exists);

  thrift::ClientOptions hms_client_opts;
  if (enable_kerberos_) {
    // Create a principal 'kudu' and configure to use it.
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal("kudu"));
    ASSERT_OK(cluster_->kdc()->Kinit("kudu"));
    ASSERT_OK(cluster_->kdc()->SetKrb5Environment());
    hms_client_opts.enable_kerberos = true;
    hms_client_opts.service_principal = "hive";
  }
  hms::HmsClient hms_client(cluster_->hms()->address(), hms_client_opts);
  ASSERT_OK(hms_client.Start());

  hive::Table hms_table;
  ASSERT_OK(hms_client.GetTable(kDatabaseName, kTableName, &hms_table));
}

} // namespace kudu
