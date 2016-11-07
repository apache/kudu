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

#include <string>

#include <gtest/gtest.h>

#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/env.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {

TEST(MiniKdcTest, TestBasicOperation) {
  MiniKdcOptions options;
  MiniKdc kdc(options);
  ASSERT_OK(kdc.Start());
  ASSERT_GT(kdc.port(), 0);
  ASSERT_OK(kdc.CreateUserPrincipal("alice"));
  ASSERT_OK(kdc.Kinit("alice"));

  ASSERT_OK(kdc.Stop());
  ASSERT_OK(kdc.Start());

  // Check that alice is kinit'd.
  string klist;
  ASSERT_OK(kdc.Klist(&klist));
  ASSERT_STR_CONTAINS(klist, "alice@KRBTEST.COM");

  ASSERT_OK(kdc.CreateUserPrincipal("bob"));
  ASSERT_OK(kdc.Kinit("bob"));

  // Check that bob has replaced alice as the kinit'd principal.
  ASSERT_OK(kdc.Klist(&klist));
  ASSERT_STR_NOT_CONTAINS(klist, "alice@KRBTEST.COM");
  ASSERT_STR_CONTAINS(klist, "bob@KRBTEST.COM");
  ASSERT_STR_CONTAINS(klist, "krbtgt/KRBTEST.COM@KRBTEST.COM");

  // Drop 'bob' credentials. We'll get a RuntimeError because klist
  // exits with a non-zero exit code if there are no cached credentials.
  ASSERT_OK(kdc.Kdestroy());
  ASSERT_TRUE(kdc.Klist(&klist).IsRuntimeError());

  // Test keytab creation.
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/foo.example.com", &kt_path));
  SCOPED_TRACE(kt_path);
  ASSERT_OK(kdc.KlistKeytab(kt_path, &klist));
  ASSERT_STR_CONTAINS(klist, "kudu/foo.example.com@KRBTEST.COM");
}

// Regression test to ensure that dropping a stopped MiniKdc doesn't panic.
TEST(MiniKdcTest, TestStopDrop) {
  MiniKdcOptions options;
  MiniKdc kdc(options);
}

} // namespace kudu
