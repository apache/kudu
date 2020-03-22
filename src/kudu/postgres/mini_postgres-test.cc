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

#include "kudu/postgres/mini_postgres.h"

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace postgres {
class PostgresTest : public KuduTest {
 public:
  void SetUp() override {
    postgres_.Start();
  }

 protected:
  MiniPostgres postgres_;
};

TEST_F(PostgresTest, TestAddUser) {
  ASSERT_OK((postgres_.AddUser("testuser", false)));
}

TEST_F(PostgresTest, TestCreateDatabaseNonExistentUser) {
  ASSERT_TRUE(postgres_.CreateDb("testdb", "nonexistentuser").IsRuntimeError());
}

TEST_F(PostgresTest, TestCreateDatabaseExistingUserWithPersistence) {
  ASSERT_OK(postgres_.AddUser("testuser", false));
  ASSERT_OK(postgres_.CreateDb("testdb1", "testuser"));
  // Restart Postgres to test persistence. The db and the user still has to
  // exist after the restart, so creating a new database with 'testuser' needs
  // to succeed but creating a new database with the same name must fail.
  // Without persistence it would be the other way around
  ASSERT_OK(postgres_.Stop());
  ASSERT_OK(postgres_.Start());
  ASSERT_OK(postgres_.CreateDb("testdb2", "testuser"));
  ASSERT_TRUE(postgres_.CreateDb("testdb1", "testuser").IsRuntimeError());
}

} // namespace postgres
} // namespace kudu
