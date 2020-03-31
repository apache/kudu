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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h" // IWYU pragma: keep
#include "kudu/util/test_util.h"

namespace kudu {
namespace postgres {

// Wrapper around Postgres to be used in MiniCluster for services requiring a
// database connection (e.g. Apache Ranger).
class MiniPostgres {
 public:
  explicit MiniPostgres(std::string host)
    : MiniPostgres(GetTestDataDirectory(), std::move(host)) {}

  ~MiniPostgres();

  MiniPostgres(std::string data_root, std::string host)
    : data_root_(std::move(data_root)),
      host_(std::move(host)),
      bin_dir_(GetBinDir()) {}

  Status Start();
  Status Stop();

  // Creates a Postgres user with the specified name. If super is true, the
  // created user will be a superuser, meaning all permission checks will be
  // bypasssed[1].
  //
  // 1. https://www.postgresql.org/docs/12/role-attributes.html
  Status AddUser(const std::string& user, bool super);

  // Creates a database with the specified name. The owner has privileges to
  // remove the database with all objects in it, even if they have different
  // owners[1].
  //
  // 1. https://www.postgresql.org/docs/12/manage-ag-createdb.html
  Status CreateDb(const std::string& db, const std::string& owner);

  uint16_t bound_port() const {
    CHECK_NE(0, port_);
    return port_;
  }

  std::string pg_root() const {
    return JoinPathSegments(data_root_, "postgres");
  }

  std::string pg_bin_dir() const {
    return JoinPathSegments(bin_dir_, "postgres");
  }

 private:
  static std::string GetBinDir() {
    Env* env = Env::Default();
    std::string exe;
    CHECK_OK(env->GetExecutablePath(&exe));
    return DirName(exe);
  }

  // 'pg_root' is the subdirectory in which the Postgres data files will live.
  Status CreateConfigs();

  // Directory in which to put all our stuff.
  const std::string data_root_;
  const std::string host_;

  // Directory that has the Postgres binary.
  // This may be in the thirdparty build, or may be shared across tests. As
  // such, its contents should be treated as read-only.
  const std::string bin_dir_;

  std::unique_ptr<kudu::Subprocess> process_;
  uint16_t port_ = 0;
};

} // namespace postgres
} // namespace kudu
