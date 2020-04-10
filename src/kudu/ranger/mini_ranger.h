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
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/postgres/mini_postgres.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {
class EasyJson;
class Subprocess;

namespace ranger {

// List of usernames to be used in PolicyItem;
typedef std::vector<std::string> UserList;

// Pair of a vector of usernames and a vector of allowed actions to be used in
// AuthorizationPolicy. Number of users and actions doesn't have to match, their
// cross-product is taken.
typedef std::pair<UserList, std::vector<ActionPB>> PolicyItem;

// Policy key used for searching policies_ (values are PolicyItems).
typedef std::tuple<std::vector<std::string>,
                   std::vector<std::string>,
                   std::vector<std::string>> PolicyKey;

// The AuthorizationPolicy contains a set of privileges on a resource to one or
// more users. 'items' is a vector of user-list of actions pair. This struct can
// be used to create new Ranger policies in tests. The policy name is based on
// its contents (list of databases, tables and columns).
struct AuthorizationPolicy {
  std::vector<std::string> databases;
  std::vector<std::string> tables;
  std::vector<std::string> columns;
  std::vector<PolicyItem> items;
};

// Wrapper around Apache Ranger to be used in integration tests.
class MiniRanger {
 public:
  explicit MiniRanger(std::string host)
    : MiniRanger(GetTestDataDirectory(), std::move(host)) {}

  ~MiniRanger();

  MiniRanger(std::string data_root, std::string host)
    : data_root_(std::move(data_root)),
      host_(std::move(host)),
      mini_pg_(data_root_, host_),
      kerberos_(false),
      env_(Env::Default()) {
        curl_.set_auth(CurlAuthType::BASIC, "admin", "admin");
      }

  // Starts Ranger and its dependencies.
  Status Start() WARN_UNUSED_RESULT;

  // Stops Ranger and its dependencies.
  Status Stop() WARN_UNUSED_RESULT;

  // Adds a new policy to Ranger.
  Status AddPolicy(AuthorizationPolicy policy) WARN_UNUSED_RESULT;

  // Creates the client configs files in the given directory.
  Status CreateClientConfig(const std::string& client_config_dir) WARN_UNUSED_RESULT;

  void EnableKerberos(std::string krb5_config,
                      std::string admin_ktpath,
                      std::string lookup_ktpath,
                      std::string spnego_ktpath) {
    kerberos_ = true;
    krb5_config_ = std::move(krb5_config);
    admin_ktpath_ = std::move(admin_ktpath);
    lookup_ktpath_ = std::move(lookup_ktpath);
    spnego_ktpath_ = std::move(spnego_ktpath);
  }

  void set_policy_poll_interval_ms(uint32_t policy_poll_interval_ms) {
    policy_poll_interval_ms_ = policy_poll_interval_ms;
  }

  std::string admin_url() const {
    return ranger_admin_url_;
  }

 private:
  // Starts the Ranger service.
  Status StartRanger() WARN_UNUSED_RESULT;

  // Initializes Ranger within 'admin_home' (home directory of the Ranger
  // admin). Sets 'fresh_install' to true if 'admin_home' didn't exist before
  // calling InitRanger().
  Status InitRanger(std::string admin_home, bool* fresh_install)
    WARN_UNUSED_RESULT;

  // Creates configuration files.
  Status CreateConfigs() WARN_UNUSED_RESULT;

  // Initializes Ranger's database.
  Status DbSetup(const std::string& admin_home, const std::string& ews_dir,
                 const std::string& web_app_dir) WARN_UNUSED_RESULT;

  // Creates a Kudu service in Ranger.
  Status CreateKuduService() WARN_UNUSED_RESULT;

  // Sends a POST request to Ranger with 'payload'.
  Status PostToRanger(std::string url, EasyJson payload) WARN_UNUSED_RESULT;

  // Returns Ranger admin's home directory.
  std::string ranger_admin_home() const {
    return JoinPathSegments(data_root_, "ranger-admin");
  }

  std::string bin_dir() const {
    std::string exe;
    CHECK_OK(env_->GetExecutablePath(&exe));
    return DirName(exe);
  }

  // Returns classpath for Ranger.
  std::string ranger_classpath() const {
    std::string admin_home = ranger_admin_home();
    return strings::Substitute(
        "$0:$1/lib/*:$2/lib/*:$3/*:$4:$5",
        admin_home, JoinPathSegments(ranger_home_, "ews"), java_home_,
        hadoop_home_, JoinPathSegments(bin_dir(), "postgresql.jar"),
        JoinPathSegments(ranger_home_, "ews/webapp"));
  }

  // Directory in which to put all our stuff.
  const std::string data_root_;
  const std::string host_;

  postgres::MiniPostgres mini_pg_;
  std::unique_ptr<Subprocess> process_;

  // URL of the Ranger admin REST API.
  std::string ranger_admin_url_;

  // Locations in which to find Hadoop, Ranger, and Java.
  // These may be in the thirdparty build, or may be shared across tests. As
  // such, their contents should be treated as read-only.
  std::string hadoop_home_;
  std::string ranger_home_;
  std::string java_home_;

  bool kerberos_;
  std::string admin_ktpath_;
  std::string lookup_ktpath_;
  std::string spnego_ktpath_;
  std::string krb5_config_;

  Env* env_;
  EasyCurl curl_;

  uint16_t port_ = 0;

  // Determines how frequently clients fetch policies from the server. The
  // default is 200ms so that tests don't have to wait too long until freshly
  // created policies can be used.
  uint32_t policy_poll_interval_ms_ = 200;

  // Stores existing policies since starting the MiniRanger instance. This is
  // used for adding new policy items (list of users and privileges) to existing
  // policies (resources) as Ranger doesn't support this and we need to delete
  // it and recreate it.
  std::map<PolicyKey, std::vector<PolicyItem>> policies_;
};

} // namespace ranger
} // namespace kudu
