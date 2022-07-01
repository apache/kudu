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

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h" // IWYU pragma: keep
#include "kudu/util/test_util.h"

namespace kudu {
namespace postgres {
class MiniPostgres;
}  // namespace postgres
namespace ranger {
class MiniRanger;
}  // namespace ranger

namespace rangerkms {

class MiniRangerKMS {
 public:
  MiniRangerKMS(std::string host,
                std::shared_ptr<postgres::MiniPostgres> mini_pg,
                std::shared_ptr<ranger::MiniRanger> mini_ranger)
      : MiniRangerKMS(GetTestDataDirectory(),
                      std::move(host),
                      std::move(mini_pg),
                      std::move(mini_ranger)) {}

  ~MiniRangerKMS();

  MiniRangerKMS(std::string data_root, std::string host,
                std::shared_ptr<postgres::MiniPostgres> mini_pg,
                std::shared_ptr<ranger::MiniRanger> mini_ranger)
        : data_root_(std::move(data_root)),
          host_(std::move(host)),
          mini_pg_(std::move(mini_pg)),
          mini_ranger_(std::move(mini_ranger)),
          env_(Env::Default()) {
            curl_.set_auth(CurlAuthType::BASIC, "admin", "admin");
  }

  // Starts Ranger and its dependencies.
  Status Start() WARN_UNUSED_RESULT;

  // Stops Ranger and its dependencies.
  Status Stop() WARN_UNUSED_RESULT;

  Status CreateKMSService();

  Status GetKeys() const;

  Status CreateClusterKey(const std::string& name, std::string* version) WARN_UNUSED_RESULT;


  void EnableKerberos(std::string krb5_config,
                      std::string ktpath,
                      std::string spnego_ktpath) {
    kerberos_ = true;
    krb5_config_ = std::move(krb5_config);
    ktpath_ = std::move(ktpath);
    spnego_ktpath_ = std::move(spnego_ktpath);
  }

  std::string url() const {
    return strings::Substitute("$0:$1/kms", host_, port_);
  }

 private:
  // Starts RangerKMS Service
  Status StartRangerKMS() WARN_UNUSED_RESULT;

  // Initializes Ranger KMS within 'kms_home' (home directory of the Ranger KMS
  // admin). Sets 'fresh_install' to true if 'kms_home' didn't exist before
  // calling InitRangerKMS().
  Status InitRangerKMS(const std::string& kms_home, bool* fresh_install) WARN_UNUSED_RESULT;

  // Creates configuration files.
  // ref:
  // https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.5/bk_security/content/ranger_kms_properties.html
  Status CreateConfigs(const std::string& conf_dir) WARN_UNUSED_RESULT;

  // Initializes Ranger KMS' database.
  Status DbSetup(const std::string& kms_home, const std::string& ews_dir,
                 const std::string& web_app_dir) WARN_UNUSED_RESULT;

  // Returns RangerKMS' home directory.
  std::string ranger_kms_home() const {
    return JoinPathSegments(data_root_, "ranger-kms");
  }

  std::string bin_dir() const {
    std::string exe;
    CHECK_OK(env_->GetExecutablePath(&exe));
    return DirName(exe);
  }

  // Returns classpath for Ranger KMS.
  std::string ranger_kms_classpath() const {
    std::string kms_home = ranger_kms_home();
    // ${RANGER_KMS_CONF_DIR}:
    // ${SQL_CONNECTOR_JAR}:
    // ${RANGER_KMS_HOME}/ews/webapp/WEB-INF/classes/lib/:
    // ${RANGER_KMS_HOME}/ews/webapp/lib/:
    // ${JAVA_HOME}/lib/:
    // ${RANGER_KMS_HADOOP_CONF_DIR}/"
    return strings::Substitute("$0:$1:$2:$3:$4",
                               kms_home,
                               JoinPathSegments(bin_dir(), "postgresql.jar"),
                               JoinPathSegments(ranger_kms_home_,
                                                "ews/webapp/WEB-INF/classes/lib/*"),
                               JoinPathSegments(ranger_kms_home_, "ews/webapp/lib/*"),
                               JoinPathSegments(java_home_, "lib/*"),
                               JoinPathSegments(hadoop_home_, "conf"));
  }
  // Directory in which to put all our stuff.
  const std::string data_root_;
  // Host url for RangerKMS.
  const std::string host_;

  std::shared_ptr<postgres::MiniPostgres> mini_pg_;
  std::shared_ptr<ranger::MiniRanger> mini_ranger_;
  std::unique_ptr<Subprocess> process_;

  // URL of the Ranger KMS REST API.
  std::string ranger_kms_url_;

  // Locations in which to find Hadoop, Ranger, and Java.
  // These may be in the thirdparty build, or may be shared across tests. As
  // such, their contents should be treated as read-only.
  std::string hadoop_home_;
  std::string ranger_kms_home_;
  std::string java_home_;

  bool kerberos_;
  std::string krb5_config_;
  std::string ktpath_;
  std::string spnego_ktpath_;

  Env* env_;
  EasyCurl curl_;

  uint16_t port_ = 0;
};


} // namespace rangerkms
} // namespace kudu
