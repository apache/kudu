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

#include "kudu/ranger-kms/mini_ranger_kms.h"

#include <csignal>

#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/postgres/mini_postgres.h"
#include "kudu/ranger-kms/mini_ranger_kms_configs.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/env_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rangerkms {

Status MiniRangerKMS::Start() {
  return StartRangerKMS();
}

MiniRangerKMS::~MiniRangerKMS() {
  WARN_NOT_OK(Stop(), "Failed to stop Ranger KMS");
}

Status MiniRangerKMS::Stop() {
  if (process_) {
    LOG(INFO) << "Stopping Ranger KMS...";
    RETURN_NOT_OK(process_->KillAndWait(SIGTERM));
    LOG(INFO) << "Stopped Ranger KMS";
    process_.reset();
  }
  return mini_ranger_->Stop();
}

Status MiniRangerKMS::InitRangerKMS(const std::string& kms_home, bool *fresh_install) {
  if (env_->FileExists(kms_home)) {
    *fresh_install = false;
    return Status::OK();
  }
  *fresh_install = true;

  RETURN_NOT_OK(env_->CreateDir(kms_home));

  RETURN_NOT_OK(mini_pg_->AddUser("rangerkms", /*super=*/false));
  LOG(INFO) << "Created minirangerkms Postgres user";

  RETURN_NOT_OK(mini_pg_->CreateDb("rangerkms", "rangerkms"));
  LOG(INFO) << "Created rangerkms Postgres database";

  return Status::OK();
}

Status MiniRangerKMS::CreateConfigs(const std::string& conf_dir) {

  if (port_ == 0) {
      RETURN_NOT_OK(GetRandomPort(host_, &port_));
  }

  string kms_home = ranger_kms_home();
  ranger_kms_url_ = Substitute("http://$0:$1", host_, port_);

  // Write config files
  RETURN_NOT_OK(WriteStringToFile(env_,
                                  GetRangerKMSInstallProperties(bin_dir(),
                                                                host_,
                                                                mini_pg_->bound_port(),
                                                                mini_ranger_->admin_url()),
                                  JoinPathSegments(kms_home, "install.properties")));

  RETURN_NOT_OK(WriteStringToFile(env_,
                                  GetRangerKMSSiteXml(host_,
                                                      port_,
                                                      JoinPathSegments(kms_home, "ews/webapp"),
                                                      conf_dir),
                                  JoinPathSegments(kms_home, "ranger-kms-site.xml")));

  RETURN_NOT_OK(WriteStringToFile(env_,
                                  GetRangerKMSDbksSiteXml(host_,
                                                          mini_pg_->bound_port(),
                                                          "postgresql.jar"),
                                  JoinPathSegments(kms_home, "dbks-site.xml")));

  RETURN_NOT_OK(WriteStringToFile(env_,
                                  GetRangerKMSLog4jProperties("info"),
                                  JoinPathSegments(kms_home, "log4j.properties")));

  RETURN_NOT_OK(WriteStringToFile(env_,
                                  GetRangerKMSSecurityXml(mini_ranger_->admin_url(), kms_home),
                                  JoinPathSegments(kms_home, "ranger-kms-security.xml")));

  RETURN_NOT_OK(WriteStringToFile(env_,
                                  GetKMSSiteXml(kerberos_, ktpath_),
                                  JoinPathSegments(kms_home, "kms-site.xml")));

  RETURN_NOT_OK(WriteStringToFile(env_,
                                  GetRangerKMSPolicymgrSSLXml(),
                                  JoinPathSegments(kms_home, "ranger-kms-policymgr-ssl.xml")));

  return Status::OK();
}

Status MiniRangerKMS::DbSetup(const std::string &kms_home, const std::string &ews_dir,
                              const std::string &web_app_dir) {
  RETURN_NOT_OK(env_->CreateDir(ews_dir));
  RETURN_NOT_OK(env_util::CopyDirectory(env_, JoinPathSegments(ranger_kms_home_, "ews/webapp"),
                                        web_app_dir, WritableFileOptions()));
  RETURN_NOT_OK(env_->CreateSymLink(JoinPathSegments(ranger_kms_home_, "jisql"),
                                    JoinPathSegments(kms_home, "jisql")));
  RETURN_NOT_OK(env_->CreateSymLink(JoinPathSegments(ranger_kms_home_, "db"),
                                    JoinPathSegments(kms_home, "db")));
  RETURN_NOT_OK(env_->CreateSymLink(JoinPathSegments(ranger_kms_home_, "cred"),
                                    JoinPathSegments(kms_home, "cred")));

  RETURN_NOT_OK(env_->DeleteRecursively(JoinPathSegments(web_app_dir, "WEB-INF/classes/conf")));
  RETURN_NOT_OK(env_->CreateDir(JoinPathSegments(web_app_dir, "WEB-INF/classes/conf")));

  static const vector<string> files_to_copy = {
    "dbks-site.xml",
    "install.properties"
    "kms-site.xml"
    "log4j.properties",
    "ranger-kms-policymgr-ssl.xml",
    "ranger-kms-security.xml"
  };

  for (const auto& file : files_to_copy) {
    RETURN_NOT_OK(
    env_util::CopyFile(env_,
                       JoinPathSegments(kms_home, file),
                       JoinPathSegments(web_app_dir,
                                        Substitute("WEB-INF/classes/conf/$0", file)),
                       WritableFileOptions()));
  }

  // replace conf files in proc dir from kms home dir
  Subprocess db_setup({ "python", JoinPathSegments(ranger_kms_home_, "db_setup.py")});

  db_setup.SetEnvVars({
    {"RANGER_KMS_HOME", kms_home},
    {"RANGER_KMS_CONF", ranger_kms_home_},
    { "JAVA_HOME", java_home_ },
  });

  db_setup.SetCurrentDir(kms_home);
  RETURN_NOT_OK(db_setup.Start());
  return db_setup.WaitAndCheckExitCode();
}

Status MiniRangerKMS::StartRangerKMS() {
  bool fresh_install;

  if (!mini_ranger_->IsRunning()) {
    return Status::IllegalState("Ranger is not running");
  }

  LOG_TIMING(INFO, "starting Ranger KMS") {
    LOG(INFO) << "Starting Ranger KMS...";
    string exe;
    RETURN_NOT_OK(env_->GetExecutablePath(&exe));
    const string bin_dir = DirName(exe);
    RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home_));
    RETURN_NOT_OK(FindHomeDir("ranger_kms", bin_dir, &ranger_kms_home_));
    RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home_));

    const string kKMSHome = ranger_kms_home();
    // kms_home/ews
    const string kEwsDir = JoinPathSegments(kKMSHome, "ews");
    // kms_home/ews/webapp
    const string kWebAppDir = JoinPathSegments(kEwsDir, "webapp");
    // kms_home/ews/webapp/WEB-INF
    const string kWebInfDir = JoinPathSegments(kWebAppDir, "WEB-INF");
    // kms_home/ews/webapp/WEB-INF/classes
    const string kClassesDir = JoinPathSegments(kWebInfDir, "classes");
    // kms_home/ews/webapp/WEB-INF/classes/conf
    const string kConfDir = JoinPathSegments(kClassesDir, "conf");
    // kms_home/ews/webapp/WEB-INF/classes/lib
    const string kLibDir = JoinPathSegments(kClassesDir, "lib");

    RETURN_NOT_OK(InitRangerKMS(kKMSHome, &fresh_install));

    LOG(INFO) << "Starting Ranger KMS out of " << kKMSHome;
    LOG(INFO) << "Using postgres at " << mini_pg_->host() << ":" << mini_pg_->bound_port();

    RETURN_NOT_OK(CreateConfigs(kConfDir));

    if (fresh_install) {
        RETURN_NOT_OK(DbSetup(kKMSHome, kEwsDir, kWebAppDir));
        RETURN_NOT_OK_PREPEND(CreateKMSService(), "Unable to create KMS Service in Ranger");
    }

    string classpath = ranger_kms_classpath();

    LOG(INFO) << "Using RangerKMS classpath: " << classpath;

    LOG(INFO) << "Using host: " << host_;

    // @todo(zchovan): add link to source
    std::vector<string> args({
      JoinPathSegments(java_home_, "bin/java"),
      "-Dproc_rangerkms",
      Substitute("-Dhostname=$0", host_),
      Substitute("-Dlog4j.configuration=file:$0",
                  JoinPathSegments(kKMSHome, "log4j.properties")),
      "-Duser=minirangerkms",
      "-Dservername=minirangerkms",
      Substitute("-Dcatalina.base=$0", kEwsDir),
      Substitute("-Dkms.config.dir=$0", kConfDir),
      Substitute("-Dlogdir=$0", JoinPathSegments(kKMSHome, "logs")),
    });

    if (kerberos_) {
      args.emplace_back(Substitute("-Djava.security.krb5.conf=$0", krb5_config_));
    }

    args.emplace_back("-cp");
    args.emplace_back(classpath);
    args.emplace_back("org.apache.ranger.server.tomcat.EmbeddedServer");
    args.emplace_back("ranger-kms-site.xml");
    process_.reset(new Subprocess(args));
    process_->SetEnvVars({
         { "JAVA_HOME", java_home_ },
         { "RANGER_KMS_PID_NAME", "rangerkms.pid" },
         { "KMS_CONF_DIR", kKMSHome },
         { "RANGER_USER", "miniranger" },
         { "RANGER_KMS_DIR", ranger_kms_home_},
         { "RANGER_KMS_EWS_DIR", kEwsDir},
         { "RANGER_KMS_EWS_CONF_DIR", kConfDir },
         { "RANGER_KMS_EWS_LIB_DIR", kLibDir }
    });
    RETURN_NOT_OK(process_->Start());
    LOG(INFO) << "Ranger KMS PID: " << process_->pid() << std::endl;
    RETURN_NOT_OK(WaitForTcpBind(process_->pid(),
                                 &port_,
                                 { "0.0.0.0", "127.0.0.1", },
                                 MonoDelta::FromMilliseconds(90000)));
    LOG(INFO) << "Ranger KMS bound to " << port_;
    LOG(INFO) << "Ranger KMS URL: " << ranger_kms_url_;
  }

  return Status::OK();
}

Status MiniRangerKMS::CreateKMSService() {
  string service_name = "kms";
  EasyJson service;
  service.Set("name", service_name);
  service.Set("type", service_name);

  EasyJson configs = service.Set("configs", EasyJson::kObject);
  configs.Set("policy.download.auth.users", service_name);
  configs.Set("tag.download.auth.users", service_name);
  configs.Set("provider", ranger_kms_url_);
  configs.Set("username", "rangerkms");
  configs.Set("password", "rangerkms");

  RETURN_NOT_OK_PREPEND(mini_ranger_->PostToRanger("service/plugins/services", service),
                        Substitute("Failed to create $0 service", service_name));
  LOG(INFO) << Substitute("Created $0 service", service_name);
  return Status::OK();
}

Status MiniRangerKMS::GetKeys() const {
  EasyCurl curl;
  faststring response;

  LOG(INFO) << Substitute("fetching: $0:$1/kms/v1/keys/names", host_, port_) << std::endl;
  RETURN_NOT_OK(curl.FetchURL(Substitute("$0:$1/kms/v1/keys/names", host_, port_), &response));
  LOG(INFO) << "response: ";
  LOG(INFO) << response << std::endl;

  return Status::OK();
}

} // namespace rangerkms
} // namespace kudu
