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

#include "kudu/hms/mini_hms.h"

#include <algorithm>
#include <csignal>
#include <map>
#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using kudu::rpc::SaslProtection;
using std::map;
using std::string;
using std::unique_ptr;
using strings::Substitute;

static constexpr int kHmsStartTimeoutMs = 60000;

namespace kudu {
namespace hms {

MiniHms::MiniHms() {
}

MiniHms::~MiniHms() {
  WARN_NOT_OK(Stop(), "Failed to stop MiniHms");
}

void MiniHms::SetNotificationLogTtl(MonoDelta ttl) {
  CHECK(hms_process_);
  notification_log_ttl_ = ttl;
}

void MiniHms::EnableKerberos(string krb5_conf,
                             string service_principal,
                             string keytab_file,
                             SaslProtection::Type protection) {
  CHECK(!hms_process_);
  CHECK(!krb5_conf.empty());
  CHECK(!service_principal.empty());
  CHECK(!keytab_file.empty());
  krb5_conf_ = std::move(krb5_conf);
  service_principal_ = std::move(service_principal);
  keytab_file_ = std::move(keytab_file);
  protection_ = protection;
}

void MiniHms::SetDataRoot(string data_root) {
  data_root_ = std::move(data_root);
}

Status MiniHms::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kHmsStartTimeoutMs / 2, "Starting HMS");
  CHECK(!hms_process_);

  VLOG(1) << "Starting HMS";

  Env* env = Env::Default();

  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);

  string hadoop_home;
  string hive_home;
  string java_home;
  RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home));
  RETURN_NOT_OK(FindHomeDir("hive", bin_dir, &hive_home));
  RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));

  if (data_root_.empty()) {
    data_root_ = GetTestDataDirectory();
  }

  RETURN_NOT_OK(CreateHiveSite());
  RETURN_NOT_OK(CreateCoreSite());
  RETURN_NOT_OK(CreateLogConfig());

  // Comma-separated list of additional jars to add to the HMS classpath.
  string aux_jars = Substitute("$0/hms-plugin.jar,$1/hcatalog/share/hcatalog/*",
                               bin_dir, hive_home);

  // List of JVM environment options to pass to the HMS.
  string java_options =
    // Ensure IPv4 is used.
    "-Djava.net.preferIPv4Stack=true "
    // Tune down the Derby deadlock timeout. The HMS's use of Derby with the
    // NOTIFICATION_SEQUENCE table is prone to deadlocks, at which point Derby
    // cancels a conflicting transaction after waiting out the timeout. This
    // typically doesn't cause issues since the HMS auto retries these
    // transactions, however the default period of 20 seconds causes tests to
    // timeout.
    "-Dderby.locks.deadlockTimeout=1";

  if (!krb5_conf_.empty()) {
    java_options += Substitute(" -Djava.security.krb5.conf=$0", krb5_conf_);
  }

  map<string, string> env_vars {
      { "JAVA_HOME", java_home },
      { "HADOOP_HOME", hadoop_home },
      { "HIVE_AUX_JARS_PATH", aux_jars },
      { "HIVE_CONF_DIR", data_root_ },
      { "JAVA_TOOL_OPTIONS", java_options },
      { "HADOOP_CONF_DIR", data_root_ },
  };

  // Start the HMS.
  hms_process_.reset(new Subprocess({
        Substitute("$0/bin/hive", hive_home),
        "--service", "metastore",
        "-v",
        "-p", std::to_string(port_),
  }));

  hms_process_->SetEnvVars(env_vars);
  RETURN_NOT_OK(hms_process_->Start());

  // Wait for HMS to start listening on its ports and commencing operation.
  VLOG(1) << "Waiting for HMS ports";
  Status wait = WaitForTcpBind(hms_process_->pid(), &port_,
                               MonoDelta::FromMilliseconds(kHmsStartTimeoutMs));
  if (!wait.ok()) {
    WARN_NOT_OK(hms_process_->Kill(SIGQUIT), "failed to send SIGQUIT to HMS");
  }
  return wait;
}

Status MiniHms::Stop() {
  if (hms_process_) {
    VLOG(1) << "Stopping HMS";
    unique_ptr<Subprocess> proc = std::move(hms_process_);
    RETURN_NOT_OK_PREPEND(proc->KillAndWait(SIGTERM), "failed to stop the Hive Metastore process");
  }
  return Status::OK();
}

Status MiniHms::Pause() {
  CHECK(hms_process_);
  VLOG(1) << "Pausing HMS";
  RETURN_NOT_OK_PREPEND(hms_process_->Kill(SIGSTOP),
                        "failed to pause the Hive Metastore process");
  return Status::OK();
}

Status MiniHms::Resume() {
  CHECK(hms_process_);
  VLOG(1) << "Resuming HMS";
  RETURN_NOT_OK_PREPEND(hms_process_->Kill(SIGCONT),
                        "failed to unpause the Hive Metastore process");
  return Status::OK();
}

string MiniHms::uris() const {
  return Substitute("thrift://127.0.0.1:$0", port_);
}

Status MiniHms::CreateHiveSite() const {

  // - datanucleus.schema.autoCreateAll
  // - hive.metastore.schema.verification
  //     Allow Hive to startup and run without first running the schemaTool.
  //
  // - hive.metastore.event.db.listener.timetolive
  //     Configures how long the Metastore will store notification log events
  //     before GCing them.
  //
  // - hive.metastore.sasl.enabled
  // - hive.metastore.kerberos.keytab.file
  // - hive.metastore.kerberos.principal
  //     Configures the HMS to use Kerberos for its Thrift RPC interface.
  //
  // - hive.metastore.disallow.incompatible.col.type.changes
  //     Configures the HMS to allow altering and dropping columns.
  //
  // - hive.support.special.characters.tablename
  //     Configures the HMS to allow special characters such as '/' in table
  //     names.
  //
  // - hive.metastore.notifications.add.thrift.objects
  //     Configured the HMS to add the entire thrift Table/Partition
  //     objects to the HMS notifications.
  static const string kFileTemplate = R"(
<configuration>
  <property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>
      org.apache.hive.hcatalog.listener.DbNotificationListener,
      org.apache.kudu.hive.metastore.KuduMetastorePlugin
    </value>
  </property>

  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>file://$1/warehouse/</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:$1/metadb;create=true</value>
  </property>

  <property>
    <name>hive.metastore.event.db.listener.timetolive</name>
    <value>$0s</value>
  </property>

  <property>
    <name>hive.metastore.sasl.enabled</name>
    <value>$2</value>
  </property>

  <property>
    <name>hive.metastore.kerberos.keytab.file</name>
    <value>$3</value>
  </property>

  <property>
    <name>hive.metastore.kerberos.principal</name>
    <value>$4</value>
  </property>

  <property>
    <name>hadoop.rpc.protection</name>
    <value>$5</value>
  </property>

  <property>
    <name>hive.metastore.disallow.incompatible.col.type.changes</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.support.special.characters.tablename</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.metastore.notifications.add.thrift.objects</name>
    <value>true</value>
  </property>
</configuration>
  )";

  string file_contents = Substitute(kFileTemplate,
                                    notification_log_ttl_.ToSeconds(),
                                    data_root_,
                                    !keytab_file_.empty(),
                                    keytab_file_,
                                    service_principal_,
                                    SaslProtection::name_of(protection_));

  return WriteStringToFile(Env::Default(),
                           file_contents,
                           JoinPathSegments(data_root_, "hive-site.xml"));
}

Status MiniHms::CreateCoreSite() const {

  // - hadoop.security.authentication
  //     The HMS uses Hadoop's UGI contraption which will refuse to login a user
  //     with Kerberos unless this special property is set. The property must
  //     not be in hive-site.xml because a new Configuration object is created
  //     to search for the property, and it only checks places Hadoop knows
  //     about.

  static const string kFileTemplate = R"(
<configuration>
  <property>
    <name>hadoop.security.authentication</name>
    <value>$0</value>
  </property>
</configuration>
  )";

  string file_contents = Substitute(kFileTemplate, keytab_file_.empty() ? "simple" : "kerberos");

  return WriteStringToFile(Env::Default(),
                           file_contents,
                           JoinPathSegments(data_root_, "core-site.xml"));
}

Status MiniHms::CreateLogConfig() const {
  // Configure the HMS to output ERROR messages to the stderr console, and INFO
  // and above to hms.log in the data root. The console messages have a special
  // 'HMS' tag included to disambiguate them from other Java component logs. The
  // HMS automatically looks for a logging configuration named
  // 'hive-log4j2.properties' in the configured HIVE_CONF_DIR.
  static const string kFileTemplate = R"(
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss.SSS} [HMS - %p - %t] (%F:%L) %m%n
appender.console.filter.threshold.type = ThresholdFilter
appender.console.filter.threshold.level = ERROR

appender.file.type = File
appender.file.name = file
appender.file.fileName = $0
appender.file.layout.type = PatternLayout
appender.file.layout.pattern = %d{HH:mm:ss.SSS} [%p - %t] (%F:%L) %m%n

rootLogger.level = INFO
rootLogger.appenderRefs = console, file
rootLogger.appenderRef.console.ref = console
rootLogger.appenderRef.file.ref = file
  )";

  string file_contents = Substitute(kFileTemplate, JoinPathSegments(data_root_, "hms.log"));

  return WriteStringToFile(Env::Default(),
                           file_contents,
                           JoinPathSegments(data_root_, "hive-log4j2.properties"));
}
} // namespace hms
} // namespace kudu
