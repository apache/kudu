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

#include <boost/optional/optional.hpp>
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

using boost::none;
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

void MiniHms::EnableSentry(const HostPort& sentry_address,
                           string sentry_service_principal,
                           int sentry_client_rpc_retry_num,
                           int sentry_client_rpc_retry_interval_ms) {
  CHECK(!hms_process_);
  DCHECK(!sentry_service_principal.empty());
  VLOG(1) << Substitute("Enabling Sentry, at $0, for HMS", sentry_address.ToString());
  sentry_address_ = sentry_address.ToString();
  sentry_service_principal_ = std::move(sentry_service_principal);
  sentry_client_rpc_retry_num_ = sentry_client_rpc_retry_num;
  sentry_client_rpc_retry_interval_ms_ = sentry_client_rpc_retry_interval_ms;
}

void MiniHms::EnableKuduPlugin(bool enable) {
  enable_kudu_plugin_ = enable;
}

void MiniHms::SetDataRoot(string data_root) {
  CHECK(!hms_process_);
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
  string sentry_home;
  RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home));
  RETURN_NOT_OK(FindHomeDir("hive", bin_dir, &hive_home));
  RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));
  RETURN_NOT_OK(FindHomeDir("sentry", bin_dir, &sentry_home));

  if (data_root_.empty()) {
    data_root_ = GetTestDataDirectory();
  }

  RETURN_NOT_OK(CreateHiveSite());
  RETURN_NOT_OK(CreateCoreSite());
  RETURN_NOT_OK(CreateLogConfig());

  // Comma-separated list of additional jars to add to the HMS classpath, including
  // the HMS plugins of Kudu and Sentry.
  string aux_jars = Substitute("$0/hms-plugin.jar,$1/hcatalog/share/hcatalog/*,$2/lib/*",
                               bin_dir, hive_home, sentry_home);

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

  // By default, Derby will create its log file in the current working
  // directory. We want it in data_root_.
  java_options += Substitute(" -Dderby.stream.error.file=$0/derby.log", data_root_);

  if (!krb5_conf_.empty()) {
    java_options += Substitute(" -Djava.security.krb5.conf=$0", krb5_conf_);
  }

  map<string, string> env_vars {
      { "JAVA_HOME", java_home },
      { "HADOOP_HOME", hadoop_home },
      { "HADOOP_CONF_DIR",  Substitute("$0/etc/hadoop", hadoop_home) },
      { "HIVE_AUX_JARS_PATH", aux_jars },
      { "HIVE_CONF_DIR", data_root_ },
      { "JAVA_TOOL_OPTIONS", java_options },
      // Set HADOOP_OS_TYPE=Linux due to HADOOP-8719.
      // TODO(ghenke): Remove after HADOOP-15966 is available (Hadoop 3.1.3+)
      { "HADOOP_OS_TYPE", "Linux" }
  };

  if (!schema_initialized_) {
    // Run the schematool to initialize the database.
    RETURN_NOT_OK(Subprocess::Call({Substitute("$0/bin/schematool", hive_home),
                                    "-dbType", "derby", "-initSchema"}, "",
                                   nullptr, nullptr,
                                   env_vars));
    schema_initialized_ = true;
  }

  // Start the HMS.
  hms_process_.reset(new Subprocess({
        Substitute("$0/bin/hive", hive_home),
        "--service", "metastore",
        "-v",
        "-p", std::to_string(port_),
  }));

  hms_process_->SetEnvVars(env_vars);
  RETURN_NOT_OK(hms_process_->Start());

  // Wait for HMS to start listening on its ports and commencing operation
  // with a wildcard binding.
  VLOG(1) << "Waiting for HMS ports";
  Status wait = WaitForTcpBind(hms_process_->pid(), &port_, /*addr=*/none,
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

bool MiniHms::IsAuthorizationEnabled() const {
  return !sentry_address_.empty() && IsKerberosEnabled();
}

Status MiniHms::CreateHiveSite() const {

  const string listeners = Substitute("org.apache.hive.hcatalog.listener.DbNotificationListener$0",
      enable_kudu_plugin_ ? ",org.apache.kudu.hive.metastore.KuduMetastorePlugin" : "");

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
  //     Configures the HMS to add the entire thrift Table/Partition
  //     objects to the HMS notifications.
  //
  // - hive.metastore.event.db.notification.api.auth
  //     Disables the authorization on the DbNotificationListener related
  //     metastore APIs such as get_next_notification. If set to true, then
  //     only the superusers in proxy settings have the permission.
  //
  // - hive.log4j.file
  //     Configures the location of the HMS log4j configuration.
  //
  static const string kHiveFileTemplate = R"(
<configuration>
  <property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>
      $0
    </value>
  </property>

  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>file://$2/warehouse/</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:$2/metadb;create=true</value>
  </property>

  <property>
    <name>hive.metastore.event.db.listener.timetolive</name>
    <value>$1s</value>
  </property>

  <property>
    <name>hive.metastore.sasl.enabled</name>
    <value>$3</value>
  </property>

  <property>
    <name>hive.metastore.kerberos.keytab.file</name>
    <value>$4</value>
  </property>

  <property>
    <name>hive.metastore.kerberos.principal</name>
    <value>$5</value>
  </property>

  <property>
    <name>hadoop.rpc.protection</name>
    <value>$6</value>
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

  <property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.log4j.file</name>
    <value>$7</value>
  </property>

  $8
</configuration>
  )";

  string sentry_properties;
  if (IsAuthorizationEnabled()) {

    // - hive.sentry.conf.url
    //     Configuration URL of the Sentry authorization plugin in the HMS.
    //
    // - hive.metastore.filter.hook
    //     Configures the HMS to use the Sentry plugin for filtering
    //     out information user has no privileges to access for operations
    //     as SHOWTABLES and SHOWDATABASES.
    //
    // - hive.metastore.pre.event.listeners
    //     Configures the HMS to use the Sentry event listener to
    //     consult Sentry service for authorization metadata when servicing
    //     requests.
    //
    // - hive.metastore.event.listeners
    //     Configures the HMS to use the Sentry post-event listener, which
    //     synchronizes the HMS events with the Sentry service. The Sentry
    //     service will be made aware of events like table renames and
    //     update itself accordingly.
    static const string kHiveSentryFileTemplate = R"(
<property>
  <name>hive.sentry.conf.url</name>
  <value>file://$0/hive-sentry-site.xml</value>
</property>

<property>
  <name>hive.metastore.filter.hook</name>
  <value>org.apache.sentry.binding.metastore.SentryMetaStoreFilterHook</value>
</property>

<property>
  <name>hive.metastore.pre.event.listeners</name>
  <value>org.apache.sentry.binding.metastore.MetastoreAuthzBinding</value>
</property>

<property>
  <name>hive.metastore.event.listeners</name>
  <value>org.apache.sentry.binding.metastore.SentrySyncHMSNotificationsPostEventListener</value>
</property>
    )";

    sentry_properties = Substitute(kHiveSentryFileTemplate, data_root_);
  }

  string hive_file_contents = Substitute(kHiveFileTemplate,
                                         listeners,
                                         notification_log_ttl_.ToSeconds(),
                                         data_root_,
                                         IsKerberosEnabled(),
                                         keytab_file_,
                                         service_principal_,
                                         SaslProtection::name_of(protection_),
                                         JoinPathSegments(data_root_, "hive-log4j2.properties"),
                                         sentry_properties);

  if (IsAuthorizationEnabled()) {
    // - hive.sentry.server
    //     Server namespace the HMS instance belongs to for defining
    //     server-level privileges in Sentry.
    //
    // - sentry.metastore.service.users
    //     Set of service users whose access will be excluded from
    //     Sentry authorization checks.
    //
    // - sentry.service.client.rpc.retry-total
    //     Maximum number of attempts that Sentry RPC client does while
    //     re-trying a remote call to Sentry.
    //
    // - sentry.service.client.rpc.retry.interval.msec
    //     Time interval between attempts of Sentry's client to retry a remote
    //     call to Sentry.
    static const string kSentryFileTemplate = R"(
<configuration>
  <property>
    <name>sentry.service.client.server.rpc-addresses</name>
    <value>$0</value>
  </property>

  <property>
    <name>sentry.service.server.principal</name>
    <value>$1</value>
  </property>

  <property>
    <name>hive.sentry.server</name>
    <value>$2</value>
  </property>

  <property>
    <name>sentry.metastore.service.users</name>
    <value>kudu</value>
  </property>

  <property>
    <name>sentry.service.client.rpc.retry-total</name>
    <value>$3</value>
  </property>

  <property>
    <name>sentry.service.client.rpc.retry.interval.msec</name>
    <value>$4</value>
  </property>
</configuration>
  )";
    auto sentry_file_contents = Substitute(
        kSentryFileTemplate,
        sentry_address_,
        sentry_service_principal_,
        "server1",
        sentry_client_rpc_retry_num_,
        sentry_client_rpc_retry_interval_ms_);
    RETURN_NOT_OK(WriteStringToFile(Env::Default(),
                                    sentry_file_contents,
                                    JoinPathSegments(data_root_, "hive-sentry-site.xml")));
  }

  return WriteStringToFile(Env::Default(),
                           hive_file_contents,
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

  string file_contents = Substitute(kFileTemplate, IsKerberosEnabled() ? "kerberos" : "simple");

  return WriteStringToFile(Env::Default(),
                           file_contents,
                           JoinPathSegments(data_root_, "core-site.xml"));
}

Status MiniHms::CreateLogConfig() const {
  // Configure the HMS to output ERROR messages to the stderr console, and INFO
  // and above to hms.log in the data root. The console messages have a special
  // 'HMS' tag included to disambiguate them from other Java component logs.
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
