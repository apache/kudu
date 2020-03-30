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

#include <string>

#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace ranger {

// Gets the contents of the install.properties file used by the db_setup.py
// script.
inline std::string GetRangerInstallProperties(const std::string& bin_dir,
                                              const std::string& pg_host,
                                              uint16_t pg_port) {
  // Taken and modified from:
  // https://github.com/apache/ranger/blob/master/security-admin/scripts/install.properties
  //
  // $0: directory containing postgresql.jar
  // $1: postgres host
  // $2: postgres port
  const char* kInstallProperties = R"(
DB_FLAVOR=POSTGRES
SQL_CONNECTOR_JAR=$0/postgresql.jar
db_root_user=postgres
db_host=$1:$2
db_root_password=
db_ssl_enabled=false
db_ssl_required=false
db_ssl_verifyServerCertificate=false
db_name=ranger
db_user=miniranger
db_password=
rangerAdmin_password=admin
rangerTagsync_password=admin
rangerUsersync_password=admin
keyadmin_password=admin
mysql_core_file=db/mysql/optimized/current/ranger_core_db_mysql.sql
oracle_core_file=db/oracle/optimized/current/ranger_core_db_oracle.sql
postgres_core_file=db/postgres/optimized/current/ranger_core_db_postgres.sql
sqlserver_core_file=db/sqlserver/optimized/current/ranger_core_db_sqlserver.sql
sqlanywhere_core_file=db/sqlanywhere/optimized/current/ranger_core_db_sqlanywhere.sql
)";
  return strings::Substitute(kInstallProperties, bin_dir, pg_host, pg_port);
}

// Gets the contents of the ranger-admin-site.xml config that has most of the
// configuration needed to start Ranger.
inline std::string GetRangerAdminSiteXml(const std::string& admin_host,
                                         uint16_t admin_port,
                                         const std::string& pg_host,
                                         uint16_t pg_port,
                                         const std::string& admin_keytab,
                                         const std::string& lookup_keytab,
                                         const std::string& spnego_keytab) {
  // For port info, see:
  // https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.5/bk_reference/content/ranger-ports.html
  //
  // postgres DB hardcoded as "ranger"
  // ranger jdbc user: miniranger
  // ranger jdbc pw: miniranger
  // hardcoded auth NONE
  //
  // $0: postgres host
  // $1: postgres port
  // $2: admin host
  // $3: admin port/RPC (REST API) port
  // $4: admin keytab
  // $5: lookup keytab
  // $6: spnego keytab
  const char* kRangerAdminSiteTemplate = R"(
<configuration>

  <!-- DB config -->

  <property>
    <name>ranger.jpa.jdbc.driver</name>
    <value>org.postgresql.Driver</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.url</name>
    <value>jdbc:postgresql://$0:$1/ranger</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.user</name>
    <value>miniranger</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.password</name>
    <value>miniranger</value>
    <description/>
  </property>

  <!-- Service config -->

  <property>
    <name>ranger.externalurl</name>
    <value>http://$2:$3</value>
    <description/>
  </property>
  <property>
    <name>ranger.service.http.enabled</name>
    <value>true</value>
    <description/>
  </property>
  <property>
    <name>ranger.authentication.method</name>
    <value>NONE</value>
    <description/>
  </property>
  <property>
    <name>ranger.service.host</name>
    <value>$2</value>
  </property>
  <property>
    <name>ranger.service.http.port</name>
    <value>$3</value>
  </property>
  <property>
    <name>ranger.admin.cookie.name</name>
    <value>RANGERADMINSESSIONID</value>
  </property>
  <property>
    <name>ranger.plugins.kudu.serviceuser</name>
    <value>kudu</value>
  </property>

  <!-- Kerberos config -->
  <property>
    <name>ranger.admin.kerberos.keytab</name>
    <value>$4</value>
  </property>
  <property>
    <name>ranger.admin.kerberos.principal</name>
    <value>rangeradmin/_HOST@KRBTEST.COM</value>
  </property>
  <property>
    <name>ranger.lookup.kerberos.keytab</name>
    <value>$5</value>
  </property>
  <property>
    <name>ranger.lookup.kerberos.principal</name>
    <value>rangerlookup/_HOST@KRBTEST.COM</value>
  </property>
  <property>
    <name>ranger.spnego.kerberos.keytab</name>
    <value>$6</value>
  </property>
  <property>
    <name>ranger.spnego.kerberos.principal</name>
    <value>HTTP/_HOST@KRBTEST.COM</value>
  </property>
</configuration>
)";
  return strings::Substitute(kRangerAdminSiteTemplate, pg_host, pg_port,
                             admin_host, admin_port, admin_keytab,
                             lookup_keytab, spnego_keytab);
}

// Gets the ranger-admin-default-site.xml that has some additional configuration
// needed to start Ranger. It's unclear why this has to be a separate file.
inline std::string GetRangerAdminDefaultSiteXml(const std::string& pg_driver,
                                                uint16_t shutdown_port) {
  // ranger-admin-default-site.xml
  // - postgres JDBC driver path
  // - RANGER_HOME (needed for jceks/KMS), impala says this is ranger-home, but the
  //   conf/jcsks directory doesn't exist for us.
  //
  // $0: postgres JDBC driver path
  // $1: ranger shutdown port
  const char* kRangerAdminDefaultSiteTemplate = R"(
<configuration>

<!-- Actual config we need -->
  <property>
    <name>ranger.jdbc.sqlconnectorjar</name>
    <value>$0</value>
    <description/>
  </property>
  <property>
    <name>ranger.service.shutdown.port</name>
    <value>$1</value>
  </property>

<!-- JPA config we can't remove because Ranger fails to start due to config resolution issues -->

  <property>
    <name>ranger.jpa.showsql</name>
    <value>false</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.dialect</name>
    <value>org.eclipse.persistence.platform.database.PostgreSQLPlatform</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.maxpoolsize</name>
    <value>40</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.minpoolsize</name>
    <value>5</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.initialpoolsize</name>
    <value>5</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.maxidletime</name>
    <value>300</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.maxstatements</name>
    <value>500</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.preferredtestquery</name>
    <value>select 1;</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.idleconnectiontestperiod</name>
    <value>60</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.credential.alias</name>
    <value>ranger.db.password</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.audit.jdbc.dialect</name>
    <value>org.eclipse.persistence.platform.database.PostgreSQLPlatform</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.audit.jdbc.credential.alias</name>
    <value>ranger.auditdb.password</value>
    <description/>
  </property>


  <property>
    <name>ranger.jpa.audit.jdbc.driver</name>
    <value>org.postgresql.Driver</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.audit.jdbc.url</name>
    <value>jdbc:log4jdbc:mysql://localhost/rangeraudit</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.audit.jdbc.user</name>
    <value>rangerlogger</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.audit.jdbc.password</name>
    <value>rangerlogger</value>
    <description/>
  </property>
</configuration>
)";
  return strings::Substitute(kRangerAdminDefaultSiteTemplate, pg_driver,
                             shutdown_port);
}

// Gets the contents of the log4j.properties file which is used to set up the
// logging in Ranger. The only modification to the default log4j.properties is
// the configurable log level.
inline std::string GetRangerLog4jProperties(const std::string& log_level) {
  // log4j.properties file.
  //
  // This is the default log4j.properties with the only difference that rootLogger
  // is made configurable if it's needed for debugging.
  //
  // $0: log level
  const char *kLog4jPropertiesTemplate = R"(
log4j.rootLogger = $0,xa_log_appender


# xa_logger
log4j.appender.xa_log_appender=org.apache.log4j.DailyRollingFileAppender
log4j.appender.xa_log_appender.file=$${logdir}/ranger-admin-$${hostname}-$${user}.log
log4j.appender.xa_log_appender.datePattern='.'yyyy-MM-dd
log4j.appender.xa_log_appender.append=true
log4j.appender.xa_log_appender.layout=org.apache.log4j.PatternLayout
log4j.appender.xa_log_appender.layout.ConversionPattern=%d [%t] %-5p %C{6} (%F:%L) - %m%n
# xa_log_appender : category and additivity
log4j.category.org.springframework=warn,xa_log_appender
log4j.additivity.org.springframework=false

log4j.category.org.apache.ranger=info,xa_log_appender
log4j.additivity.org.apache.ranger=false

log4j.category.xa=info,xa_log_appender
log4j.additivity.xa=false

# perf_logger
log4j.appender.perf_appender=org.apache.log4j.DailyRollingFileAppender
log4j.appender.perf_appender.file=$${logdir}/ranger_admin_perf.log
log4j.appender.perf_appender.datePattern='.'yyyy-MM-dd
log4j.appender.perf_appender.append=true
log4j.appender.perf_appender.layout=org.apache.log4j.PatternLayout
log4j.appender.perf_appender.layout.ConversionPattern=%d [%t] %m%n


# sql_appender
log4j.appender.sql_appender=org.apache.log4j.DailyRollingFileAppender
log4j.appender.sql_appender.file=$${logdir}/ranger_admin_sql.log
log4j.appender.sql_appender.datePattern='.'yyyy-MM-dd
log4j.appender.sql_appender.append=true
log4j.appender.sql_appender.layout=org.apache.log4j.PatternLayout
log4j.appender.sql_appender.layout.ConversionPattern=%d [%t] %-5p %C{6} (%F:%L) - %m%n

# sql_appender : category and additivity
log4j.category.org.hibernate.SQL=warn,sql_appender
log4j.additivity.org.hibernate.SQL=false

log4j.category.jdbc.sqlonly=fatal,sql_appender
log4j.additivity.jdbc.sqlonly=false

log4j.category.jdbc.sqltiming=warn,sql_appender
log4j.additivity.jdbc.sqltiming=false

log4j.category.jdbc.audit=fatal,sql_appender
log4j.additivity.jdbc.audit=false

log4j.category.jdbc.resultset=fatal,sql_appender
log4j.additivity.jdbc.resultset=false

log4j.category.jdbc.connection=fatal,sql_appender
log4j.additivity.jdbc.connection=false
)";
  return strings::Substitute(kLog4jPropertiesTemplate, log_level);
}

// Gets the core-site.xml that configures authentication.
inline std::string GetRangerCoreSiteXml(bool secure) {
  // core-site.xml containing authentication method.
  //
  // $0: authn method (simple or kerberos)
  const char* kCoreSiteTemplate = R"(
<configuration>
  <property>
    <name>hadoop.security.authentication</name>
    <value>$0</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping</name>
    <value>org.apache.hadoop.security.NullGroupsMapping</value>
  </property>
</configuration>
)";
  if (secure) {
    return strings::Substitute(kCoreSiteTemplate, "kerberos", "true");
  }

  return strings::Substitute(kCoreSiteTemplate, "simple", "false");
}

// Gets the contents of ranger-kudu-security.xml for configuring the client.
inline std::string GetRangerKuduSecurityXml(const std::string& policy_cache_dir,
                                            const std::string& service_name,
                                            const std::string& admin_url,
                                            uint32_t policy_poll_interval_ms ) {
  // ranger-kudu-security.xml (client configuration file).
  //
  // $0: Range policy cache dir
  // $1: Kudu service name
  // $2: Ranger admin URL
  // $3: Policy poll interval (ms)
  const char* kRangerKuduSecurity = R"(
<configuration>
  <property>
    <name>ranger.plugin.kudu.policy.cache.dir</name>
    <value>$0</value>
  </property>
  <property>
    <name>ranger.plugin.kudu.service.name</name>
    <value>$1</value>
  </property>
  <property>
    <name>ranger.plugin.kudu.policy.rest.url</name>
    <value>$2</value>
  </property>
  <property>
    <name>ranger.plugin.kudu.policy.source.impl</name>
    <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
  </property>
  <property>
    <name>ranger.plugin.kudu.policy.rest.ssl.config.file</name>
    <value></value>
  </property>
  <property>
    <name>ranger.plugin.kudu.policy.pollIntervalMs</name>
    <value>$3</value>
  </property>
  <property>
    <name>ranger.plugin.kudu.access.cluster.name</name>
    <value>Cluster 1</value>
  </property>
</configuration>
)";
  return strings::Substitute(kRangerKuduSecurity, policy_cache_dir,
                             service_name, admin_url, policy_poll_interval_ms);
}

} // namespace ranger
} // namespace kudu
