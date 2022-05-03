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
namespace rangerkms {

// Gets the contents of the install.properties file used by the db_setup.py
// script.
inline std::string GetRangerKMSInstallProperties(const std::string& bin_dir,
                                                 const std::string& pg_host,
                                                 uint16_t pg_port,
                                                 const std::string& ranger_url) {
  // Taken and modified from:
  // https://github.com/apache/ranger/blob/master/kms/scripts/install.properties
  //
  // $0: directory containing postgresql.jar
  // $1: postgres host
  // $2: postgres port
  // $3: ranger url
  constexpr const char* const kRangerKMSInstallPropertiesTemplate = R"(
PYTHON_COMMAND_INVOKER=python
DB_FLAVOR=POSTGRES
SQL_CONNECTOR_JAR=$0/postgresql.jar
db_root_user=postgres
db_root_password=
db_host=$1:$2
db_ssl_enabled=false
db_ssl_required=false
db_ssl_verifyServerCertificate=false
db_name=rangerkms
db_user=rangerkms
db_password=
ranger_kms_http_enabled=true
KMS_MASTER_KEY_PASSWD=Str0ngPassw0rd
POLICY_MGR_URL=$3
REPOSITORY_NAME=kms
XAAUDIT.SUMMARY.ENABLE=false
XAAUDIT.ELASTICSEARCH.ENABLE=false
XAAUDIT.HDFS.ENABLE=false
XAAUDIT.LOG4J.ENABLE=false
mysql_core_file=db/mysql/kms_core_db.sql
oracle_core_file=db/oracle/kms_core_db_oracle.sql
postgres_core_file=db/postgres/kms_core_db_postgres.sql
sqlserver_core_file=db/sqlserver/kms_core_db_sqlserver.sql
sqlanywhere_core_file=db/sqlanywhere/kms_core_db_sqlanywhere.sql
KMS_BLACKLIST_DECRYPT_EEK=hdfs)";

  return strings::Substitute(kRangerKMSInstallPropertiesTemplate,
                             bin_dir, pg_host, pg_port, ranger_url);
}

inline std::string GetRangerKMSSiteXml(const std::string& kms_host,
                                       uint16_t kms_port,
                                       const std::string& webapp_dir,
                                       const std::string& conf_dir) {
  constexpr const char* const kRangerKMSSiteXmlTemplate = R"(
<configuration>
  <property>
    <name>ranger.service.host</name>
    <value>$0</value>
  </property>
  <property>
    <name>ranger.service.http.port</name>
    <value>$1</value>
  </property>
  <property>
    <name>ranger.service.shutdown.port</name>
    <value>0</value>
  </property>
  <property>
    <name>ranger.contextName</name>
    <value>/kms</value>
  </property>
  <property>
    <name>xa.webapp.dir</name>
    <value>$2</value>
  </property>
  <property>
    <name>ranger.service.https.attrib.ssl.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>ajp.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.service.https.attrib.client.auth</name>
    <value>want</value>
  </property>
  <property>
    <name>ranger.credential.provider.path</name>
    <value>/etc/ranger/kms/rangerkms.jceks</value>
  </property>
  <property>
    <name>ranger.service.https.attrib.keystore.file</name>
    <value />
  </property>
  <property>
    <name>ranger.service.https.attrib.keystore.keyalias</name>
    <value>rangerkms</value>
  </property>
  <property>
    <name>ranger.service.https.attrib.keystore.pass</name>
    <value />
  </property>
  <property>
    <name>ranger.service.https.attrib.keystore.credential.alias</name>
    <value>keyStoreCredentialAlias</value>
  </property>
  <property>
    <name>kms.config.dir</name>
    <value>$3</value>
  </property>
</configuration>)";
  return strings::Substitute(kRangerKMSSiteXmlTemplate, kms_host, kms_port, webapp_dir, conf_dir);
}

inline std::string GetRangerKMSDbksSiteXml(const std::string& pg_host,
                                           const uint16_t pg_port,
                                           const std::string& pg_driver) {
  constexpr const char* const kRangerKMSDbksSiteXmlTemplate = R"(
<configuration>
  <property>
    <name>hadoop.kms.blacklist.DECRYPT_EEK</name>
    <value>hdfs</value>
    <description>
          Blacklist for decrypt EncryptedKey
          CryptoExtension operations
    </description>
  </property>
  <property>
    <name>ranger.db.encrypt.key.password</name>
    <value>Str0ngPassw0rd</value>
    <description>
            Password used for encrypting Master Key
    </description>
  </property>
  <property>
    <name>ranger.kms.service.masterkey.password.cipher</name>
    <value>AES</value>
    <description>
            Cipher used for encrypting Master Key
    </description>
  </property>
  <property>
   <name>ranger.kms.service.masterkey.password.size</name>
   <value>256</value>
    <description>
            Size of masterkey
    </description>
 </property>
  <property>
    <name>ranger.kms.service.masterkey.password.salt.size</name>
    <value>8</value>
    <description>
            Salt size to encrypt Master Key
    </description>
  </property>
  <property>
    <name>ranger.kms.service.masterkey.password.salt</name>
    <value>abcdefghijklmnopqrstuvwxyz01234567890</value>
    <description>
            Salt to encrypt Master Key
    </description>
  </property>
  <property>
    <name>ranger.kms.service.masterkey.password.iteration.count</name>
    <value>1000</value>
    <description>
            Iteration count to encrypt Master Key
    </description>
  </property>
  <property>
    <name>ranger.kms.service.masterkey.password.encryption.algorithm</name>
    <value>PBEWithMD5AndDES</value>
    <description>
            Algorithm to encrypt Master Key
    </description>
  </property>
  <property>
    <name>ranger.kms.service.masterkey.password.md.algorithm</name>
    <value>SHA</value>
    <description>
            Message Digest algorithn to encrypt Master Key
    </description>
  </property>
  <property>
    <name>ranger.ks.jpa.jdbc.url</name>
    <value>jdbc:postgresql://$0:$1/rangerkms</value>
    <description>
      URL for Database
    </description>
  </property>
  <property>
    <name>ranger.ks.jpa.jdbc.user</name>
    <value>rangerkms</value>
    <description>
      Database username used for operation
    </description>
  </property>
  <property>
    <name>ranger.ks.jpa.jdbc.password</name>
    <value></value>
    <description>
      Database user's password
    </description>
  </property>
  <property>
    <name>ranger.ks.jpa.jdbc.credential.provider.path</name>
    <value>/root/ranger-2.1.0-kms/ews/webapp/WEB-INF/classes/conf/.jceks/rangerkms.jceks</value>
    <description>
      Credential provider path
    </description>
  </property>
  <property>
    <name>ranger.ks.jpa.jdbc.credential.alias</name>
    <value>ranger.ks.jpa.jdbc.credential.alias</value>
    <description>
      Credential alias used for password
    </description>
  </property>
  <property>
    <name>ranger.ks.masterkey.credential.alias</name>
    <value>ranger.ks.masterkey.password</value>
    <description>
      Credential alias used for masterkey
    </description>
  </property>
  <property>
    <name>ranger.ks.jpa.jdbc.dialect</name>
    <value>org.eclipse.persistence.platform.database.PostgreSQLPlatform</value>
    <description>
      Dialect used for database
    </description>
  </property>
  <property>
    <name>ranger.ks.jpa.jdbc.driver</name>
    <value>org.postgresql.Driver</value>
    <description>
      Driver used for database
    </description>
  </property>
  <property>
    <name>ranger.ks.jdbc.sqlconnectorjar</name>
    <value>$2</value>
    <description>
      Driver used for database
    </description>
  </property>
  <property>
    <name>ranger.ks.kerberos.principal</name>
    <value>rangerkms/_HOST@KRBTEST.COM</value>
  </property>
  <property>
    <name>ranger.ks.kerberos.keytab</name>
    <value />
  </property>
  <property>
    <name>ranger.kms.keysecure.enabled</name>
    <value>false</value>
    <description />
  </property>
  <property>
    <name>ranger.kms.keysecure.UserPassword.Authentication</name>
    <value>true</value>
    <description />
  </property>
  <property>
    <name>ranger.kms.keysecure.masterkey.name</name>
    <value>safenetmasterkey</value>
    <description>Safenet key secure master key name</description>
  </property>
  <property>
    <name>ranger.kms.keysecure.login.username</name>
    <value>user1</value>
    <description>Safenet key secure username</description>
  </property>
  <property>
    <name>ranger.kms.keysecure.login.password</name>
    <value>t1e2s3t4</value>
    <description>Safenet key secure user password</description>
  </property>
  <property>
    <name>ranger.kms.keysecure.login.password.alias</name>
    <value>ranger.ks.login.password</value>
    <description>Safenet key secure user password</description>
  </property>
  <property>
    <name>ranger.kms.keysecure.hostname</name>
    <value>SunPKCS11-keysecurehn</value>
    <description>Safenet key secure hostname</description>
  </property>
  <property>
    <name>ranger.kms.keysecure.masterkey.size</name>
    <value>256</value>
    <description>key size</description>
  </property>
  <property>
    <name>ranger.kms.keysecure.sunpkcs11.cfg.filepath</name>
    <value>/opt/safenetConf/64/8.3.1/sunpkcs11.cfg</value>
    <description>Location of Safenet key secure library configuration file</description>
  </property>
  <property>
    <name>ranger.kms.keysecure.provider.type</name>
    <value>SunPKCS11</value>
    <description>Security Provider for key secure</description>
  </property>
  <property>
    <name>ranger.ks.db.ssl.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.ks.db.ssl.required</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.ks.db.ssl.verifyServerCertificate</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.ks.db.ssl.auth.type</name>
    <value>2-way</value>
  </property>
</configuration>
)";
  return strings::Substitute(kRangerKMSDbksSiteXmlTemplate, pg_host, pg_port, pg_driver);
}

inline std::string GetRangerKMSLog4jProperties(const std::string& log_level) {
  // log4j.properties file
  //
  // This is the default log4j.properties with the only difference that rootLogger
  // is made configurable if it's needed for debugging.
  //
  // $0: log level
  constexpr const char* const kLog4jPropertiesTemplate = R"(
log4j.appender.kms=org.apache.log4j.DailyRollingFileAppender
log4j.appender.kms.DatePattern='.'yyyy-MM-dd
log4j.appender.kms.File=$${logdir}/ranger-kms-$${hostname}-$${user}.log
log4j.appender.kms.Append=true
log4j.appender.kms.layout=org.apache.log4j.PatternLayout
log4j.appender.kms.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n

log4j.appender.kms-audit=org.apache.log4j.DailyRollingFileAppender
log4j.appender.kms-audit.DatePattern='.'yyyy-MM-dd
log4j.appender.kms-audit.File=$${logdir}/kms-audit-$${hostname}-$${user}.log
log4j.appender.kms-audit.Append=true
log4j.appender.kms-audit.layout=org.apache.log4j.PatternLayout
log4j.appender.kms-audit.layout.ConversionPattern=%d{ISO8601} %m%n

log4j.logger.kms-audit=INFO, kms-audit
log4j.additivity.kms-audit=false

log4j.logger=$0, kms
log4j.rootLogger=WARN, kms
log4j.logger.org.apache.hadoop.conf=INFO
log4j.logger.org.apache.hadoop=INFO
log4j.logger.org.apache.ranger=INFO
log4j.logger.com.sun.jersey.server.wadl.generators.WadlGeneratorJAXBGrammarGenerator=OFF
)";
  return strings::Substitute(kLog4jPropertiesTemplate, log_level);
}

inline std::string GetRangerKMSSecurityXml(const std::string& ranger_url,
                                           const std::string& kms_home) {
  constexpr const char* const kRangerKmsSecurityXmlTemplate = R"(
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">
  <property>
    <name>ranger.plugin.kms.service.name</name>
    <value>kms</value>
    <description>
      Name of the Ranger service containing policies for this kms instance
    </description>
  </property>
  <property>
    <name>ranger.plugin.kms.policy.source.impl</name>
    <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
    <description>
      Class to retrieve policies from the source
    </description>
  </property>
  <property>
    <name>ranger.plugin.kms.policy.rest.url</name>
    <value>$0</value>
    <description>
      URL to Ranger Admin
    </description>
  </property>
  <property>
    <name>ranger.plugin.kms.policy.rest.ssl.config.file</name>
    <value>$1/ranger-kms-policymgr-ssl.xml</value>
    <description>
      Path to the file containing SSL details to contact Ranger Admin
    </description>
  </property>
  <property>
    <name>ranger.plugin.kms.policy.pollIntervalMs</name>
    <value>30000</value>
    <description>
      How often to poll for changes in policies?
    </description>
  </property>
  <property>
    <name>ranger.plugin.kms.policy.cache.dir</name>
    <value>$2/policycache</value>
    <description>
      Directory where Ranger policies are cached after successful retrieval from the source
    </description>
  </property>
  <property>
    <name>ranger.plugin.kms.policy.rest.client.connection.timeoutMs</name>
    <value>120000</value>
    <description>
      RangerRestClient Connection Timeout in Milli Seconds
    </description>
  </property>
  <property>
    <name>ranger.plugin.kms.policy.rest.client.read.timeoutMs</name>
    <value>30000</value>
    <description>
      RangerRestClient read Timeout in Milli Seconds
    </description>
  </property>
</configuration>
)";
  return strings::Substitute(kRangerKmsSecurityXmlTemplate, ranger_url, kms_home, kms_home);
}

inline std::string GetKMSSiteXml(bool secure, const std::string& keytab) {
  constexpr const char* const kmsSiteXml = R"(
  <configuration>

  <!-- KMS Backend KeyProvider -->

  <property>
    <name>hadoop.kms.key.provider.uri</name>
    <value>dbks://http@localhost:9292/kms</value>
    <description>
      URI of the backing KeyProvider for the KMS.
    </description>
  </property>

  <property>
    <name>hadoop.security.keystore.JavaKeyStoreProvider.password</name>
    <value>none</value>
    <description>
      If using the JavaKeyStoreProvider, the password for the keystore file.
    </description>
  </property>

  <!-- KMS Cache -->

  <property>
    <name>hadoop.kms.cache.enable</name>
    <value>true</value>
    <description>
      Whether the KMS will act as a cache for the backing KeyProvider.
      When the cache is enabled, operations like getKeyVersion, getMetadata,
      and getCurrentKey will sometimes return cached data without consulting
      the backing KeyProvider. Cached values are flushed when keys are deleted
      or modified.
    </description>
  </property>

  <property>
    <name>hadoop.kms.cache.timeout.ms</name>
    <value>600000</value>
    <description>
      Expiry time for the KMS key version and key metadata cache, in
      milliseconds. This affects getKeyVersion and getMetadata.
    </description>
  </property>

  <property>
    <name>hadoop.kms.current.key.cache.timeout.ms</name>
    <value>30000</value>
    <description>
      Expiry time for the KMS current key cache, in milliseconds. This
      affects getCurrentKey operations.
    </description>
  </property>

  <!-- KMS Audit -->

  <property>
    <name>hadoop.kms.audit.aggregation.window.ms</name>
    <value>10000</value>
    <description>
      Duplicate audit log events within the aggregation window (specified in
      ms) are quashed to reduce log traffic. A single message for aggregated
      events is printed at the end of the window, along with a count of the
      number of aggregated events.
    </description>
  </property>

  <!-- KMS Security -->

  <property>
    <name>hadoop.kms.authentication.type</name>
    <value>$0</value>
    <description>
      Authentication type for the KMS. Can be either &quot;simple&quot;
      or &quot;kerberos&quot;.
    </description>
  </property>

  <property>
    <name>hadoop.kms.authentication.kerberos.keytab</name>
    <value>$1</value>
    <description>
      Path to the keytab with credentials for the configured Kerberos principal.
    </description>
  </property>

  <property>
    <name>hadoop.kms.authentication.kerberos.principal</name>
    <value>HTTP/localhost</value>
    <description>
      The Kerberos principal to use for the HTTP endpoint.
      The principal must start with 'HTTP/' as per the Kerberos HTTP SPNEGO specification.
    </description>
  </property>

  <property>
    <name>hadoop.kms.authentication.kerberos.name.rules</name>
    <value>DEFAULT</value>
    <description>
      Rules used to resolve Kerberos principal names.
    </description>
  </property>

  <!-- Authentication cookie signature source -->

  <property>
    <name>hadoop.kms.authentication.signer.secret.provider</name>
    <value>random</value>
    <description>
      Indicates how the secret to sign the authentication cookies will be
      stored. Options are 'random' (default), 'string' and 'zookeeper'.
      If using a setup with multiple KMS instances, 'zookeeper' should be used.
    </description>
  </property>

  <!-- Configuration for 'zookeeper' authentication cookie signature source -->

  <property>
    <name>hadoop.kms.authentication.signer.secret.provider.zookeeper.path</name>
    <value>/hadoop-kms/hadoop-auth-signature-secret</value>
    <description>
      The Zookeeper ZNode path where the KMS instances will store and retrieve
      the secret from.
    </description>
  </property>

  <property>
    <name>hadoop.kms.authentication.signer.secret.provider.zookeeper.connection.string</name>
    <value>#HOSTNAME#:#PORT#,...</value>
    <description>
      The Zookeeper connection string, a list of hostnames and port comma
      separated.
    </description>
  </property>

  <property>
    <name>hadoop.kms.authentication.signer.secret.provider.zookeeper.auth.type</name>
    <value>kerberos</value>
    <description>
      The Zookeeper authentication type, 'none' or 'sasl' (Kerberos).
    </description>
  </property>

  <property>
    <name>hadoop.kms.authentication.signer.secret.provider.zookeeper.kerberos.keytab</name>
    <value>/etc/hadoop/conf/kms.keytab</value>
    <description>
      The absolute path for the Kerberos keytab with the credentials to
      connect to Zookeeper.
    </description>
  </property>

  <property>
    <name>hadoop.kms.authentication.signer.secret.provider.zookeeper.kerberos.principal</name>
    <value>kms/#HOSTNAME#</value>
    <description>
      The Kerberos service principal used to connect to Zookeeper.
    </description>
  </property>

  <property>
    <name>hadoop.kms.security.authorization.manager</name>
    <value>org.apache.ranger.authorization.kms.authorizer.RangerKmsAuthorizer</value>
  </property>

  <property>
    <name>hadoop.kms.proxyuser.ranger.groups</name>
    <value>*</value>
  </property>

  <property>
    <name>hadoop.kms.proxyuser.ranger.hosts</name>
    <value>*</value>
  </property>

  <property>
    <name>hadoop.kms.proxyuser.ranger.users</name>
    <value>*</value>
  </property>
</configuration>
)";
  if (secure) {
    return strings::Substitute(kmsSiteXml, "kerberos", keytab);
  }
  return strings::Substitute(kmsSiteXml, "simple", keytab);
}

inline std::string GetRangerKMSAuditXml() {
  constexpr const char* const kRangerKMSAuditXml = R"(
<configuration>
  <property>
    <name>xasecure.audit.is.enabled</name>
    <value>false</value>
  </property>
  <!-- DB audit provider configuration -->
  <property>
    <name>xasecure.audit.db.is.enabled</name>
    <value>false</value>
  </property>
  <!-- HDFS audit provider configuration -->
  <property>
    <name>xasecure.audit.hdfs.is.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>xasecure.audit.destination.hdfs</name>
    <value>disabled</value>
  </property>
  <property>
    <name>xasecure.audit.log4j.is.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>xasecure.audit.kafka.is.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>xasecure.audit.provider.summary.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>xasecure.audit.destination.solr</name>
    <value>false</value>
  </property>
  <property>
    <name>xasecure.audit.destination.hdfs</name>
    <value>false</value>
  </property>
</configuration>
)";
  return kRangerKMSAuditXml;
}

inline std::string GetRangerKMSPolicymgrSSLXml() {
  constexpr const char* const kRangerKMSPolicymgrSSLXml = R"(
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">
  <!--  The following properties are used for 2-way SSL client server validation -->
  <property>
    <name>xasecure.policymgr.clientssl.keystore</name>
    <value>/etc/ranger/kms/conf/ranger-plugin-keystore.jks</value>
    <description>
      Java Keystore files
    </description>
  </property>
  <property>
    <name>xasecure.policymgr.clientssl.truststore</name>
    <value>/etc/ranger/kms/conf/ranger-plugin-truststore.jks</value>
    <description>
      java truststore file
    </description>
  </property>
    <property>
    <name>xasecure.policymgr.clientssl.keystore.credential.file</name>
    <value>jceks://file/etc/ranger/kmsdev/cred.jceks</value>
    <description>
      java  keystore credential file
    </description>
  </property>
  <property>
    <name>xasecure.policymgr.clientssl.truststore.credential.file</name>
    <value>jceks://file/etc/ranger/kmsdev/cred.jceks</value>
    <description>
      java  truststore credential file
    </description>
  </property>
</configuration>
)";
  return kRangerKMSPolicymgrSSLXml;
}


} // namespace rangerkms
} // namespace kudu
