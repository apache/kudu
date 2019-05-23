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
package org.apache.kudu.backup

import org.apache.kudu.spark.kudu.KuduTestSuite
import org.junit.Assert._
import org.junit.Test

class TestOptions extends KuduTestSuite {

  @Test
  def testBackupOptionsHelp() {
    val expectedStr =
      """Usage: spark-submit --class org.apache.kudu.backup.KuduBackup [spark-options] <application-jar> [options] <table>...
        |
        |  --rootPath <value>       The root path to output backup data. Accepts any Spark compatible path.
        |  --kuduMasterAddresses <value>
        |                           Comma-separated addresses of Kudu masters. Default: localhost
        |  --forceFull <value>      If true, this will be a full backup even if another full already exists. Default: false
        |  --fromMs <value>         A UNIX timestamp in milliseconds that defines the start time of an incremental backup. If unset, the fromMs will be defined by previous backups in the root directory.
        |  --timestampMs <value>    A UNIX timestamp in milliseconds since the epoch to execute scans at. Default: `System.currentTimeMillis()`
        |  --scanBatchSize <value>  The maximum number of bytes returned by the scanner, on each batch. Default: 20971520
        |  --scanRequestTimeoutMs <value>
        |                           Sets how long in milliseconds each scan request to a server can last. Default: 30000
        |  --keepAlivePeriodMs <value>
        |                           Sets the period at which to send keep-alive requests to the tablet server to ensure that scanners do not time out. Default: 15000
        |  --failOnFirstError       Whether to fail the backup job as soon as a single table backup fails. Default: false
        |  --help                   prints this usage text
        |  <table>...               A list of tables to be backed up.""".stripMargin
    assertEquals(expectedStr, BackupOptions.parser.renderTwoColumnsUsage)
  }

  @Test
  def testRestoreOptionsHelp() {
    val expectedStr =
      """Usage: spark-submit --class org.apache.kudu.backup.KuduRestore [spark-options] <application-jar> [options] <table>...
        |
        |  --rootPath <value>       The root path to the backup data. Accepts any Spark compatible path.
        |  --kuduMasterAddresses <value>
        |                           Comma-separated addresses of Kudu masters. Default: localhost
        |  --createTables <value>   If true, create the tables during restore. Set to false if the target tables already exist. Default: true
        |  --tableSuffix <value>    If set, the suffix to add to the restored table names. Only used when createTables is true.
        |  --timestampMs <value>    A UNIX timestamp in milliseconds that defines the latest time to use when selecting restore candidates. Default: `System.currentTimeMillis()`
        |  --failOnFirstError       Whether to fail the restore job as soon as a single table restore fails. Default: false
        |  --help                   prints this usage text
        |  <table>...               A list of tables to be restored.""".stripMargin
    assertEquals(expectedStr, RestoreOptions.parser.renderTwoColumnsUsage)
  }
}
