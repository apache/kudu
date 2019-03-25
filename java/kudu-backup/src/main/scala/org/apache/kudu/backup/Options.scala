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

import java.net.InetAddress

import org.apache.kudu.client.AsyncKuduClient
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import scopt.OptionParser

@InterfaceAudience.Private
@InterfaceStability.Unstable
trait CommonOptions {
  val tables: Seq[String]
  val rootPath: String
  val kuduMasterAddresses: String
}

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class BackupOptions(
    tables: Seq[String],
    rootPath: String,
    kuduMasterAddresses: String = InetAddress.getLocalHost.getCanonicalHostName,
    toMs: Long = System.currentTimeMillis(),
    forceFull: Boolean = BackupOptions.DefaultForceFull,
    fromMs: Long = 0,
    format: String = BackupOptions.DefaultFormat,
    scanBatchSize: Int = BackupOptions.DefaultScanBatchSize,
    scanRequestTimeoutMs: Long = BackupOptions.DefaultScanRequestTimeoutMs,
    scanPrefetching: Boolean = BackupOptions.DefaultScanPrefetching,
    keepAlivePeriodMs: Long = BackupOptions.DefaultKeepAlivePeriodMs)
    extends CommonOptions {

  // If not forcing a full backup and fromMs is not zero, this is an incremental backup.
  def isIncremental: Boolean = {
    !forceFull && fromMs != 0
  }
}

object BackupOptions {
  val DefaultForceFull: Boolean = false
  val DefaultFormat: String = "parquet"
  val DefaultScanBatchSize: Int = 1024 * 1024 * 20 // 20 MiB
  val DefaultScanRequestTimeoutMs: Long =
    AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS // 30 seconds
  // TODO: Add a test per KUDU-1260 and enable by default?
  val DefaultScanPrefetching: Boolean = false
  val DefaultKeepAlivePeriodMs: Long = AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS

  // TODO: clean up usage output.
  // TODO: timeout configurations.
  private val parser: OptionParser[BackupOptions] =
    new OptionParser[BackupOptions]("KuduBackup") {
      opt[String]("rootPath")
        .action((v, o) => o.copy(rootPath = v))
        .text("The root path to output backup data. Accepts any Spark compatible path.")
        .optional()

      opt[String]("kuduMasterAddresses")
        .action((v, o) => o.copy(kuduMasterAddresses = v))
        .text("Comma-separated addresses of Kudu masters.")
        .optional()

      opt[Boolean]("forceFull")
        .action((v, o) => o.copy(forceFull = v))
        .text("If true, this will be a full backup even if another full already exists.")
        .optional()

      opt[Long]("fromMs")
        .action((v, o) => o.copy(fromMs = v))
        .text(
          "A UNIX timestamp in milliseconds that defines the start time of an incremental backup.")
        .optional()

      opt[Long]("timestampMs")
        .action((v, o) => o.copy(toMs = v))
        // TODO: Document the limitations based on cluster configuration
        //  (ex: ancient history watermark).
        .text("A UNIX timestamp in milliseconds since the epoch to execute scans at.")
        .optional()

      opt[String]("format")
        .action((v, o) => o.copy(format = v))
        .text("The file format to use when writing the data.")
        .optional()

      opt[Int]("scanBatchSize")
        .action((v, o) => o.copy(scanBatchSize = v))
        .text("The maximum number of bytes returned by the scanner, on each batch.")
        .optional()

      opt[Int]("scanRequestTimeoutMs")
        .action((v, o) => o.copy(scanRequestTimeoutMs = v))
        .text("Sets how long in milliseconds each scan request to a server can last.")
        .optional()

      opt[Unit]("scanPrefetching")
        .action((_, o) => o.copy(scanPrefetching = true))
        .text("An experimental flag to enable pre-fetching data.")
        .optional()

      opt[Long]("keepAlivePeriodMs")
        .action((v, o) => o.copy(keepAlivePeriodMs = v))
        .text(
          "Sets the period at which to send keep-alive requests to the tablet server to ensure" +
            " that scanners do not time out")
        .optional()

      arg[String]("<table>...")
        .unbounded()
        .action((v, o) => o.copy(tables = o.tables :+ v))
        .text("A list of tables to be backed up.")
    }

  /**
   * Parses the passed arguments into Some[KuduBackupOptions].
   *
   * If the arguments are bad, an error message is displayed
   * and None is returned.
   *
   * @param args The arguments to parse.
   * @return Some[KuduBackupOptions] if parsing was successful, None if not.
   */
  def parse(args: Seq[String]): Option[BackupOptions] = {
    parser.parse(args, BackupOptions(Seq(), null))
  }
}

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class RestoreOptions(
    tables: Seq[String],
    rootPath: String,
    kuduMasterAddresses: String = InetAddress.getLocalHost.getCanonicalHostName,
    tableSuffix: String = RestoreOptions.DefaultTableSuffix,
    createTables: Boolean = RestoreOptions.DefaultCreateTables,
    timestampMs: Long = System.currentTimeMillis()
) extends CommonOptions

object RestoreOptions {
  val DefaultTableSuffix: String = "-restore"
  val DefaultCreateTables: Boolean = true

  // TODO: clean up usage output.
  // TODO: timeout configurations.
  private val parser: OptionParser[RestoreOptions] =
    new OptionParser[RestoreOptions]("KuduRestore") {
      opt[String]("rootPath")
        .action((v, o) => o.copy(rootPath = v))
        .text("The root path to the backup data. Accepts any Spark compatible path.")
        .optional()

      opt[String]("kuduMasterAddresses")
        .action((v, o) => o.copy(kuduMasterAddresses = v))
        .text("Comma-separated addresses of Kudu masters.")
        .optional()

      opt[Boolean]("createTables")
        .action((v, o) => o.copy(createTables = v))
        .text("true to create tables during restore, false if they already exist.")
        .optional()

      opt[String]("tableSuffix")
        .action((v, o) => o.copy(tableSuffix = v))
        .text("The suffix to add to the restored table names. Only used when createTables is true.")
        .optional()

      opt[Long]("timestampMs")
        .action((v, o) => o.copy(timestampMs = v))
        .text(
          "A UNIX timestamp in milliseconds that define the latest time to use when selecting " +
            "restore candidates.")
        .optional()

      arg[String]("<table>...")
        .unbounded()
        .action((v, o) => o.copy(tables = o.tables :+ v))
        .text("A list of tables to be restored.")
    }

  /**
   * Parses the passed arguments into Some[KuduRestoreOptions].
   *
   * If the arguments are bad, an error message is displayed
   * and None is returned.
   *
   * @param args The arguments to parse.
   * @return Some[KuduRestoreOptions] if parsing was successful, None if not.
   */
  def parse(args: Seq[String]): Option[RestoreOptions] = {
    parser.parse(args, RestoreOptions(Seq(), null))
  }
}
