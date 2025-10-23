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
case class BackupOptions(
    tables: Seq[String],
    rootPath: String,
    kuduMasterAddresses: String = InetAddress.getLocalHost.getCanonicalHostName,
    toMs: Long = System.currentTimeMillis(),
    forceFull: Boolean = BackupOptions.DefaultForceFull,
    fromMs: Long = BackupOptions.DefaultFromMS,
    format: String = BackupOptions.DefaultFormat,
    scanBatchSize: Int = BackupOptions.DefaultScanBatchSize,
    scanRequestTimeoutMs: Long = BackupOptions.DefaultScanRequestTimeoutMs,
    scanLeaderOnly: Boolean = BackupOptions.DefaultScanLeaderOnly,
    scanPrefetching: Boolean = BackupOptions.DefaultScanPrefetching,
    keepAlivePeriodMs: Long = BackupOptions.DefaultKeepAlivePeriodMs,
    failOnFirstError: Boolean = BackupOptions.DefaultFailOnFirstError,
    numParallelBackups: Int = BackupOptions.DefaultNumParallelBackups,
    splitSizeBytes: Option[Long] = None)

object BackupOptions {
  val DefaultForceFull: Boolean = false
  val DefaultFromMS: Long = 0
  val DefaultFormat: String = "parquet"
  val DefaultScanBatchSize: Int = 1024 * 1024 * 20 // 20 MiB
  val DefaultScanRequestTimeoutMs: Long =
    AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS // 30 seconds
  val DefaultScanLeaderOnly: Boolean = false
  // TODO (KUDU-1260): Add a test and enable by default?
  val DefaultScanPrefetching: Boolean = false
  val DefaultKeepAlivePeriodMs: Long = AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS
  val DefaultFailOnFirstError: Boolean = false
  val DefaultNumParallelBackups = 1
  val DefaultSplitSizeBytes: Option[Long] = None

  // We use the program name to make the help output show a the spark invocation required.
  val ClassName: String = KuduBackup.getClass.getCanonicalName.dropRight(1) // Remove trailing `$`
  val ProgramName: String = "spark-submit --class " + ClassName + " [spark-options] " +
    "<application-jar>"

  val parser: OptionParser[BackupOptions] =
    new OptionParser[BackupOptions](ProgramName) {
      opt[String]("rootPath")
        .action((v, o) => o.copy(rootPath = v))
        .text("The root path to output backup data. Accepts any Spark compatible path.")
        .required()

      opt[String]("kuduMasterAddresses")
        .action((v, o) => o.copy(kuduMasterAddresses = v))
        .text("Comma-separated addresses of Kudu masters. Default: localhost")
        .optional()

      opt[Boolean]("forceFull")
        .action((v, o) => o.copy(forceFull = v))
        .text("If true, this will be a full backup even if another full already exists. " +
          "Default: " + DefaultForceFull)
        .optional()

      opt[Long]("fromMs")
        .action((v, o) => o.copy(fromMs = v))
        .text(
          "A UNIX timestamp in milliseconds that defines the start time of an incremental " +
            "backup. If unset, the fromMs will be defined by previous backups in the root " +
            "directory.")
        .optional()

      opt[Long]("timestampMs")
        .action((v, o) => o.copy(toMs = v))
        // TODO (KUDU-2677): Document the limitations based on cluster configuration.
        .text("A UNIX timestamp in milliseconds since the epoch to execute scans at. " +
          "Default: `System.currentTimeMillis()`")
        .optional()

      opt[Int]("scanBatchSize")
        .action((v, o) => o.copy(scanBatchSize = v))
        .text("The maximum number of bytes returned by the scanner, on each batch. " +
          "Default: " + DefaultScanBatchSize)
        .optional()

      opt[Int]("scanRequestTimeoutMs")
        .action((v, o) => o.copy(scanRequestTimeoutMs = v))
        .text("Sets how long in milliseconds each scan request to a server can last. " +
          "Default: " + DefaultScanRequestTimeoutMs)
        .optional()

      opt[Long]("keepAlivePeriodMs")
        .action((v, o) => o.copy(keepAlivePeriodMs = v))
        .text("Sets the period at which to send keep-alive requests to the tablet server to " +
          "ensure that scanners do not time out. Default: " + DefaultKeepAlivePeriodMs)
        .optional()

      opt[Boolean]("scanLeaderOnly")
        .action((v, o) => o.copy(scanLeaderOnly = v))
        .text("If true scans will only use the leader replica, otherwise scans will take place " +
          "at the closest replica. Default: " + DefaultScanLeaderOnly)
        .hidden()
        .optional()

      opt[String]("format")
        .action((v, o) => o.copy(format = v))
        .text("The file format to use when writing the data. Default: " + DefaultFormat)
        .hidden()
        .optional()

      opt[Unit]("scanPrefetching")
        .action((_, o) => o.copy(scanPrefetching = true))
        .text("An experimental flag to enable pre-fetching data. " +
          "Default: " + DefaultScanPrefetching)
        .hidden()
        .optional()

      opt[Unit]("failOnFirstError")
        .action((_, o) => o.copy(failOnFirstError = true))
        .text("Whether to fail the backup job as soon as a single table backup fails. " +
          "Default: " + DefaultFailOnFirstError)
        .optional()

      opt[Int]("numParallelBackups")
        .action((v, o) => o.copy(numParallelBackups = v))
        .text(
          "The number of tables to back up in parallel. Backup leaves it to Spark to manage " +
            "the resources of parallel jobs. Overrides --failOnFirstError. This option is " +
            "experimental. Default: " + DefaultNumParallelBackups)
        .hidden()
        .optional()

      opt[Long]("splitSizeBytes")
        .action((v, o) => o.copy(splitSizeBytes = Some(v)))
        .text(
          "Sets the target number of bytes per spark task. If set, tablet's primary key range " +
            "will be split to generate uniform task sizes instead of the default of 1 task per " +
            "tablet. This option is experimental.")
        .hidden()
        .optional()

      help("help").text("prints this usage text")

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
    removeImpalaPrefix: Boolean = RestoreOptions.DefaultRemoveImpalaPrefix,
    newDatabaseName: String = "",
    tableSuffix: String = "",
    createTables: Boolean = RestoreOptions.DefaultCreateTables,
    timestampMs: Long = System.currentTimeMillis(),
    failOnFirstError: Boolean = RestoreOptions.DefaultFailOnFirstError,
    numParallelRestores: Int = RestoreOptions.DefaultNumParallelRestores,
    restoreOwner: Boolean = RestoreOptions.DefaultRestoreOwner)

object RestoreOptions {
  val DefaultRemoveImpalaPrefix: Boolean = false
  val DefaultCreateTables: Boolean = true
  val DefaultFailOnFirstError = false
  val DefaultNumParallelRestores = 1
  val DefaultRestoreOwner: Boolean = true

  val ClassName: String = KuduRestore.getClass.getCanonicalName.dropRight(1) // Remove trailing `$`
  val ProgramName: String = "spark-submit --class " + ClassName + " [spark-options] " +
    "<application-jar>"

  val parser: OptionParser[RestoreOptions] =
    new OptionParser[RestoreOptions](ProgramName) {
      opt[String]("rootPath")
        .action((v, o) => o.copy(rootPath = v))
        .text("The root path to the backup data. Accepts any Spark compatible path.")
        .required()

      opt[String]("kuduMasterAddresses")
        .action((v, o) => o.copy(kuduMasterAddresses = v))
        .text("Comma-separated addresses of Kudu masters. Default: localhost")
        .optional()

      opt[Boolean]("createTables")
        .action((v, o) => o.copy(createTables = v))
        .text("If true, create the tables during restore. Set to false if the target tables " +
          "already exist. Default: " + DefaultCreateTables)
        .optional()

      opt[Boolean]("removeImpalaPrefix")
        .action((v, o) => o.copy(removeImpalaPrefix = v))
        .text("If true, removes the \"impala::\" prefix, if present from the restored table names. This is " +
          "advisable if backup was taken in a Kudu cluster without HMS sync and restoring to " +
          "Kudu cluster which has HMS sync in place. Only used when createTables is true. Default: " +
          DefaultRemoveImpalaPrefix)
        .optional()

      opt[String]("newDatabaseName")
        .action((v, o) => o.copy(newDatabaseName = v))
        .text(
          "If set, replaces the existing database name and if there is no existing database name, a new database " +
            "name is added. Setting this to an empty string will have the same effect of not using the flag at all. " +
            "For example, if this is set to newdb for the tables testtable and impala::db.testtable the restored " +
            "tables will have the names newdb.testtable and impala::newdb.testtable respectively, assuming " +
            "removeImpalaPrefix is set to false")
        .optional()

      opt[String]("tableSuffix")
        .action((v, o) => o.copy(tableSuffix = v))
        .text("If set, the suffix to add to the restored table names. Only used when " +
          "createTables is true.")
        .optional()

      opt[Long]("timestampMs")
        .action((v, o) => o.copy(timestampMs = v))
        .text("A UNIX timestamp in milliseconds that defines the latest time to use when " +
          "selecting restore candidates. Default: `System.currentTimeMillis()`")
        .optional()

      opt[Unit]("failOnFirstError")
        .action((v, o) => o.copy(failOnFirstError = true))
        .text("Whether to fail the restore job as soon as a single table restore fails. " +
          "Default: " + DefaultFailOnFirstError)
        .optional()

      opt[Int]("numParallelRestores")
        .action((v, o) => o.copy(numParallelRestores = v))
        .text(
          "The number of tables to restore in parallel. Restore leaves it to Spark to manage " +
            "the resources of parallel jobs. Overrides --failOnFirstError. This option is " +
            "experimental. Default: " + DefaultNumParallelRestores)
        .hidden()
        .optional()

      opt[Boolean]("restoreOwner")
        .action((v, o) => o.copy(restoreOwner = v))
        .text(
          "If true, it restores table ownership when creating new tables, otherwise creates " +
            "tables as the logged in user. Only used when createTables is true. Default: " +
            DefaultRestoreOwner)
        .optional()

      help("help").text("prints this usage text")

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
