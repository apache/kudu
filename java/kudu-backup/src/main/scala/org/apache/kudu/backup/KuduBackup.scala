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

import scala.concurrent.forkjoin.ForkJoinPool

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * The main class for a Kudu backup spark job.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduBackup {
  val log: Logger = LoggerFactory.getLogger(getClass)

  private def doBackup(
      tableName: String,
      context: KuduContext,
      session: SparkSession,
      io: BackupIO,
      options: BackupOptions,
      backupMap: Map[String, BackupGraph]): Unit = {
    var tableOptions = options.copy() // Copy the options so we can modify them for the table.
    val table = context.syncClient.openTable(tableName)
    val tableId = table.getTableId
    val backupPath = io.backupPath(tableId, tableName, tableOptions.toMs)
    val metadataPath = io.backupMetadataPath(backupPath)
    log.info(s"Backing up table $tableName to path: $backupPath")

    // Unless we are forcing a full backup or a fromMs was set, find the previous backup and
    // use the `to_ms` metadata as the `from_ms` time for this backup.
    var incremental = false
    if (tableOptions.forceFull) {
      log.info("Performing a full backup: forceFull was set to true")
    } else if (tableOptions.fromMs != BackupOptions.DefaultFromMS) {
      log.info(s"Performing an incremental backup: fromMs was set to ${tableOptions.fromMs}")
      incremental = true
    } else {
      log.info("Looking for a previous backup: forceFull and fromMs options are not set.")
      if (backupMap.contains(tableId) && backupMap(tableId).hasFullBackup) {
        val base = backupMap(tableId).backupBase
        log.info(s"Setting fromMs to ${base.metadata.getToMs} from backup in path: ${base.path}")
        tableOptions = tableOptions.copy(fromMs = base.metadata.getToMs)
        incremental = true
      } else {
        log.info("No previous backup was found. Starting a full backup.")
        tableOptions = tableOptions.copy(forceFull = true)
      }
    }

    val jobTypeStr = if (incremental) "incremental" else "full"
    session.sparkContext.setJobDescription(s"Kudu Backup($jobTypeStr): $tableName")

    val rdd = new KuduBackupRDD(table, tableOptions, incremental, context, session.sparkContext)
    val df =
      session.sqlContext
        .createDataFrame(rdd, BackupUtils.dataSchema(table.getSchema, incremental))

    // Write the data to the backup path.
    // The backup path contains the timestampMs and should not already exist.
    val writer = df.write.mode(SaveMode.ErrorIfExists)
    writer
      .format(tableOptions.format)
      .save(backupPath.toString)

    // Generate and output the new metadata for this table.
    // The existence of metadata indicates this backup was successful.
    val tableMetadata = TableMetadata
      .getTableMetadata(table, tableOptions.fromMs, tableOptions.toMs, tableOptions.format)
    io.writeTableMetadata(tableMetadata, metadataPath)
  }

  def run(options: BackupOptions, session: SparkSession): Int = {
    // Set the job group for all the spark backup jobs.
    // Note: The job description will be overridden by each Kudu table job.
    session.sparkContext.setJobGroup(s"Kudu Backup @ ${options.toMs}", "Kudu Backup")

    log.info(s"Backing up to root path: ${options.rootPath}")
    val context =
      new KuduContext(
        options.kuduMasterAddresses,
        session.sparkContext
      )
    val io = new BackupIO(session.sparkContext.hadoopConfiguration, options.rootPath)

    // Read the required backup metadata.
    val backupGraphs =
      // Only read the backup metadata if it will be used.
      if (!options.forceFull || options.fromMs != BackupOptions.DefaultFromMS) {
        // Convert the input table names to be backed up into table IDs.
        // This will allow us to link against old backup data by referencing
        // the static table ID even when the table name changes between backups.
        val nameToId = context.syncClient.getTablesList.getTableInfosList.asScala
          .filter(info => options.tables.contains(info.getTableName))
          .map(info => (info.getTableName, info.getTableId))
          .toMap
        val tableIds = options.tables.flatMap(nameToId.get)
        io.readBackupGraphsByTableId(tableIds)
      } else {
        Seq[BackupGraph]()
      }
    // Key the backupMap by the table ID.
    val backupMap = backupGraphs.map { graph =>
      (graph.tableId, graph)
    }.toMap

    // Parallelize the processing. Managing resources of parallel backup jobs is very complex, so
    // only the simplest possible thing is attempted. Kudu trusts Spark to manage resources.
    val parallelTables = options.tables.par
    val pool = new ForkJoinPool(options.numParallelBackups) // Need a clean-up reference.
    parallelTables.tasksupport = new ForkJoinTaskSupport(pool)
    val backupResults = parallelTables.map { tableName =>
      val backupResult = Try(doBackup(tableName, context, session, io, options, backupMap))
      backupResult match {
        case Success(()) =>
          log.info(s"Successfully backed up up table $tableName")
        case Failure(ex) =>
          if (options.numParallelBackups == 1 && options.failOnFirstError)
            throw ex
          else
            log.error(s"Failed to back up table $tableName", ex)
      }
      (tableName, backupResult)
    }
    pool.shutdown()

    backupResults.filter(_._2.isFailure).foreach {
      case (tableName, ex) =>
        log.error(
          s"Failed to back up table $tableName: Look back in the logs for the full exception. Error: ${ex.toString}")
    }
    if (backupResults.exists(_._2.isFailure))
      1
    else
      0
  }

  def main(args: Array[String]): Unit = {
    val options = BackupOptions
      .parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))

    val session = SparkSession
      .builder()
      .appName("Kudu Table Backup")
      .getOrCreate()

    System.exit(run(options, session))
  }
}
