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

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * The main class for a Kudu backup spark job.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduBackup {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def run(options: BackupOptions, session: SparkSession): Unit = {
    log.info(s"Backing up to root path: ${options.rootPath}")
    val context =
      new KuduContext(
        options.kuduMasterAddresses,
        session.sparkContext
      )
    val io = new SessionIO(session, options)
    // TODO (KUDU-2786): Make parallel so each table isn't process serially.
    // TODO (KUDU-2787): Handle single table failures.
    options.tables.foreach { tableName =>
      var tableOptions = options.copy() // Copy the options so we can modify them for the table.
      val table = context.syncClient.openTable(tableName)
      val backupPath = io.backupPath(tableName, tableOptions.toMs)
      val metadataPath = io.backupMetadataPath(backupPath)
      log.info(s"Backing up table $tableName to path: $backupPath")

      // Unless we are forcing a full backup or a fromMs was set, find the previous backup and
      // use the `to_ms` metadata as the `from_ms` time for this backup.
      var incremental = false
      if (tableOptions.forceFull) {
        log.info("Performing a full backup, forceFull was set to true")
      } else if (tableOptions.fromMs != BackupOptions.DefaultFromMS) {
        log.info(s"Performing an incremental backup, fromMs was set to ${tableOptions.fromMs}")
        incremental = true
      } else {
        log.info("Looking for a previous backup, forceFull or fromMs options are not set.")
        val graph = io.readBackupGraph(tableName)
        if (graph.hasFullBackup) {
          val base = graph.backupBase
          log.info(s"Setting fromMs to ${base.metadata.getToMs} from backup in path: ${base.path}")
          tableOptions = tableOptions.copy(fromMs = base.metadata.getToMs)
          incremental = true
        } else {
          log.info("No previous backup was found. Starting a full backup.")
          tableOptions = tableOptions.copy(forceFull = true)
        }
      }
      val rdd = new KuduBackupRDD(table, tableOptions, incremental, context, session.sparkContext)
      val df =
        session.sqlContext
          .createDataFrame(rdd, io.dataSchema(table.getSchema, incremental))

      // Write the data to the backup path.
      // The backup path contains the timestampMs and should not already exist.
      val writer = df.write.mode(SaveMode.ErrorIfExists)
      writer
        .format(tableOptions.format)
        .save(backupPath.toString)

      // Generate and output the new metadata for this table.
      // The existence of metadata indicates this backup was successful.
      val tableMetadata = TableMetadata.getTableMetadata(table, tableOptions)
      io.writeTableMetadata(tableMetadata, metadataPath)
    }
  }

  def main(args: Array[String]): Unit = {
    val options = BackupOptions
      .parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))

    val session = SparkSession
      .builder()
      .appName("Kudu Table Backup")
      .getOrCreate()

    run(options, session)
  }
}
