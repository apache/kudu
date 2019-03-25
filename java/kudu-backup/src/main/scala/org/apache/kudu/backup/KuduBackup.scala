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
 *
 * Example Usage:
 *   spark-submit --class org.apache.kudu.backup.KuduBackup kudu-backup2_2.11-*.jar \
 *     --kuduMasterAddresses master1-host,master-2-host,master-3-host \
 *     --rootPath hdfs:///kudu/backup/path \
 *     my_kudu_table
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
    // TODO: Make parallel so each table isn't process serially?
    options.tables.foreach { tableName =>
      var tableOptions = options.copy() // Copy the options so we can modify them for the table.
      val table = context.syncClient.openTable(tableName)
      val backupPath = io.backupPath(tableName, tableOptions.toMs)
      val metadataPath = io.backupMetadataPath(backupPath)
      log.info(s"Backing up table $tableName to path: $backupPath")

      // Unless we are forcing a full backup or a fromMs was set, find the previous backup and
      // use the `to_ms` metadata as the `from_ms` time for this backup.
      if (!tableOptions.forceFull || tableOptions.fromMs != 0) {
        log.info("No fromMs option set, looking for a previous backup.")
        val graph = io.readBackupGraph(tableName)
        if (graph.hasFullBackup) {
          val base = graph.backupBase
          log.info(s"Setting fromMs to ${base.metadata.getToMs} from backup in path: ${base.path}")
          tableOptions = tableOptions.copy(fromMs = base.metadata.getToMs)
        } else {
          log.info("No full backup was found. Starting a full backup.")
          tableOptions = tableOptions.copy(fromMs = 0)
        }
      }
      val rdd = new KuduBackupRDD(table, tableOptions, context, session.sparkContext)
      val df =
        session.sqlContext
          .createDataFrame(rdd, io.dataSchema(table.getSchema, options.isIncremental))

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
