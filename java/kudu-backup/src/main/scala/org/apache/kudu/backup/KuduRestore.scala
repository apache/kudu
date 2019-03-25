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

import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.client.AlterTableOptions
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu.RowConverter
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * The main class for a Kudu restore spark job.
 *
 * Example Usage:
 *   spark-submit --class org.apache.kudu.backup.KuduRestore kudu-backup2_2.11-*.jar \
 *     --kuduMasterAddresses master1-host,master-2-host,master-3-host \
 *     --rootPath hdfs:///kudu/backup/path \
 *     my_kudu_table
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduRestore {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def run(options: RestoreOptions, session: SparkSession): Unit = {
    log.info(s"Restoring from path: ${options.rootPath}")
    val context =
      new KuduContext(
        options.kuduMasterAddresses,
        session.sparkContext
      )
    val io = new SessionIO(session, options)

    // TODO: Make parallel so each table isn't processed serially.
    options.tables.foreach { tableName =>
      // TODO: Consider an option to enforce an exact toMS match.
      val graph = io.readBackupGraph(tableName).filterByTime(options.timestampMs)
      graph.restorePath.backups.foreach { backup =>
        log.info(s"Restoring table $tableName from path: ${backup.path}")
        val isFullRestore = backup.metadata.getFromMs == 0
        val restoreName = s"${backup.metadata.getTableName}${options.tableSuffix}"
        // TODO: Store the full metadata to compare/validate for each applied partial.

        // On the full restore we may need to create the table.
        if (isFullRestore) {
          if (options.createTables) {
            log.info(s"Creating restore table $restoreName")
            createTableRangePartitionByRangePartition(restoreName, backup.metadata, context)
          }
        }
        val table = context.syncClient.openTable(restoreName)
        val restoreSchema = io.dataSchema(table.getSchema)
        val rowActionCol = restoreSchema.fields.last.name

        // TODO: Restrict format option.
        var data = session.sqlContext.read
          .format(backup.metadata.getDataFormat)
          .schema(restoreSchema)
          .load(backup.path.toString)
          // Default the the row action column with a value of "UPSERT" so that the
          // rows from a full backup, which don't have a row action, are upserted.
          .na
          .fill(RowAction.UPSERT.getValue, Seq(rowActionCol))

        // TODO: Expose more configuration options:
        //   (session timeout, consistency mode, flush interval, mutation buffer space)
        data.queryExecution.toRdd.foreachPartition { internalRows =>
          val table = context.syncClient.openTable(restoreName)
          val converter = new RowConverter(table.getSchema, restoreSchema, false)
          val session = context.syncClient.newSession
          session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
          try {
            for (internalRow <- internalRows) {
              // Convert the InternalRows to Rows.
              // This avoids any corruption as reported in SPARK-26880.
              val row = converter.toRow(internalRow)
              // Get the operation type based on the row action column.
              // This will always be the last column in the row.
              val rowActionValue = row.getByte(row.length - 1)
              val rowAction = RowAction.fromValue(rowActionValue)
              // Generate an operation based on the row action.
              val operation = rowAction match {
                case RowAction.UPSERT => table.newUpsert()
                case RowAction.DELETE => table.newDelete()
                case _ => throw new IllegalStateException(s"Unsupported RowAction: $rowAction")
              }
              // Convert the Spark row to a partial row and set it on the operation.
              val partialRow = converter.toPartialRow(row)
              operation.setRow(partialRow)
              session.apply(operation)
            }
          } finally {
            session.close()
          }
          // Fail the task if there are any errors.
          val errorCount = session.getPendingErrors.getRowErrors.length
          if (errorCount > 0) {
            val errors =
              session.getPendingErrors.getRowErrors.take(5).map(_.getErrorStatus).mkString
            throw new RuntimeException(
              s"failed to write $errorCount rows from DataFrame to Kudu; sample errors: $errors")
          }
        }
      }
    }
  }

  // Kudu isn't good at creating a lot of tablets at once, and by default tables may only be created
  // with at most 60 tablets. Additional tablets can be added later by adding range partitions. So,
  // to restore tables with more tablets than that, we need to create the table piece-by-piece. This
  // does so in the simplest way: creating the table with the first range partition, if there is
  // one, and then altering it to add the rest of the partitions, one partition at a time.
  private def createTableRangePartitionByRangePartition(
      restoreName: String,
      metadata: TableMetadataPB,
      context: KuduContext): Unit = {
    // Create the table with the first range partition (or none if there are none).
    val schema = TableMetadata.getKuduSchema(metadata)
    val options = TableMetadata.getCreateTableOptionsWithoutRangePartitions(metadata)
    val bounds = TableMetadata.getRangeBoundPartialRows(metadata)
    bounds.headOption.foreach(bound => {
      val (lower, upper) = bound
      options.addRangePartition(lower, upper)
    })
    context.createTable(restoreName, schema, options)

    // Add the rest of the range partitions through alters.
    bounds.tail.foreach(bound => {
      val (lower, upper) = bound
      val options = new AlterTableOptions()
      options.addRangePartition(lower, upper)
      context.syncClient.alterTable(restoreName, options)
    })
  }

  def main(args: Array[String]): Unit = {
    val options = RestoreOptions
      .parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))

    val session = SparkSession
      .builder()
      .appName("Kudu Table Restore")
      .getOrCreate()

    run(options, session)
  }
}
