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
import org.apache.kudu.client.KuduPartitioner
import org.apache.kudu.client.Partition
import org.apache.kudu.client.RangePartitionBound
import org.apache.kudu.client.RangePartitionWithCustomHashSchema
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu.RowConverter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport
import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.CollectionConverters.seqIsParallelizable
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * The main class for a Kudu restore spark job.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduRestore {
  val log: Logger = LoggerFactory.getLogger(getClass)
  val ImpalaPrefix = "impala::"

  /**
   * Returns the table name in which the data will be restored considering the flags removeImpalaPrefix,
   * newDatabaseName and tableSuffix
   */
  def getRestoreTableName(fullTableName: String, options: RestoreOptions): String = {
    // Break the table down into prefix::databaseName.tableName
    var prefix = ""
    var databaseName = ""
    var tableName = fullTableName
    val hasImpalaPrefix = tableName.startsWith(ImpalaPrefix)
    if (hasImpalaPrefix) {
      prefix = ImpalaPrefix
      tableName = tableName.substring(ImpalaPrefix.length)
    }
    val hasDatabase = tableName.contains(".")
    if (hasDatabase) {
      databaseName = tableName.substring(0, tableName.indexOf(".") + 1)
      tableName = tableName.substring(tableName.indexOf(".") + 1)
    }

    // If the user does not want the Impala prefix, drop it
    if (options.removeImpalaPrefix) {
      prefix = ""
    }

    // If there is a databaseName specified by the user, use that
    if (options.newDatabaseName.nonEmpty) {
      databaseName = options.newDatabaseName.concat(".")
    }

    s"${prefix}${databaseName}${tableName}${options.tableSuffix}"
  }

  private def doRestore(
      tableName: String,
      context: KuduContext,
      session: SparkSession,
      io: BackupIO,
      options: RestoreOptions,
      backupMap: Map[String, BackupGraph]): Unit = {
    if (!backupMap.contains(tableName)) {
      throw new RuntimeException(s"No valid backups found for table: $tableName")
    }
    val graph = backupMap(tableName)
    val restorePath = graph.restorePath
    val lastMetadata = restorePath.backups.last.metadata
    val restoreName = getRestoreTableName(lastMetadata.getTableName, options)
    val numJobs = restorePath.backups.size
    var currentJob = 1
    restorePath.backups.foreach { backup =>
      session.sparkContext.setJobDescription(s"Kudu Restore($currentJob/$numJobs): $tableName")

      log.info(s"Restoring table $tableName from path: ${backup.path}")
      val metadata = backup.metadata
      val isFullRestore = metadata.getFromMs == 0
      // TODO (KUDU-2788): Store the full metadata to compare/validate for each applied partial.

      // On the full restore we may need to create the table.
      if (isFullRestore) {
        if (options.createTables) {
          log.info(s"Creating restore table $restoreName")
          // We use the last schema in the restore path when creating the table to
          // ensure the table is created in its final state.
          createTableRangePartitionByRangePartition(
            restoreName,
            lastMetadata,
            options.restoreOwner,
            context)
        }
      }
      val backupSchema = BackupUtils.dataSchema(TableMetadata.getKuduSchema(metadata))
      val rowActionCol = backupSchema.fields.last.name

      var data = session.sqlContext.read
        .format(metadata.getDataFormat)
        .schema(backupSchema)
        .load(backup.path.toString)
        // Default the the row action column with a value of "UPSERT" so that the
        // rows from a full backup, which don't have a row action, are upserted.
        // TODO(ghenke): Consider using INSERT_IGNORE for full backups.
        .na
        .fill(RowAction.UPSERT.getValue, Seq(rowActionCol))

      // Adjust for dropped and renamed columns.
      data = adjustSchema(data, metadata, lastMetadata, rowActionCol)
      val restoreSchema = data.schema

      // Write the data to Kudu.
      data.queryExecution.toRdd.foreachPartition { internalRows =>
        val table = context.syncClient.openTable(restoreName)
        val converter = new RowConverter(table.getSchema, restoreSchema, false)
        val partitioner = createPartitionFilter(metadata, lastMetadata)
        val session = context.syncClient.newSession
        session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
        // In the case of task retries we need to ignore NotFound errors for deleted rows.
        // This can't occur if DELETE_IGNORE is used, but still needs to be set in the case
        // DELETE is used for backwards compatibility.
        session.setIgnoreAllNotFoundRows(true)
        try for (internalRow <- internalRows) {
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
            case RowAction.DELETE => {
              if (context.supportsIgnoreOperations) {
                table.newDeleteIgnore()
              } else {
                table.newDelete()
              }
            }
            case _ => throw new IllegalStateException(s"Unsupported RowAction: $rowAction")
          }
          // Convert the Spark row to a partial row and set it on the operation.
          val partialRow = converter.toPartialRow(row)
          // Drop rows that are not covered by the partitioner. This is how we
          // detect a partition which was dropped between backups and filter
          // out the rows from that dropped partition.
          if (partitioner.isCovered(partialRow)) {
            operation.setRow(partialRow)
            session.apply(operation)
          }
        } finally {
          session.close()
        }
        // Fail the task if there are any errors.
        // It is important to capture all of the errors via getRowErrors and then check
        // the length because each call to session.getPendingErrors clears the ErrorCollector.
        val pendingErrors = session.getPendingErrors
        if (pendingErrors.getRowErrors.nonEmpty) {
          val errors = pendingErrors.getRowErrors
          val sample = errors.take(5).map(_.getErrorStatus).mkString
          if (pendingErrors.isOverflowed) {
            throw new RuntimeException(
              s"PendingErrors overflowed. Failed to write at least ${errors.length} rows " +
                s"to Kudu; Sample errors: $sample")
          } else {
            throw new RuntimeException(
              s"Failed to write ${errors.length} rows to Kudu; Sample errors: $sample")
          }
        }
      }
      currentJob += 1
    }
  }

  def run(options: RestoreOptions, session: SparkSession): Boolean = {
    // Set the job group for all the spark restore jobs.
    // Note: The job description will be overridden by each Kudu table job.
    session.sparkContext.setJobGroup(s"Kudu Restore @ ${options.timestampMs}", "Kudu Restore")

    log.info(s"Restoring from path: ${options.rootPath}")
    val context =
      new KuduContext(
        options.kuduMasterAddresses,
        session.sparkContext
      )
    val io = new BackupIO(session.sparkContext.hadoopConfiguration, options.rootPath)

    // Read the required backup metadata.
    val backupGraphs = io.readBackupGraphsByTableName(options.tables, options.timestampMs)

    // Key the backupMap by the last table name.
    val backupMap = backupGraphs
      .groupBy(_.restorePath.tableName)
      .mapValues(_.maxBy(_.restorePath.toMs))

    // Parallelize the processing. Managing resources of parallel restore jobs is very complex, so
    // only the simplest possible thing is attempted. Kudu trusts Spark to manage resources.
    // TODO (KUDU-2832): If the job fails to restore a table it may still create the table, which
    //  will cause subsequent restores to fail unless the table is deleted or the restore suffix is
    //  changed. We ought to try to clean up the mess when a failure happens.
    val parallelTables = options.tables.par
    parallelTables.tasksupport = new ForkJoinTaskSupport(
      new ForkJoinPool(options.numParallelRestores))
    val restoreResults = parallelTables.map { tableName =>
      val restoreResult =
        Try(doRestore(tableName, context, session, io, options, backupMap.toMap))
      restoreResult match {
        case Success(()) =>
          log.info(s"Successfully restored table $tableName")
        case Failure(ex) =>
          if (options.numParallelRestores == 1 && options.failOnFirstError)
            throw ex
          else
            log.error(s"Failed to restore table $tableName", ex)
      }
      (tableName, restoreResult)
    }

    restoreResults.filter(_._2.isFailure).foreach {
      case (tableName, ex) =>
        log.error(
          s"Failed to restore table $tableName: Look back in the logs for the full exception. Error: ${ex.toString}")
    }
    !restoreResults.exists(_._2.isFailure)
  }

  // Kudu isn't good at creating a lot of tablets at once, and by default tables may only be created
  // with at most 60 tablets. Additional tablets can be added later by adding range partitions. So,
  // to restore tables with more tablets than that, we need to create the table piece-by-piece. This
  // does so in the simplest way: creating the table with the first range partition, if there is
  // one, and then altering it to add the rest of the partitions, one partition at a time.
  private def createTableRangePartitionByRangePartition(
      restoreName: String,
      metadata: TableMetadataPB,
      restoreOwner: Boolean,
      context: KuduContext): Unit = {
    // Create the table with the first range partition (or none if there are none).
    val schema = TableMetadata.getKuduSchema(metadata)
    val options = TableMetadata.getCreateTableOptionsWithoutRangePartitions(metadata, restoreOwner)
    // Returns the range bounds of the ranges that contain the table wide hash schema.
    val boundsWithoutHashSchema = TableMetadata.getRangeBoundPartialRows(metadata)
    // Returns the range bounds and hash schema of the ranges that contain a custom hash schema.
    val boundsWithCustomHashSchema =
      TableMetadata.getRangeBoundsPartialRowsWithHashSchemas(metadata)
    if (boundsWithoutHashSchema.nonEmpty) {
      // Adds the first range partition with table wide hash schema through create.
      boundsWithoutHashSchema.headOption.foreach(bound => {
        val (lower, upper) = bound
        options.addRangePartition(lower, upper)
      })
      context.createTable(restoreName, schema, options)

      // Add the rest of the range partitions with table wide hash schema through alters.
      boundsWithoutHashSchema.tail.foreach(bound => {
        val (lower, upper) = bound
        val options = new AlterTableOptions()
        options.addRangePartition(lower, upper)
        context.syncClient.alterTable(restoreName, options)
      })

      // Adds range partitions with custom hash schema through alters.
      boundsWithCustomHashSchema.foreach(bound => {
        val rangePartition = new RangePartitionWithCustomHashSchema(
          bound.lowerBound,
          bound.upperBound,
          RangePartitionBound.INCLUSIVE_BOUND,
          RangePartitionBound.EXCLUSIVE_BOUND)
        bound.hashSchemas.asScala.foreach { hp =>
          val columnNames = hp.getColumnIds.asScala.map { id =>
            schema.getColumnByIndex(id).getName
          }
          rangePartition.addHashPartitions(columnNames.asJava, hp.getNumBuckets, hp.getSeed)
        }
        val options = new AlterTableOptions()
        options.addRangePartition(rangePartition)
        context.syncClient.alterTable(restoreName, options)
      })
    } else if (boundsWithCustomHashSchema.nonEmpty) {
      // Adds first range partition with custom hash schema through create.
      boundsWithCustomHashSchema.headOption.foreach(bound => {
        val rangePartition = new RangePartitionWithCustomHashSchema(
          bound.lowerBound,
          bound.upperBound,
          RangePartitionBound.INCLUSIVE_BOUND,
          RangePartitionBound.EXCLUSIVE_BOUND)
        bound.hashSchemas.asScala.foreach { hp =>
          val columnNames = hp.getColumnIds.asScala.map { id =>
            schema.getColumnByIndex(id).getName
          }
          rangePartition.addHashPartitions(columnNames.asJava, hp.getNumBuckets, hp.getSeed)
        }
        options.addRangePartition(rangePartition)
      })
      context.createTable(restoreName, schema, options)
      // Adds rest of range partitions with custom hash schema through alters.
      boundsWithCustomHashSchema.tail.foreach(bound => {
        val rangePartition = new RangePartitionWithCustomHashSchema(
          bound.lowerBound,
          bound.upperBound,
          RangePartitionBound.INCLUSIVE_BOUND,
          RangePartitionBound.EXCLUSIVE_BOUND)
        bound.hashSchemas.asScala.foreach { hp =>
          val columnNames = hp.getColumnIds.asScala.map { id =>
            schema.getColumnByIndex(id).getName
          }
          rangePartition.addHashPartitions(columnNames.asJava, hp.getNumBuckets, hp.getSeed)
        }
        val options = new AlterTableOptions()
        options.addRangePartition(rangePartition)
        context.syncClient.alterTable(restoreName, options)
      })
    }
  }

  /**
   * Returns a modified DataFrame with columns adjusted to match the lastMetadata.
   */
  private def adjustSchema(
      df: DataFrame,
      currentMetadata: TableMetadataPB,
      lastMetadata: TableMetadataPB,
      rowActionCol: String): DataFrame = {
    log.info("Adjusting columns to handle alterations")
    val idToName = lastMetadata.getColumnIdsMap.asScala.map(_.swap)
    // Ignore the rowActionCol, which isn't a real column.
    val currentColumns = currentMetadata.getColumnIdsMap.asScala.filter(_._1 != rowActionCol)
    var result = df
    // First drop all the columns that no longer exist.
    // This is required to be sure a rename doesn't collide with an old column.
    currentColumns.foreach {
      case (colName, id) =>
        if (!idToName.contains(id)) {
          // If the last metadata doesn't contain the id, the column is dropped.
          log.info(s"Dropping the column $colName from backup data")
          result = result.drop(colName)
        }
    }
    // Then rename all the columns that were renamed in the last metadata.
    currentColumns.foreach {
      case (colName, id) =>
        if (idToName.contains(id) && idToName(id) != colName) {
          // If the final name doesn't match the current name, the column is renamed.
          log.info(s"Renamed the column $colName to ${idToName(id)} in backup data")
          result = result.withColumnRenamed(colName, idToName(id))
        }
    }
    result
  }

  /**
   * Creates a KuduPartitioner that can be used to filter out rows for the current
   * backup data which no longer apply to partitions in the last metadata.
   *
   * In order to do this, tablet metadata are compared in the current metadata to the
   * last metadata. Tablet IDs that are not in the final metadata are filtered out and
   * the remaining tablet metadata is used to create a KuduPartitioner. The resulting
   * KuduPartitioner can then be used to filter out rows that are no longer valid
   * because those rows will fall into a non-covered range.
   */
  private def createPartitionFilter(
      currentMetadata: TableMetadataPB,
      lastMetadata: TableMetadataPB): KuduPartitioner = {
    val lastTablets = lastMetadata.getTabletsMap
    val validTablets =
      currentMetadata.getTabletsMap.asScala.flatMap {
        case (id, pm) =>
          if (lastTablets.containsKey(id)) {
            // Create the partition object needed for the KuduPartitioner.
            val partition = new Partition(
              pm.getPartitionKeyStart.toByteArray,
              pm.getPartitionKeyEnd.toByteArray,
              pm.getHashBucketsList)
            Some((id, partition))
          } else {
            // Ignore tablets that are no longer valid
            None
          }
      }
    val partitionSchema = TableMetadata.getPartitionSchema(currentMetadata)
    new KuduPartitioner(partitionSchema, validTablets.asJava)
  }

  def main(args: Array[String]): Unit = {
    val options = RestoreOptions
      .parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))

    val session = SparkSession
      .builder()
      .appName("Kudu Table Restore")
      .getOrCreate()

    val isRunSuccessful: Boolean = run(options, session)
    if (!isRunSuccessful) {
      throw new RuntimeException("Kudu Table Restore application failed!")
    }

    session.stop()
  }
}
