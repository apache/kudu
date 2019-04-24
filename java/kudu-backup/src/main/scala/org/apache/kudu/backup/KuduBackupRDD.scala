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

import java.util.concurrent.TimeUnit

import org.apache.kudu.client.AsyncKuduScanner.ReadMode
import org.apache.kudu.client.KuduScannerIterator.NextRowsCallback
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.util.HybridTimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

import scala.collection.JavaConverters._

@InterfaceAudience.Private
@InterfaceStability.Unstable
class KuduBackupRDD private[kudu] (
    @transient val table: KuduTable,
    @transient val options: BackupOptions,
    val incremental: Boolean,
    val kuduContext: KuduContext,
    @transient val sc: SparkContext)
    extends RDD[Row](sc, Nil) {

  // TODO (KUDU-2785): Split large tablets into smaller scan tokens?
  override protected def getPartitions: Array[Partition] = {
    val client = kuduContext.syncClient

    val builder = client
      .newScanTokenBuilder(table)
      .cacheBlocks(false)
      .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
      .readMode(ReadMode.READ_AT_SNAPSHOT)
      .batchSizeBytes(options.scanBatchSize)
      .scanRequestTimeout(options.scanRequestTimeoutMs)
      .prefetching(options.scanPrefetching)
      .keepAlivePeriodMs(options.keepAlivePeriodMs)

    // Set a hybrid time for the scan to ensure application consistency.
    val toMicros = TimeUnit.MILLISECONDS.toMicros(options.toMs)
    val toHTT =
      HybridTimeUtil.physicalAndLogicalToHTTimestamp(toMicros, 0)

    if (incremental) {
      val fromMicros = TimeUnit.MILLISECONDS.toMicros(options.fromMs)
      val fromHTT =
        HybridTimeUtil.physicalAndLogicalToHTTimestamp(fromMicros, 0)
      builder.diffScan(fromHTT, toHTT)
    } else {
      builder.snapshotTimestampRaw(toHTT)
    }

    // Create the scan tokens for each partition.
    val tokens = builder.build()
    tokens.asScala.zipWithIndex.map {
      case (token, index) =>
        // Only list the leader replica as the preferred location if
        // replica selection policy is leader only, to take advantage
        // of scan locality.
        val locations: Array[String] = {
          if (options.scanLeaderOnly) {
            Array(token.getTablet.getLeaderReplica.getRpcHost)
          } else {
            token.getTablet.getReplicas.asScala.map(_.getRpcHost).toArray
          }
        }
        KuduBackupPartition(index, token.serialize(), locations)
    }.toArray
  }

  override def compute(part: Partition, taskContext: TaskContext): Iterator[Row] = {
    val client: KuduClient = kuduContext.syncClient
    val partition: KuduBackupPartition = part.asInstanceOf[KuduBackupPartition]
    val scanner =
      KuduScanToken.deserializeIntoScanner(partition.scanToken, client)
    // We don't store the RowResult so we can enable the reuseRowResult optimization.
    scanner.setReuseRowResult(true)
    new RowIterator(scanner, kuduContext, incremental)
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[KuduBackupPartition].locations
  }
}

private case class KuduBackupPartition(index: Int, scanToken: Array[Byte], locations: Array[String])
    extends Partition

/**
 * This iterator wraps a KuduScanner, converts the returned RowResults into a
 * Spark Row, and allows iterating over those scanned results.
 *
 * The Spark RDD abstraction has an abstract compute method, implemented in KuduBackupRDD,
 * that takes the job partitions and task context and expects to return an Iterator[Row].
 * This implementation facilitates that.
 */
private class RowIterator(
    private val scanner: KuduScanner,
    val kuduContext: KuduContext,
    val incremental: Boolean)
    extends Iterator[Row] {

  private val scannerIterator = scanner.iterator()
  private val nextRowsCallback = new NextRowsCallback {
    override def call(numRows: Int): Unit = {
      if (TaskContext.get().isInterrupted()) {
        throw new RuntimeException("Kudu task interrupted")
      }
      kuduContext.timestampAccumulator.add(kuduContext.syncClient.getLastPropagatedTimestamp)
    }
  }

  override def hasNext: Boolean = {
    scannerIterator.hasNext(nextRowsCallback)
  }

  override def next(): Row = {
    val rowResult = scannerIterator.next()
    val fieldCount = rowResult.getColumnProjection.getColumnCount
    // If this is an incremental backup, the last column is the is_deleted column.
    val columnCount = if (incremental) fieldCount - 1 else fieldCount
    val columns = Array.ofDim[Any](fieldCount)
    for (i <- 0 until columnCount) {
      columns(i) = rowResult.getObject(i)
    }
    // If this is an incremental backup, translate the is_deleted column into
    // the "change_type" column as the last field.
    if (incremental) {
      val rowAction = if (rowResult.isDeleted) {
        RowAction.DELETE.getValue
      } else {
        // If the row is not deleted, we do not know if it was inserted or updated,
        // so we use upsert.
        RowAction.UPSERT.getValue
      }
      columns(fieldCount - 1) = rowAction
    }
    Row.fromSeq(columns)
  }
}
