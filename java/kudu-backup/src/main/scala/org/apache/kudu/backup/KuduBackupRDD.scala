/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    @transient val options: KuduBackupOptions,
    val kuduContext: KuduContext,
    @transient val sc: SparkContext)
    extends RDD[Row](sc, Nil) {

  // Defined here because the options are transient.
  private val keepAlivePeriodMs = options.keepAlivePeriodMs

  // TODO: Split large tablets into smaller scan tokens?
  override protected def getPartitions: Array[Partition] = {
    val client = kuduContext.syncClient

    // Set a hybrid time for the scan to ensure application consistency.
    val timestampMicros = TimeUnit.MILLISECONDS.toMicros(options.timestampMs)
    val hybridTime =
      HybridTimeUtil.physicalAndLogicalToHTTimestamp(timestampMicros, 0)

    // Create the scan tokens for each partition.
    val tokens = client
      .newScanTokenBuilder(table)
      .cacheBlocks(false)
      // TODO: Use fault tolerant scans to get mostly.
      // ordered results when KUDU-2466 is fixed.
      // .setFaultTolerant(true)
      .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
      .readMode(ReadMode.READ_AT_SNAPSHOT)
      .snapshotTimestampRaw(hybridTime)
      .batchSizeBytes(options.scanBatchSize)
      .scanRequestTimeout(options.scanRequestTimeoutMs)
      .prefetching(options.scanPrefetching)
      .keepAlivePeriodMs(options.keepAlivePeriodMs)
      .build()

    tokens.asScala.zipWithIndex.map {
      case (token, index) =>
        // TODO: Support backups from any replica or followers only.
        // Always run on the leader for data locality.
        val leaderLocation = token.getTablet.getLeaderReplica.getRpcHost
        KuduBackupPartition(index, token.serialize(), Array(leaderLocation))
    }.toArray
  }

  // TODO: Do we need a custom spark partitioner for any guarantees?
  // override val partitioner = None

  override def compute(part: Partition, taskContext: TaskContext): Iterator[Row] = {
    val client: KuduClient = kuduContext.syncClient
    val partition: KuduBackupPartition = part.asInstanceOf[KuduBackupPartition]
    // TODO: Get deletes and updates for incremental backups.
    val scanner =
      KuduScanToken.deserializeIntoScanner(partition.scanToken, client)
    // We don't store the RowResult so we can enable the reuseRowResult optimization.
    scanner.setReuseRowResult(true)
    new RowIterator(scanner, kuduContext)
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
private class RowIterator(private val scanner: KuduScanner, val kuduContext: KuduContext)
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

  // TODO: There may be an old KuduRDD implementation where we did some
  // sort of zero copy/object pool pattern for performance (we could use that here).
  override def next(): Row = {
    val rowResult = scannerIterator.next()
    val columnCount = rowResult.getColumnProjection.getColumnCount
    val columns = Array.ofDim[Any](columnCount)
    for (i <- 0 until columnCount) {
      columns(i) = rowResult.getObject(i)
    }
    Row.fromSeq(columns)
  }
}
