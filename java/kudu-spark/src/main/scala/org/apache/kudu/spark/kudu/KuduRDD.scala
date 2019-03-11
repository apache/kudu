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
package org.apache.kudu.spark.kudu

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.util.LongAccumulator
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.apache.kudu.client
import org.apache.kudu.client._
import org.apache.kudu.client.KuduScannerIterator.NextRowsCallback

/**
 * A Resilient Distributed Dataset backed by a Kudu table.
 *
 * To construct a KuduRDD, use [[KuduContext#kuduRDD]] or a Kudu DataSource.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
class KuduRDD private[kudu] (
    val kuduContext: KuduContext,
    @transient val table: KuduTable,
    @transient val projectedCols: Array[String],
    @transient val predicates: Array[client.KuduPredicate],
    @transient val options: KuduReadOptions,
    @transient val sc: SparkContext)
    extends RDD[Row](sc, Nil) {

  // Defined here because the options are transient.
  private val keepAlivePeriodMs = options.keepAlivePeriodMs

  // A metric for the rows read from Kudu for this RDD.
  // TODO(wdberkeley): Add bytes read if it becomes available from the Java client.
  private[kudu] val rowsRead = sc.longAccumulator("kudu.rows_read")

  override protected def getPartitions: Array[Partition] = {
    val builder = kuduContext.syncClient
      .newScanTokenBuilder(table)
      .batchSizeBytes(options.batchSize)
      .setProjectedColumnNames(projectedCols.toSeq.asJava)
      .setFaultTolerant(options.faultTolerantScanner)
      .keepAlivePeriodMs(options.keepAlivePeriodMs)
      .cacheBlocks(true)

    // A scan is partitioned to multiple ones. If scan locality is enabled,
    // each will take place at the closet replica from the executor. In this
    // case, to ensure the consistency of such scan, we use READ_AT_SNAPSHOT
    // read mode without setting a timestamp.
    builder.replicaSelection(options.scanLocality)
    if (options.scanLocality == ReplicaSelection.CLOSEST_REPLICA) {
      builder.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
    }

    options.scanRequestTimeoutMs.foreach { timeout =>
      builder.scanRequestTimeout(timeout)
    }

    for (predicate <- predicates) {
      builder.addPredicate(predicate)
    }

    val tokens = builder.build().asScala
    tokens.zipWithIndex.map {
      case (token, index) =>
        // Only list the leader replica as the preferred location if
        // replica selection policy is leader only, to take advantage
        // of scan locality.
        var locations: Array[String] = null
        if (options.scanLocality == ReplicaSelection.LEADER_ONLY) {
          locations = Array(token.getTablet.getLeaderReplica.getRpcHost)
        } else {
          locations = token.getTablet.getReplicas.asScala.map(_.getRpcHost).toArray
        }
        new KuduPartition(index, token.serialize(), locations)
    }.toArray
  }

  override def compute(part: Partition, taskContext: TaskContext): Iterator[Row] = {
    val client: KuduClient = kuduContext.syncClient
    val partition: KuduPartition = part.asInstanceOf[KuduPartition]
    val scanner =
      KuduScanToken.deserializeIntoScanner(partition.scanToken, client)
    // We don't store the RowResult so we can enable the reuseRowResult optimization.
    scanner.setReuseRowResult(true)
    new RowIterator(scanner, kuduContext, rowsRead)
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[KuduPartition].locations
  }
}

/**
 * A Spark SQL [[Partition]] which wraps a [[KuduScanToken]].
 */
private class KuduPartition(
    val index: Int,
    val scanToken: Array[Byte],
    val locations: Array[String])
    extends Partition {}

/**
 * A Spark SQL [[Row]] iterator which wraps a [[KuduScanner]].
 * @param scanner the wrapped scanner
 * @param kuduContext the kudu context
 * @param rowsRead an accumulator to track the number of rows read from Kudu
 */
private class RowIterator(
    val scanner: KuduScanner,
    val kuduContext: KuduContext,
    val rowsRead: LongAccumulator)
    extends Iterator[Row] {

  private val scannerIterator = scanner.iterator()
  private val nextRowsCallback = new NextRowsCallback {
    override def call(numRows: Int): Unit = {
      if (TaskContext.get().isInterrupted()) {
        throw new RuntimeException("Kudu task interrupted")
      }
      kuduContext.timestampAccumulator.add(kuduContext.syncClient.getLastPropagatedTimestamp)
      rowsRead.add(numRows)
    }
  }

  override def hasNext: Boolean = {
    scannerIterator.hasNext(nextRowsCallback)
  }

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
