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

import org.apache.kudu.client._
import org.apache.kudu.Type
import org.apache.kudu.client

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
    new RowIterator(scanner, kuduContext, keepAlivePeriodMs, rowsRead)
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
 * @param keepAlivePeriodMs the period in which to call the keepAlive on the scanners
 * @param rowsRead an accumulator to track the number of rows read from Kudu
 */
private class RowIterator(
    val scanner: KuduScanner,
    val kuduContext: KuduContext,
    val keepAlivePeriodMs: Long,
    val rowsRead: LongAccumulator)
    extends Iterator[Row] {

  private var currentIterator: RowResultIterator = RowResultIterator.empty
  private var lastKeepAliveTimeMs = System.currentTimeMillis()

  /**
   * Calls the keepAlive API on the current scanner if the keepAlivePeriodMs has passed.
   */
  private def KeepKuduScannerAlive(): Unit = {
    val now = System.currentTimeMillis
    if (now >= lastKeepAliveTimeMs + keepAlivePeriodMs && !scanner.isClosed) {
      scanner.keepAlive()
      lastKeepAliveTimeMs = now
    }
  }

  override def hasNext: Boolean = {
    while (!currentIterator.hasNext && scanner.hasMoreRows) {
      if (TaskContext.get().isInterrupted()) {
        throw new RuntimeException("Kudu task interrupted")
      }
      currentIterator = scanner.nextRows()
      // Update timestampAccumulator with the client's last propagated
      // timestamp on each executor.
      kuduContext.timestampAccumulator.add(kuduContext.syncClient.getLastPropagatedTimestamp)
      rowsRead.add(currentIterator.getNumRows)
    }
    KeepKuduScannerAlive()
    currentIterator.hasNext
  }

  private def get(rowResult: RowResult, i: Int): Any = {
    if (rowResult.isNull(i)) null
    else
      rowResult.getColumnType(i) match {
        case Type.BOOL => rowResult.getBoolean(i)
        case Type.INT8 => rowResult.getByte(i)
        case Type.INT16 => rowResult.getShort(i)
        case Type.INT32 => rowResult.getInt(i)
        case Type.INT64 => rowResult.getLong(i)
        case Type.UNIXTIME_MICROS => rowResult.getTimestamp(i)
        case Type.FLOAT => rowResult.getFloat(i)
        case Type.DOUBLE => rowResult.getDouble(i)
        case Type.STRING => rowResult.getString(i)
        case Type.BINARY => rowResult.getBinaryCopy(i)
        case Type.DECIMAL => rowResult.getDecimal(i)
      }
  }

  override def next(): Row = {
    val rowResult = currentIterator.next()
    val columnCount = rowResult.getColumnProjection.getColumnCount
    val columns = Array.ofDim[Any](columnCount)
    for (i <- 0 until columnCount) {
      columns(i) = get(rowResult, i)
    }
    Row.fromSeq(columns)
  }
}
