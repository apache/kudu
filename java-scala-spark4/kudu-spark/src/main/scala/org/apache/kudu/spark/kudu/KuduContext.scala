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

package org.apache.kudu.spark.kudu

import java.security.AccessController
import java.security.PrivilegedAction
import javax.security.auth.Subject
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.util.CollectionAccumulator
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.SparkUtil.kuduSchema
import org.apache.kudu.spark.kudu.SparkUtil._
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.client.AsyncKuduClient.EncryptionPolicy
import org.apache.spark.unsafe.types.ByteArray

/**
 * KuduContext is a serializable container for Kudu client connections.
 *
 * If a Kudu client connection is needed as part of a Spark application, a
 * [[KuduContext]] should be created in the driver, and shared with executors
 * as a serializable field.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SerialVersionUID(1L)
class KuduContext(
    val kuduMaster: String,
    sc: SparkContext,
    val socketReadTimeoutMs: Option[Long],
    val saslProtocolName: Option[String] = None,
    val requireAuthentication: Boolean = false,
    val encryptionPolicy: EncryptionPolicy = EncryptionPolicy.OPTIONAL)
    extends Serializable {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def this(kuduMaster: String, sc: SparkContext) = this(kuduMaster, sc, None)

  // An accumulator that collects all the rows written to Kudu for testing only.
  // Enabled by setting captureRows = true.
  private[kudu] var captureRows = false
  private[kudu] var rowsAccumulator: CollectionAccumulator[Row] =
    sc.collectionAccumulator[Row]("kudu.rows")

  /**
   * A collection of accumulator metrics describing the usage of a KuduContext.
   */
  private[kudu] val numInserts: MapAccumulator[String, Long] =
    new MapAccumulator[String, Long](Math.addExact)
  private[kudu] val numUpserts: MapAccumulator[String, Long] =
    new MapAccumulator[String, Long](Math.addExact)
  private[kudu] val numUpdates: MapAccumulator[String, Long] =
    new MapAccumulator[String, Long](Math.addExact)
  private[kudu] val numDeletes: MapAccumulator[String, Long] =
    new MapAccumulator[String, Long](Math.addExact)
  sc.register(numInserts, "kudu.num_inserts")
  sc.register(numUpserts, "kudu.num_upserts")
  sc.register(numUpdates, "kudu.num_updates")
  sc.register(numDeletes, "kudu.num_deletes")

  // Increments the appropriate metric given an OperationType and a count per table.
  private def addForOperation(count: Long, opType: OperationType, tableName: String): Unit = {
    opType match {
      case org.apache.kudu.spark.kudu.Insert => numInserts.add((tableName, count))
      case org.apache.kudu.spark.kudu.InsertIgnore => numInserts.add((tableName, count))
      case org.apache.kudu.spark.kudu.Upsert => numUpserts.add((tableName, count))
      case org.apache.kudu.spark.kudu.Update => numUpdates.add((tableName, count))
      case org.apache.kudu.spark.kudu.UpdateIgnore => numUpdates.add((tableName, count))
      case org.apache.kudu.spark.kudu.Delete => numDeletes.add((tableName, count))
      case org.apache.kudu.spark.kudu.DeleteIgnore => numDeletes.add((tableName, count))
    }
  }

  /**
   * TimestampAccumulator accumulates the maximum value of client's
   * propagated timestamp of all executors and can only read by the
   * driver.
   */
  private[kudu] class TimestampAccumulator(var timestamp: Long = 0L)
      extends AccumulatorV2[Long, Long] {
    override def isZero: Boolean = {
      timestamp == 0
    }

    override def copy(): AccumulatorV2[Long, Long] = {
      new TimestampAccumulator(timestamp)
    }

    override def reset(): Unit = {
      timestamp = 0L
    }

    override def add(v: Long): Unit = {
      timestamp = timestamp.max(v)
    }

    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
      timestamp = timestamp.max(other.value)

      // Since for every write/scan operation, each executor holds its own copy of
      // client. We need to update the propagated timestamp on the driver based on
      // the latest propagated timestamp from all executors through TimestampAccumulator.
      syncClient.updateLastPropagatedTimestamp(timestampAccumulator.value)
    }

    override def value: Long = timestamp
  }

  val timestampAccumulator = new TimestampAccumulator()
  sc.register(timestampAccumulator)

  val durationHistogram = new HdrHistogramAccumulator()
  sc.register(durationHistogram, "kudu.write_duration")

  @deprecated("Use KuduContext constructor", "1.4.0")
  def this(kuduMaster: String) {
    this(kuduMaster, new SparkContext())
  }

  @transient lazy val syncClient: KuduClient = asyncClient.syncClient()

  @transient lazy val asyncClient: AsyncKuduClient = {
    val c = KuduClientCache
      .getAsyncClient(kuduMaster, saslProtocolName, requireAuthentication, encryptionPolicy)
    if (authnCredentials != null) {
      c.importAuthenticationCredentials(authnCredentials)
    }
    c
  }

  @transient lazy val supportsIgnoreOperations: Boolean = syncClient.supportsIgnoreOperations()

  // Visible for testing.
  private[kudu] val authnCredentials: Array[Byte] = {
    Subject.doAs(KuduContext.getSubject(sc), new PrivilegedAction[Array[Byte]] {
      override def run(): Array[Byte] =
        syncClient.exportAuthenticationCredentials()
    })
  }

  /**
   * Create an RDD from a Kudu table.
   *
   * @param tableName          table to read from
   * @param columnProjection   list of columns to read. Not specifying this at all
   *                           (i.e. setting to null) or setting to the special
   *                           string '*' means to project all columns
   * @return a new RDD that maps over the given table for the selected columns
   */
  def kuduRDD(
      sc: SparkContext,
      tableName: String,
      columnProjection: Seq[String] = Nil,
      options: KuduReadOptions = KuduReadOptions()): RDD[Row] = {
    new KuduRDD(
      this,
      syncClient.openTable(tableName),
      columnProjection.toArray,
      Array(),
      options,
      sc)
  }

  /**
   * Check if kudu table already exists
   *
   * @param tableName name of table to check
   * @return true if table exists, false if table does not exist
   */
  def tableExists(tableName: String): Boolean =
    syncClient.tableExists(tableName)

  /**
   * Delete kudu table
   *
   * @param tableName name of table to delete
   * @return DeleteTableResponse
   */
  def deleteTable(tableName: String): DeleteTableResponse =
    syncClient.deleteTable(tableName)

  /**
   * Creates a kudu table for the given schema. Partitioning can be specified through options.
   *
   * @param tableName table to create
   * @param schema struct schema of table
   * @param keys primary keys of the table
   * @param options replication and partitioning options for the table
   * @return the KuduTable that was created
   */
  def createTable(
      tableName: String,
      schema: StructType,
      keys: Seq[String],
      options: CreateTableOptions): KuduTable = {
    val kuduSchema = createSchema(schema, keys)
    createTable(tableName, kuduSchema, options)
  }

  /**
   * Creates a kudu table for the given schema. Partitioning can be specified through options.
   *
   * @param tableName table to create
   * @param schema schema of table
   * @param options replication and partitioning options for the table
   * @return the KuduTable that was created
   */
  def createTable(tableName: String, schema: Schema, options: CreateTableOptions): KuduTable = {
    syncClient.createTable(tableName, schema, options)
  }

  /**
   * Creates a kudu schema for the given struct schema.
   *
   * @param schema struct schema of table
   * @param keys primary keys of the table
   * @return the Kudu schema
   */
  def createSchema(schema: StructType, keys: Seq[String]): Schema = {
    kuduSchema(schema, keys)
  }

  /** Map Spark SQL type to Kudu type */
  def kuduType(dt: DataType): Type = {
    sparkTypeToKuduType(dt)
  }

  /**
   * Inserts the rows of a [[DataFrame]] into a Kudu table.
   *
   * @param data the data to insert
   * @param tableName the Kudu table to insert into
   * @param writeOptions the Kudu write options
   */
  def insertRows(
      data: DataFrame,
      tableName: String,
      writeOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    log.info(s"inserting into table '$tableName'")
    writeRows(data, tableName, org.apache.kudu.spark.kudu.Insert, writeOptions)
    log.info(s"inserted ${numInserts.value.get(tableName)} rows into table '$tableName'")
  }

  /**
   * Inserts the rows of a [[DataFrame]] into a Kudu table, ignoring any new
   * rows that have a primary key conflict with existing rows.
   *
   * This function call is equivalent to the following, which is preferred:
   * {{{
   * insertRows(data, tableName, new KuduWriteOptions(ignoreDuplicateRowErrors = true))
   * }}}
   *
   * @param data the data to insert into Kudu
   * @param tableName the Kudu table to insert into
   */
  @deprecated(
    "Use KuduContext.insertRows(data, tableName, new KuduWriteOptions(ignoreDuplicateRowErrors = true))",
    "1.8.0")
  def insertIgnoreRows(data: DataFrame, tableName: String): Unit = {
    val writeOptions = KuduWriteOptions(ignoreDuplicateRowErrors = true)
    log.info(s"inserting into table '$tableName'")
    writeRows(data, tableName, org.apache.kudu.spark.kudu.Insert, writeOptions)
    log.info(s"inserted ${numInserts.value.get(tableName)} rows into table '$tableName'")
  }

  /**
   * Upserts the rows of a [[DataFrame]] into a Kudu table.
   *
   * @param data the data to upsert into Kudu
   * @param tableName the Kudu table to upsert into
   * @param writeOptions the Kudu write options
   */
  def upsertRows(
      data: DataFrame,
      tableName: String,
      writeOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    log.info(s"upserting into table '$tableName'")
    writeRows(data, tableName, org.apache.kudu.spark.kudu.Upsert, writeOptions)
    log.info(s"upserted ${numUpserts.value.get(tableName)} rows into table '$tableName'")
  }

  /**
   * Updates a Kudu table with the rows of a [[DataFrame]].
   *
   * @param data the data to update into Kudu
   * @param tableName the Kudu table to update
   * @param writeOptions the Kudu write options
   */
  def updateRows(
      data: DataFrame,
      tableName: String,
      writeOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    log.info(s"updating rows in table '$tableName'")
    writeRows(data, tableName, org.apache.kudu.spark.kudu.Update, writeOptions)
    log.info(s"updated ${numUpdates.value.get(tableName)} rows in table '$tableName'")
  }

  /**
   * Deletes the rows of a [[DataFrame]] from a Kudu table, ignoring any none-existing
   * rows.
   *
   * @param data the data to delete from Kudu
   *             note that only the key columns should be specified for deletes
   * @param tableName The Kudu tabe to delete from
   * @param writeOptions the Kudu write options
   */
  def deleteIgnoreRows(
      data: DataFrame,
      tableName: String,
      writeOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    log.info(s"deleting rows from table '$tableName'")
    writeRows(data, tableName, org.apache.kudu.spark.kudu.DeleteIgnore, writeOptions)
    log.info(
      s"deleted up to ${numDeletes.value.get(tableName)} rows from table '$tableName' using DELETE_IGNORE")
  }

  /**
   * Deletes the rows of a [[DataFrame]] from a Kudu table.
   *
   * @param data the data to delete from Kudu
   *             note that only the key columns should be specified for deletes
   * @param tableName The Kudu tabe to delete from
   * @param writeOptions the Kudu write options
   */
  def deleteRows(
      data: DataFrame,
      tableName: String,
      writeOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    log.info(s"deleting rows from table '$tableName'")
    writeRows(data, tableName, org.apache.kudu.spark.kudu.Delete, writeOptions)
    log.info(s"deleted ${numDeletes.value.get(tableName)} rows from table '$tableName'")
  }

  private[kudu] def writeRows(
      data: DataFrame,
      tableName: String,
      operation: OperationType,
      writeOptions: KuduWriteOptions = new KuduWriteOptions) {
    val schema = data.schema
    // Get the client's last propagated timestamp on the driver.
    val lastPropagatedTimestamp = syncClient.getLastPropagatedTimestamp

    if (writeOptions.handleSchemaDrift) {
      val kuduSchema = syncClient.openTable(tableName).getSchema
      val newColumns = schema.fields.filter(f => !kuduSchema.hasColumn(f.name))
      if (!newColumns.isEmpty) {
        log.info(
          s"adding ${newColumns.length} columns to table '$tableName' to handle schema drift")
        val alter = new AlterTableOptions()
        newColumns.foreach { col =>
          alter.addNullableColumn(col.name, sparkTypeToKuduType(col.dataType))
        }
        try {
          syncClient.alterTable(tableName, alter)
        } catch {
          case e: KuduException =>
            // Ignore the exception if the column already exists due to concurrent
            // applications attempting to handle schema drift.
            if (e.getStatus.isAlreadyPresent) {
              log.info(s"column already exists in table '$tableName' while handling schema drift")
            } else {
              throw e
            }
        }
      }
    }

    // Convert to an RDD and map the InternalRows to Rows.
    // This avoids any corruption as reported in SPARK-26880.
    var rdd = data.queryExecution.toRdd.mapPartitions { rows =>
      val table = syncClient.openTable(tableName)
      val converter = new RowConverter(table.getSchema, schema, writeOptions.ignoreNull)
      rows.map(converter.toRow)
    }

    if (writeOptions.repartition) {
      rdd = repartitionRows(rdd, tableName, schema, writeOptions)
    }

    // If the cluster doesn't support the INSERT_IGNORE operation fallback to the old
    // session based style.
    var adjustedOperation = operation
    var adjustedWriteOptions = writeOptions
    if (operation == org.apache.kudu.spark.kudu.InsertIgnore && !supportsIgnoreOperations) {
      adjustedOperation = org.apache.kudu.spark.kudu.Insert;
      adjustedWriteOptions = writeOptions.copy(ignoreDuplicateRowErrors = true);
    }

    // Write the rows for each Spark partition.
    rdd.foreachPartition(iterator => {
      val pendingErrors = writePartitionRows(
        iterator,
        schema,
        tableName,
        adjustedOperation,
        lastPropagatedTimestamp,
        adjustedWriteOptions)
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
    })
    log.info(s"completed $operation ops: duration histogram: $durationHistogram")
  }

  private[spark] def repartitionRows(
      rdd: RDD[Row],
      tableName: String,
      schema: StructType,
      writeOptions: KuduWriteOptions): RDD[Row] = {
    val partitionCount = getPartitionCount(tableName)
    val sparkPartitioner = new Partitioner {
      override def numPartitions: Int = partitionCount
      override def getPartition(key: Any): Int = {
        key.asInstanceOf[(Int, Row)]._1
      }
    }

    // Key the rows by the Kudu partition index using the KuduPartitioner and the
    // table's primary key. This allows us to re-partition and sort the columns.
    val keyedRdd = rdd.mapPartitions { rows =>
      val table = syncClient.openTable(tableName)
      val converter = new RowConverter(table.getSchema, schema, writeOptions.ignoreNull)
      val partitioner = new KuduPartitioner.KuduPartitionerBuilder(table).build()
      rows.map { row =>
        val partialRow = converter.toPartialRow(row)
        val partitionIndex = partitioner.partitionRow(partialRow)
        ((partitionIndex, partialRow.encodePrimaryKey()), row)
      }
    }

    // Define an implicit Ordering trait for the encoded primary key
    // to enable rdd sorting functions below.
    implicit val byteArrayOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
      def compare(x: Array[Byte], y: Array[Byte]): Int = {
        ByteArray.compareBinary(x, y)
      }
    }

    // Partition the rows by the Kudu partition index to ensure the Spark partitions
    // match the Kudu partitions. This will make the number of Spark tasks match the number
    // of Kudu partitions. Optionally sort while repartitioning.
    // TODO: At some point we may want to support more or less tasks while still partitioning.
    val shuffledRDD = if (writeOptions.repartitionSort) {
      keyedRdd.repartitionAndSortWithinPartitions(sparkPartitioner)
    } else {
      keyedRdd.partitionBy(sparkPartitioner)
    }
    // Drop the partitioning key.
    shuffledRDD.map { case (_, row) => row }
  }

  private def writePartitionRows(
      rows: Iterator[Row],
      schema: StructType,
      tableName: String,
      opType: OperationType,
      lastPropagatedTimestamp: Long,
      writeOptions: KuduWriteOptions): RowErrorsAndOverflowStatus = {
    // Since each executor has its own KuduClient, update executor's propagated timestamp
    // based on the last one on the driver.
    syncClient.updateLastPropagatedTimestamp(lastPropagatedTimestamp)
    val table = syncClient.openTable(tableName)
    val rowConverter = new RowConverter(table.getSchema, schema, writeOptions.ignoreNull)
    val session: KuduSession = syncClient.newSession
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    if (writeOptions.ignoreDuplicateRowErrors) {
      log.warn(
        "kudu.ignoreDuplicateRowErrors is deprecated and slow. Use the insert_ignore operation instead.")
    }
    session.setIgnoreAllDuplicateRows(writeOptions.ignoreDuplicateRowErrors)
    var numRows = 0
    log.info(s"applying operations of type '${opType.toString}' to table '$tableName'")
    val startTime = System.currentTimeMillis()
    try {
      for (row <- rows) {
        if (captureRows) {
          rowsAccumulator.add(row)
        }
        val partialRow = rowConverter.toPartialRow(row)
        val operation = opType.operation(table)
        operation.setRow(partialRow)
        session.apply(operation)
        numRows += 1
      }
    } finally {
      session.close()
      // Update timestampAccumulator with the client's last propagated
      // timestamp on each executor.
      timestampAccumulator.add(syncClient.getLastPropagatedTimestamp)
      addForOperation(numRows, opType, tableName)
      val elapsedTime = (System.currentTimeMillis() - startTime).toInt
      durationHistogram.add(elapsedTime)
      log.info(s"applied $numRows ${opType}s to table '$tableName' in ${elapsedTime}ms")
    }
    session.getPendingErrors
  }

  private def getPartitionCount(tableName: String): Int = {
    val table = syncClient.openTable(tableName)
    val partitioner = new KuduPartitioner.KuduPartitionerBuilder(table).build()
    partitioner.numPartitions()
  }
}

private object KuduContext {
  val log: Logger = LoggerFactory.getLogger(classOf[KuduContext])

  /**
   * Returns a new Kerberos-authenticated [[Subject]] if the Spark context contains
   * principal and keytab options, otherwise returns the currently active subject.
   *
   * The keytab and principal options should be set when deploying a Spark
   * application in cluster mode with Yarn against a secure Kudu cluster. Spark
   * internally will grab HDFS and HBase delegation tokens (see
   * [[org.apache.spark.deploy.SparkSubmit]]), so we do something similar.
   *
   * This method can only be called on the driver, where the SparkContext is
   * available.
   *
   * @return A Kerberos-authenticated subject if the Spark context contains
   *         principal and keytab options, otherwise returns the currently
   *         active subject
   */
  private def getSubject(sc: SparkContext): Subject = {
    val subject = Subject.getSubject(AccessController.getContext)

    val principal =
      sc.getConf.getOption("spark.yarn.principal").getOrElse(return subject)
    val keytab =
      sc.getConf.getOption("spark.yarn.keytab").getOrElse(return subject)

    log.info(s"Logging in as principal $principal with keytab $keytab")

    val conf = new Configuration {
      override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
        val options = Map(
          "principal" -> principal,
          "keyTab" -> keytab,
          "useKeyTab" -> "true",
          "useTicketCache" -> "false",
          "doNotPrompt" -> "true",
          "refreshKrb5Config" -> "true"
        )

        Array(
          new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options.asJava))
      }
    }

    val loginContext = new LoginContext("kudu-spark", new Subject(), null, conf)
    loginContext.login()
    loginContext.getSubject
  }
}

private object KuduClientCache {
  val log: Logger = LoggerFactory.getLogger(KuduClientCache.getClass)

  private case class CacheValue(kuduClient: AsyncKuduClient, shutdownHookHandle: Runnable)

  /**
   * Set to
   * [[org.apache.spark.util.ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY]].
   * The client instances are closed through the JVM shutdown hook
   * mechanism in order to make sure that any unflushed writes are cleaned up
   * properly. Spark has no shutdown notifications.
   */
  private val ShutdownHookPriority = 100

  private val clientCache = new mutable.HashMap[String, CacheValue]()

  // Visible for testing.
  private[kudu] def clearCacheForTests() = {
    clientCache.values.foreach {
      case cacheValue =>
        try {
          cacheValue.kuduClient.close()
        } catch {
          case e: Exception => log.warn("Error while shutting down the test client", e);
        }

        // A client may only be closed once, so once we've close this client,
        // we mustn't close it again at shutdown time.
        ShutdownHookManager.get().removeShutdownHook(cacheValue.shutdownHookHandle)
    }
    clientCache.clear()
  }

  def getAsyncClient(
      kuduMaster: String,
      saslProtocolName: Option[String],
      requireAuthentication: Boolean = false,
      encryptionPolicy: EncryptionPolicy = EncryptionPolicy.OPTIONAL): AsyncKuduClient = {
    clientCache.synchronized {
      if (!clientCache.contains(kuduMaster)) {
        val builder = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster)
        if (saslProtocolName.nonEmpty) {
          builder.saslProtocolName(saslProtocolName.get)
        }
        builder.requireAuthentication(requireAuthentication)
        builder.encryptionPolicy(encryptionPolicy)
        val asyncClient = builder.build()
        val hookHandle = new Runnable {
          override def run(): Unit = asyncClient.close()
        }
        ShutdownHookManager.get().addShutdownHook(hookHandle, ShutdownHookPriority)
        val cacheValue = CacheValue(asyncClient, hookHandle)
        clientCache.put(kuduMaster, cacheValue)
      }
      return clientCache(kuduMaster).kuduClient
    }
  }
}
