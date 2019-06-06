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

import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.KuduReadOptions._
import org.apache.kudu.spark.kudu.KuduWriteOptions._
import org.apache.kudu.spark.kudu.SparkUtil._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
 * Data source for integration with Spark's [[DataFrame]] API.
 *
 * Serves as a factory for [[KuduRelation]] instances for Spark. Spark will
 * automatically look for a [[RelationProvider]] implementation named
 * `DefaultSource` when the user specifies the path of a source during DDL
 * operations through [[org.apache.spark.sql.DataFrameReader.format]].
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class DefaultSource
    extends DataSourceRegister with RelationProvider with CreatableRelationProvider
    with SchemaRelationProvider with StreamSinkProvider {

  val TABLE_KEY = "kudu.table"
  val KUDU_MASTER = "kudu.master"
  val OPERATION = "kudu.operation"
  val FAULT_TOLERANT_SCANNER = "kudu.faultTolerantScan"
  val SCAN_LOCALITY = "kudu.scanLocality"
  val IGNORE_NULL = "kudu.ignoreNull"
  val IGNORE_DUPLICATE_ROW_ERRORS = "kudu.ignoreDuplicateRowErrors"
  val REPARTITION = "kudu.repartition"
  val REPARTITION_SORT = "kudu.repartition.sort"
  val SCAN_REQUEST_TIMEOUT_MS = "kudu.scanRequestTimeoutMs"
  val SOCKET_READ_TIMEOUT_MS = "kudu.socketReadTimeoutMs"
  val BATCH_SIZE = "kudu.batchSize"
  val KEEP_ALIVE_PERIOD_MS = "kudu.keepAlivePeriodMs"
  val SPLIT_SIZE_BYTES = "kudu.splitSizeBytes"

  /**
   * A nice alias for the data source so that when specifying the format
   * "kudu" can be used in place of "org.apache.kudu.spark.kudu".
   * Note: This class is discovered by Spark via the entry in
   * `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
   */
  override def shortName(): String = "kudu"

  /**
   * Construct a BaseRelation using the provided context and parameters.
   *
   * @param sqlContext SparkSQL context
   * @param parameters parameters given to us from SparkSQL
   * @return           a BaseRelation Object
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Construct a BaseRelation using the provided context, parameters and schema.
   *
   * @param sqlContext SparkSQL context
   * @param parameters parameters given to us from SparkSQL
   * @param schema     the schema used to select columns for the relation
   * @return           a BaseRelation Object
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val tableName = getTableName(parameters)
    val kuduMaster = getMasterAddrs(parameters)
    val operationType = getOperationType(parameters)
    val schemaOption = Option(schema)
    val readOptions = getReadOptions(parameters)
    val writeOptions = getWriteOptions(parameters)

    new KuduRelation(
      tableName,
      kuduMaster,
      operationType,
      schemaOption,
      readOptions,
      writeOptions
    )(sqlContext)
  }

  /**
   * Creates a relation and inserts data to specified table.
   *
   * @param sqlContext
   * @param mode Only Append mode is supported. It will upsert or insert data
   *             to an existing table, depending on the upsert parameter
   * @param parameters Necessary parameters for kudu.table, kudu.master, etc...
   * @param data Dataframe to save into kudu
   * @return returns populated base relation
   */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val kuduRelation = createRelation(sqlContext, parameters)
    mode match {
      case SaveMode.Append =>
        kuduRelation.asInstanceOf[KuduRelation].insert(data, false)
      case _ =>
        throw new UnsupportedOperationException("Currently, only Append is supported")
    }
    kuduRelation
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {

    val tableName = getTableName(parameters)
    val masterAddrs = getMasterAddrs(parameters)
    val operationType = getOperationType(parameters)
    val readOptions = getReadOptions(parameters)
    val writeOptions = getWriteOptions(parameters)

    new KuduSink(
      tableName,
      masterAddrs,
      operationType,
      readOptions,
      writeOptions
    )(sqlContext)
  }

  private def getTableName(parameters: Map[String, String]): String = {
    parameters.getOrElse(
      TABLE_KEY,
      throw new IllegalArgumentException(
        s"Kudu table name must be specified in create options using key '$TABLE_KEY'"))
  }
  private def getReadOptions(parameters: Map[String, String]): KuduReadOptions = {
    val batchSize = parameters.get(BATCH_SIZE).map(_.toInt).getOrElse(defaultBatchSize)
    val faultTolerantScanner =
      parameters.get(FAULT_TOLERANT_SCANNER).map(_.toBoolean).getOrElse(defaultFaultTolerantScanner)
    val scanLocality =
      parameters.get(SCAN_LOCALITY).map(getScanLocalityType).getOrElse(defaultScanLocality)
    val scanRequestTimeoutMs = parameters.get(SCAN_REQUEST_TIMEOUT_MS).map(_.toLong)
    val keepAlivePeriodMs =
      parameters.get(KEEP_ALIVE_PERIOD_MS).map(_.toLong).getOrElse(defaultKeepAlivePeriodMs)
    val splitSizeBytes = parameters.get(SPLIT_SIZE_BYTES).map(_.toLong)

    KuduReadOptions(
      batchSize,
      scanLocality,
      faultTolerantScanner,
      keepAlivePeriodMs,
      scanRequestTimeoutMs,
      /* socketReadTimeoutMs= */ None,
      splitSizeBytes)
  }

  private def getWriteOptions(parameters: Map[String, String]): KuduWriteOptions = {
    val ignoreDuplicateRowErrors =
    Try(parameters(IGNORE_DUPLICATE_ROW_ERRORS).toBoolean).getOrElse(false) ||
    Try(parameters(OPERATION) == "insert-ignore").getOrElse(false)
    val ignoreNull =
      parameters.get(IGNORE_NULL).map(_.toBoolean).getOrElse(defaultIgnoreNull)
    val repartition =
      parameters.get(REPARTITION).map(_.toBoolean).getOrElse(defaultRepartition)
    val repartitionSort =
      parameters.get(REPARTITION_SORT).map(_.toBoolean).getOrElse(defaultRepartitionSort)
    KuduWriteOptions(ignoreDuplicateRowErrors, ignoreNull, repartition, repartitionSort)
  }

  private def getMasterAddrs(parameters: Map[String, String]): String = {
    parameters.getOrElse(KUDU_MASTER, InetAddress.getLocalHost.getCanonicalHostName)
  }

  private def getScanLocalityType(opParam: String): ReplicaSelection = {
    opParam.toLowerCase match {
      case "leader_only" => ReplicaSelection.LEADER_ONLY
      case "closest_replica" => ReplicaSelection.CLOSEST_REPLICA
      case _ =>
        throw new IllegalArgumentException(s"Unsupported replica selection type '$opParam'")
    }
  }

  private def getOperationType(parameters: Map[String, String]): OperationType = {
    parameters.get(OPERATION).map(stringToOperationType).getOrElse(Upsert)
  }

  private def stringToOperationType(opParam: String): OperationType = {
    opParam.toLowerCase match {
      case "insert" => Insert
      case "insert-ignore" => Insert
      case "upsert" => Upsert
      case "update" => Update
      case "delete" => Delete
      case _ =>
        throw new IllegalArgumentException(s"Unsupported operation type '$opParam'")
    }
  }
}

/**
 * Implementation of Spark BaseRelation.
 *
 * @param tableName Kudu table that we plan to read from
 * @param masterAddrs Kudu master addresses
 * @param operationType The default operation type to perform when writing to the relation
 * @param userSchema A schema used to select columns for the relation
 * @param readOptions Kudu read options
 * @param writeOptions Kudu write options
 * @param sqlContext SparkSQL context
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class KuduRelation(
    val tableName: String,
    val masterAddrs: String,
    val operationType: OperationType,
    val userSchema: Option[StructType],
    val readOptions: KuduReadOptions = new KuduReadOptions,
    val writeOptions: KuduWriteOptions = new KuduWriteOptions)(val sqlContext: SQLContext)
    extends BaseRelation with PrunedFilteredScan with InsertableRelation {

  private val context: KuduContext =
    new KuduContext(masterAddrs, sqlContext.sparkContext)

  private val table: KuduTable = context.syncClient.openTable(tableName)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] =
    filters.filterNot(KuduRelation.supportsFilter)

  /**
   * Generates a SparkSQL schema object so SparkSQL knows what is being
   * provided by this BaseRelation.
   *
   * @return schema generated from the Kudu table's schema
   */
  override def schema: StructType = {
    sparkSchema(table.getSchema, userSchema.map(_.fieldNames))
  }

  /**
   * Build the RDD to scan rows.
   *
   * @param requiredColumns columns that are being requested by the requesting query
   * @param filters         filters that are being applied by the requesting query
   * @return RDD will all the results from Kudu
   */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val predicates = filters.flatMap(filterToPredicate)
    new KuduRDD(
      context,
      table,
      requiredColumns,
      predicates,
      readOptions,
      sqlContext.sparkContext
    )
  }

  /**
   * Converts a Spark [[Filter]] to a Kudu [[KuduPredicate]].
   *
   * @param filter the filter to convert
   * @return the converted filter
   */
  private def filterToPredicate(filter: Filter): Array[KuduPredicate] = {
    filter match {
      case EqualTo(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.EQUAL, value))
      case GreaterThan(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.GREATER, value))
      case GreaterThanOrEqual(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, value))
      case LessThan(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.LESS, value))
      case LessThanOrEqual(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.LESS_EQUAL, value))
      case In(column, values) =>
        Array(inListPredicate(column, values))
      case StringStartsWith(column, prefix) =>
        prefixInfimum(prefix) match {
          case None =>
            Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, prefix))
          case Some(inf) =>
            Array(
              comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, prefix),
              comparisonPredicate(column, ComparisonOp.LESS, inf))
        }
      case IsNull(column) => Array(isNullPredicate(column))
      case IsNotNull(column) => Array(isNotNullPredicate(column))
      case And(left, right) =>
        filterToPredicate(left) ++ filterToPredicate(right)
      case _ => Array()
    }
  }

  /**
   * Returns the smallest string s such that, if p is a prefix of t,
   * then t < s, if one exists.
   *
   * @param p the prefix
   * @return Some(the prefix infimum), or None if none exists.
   */
  private def prefixInfimum(p: String): Option[String] = {
    p.reverse.dropWhile(_ == Char.MaxValue).reverse match {
      case "" => None
      case q => Some(q.slice(0, q.length - 1) + (q(q.length - 1) + 1).toChar)
    }
  }

  /**
   * Creates a new comparison predicate for the column, comparison operator, and comparison value.
   *
   * @param column the column name
   * @param operator the comparison operator
   * @param value the comparison value
   * @return the comparison predicate
   */
  private def comparisonPredicate(
      column: String,
      operator: ComparisonOp,
      value: Any): KuduPredicate = {
    KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(column), operator, value)
  }

  /**
   * Creates a new in list predicate for the column and values.
   *
   * @param column the column name
   * @param values the values
   * @return the in list predicate
   */
  private def inListPredicate(column: String, values: Array[Any]): KuduPredicate = {
    KuduPredicate.newInListPredicate(table.getSchema.getColumn(column), values.toList.asJava)
  }

  /**
   * Creates a new `IS NULL` predicate for the column.
   *
   * @param column the column name
   * @return the `IS NULL` predicate
   */
  private def isNullPredicate(column: String): KuduPredicate = {
    KuduPredicate.newIsNullPredicate(table.getSchema.getColumn(column))
  }

  /**
   * Creates a new `IS NOT NULL` predicate for the column.
   *
   * @param column the column name
   * @return the `IS NOT NULL` predicate
   */
  private def isNotNullPredicate(column: String): KuduPredicate = {
    KuduPredicate.newIsNotNullPredicate(table.getSchema.getColumn(column))
  }

  /**
   * Writes data into an existing Kudu table.
   *
   * If the `kudu.operation` parameter is set, the data will use that operation
   * type. If the parameter is unset, the data will be upserted.
   *
   * @param data [[DataFrame]] to be inserted into Kudu
   * @param overwrite must be false; otherwise, throws [[UnsupportedOperationException]]
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      throw new UnsupportedOperationException("overwrite is not yet supported")
    }
    context.writeRows(data, tableName, operationType, writeOptions)
  }

  /**
   * Returns the string representation of this KuduRelation
   * @return Kudu + tableName of the relation
   */
  override def toString(): String = {
    "Kudu " + this.tableName
  }
}

private[spark] object KuduRelation {

  /**
   * Returns `true` if the filter is able to be pushed down to Kudu.
   *
   * @param filter the filter to test
   */
  // formatter: off
  private def supportsFilter(filter: Filter): Boolean = filter match {
    case EqualTo(_, _) | GreaterThan(_, _) | GreaterThanOrEqual(_, _) | LessThan(_, _) |
        LessThanOrEqual(_, _) | In(_, _) | StringStartsWith(_, _) | IsNull(_) | IsNotNull(_) =>
      true
    case And(left, right) => supportsFilter(left) && supportsFilter(right)
    case _ => false
  }
  // formatter: on
}

/**
 * Sinks provide at-least-once semantics by retrying failed batches,
 * and provide a `batchId` interface to implement exactly-once-semantics.
 * Since Kudu does not internally track batch IDs, this is ignored,
 * and it is up to the user to specify an appropriate `operationType` to achieve
 * the desired semantics when adding batches.
 *
 * The default `Upsert` allows for KuduSink to handle duplicate data and such retries.
 *
 * Insert ignore support (KUDU-1563) would be useful, but while that doesn't exist,
 * using Upsert will work. Delete ignore would also be useful.
 */
class KuduSink(
    val tableName: String,
    val masterAddrs: String,
    val operationType: OperationType,
    val readOptions: KuduReadOptions = new KuduReadOptions,
    val writeOptions: KuduWriteOptions)(val sqlContext: SQLContext)
    extends Sink {

  private val context: KuduContext =
    new KuduContext(masterAddrs, sqlContext.sparkContext)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    context.writeRows(data, tableName, operationType, writeOptions)
  }
}
