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

import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.yetus.audience.InterfaceStability

import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client._
import org.apache.kudu.{ColumnSchema, Type}

/**
  * Data source for integration with Spark's [[DataFrame]] API.
  *
  * Serves as a factory for [[KuduRelation]] instances for Spark. Spark will
  * automatically look for a [[RelationProvider]] implementation named
  * `DefaultSource` when the user specifies the path of a source during DDL
  * operations through [[org.apache.spark.sql.DataFrameReader.format]].
  */
@InterfaceStability.Unstable
class DefaultSource extends RelationProvider with CreatableRelationProvider
  with SchemaRelationProvider {

  val TABLE_KEY = "kudu.table"
  val KUDU_MASTER = "kudu.master"
  val OPERATION = "kudu.operation"
  val FAULT_TOLERANT_SCANNER = "kudu.faultTolerantScan"

  /**
    * Construct a BaseRelation using the provided context and parameters.
    *
    * @param sqlContext SparkSQL context
    * @param parameters parameters given to us from SparkSQL
    * @return           a BaseRelation Object
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]):
  BaseRelation = {
    val tableName = parameters.getOrElse(TABLE_KEY,
      throw new IllegalArgumentException(
        s"Kudu table name must be specified in create options using key '$TABLE_KEY'"))
    val kuduMaster = parameters.getOrElse(KUDU_MASTER, "localhost")
    val operationType = getOperationType(parameters.getOrElse(OPERATION, "upsert"))
    val faultTolerantScanner = Try(parameters.getOrElse(FAULT_TOLERANT_SCANNER, "false").toBoolean)
      .getOrElse(false)

    new KuduRelation(tableName, kuduMaster, faultTolerantScanner, operationType,
      None)(sqlContext)
  }

  /**
    * Creates a relation and inserts data to specified table.
    *
    * @param sqlContext
    * @param mode Only Append mode is supported. It will upsert or insert data
    *             to an existing table, depending on the upsert parameter
    * @param parameters Necessary parameters for kudu.table and kudu.master
    * @param data Dataframe to save into kudu
    * @return returns populated base relation
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val kuduRelation = createRelation(sqlContext, parameters)
    mode match {
      case SaveMode.Append => kuduRelation.asInstanceOf[KuduRelation].insert(data, false)
      case _ => throw new UnsupportedOperationException("Currently, only Append is supported")
    }

    kuduRelation
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val tableName = parameters.getOrElse(TABLE_KEY,
      throw new IllegalArgumentException(s"Kudu table name must be specified in create options " +
        s"using key '$TABLE_KEY'"))
    val kuduMaster = parameters.getOrElse(KUDU_MASTER, "localhost")
    val operationType = getOperationType(parameters.getOrElse(OPERATION, "upsert"))
    val faultTolerantScanner = Try(parameters.getOrElse(FAULT_TOLERANT_SCANNER, "false").toBoolean)
      .getOrElse(false)

    new KuduRelation(tableName, kuduMaster, faultTolerantScanner, operationType,
      Some(schema))(sqlContext)
  }

  private def getOperationType(opParam: String): OperationType = {
    opParam.toLowerCase match {
      case "insert" => Insert
      case "insert-ignore" => InsertIgnore
      case "upsert" => Upsert
      case "update" => Update
      case "delete" => Delete
      case _ => throw new IllegalArgumentException(s"Unsupported operation type '$opParam'")
    }
  }
}

/**
  * Implementation of Spark BaseRelation.
  *
  * @param tableName Kudu table that we plan to read from
  * @param masterAddrs Kudu master addresses
  * @param faultTolerantScanner scanner type to be used. Fault tolerant if true,
  *                             otherwise, use non fault tolerant one
  * @param operationType The default operation type to perform when writing to the relation
  * @param userSchema A schema used to select columns for the relation
  * @param sqlContext SparkSQL context
  */
@InterfaceStability.Unstable
class KuduRelation(private val tableName: String,
                   private val masterAddrs: String,
                   private val faultTolerantScanner: Boolean,
                   private val operationType: OperationType,
                   private val userSchema: Option[StructType])(
                   val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {

  import KuduRelation._

  private val context: KuduContext = new KuduContext(masterAddrs, sqlContext.sparkContext)
  private val table: KuduTable = context.syncClient.openTable(tableName)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] =
    filters.filterNot(supportsFilter)

  /**
    * Generates a SparkSQL schema object so SparkSQL knows what is being
    * provided by this BaseRelation.
    *
    * @return schema generated from the Kudu table's schema
    */
  override def schema: StructType = {
    userSchema match {
      case Some(x) =>
        StructType(x.fields.map(uf => table.getSchema.getColumn(uf.name))
          .map(kuduColumnToSparkField))
      case None =>
        StructType(table.getSchema.getColumns.asScala.map(kuduColumnToSparkField).toArray)
    }
  }

  def kuduColumnToSparkField: (ColumnSchema) => StructField = {
    columnSchema =>
      val sparkType = kuduTypeToSparkType(columnSchema.getType)
      new StructField(columnSchema.getName, sparkType, columnSchema.isNullable)
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
    new KuduRDD(context, 1024 * 1024 * 20, requiredColumns, predicates,
                table, faultTolerantScanner, sqlContext.sparkContext)
  }

  /**
    * Converts a Spark [[Filter]] to a Kudu [[KuduPredicate]].
    *
    * @param filter the filter to convert
    * @return the converted filter
    */
  private def filterToPredicate(filter : Filter) : Array[KuduPredicate] = {
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
          case None => Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, prefix))
          case Some(inf) =>
            Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, prefix),
                  comparisonPredicate(column, ComparisonOp.LESS, inf))
        }
      case IsNull(column) => Array(isNullPredicate(column))
      case IsNotNull(column) => Array(isNotNullPredicate(column))
      case And(left, right) => filterToPredicate(left) ++ filterToPredicate(right)
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
  private def comparisonPredicate(column: String,
                                  operator: ComparisonOp,
                                  value: Any): KuduPredicate = {
    val columnSchema = table.getSchema.getColumn(column)
    value match {
      case value: Boolean => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Byte => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Short => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Int => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Long => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Timestamp => KuduPredicate.newComparisonPredicate(columnSchema, operator, timestampToMicros(value))
      case value: Float => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Double => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: String => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Array[Byte] => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
    }
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
    * Creates a new `IS NULL` predicate for the column.
    *
    * @param column the column name
    * @return the `IS NULL` predicate
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
    context.writeRows(data, tableName, operationType)
  }
}

private[spark] object KuduRelation {
  /**
    * Converts a Kudu [[Type]] to a Spark SQL [[DataType]].
    *
    * @param t the Kudu type
    * @return the corresponding Spark SQL type
    */
  private def kuduTypeToSparkType(t: Type): DataType = t match {
    case Type.BOOL => BooleanType
    case Type.INT8 => ByteType
    case Type.INT16 => ShortType
    case Type.INT32 => IntegerType
    case Type.INT64 => LongType
    case Type.UNIXTIME_MICROS => TimestampType
    case Type.FLOAT => FloatType
    case Type.DOUBLE => DoubleType
    case Type.STRING => StringType
    case Type.BINARY => BinaryType
  }

  /**
    * Returns `true` if the filter is able to be pushed down to Kudu.
    *
    * @param filter the filter to test
    */
  private def supportsFilter(filter: Filter): Boolean = filter match {
    case EqualTo(_, _)
       | GreaterThan(_, _)
       | GreaterThanOrEqual(_, _)
       | LessThan(_, _)
       | LessThanOrEqual(_, _)
       | In(_, _)
       | StringStartsWith(_, _)
       | IsNull(_)
       | IsNotNull(_) => true
    case And(left, right) => supportsFilter(left) && supportsFilter(right)
    case _ => false
  }

  /**
    * Converts a [[Timestamp]] to microseconds since the Unix epoch (1970-01-01T00:00:00Z).
    *
    * @param timestamp the timestamp to convert to microseconds
    * @return the microseconds since the Unix epoch
    */
  def timestampToMicros(timestamp: Timestamp): Long = {
    // Number of whole milliseconds since the Unix epoch, in microseconds.
    val millis = timestamp.getTime * 1000
    // Sub millisecond time since the Unix epoch, in microseconds.
    val micros = (timestamp.getNanos % 1000000) / 1000
    if (micros >= 0) {
      millis + micros
    } else {
      millis + 1000000 + micros
    }
  }

  /**
    * Converts a microsecond offset from the Unix epoch (1970-01-01T00:00:00Z) to a [[Timestamp]].
    *
    * @param micros the offset in microseconds since the Unix epoch
    * @return the corresponding timestamp
    */
  def microsToTimestamp(micros: Long): Timestamp = {
    var millis = micros / 1000
    var nanos = (micros % 1000000) * 1000
    if (nanos < 0) {
      millis -= 1
      nanos += 1000000000
    }

    val timestamp = new Timestamp(millis)
    timestamp.setNanos(nanos.asInstanceOf[Int])
    timestamp
  }
}
