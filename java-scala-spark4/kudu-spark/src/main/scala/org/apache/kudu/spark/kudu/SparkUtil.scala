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

import java.util

import org.apache.spark.sql.types._
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

import org.apache.kudu.ColumnTypeAttributes.ColumnTypeAttributesBuilder
import org.apache.kudu.ColumnSchema
import org.apache.kudu.ColumnTypeAttributes
import org.apache.kudu.Schema
import org.apache.kudu.Type

import scala.jdk.CollectionConverters._

@InterfaceAudience.Private
@InterfaceStability.Unstable
object SparkUtil {

  /**
   * Converts a Kudu [[Type]] to a Spark SQL [[DataType]].
   *
   * @param t the Kudu type
   * @param a the Kudu type attributes
   * @return the corresponding Spark SQL type
   */
  def kuduTypeToSparkType(t: Type, a: ColumnTypeAttributes): DataType =
    t match {
      case Type.BOOL => BooleanType
      case Type.INT8 => ByteType
      case Type.INT16 => ShortType
      case Type.INT32 => IntegerType
      case Type.INT64 => LongType
      case Type.UNIXTIME_MICROS => TimestampType
      case Type.DATE => DateType
      case Type.FLOAT => FloatType
      case Type.DOUBLE => DoubleType
      case Type.VARCHAR => StringType
      case Type.STRING => StringType
      case Type.BINARY => BinaryType
      case Type.DECIMAL => DecimalType(a.getPrecision, a.getScale)
      case _ =>
        throw new IllegalArgumentException(s"No support for Kudu type $t")
    }

  /**
   * Converts a Spark SQL [[DataType]] to a Kudu [[Type]].
   *
   * @param dt the Spark SQL type
   * @return
   */
  def sparkTypeToKuduType(dt: DataType): Type = dt match {
    case DataTypes.BinaryType => Type.BINARY
    case DataTypes.BooleanType => Type.BOOL
    case DataTypes.StringType => Type.STRING
    case DataTypes.TimestampType => Type.UNIXTIME_MICROS
    case DataTypes.DateType => Type.DATE
    case DataTypes.ByteType => Type.INT8
    case DataTypes.ShortType => Type.INT16
    case DataTypes.IntegerType => Type.INT32
    case DataTypes.LongType => Type.INT64
    case DataTypes.FloatType => Type.FLOAT
    case DataTypes.DoubleType => Type.DOUBLE
    case DecimalType() => Type.DECIMAL
    case _ =>
      throw new IllegalArgumentException(s"No support for Spark SQL type $dt")
  }

  /**
   * Generates a SparkSQL schema from a Kudu schema.
   *
   * @param kuduSchema the Kudu schema
   * @param fields an optional column projection
   * @return the SparkSQL schema
   */
  def sparkSchema(kuduSchema: Schema, fields: Option[Seq[String]] = None): StructType = {
    val kuduColumns = fields match {
      case Some(fieldNames) => fieldNames.map(kuduSchema.getColumn)
      case None => kuduSchema.getColumns.asScala
    }
    val sparkColumns = kuduColumns.map { col =>
      val sparkType = kuduTypeToSparkType(col.getType, col.getTypeAttributes)
      StructField(col.getName, sparkType, col.isNullable)
    }
    StructType(sparkColumns.asJava)
  }

  /**
   * Generates a Kudu schema from a SparkSQL schema.
   *
   * @param sparkSchema the SparkSQL schema
   * @param keys the ordered names of key columns
   * @return the Kudu schema
   */
  def kuduSchema(sparkSchema: StructType, keys: Seq[String]): Schema = {
    val kuduCols = new util.ArrayList[ColumnSchema]()
    // add the key columns first, in the order specified
    for (key <- keys) {
      val field = sparkSchema.fields(sparkSchema.fieldIndex(key))
      val col = createColumnSchema(field, isKey = true)
      kuduCols.add(col)
    }
    // now add the non-key columns
    for (field <- sparkSchema.fields.filter(field => !keys.contains(field.name))) {
      val col = createColumnSchema(field, isKey = false)
      kuduCols.add(col)
    }
    new Schema(kuduCols)
  }

  /**
   * Generates a Kudu column schema from a SparkSQL field.
   *
   * @param field the SparkSQL field
   * @param isKey true if the column is a key
   * @return the Kudu column schema
   */
  private def createColumnSchema(field: StructField, isKey: Boolean): ColumnSchema = {
    val kt = sparkTypeToKuduType(field.dataType)
    val col = new ColumnSchema.ColumnSchemaBuilder(field.name, kt)
      .key(isKey)
      .nullable(field.nullable)
    // Add ColumnTypeAttributesBuilder to DECIMAL columns
    if (kt == Type.DECIMAL) {
      val dt = field.dataType.asInstanceOf[DecimalType]
      col.typeAttributes(
        new ColumnTypeAttributesBuilder()
          .precision(dt.precision)
          .scale(dt.scale)
          .build()
      )
    }
    col.build()
  }

}
