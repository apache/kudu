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

import org.apache.kudu.Schema
import org.apache.kudu.client.PartialRow
import org.apache.kudu.client.RowResult
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructType
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

@InterfaceAudience.Private
@InterfaceStability.Unstable
class RowConverter(kuduSchema: Schema, schema: StructType, ignoreNull: Boolean) {

  private val typeConverter = CatalystTypeConverters.createToScalaConverter(schema)
  private val indices: Array[(Int, Int)] = schema.fields.zipWithIndex.map({
    case (field, sparkIdx) =>
      sparkIdx -> kuduSchema.getColumnIndex(field.name)
  })

  /**
   * Converts a Spark internal row to a Kudu PartialRow.
   */
  def toPartialRow(internalRow: InternalRow): PartialRow = {
    val row = typeConverter(internalRow).asInstanceOf[Row]
    toPartialRow(row)
  }

  /**
   * Converts a Spark row to a Kudu PartialRow.
   */
  def toPartialRow(row: Row): PartialRow = {
    val partialRow = kuduSchema.newPartialRow()
    for ((sparkIdx, kuduIdx) <- indices) {
      if (row.isNullAt(sparkIdx)) {
        if (kuduSchema.getColumnByIndex(kuduIdx).isKey) {
          val key_name = kuduSchema.getColumnByIndex(kuduIdx).getName
          throw new IllegalArgumentException(s"Can't set primary key column '$key_name' to null")
        }
        if (!ignoreNull) partialRow.setNull(kuduIdx)
      } else {
        schema.fields(sparkIdx).dataType match {
          case DataTypes.StringType =>
            partialRow.addString(kuduIdx, row.getString(sparkIdx))
          case DataTypes.BinaryType =>
            partialRow.addBinary(kuduIdx, row.getAs[Array[Byte]](sparkIdx))
          case DataTypes.BooleanType =>
            partialRow.addBoolean(kuduIdx, row.getBoolean(sparkIdx))
          case DataTypes.ByteType =>
            partialRow.addByte(kuduIdx, row.getByte(sparkIdx))
          case DataTypes.ShortType =>
            partialRow.addShort(kuduIdx, row.getShort(sparkIdx))
          case DataTypes.IntegerType =>
            partialRow.addInt(kuduIdx, row.getInt(sparkIdx))
          case DataTypes.LongType =>
            partialRow.addLong(kuduIdx, row.getLong(sparkIdx))
          case DataTypes.FloatType =>
            partialRow.addFloat(kuduIdx, row.getFloat(sparkIdx))
          case DataTypes.DoubleType =>
            partialRow.addDouble(kuduIdx, row.getDouble(sparkIdx))
          case DataTypes.TimestampType =>
            partialRow.addTimestamp(kuduIdx, row.getTimestamp(sparkIdx))
          case DecimalType() =>
            partialRow.addDecimal(kuduIdx, row.getDecimal(sparkIdx))
          case t =>
            throw new IllegalArgumentException(s"No support for Spark SQL type $t")
        }
      }
    }
    partialRow
  }

  /**
   * Converts a Kudu RowResult to a Spark row.
   */
  def toRow(rowResult: RowResult): Row = {
    val columnCount = rowResult.getColumnProjection.getColumnCount
    val columns = Array.ofDim[Any](columnCount)
    for (i <- 0 until columnCount) {
      columns(i) = rowResult.getObject(i)
    }
    new GenericRowWithSchema(columns, schema)
  }

  /**
   * Converts a Kudu PartialRow to a Spark row.
   */
  def toRow(partialRow: PartialRow): Row = {
    val columnCount = partialRow.getSchema.getColumnCount
    val columns = Array.ofDim[Any](columnCount)
    for (i <- 0 until columnCount) {
      columns(i) = partialRow.getObject(i)
    }
    new GenericRowWithSchema(columns, schema)
  }
}
