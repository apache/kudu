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

import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.client.PartialRow
import org.apache.kudu.client.RowResult
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructType
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import scala.collection.JavaConverters._

@InterfaceAudience.Private
@InterfaceStability.Unstable
class RowConverter(kuduSchema: Schema, schema: StructType, ignoreNull: Boolean) {

  private val typeConverter = CatalystTypeConverters.createToScalaConverter(schema)
  private val indices: Array[(Int, Int)] = schema.fields.zipWithIndex.flatMap {
    case (field, sparkIdx) =>
      // Support Spark schemas that have more columns than the Kudu table by
      // ignoring missing Kudu columns.
      if (kuduSchema.hasColumn(field.name)) {
        Some(sparkIdx -> kuduSchema.getColumnIndex(field.name))
      } else None
  }

  /**
   * Converts a Spark internalRow to a Spark Row.
   */
  def toRow(internalRow: InternalRow): Row = {
    typeConverter(internalRow).asInstanceOf[Row]
  }

  /**
   * Converts a Spark row to a Kudu PartialRow.
   */
  def toPartialRow(row: Row): PartialRow = {
    val partialRow = kuduSchema.newPartialRow()
    for ((sparkIdx, kuduIdx) <- indices) {
      val col = kuduSchema.getColumnByIndex(kuduIdx)
      if (row.isNullAt(sparkIdx)) {
        if (col.isKey) {
          throw new IllegalArgumentException(
            s"Can't set primary key column '${col.getName}' to null")
        }
        if (!ignoreNull) partialRow.setNull(kuduIdx)
      } else {
        schema.fields(sparkIdx).dataType match {

          // ========== ARRAY WRITE ==========
          case ArrayType(elemType, containsNull) =>
            val seq = row.getList[Any](sparkIdx).asScala
            writeArray(
              partialRow,
              kuduIdx,
              col.getNestedTypeDescriptor.getArrayDescriptor.getElemType,
              seq)

          // ========== SCALAR TYPES ==========
          case DataTypes.StringType =>
            col.getType match {
              case Type.STRING =>
                partialRow.addString(kuduIdx, row.getString(sparkIdx))
              case Type.VARCHAR =>
                partialRow.addVarchar(kuduIdx, row.getString(sparkIdx))
              case t =>
                throw new IllegalArgumentException(s"Invalid Kudu column type $t")
            }
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
          case DataTypes.DateType =>
            partialRow.addDate(kuduIdx, row.getDate(sparkIdx))
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
      val col = rowResult.getColumnProjection.getColumnByIndex(i)
      if (rowResult.isNull(i)) {
        columns(i) = null
      } else if (col.isArray) {
        val arrObj = rowResult.getArrayData(i)
        columns(i) =
          if (arrObj == null) null
          else arrObj.asInstanceOf[Array[_]].toIndexedSeq
      } else {
        columns(i) = rowResult.getObject(i)
      }
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
      val col = partialRow.getSchema.getColumnByIndex(i)
      if (partialRow.isSet(i)) {
        if (col.isArray) {
          val arrObj = partialRow.getArrayData(i)
          columns(i) =
            if (arrObj == null) null
            else arrObj.asInstanceOf[Array[_]].toIndexedSeq
        } else {
          columns(i) = partialRow.getObject(i)
        }
      } else {
        columns(i) = null
      }
    }
    new GenericRowWithSchema(columns, schema)
  }

  // ---------------------------------------------------------------------
  // Array write helper
  // ---------------------------------------------------------------------
  //
  // Converts a Scala Seq[Any] into Kudu's array cell format for the given
  // element type. Builds parallel 'data' and 'validity' arrays, then calls
  // the corresponding PartialRow.addArray*() method (e.g. addArrayInt32,
  // addArrayString, etc.). Null elements are recorded as invalid in the
  // validity mask.
  private def writeArray(pr: PartialRow, idx: Int, elemKuduType: Type, seq: Seq[Any]): Unit = {

    val n = seq.length
    val validity = new Array[Boolean](n)

    elemKuduType match {
      case Type.BOOL =>
        val data = new Array[Boolean](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.asInstanceOf[Boolean] }
          i += 1
        }
        pr.addArrayBool(idx, data, validity)

      case Type.INT8 =>
        val data = new Array[Byte](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.asInstanceOf[Byte] }
          i += 1
        }
        pr.addArrayInt8(idx, data, validity)

      case Type.INT16 =>
        val data = new Array[Short](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.asInstanceOf[Short] }
          i += 1
        }
        pr.addArrayInt16(idx, data, validity)

      case Type.INT32 =>
        val data = new Array[Int](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.asInstanceOf[Int] }
          i += 1
        }
        pr.addArrayInt32(idx, data, validity)

      case Type.INT64 =>
        val data = new Array[Long](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.asInstanceOf[Long] }
          i += 1
        }
        pr.addArrayInt64(idx, data, validity)

      case Type.DOUBLE =>
        val data = new Array[Double](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.asInstanceOf[Double] }
          i += 1
        }
        pr.addArrayDouble(idx, data, validity)

      case Type.STRING | Type.VARCHAR =>
        val data = new Array[String](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.toString }
          i += 1
        }
        pr.addArrayString(idx, data, validity)

      case Type.BINARY =>
        val data = new Array[Array[Byte]](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else { validity(i) = true; data(i) = v.asInstanceOf[Array[Byte]] }
          i += 1
        }
        pr.addArrayBinary(idx, data, validity)

      case Type.FLOAT =>
        val data = new Array[Float](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else {
            validity(i) = true; data(i) = v.asInstanceOf[Float]
          }
          i += 1
        }
        pr.addArrayFloat(idx, data, validity)

      case Type.DATE =>
        val data = new Array[java.sql.Date](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else {
            validity(i) = true
            data(i) = v.asInstanceOf[java.sql.Date]
          }
          i += 1
        }
        pr.addArrayDate(idx, data, validity)

      case Type.UNIXTIME_MICROS =>
        val data = new Array[java.sql.Timestamp](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else {
            validity(i) = true
            data(i) = v.asInstanceOf[java.sql.Timestamp]
          }
          i += 1
        }
        pr.addArrayTimestamp(idx, data, validity)

      case Type.DECIMAL =>
        val data = new Array[java.math.BigDecimal](n)
        var i = 0
        while (i < n) {
          val v = seq(i)
          if (v == null) validity(i) = false
          else {
            validity(i) = true;
            data(i) = v.asInstanceOf[java.math.BigDecimal]
          }
          i += 1
        }
        pr.addArrayDecimal(idx, data, validity)

      case t =>
        throw new IllegalArgumentException(s"Unsupported Kudu array element type $t")
    }
  }
}
