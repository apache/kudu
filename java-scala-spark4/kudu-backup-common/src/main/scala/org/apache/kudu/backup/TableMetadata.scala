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

import java.math.BigDecimal
import java.sql.Date
import java.util

import com.google.protobuf.ByteString
import com.google.protobuf.StringValue
import org.apache.commons.net.util.Base64
import org.apache.kudu.backup.Backup._
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.ColumnSchema.CompressionAlgorithm
import org.apache.kudu.ColumnSchema.Encoding
import org.apache.kudu.ColumnTypeAttributes.ColumnTypeAttributesBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduTable
import org.apache.kudu.client.PartialRow
import org.apache.kudu.client.PartitionSchema
import org.apache.kudu.ColumnSchema
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.client.KuduPartitioner.KuduPartitionerBuilder
import org.apache.kudu.client.PartitionSchema.HashBucketSchema
import org.apache.kudu.client.PartitionSchema.RangeSchema
import org.apache.kudu.client.PartitionSchema.RangeWithHashSchema
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

import scala.collection.JavaConverters._

@InterfaceAudience.Private
@InterfaceStability.Unstable
object TableMetadata {

  val MetadataFileName = ".kudu-metadata.json"
  val MetadataVersion = 1

  def getTableMetadata(
      table: KuduTable,
      fromMs: Long,
      toMs: Long,
      format: String): TableMetadataPB = {
    val columnIds = new util.HashMap[String, Integer]()
    val columns = table.getSchema.getColumns.asScala.map { col =>
      columnIds.put(col.getName, table.getSchema.getColumnId(col.getName))
      val builder = ColumnMetadataPB
        .newBuilder()
        .setName(col.getName)
        .setType(col.getType.name())
        .setIsKey(col.isKey)
        .setIsNullable(col.isNullable)
        .setEncoding(col.getEncoding.toString)
        .setCompression(col.getCompressionAlgorithm.toString)
        .setBlockSize(col.getDesiredBlockSize)
        .setComment(col.getComment)
        .setIsAutoIncrementing(col.isAutoIncrementing)
      if (col.getTypeAttributes != null) {
        builder.setTypeAttributes(getTypeAttributesMetadata(col))
      }
      if (col.getDefaultValue != null) {
        builder.setDefaultValue(StringValue.of(valueToString(col.getDefaultValue, col.getType)))
      }
      builder.build()
    }

    val partitioner = new KuduPartitionerBuilder(table).build()
    val tablets = partitioner.getTabletMap.asScala.map {
      case (id, partition) =>
        val metadata = PartitionMetadataPB
          .newBuilder()
          .setPartitionKeyStart(ByteString.copyFrom(partition.getPartitionKeyStart))
          .setPartitionKeyEnd(ByteString.copyFrom(partition.getPartitionKeyStart))
          .addAllHashBuckets(partition.getHashBuckets)
          .build()
        (id, metadata)
    }

    val builder = TableMetadataPB
      .newBuilder()
      .setVersion(MetadataVersion)
      .setFromMs(fromMs)
      .setToMs(toMs)
      .setDataFormat(format)
      .setTableName(table.getName)
      .setTableId(table.getTableId)
      .addAllColumns(columns.asJava)
      .putAllColumnIds(columnIds)
      .setNumReplicas(table.getNumReplicas)
      .setPartitions(getPartitionSchemaMetadata(table))
      .putAllTablets(tablets.asJava)
      .setTableOwner(table.getOwner)
      .setTableComment(table.getComment)
    builder.build()
  }

  private def getTypeAttributesMetadata(col: ColumnSchema): ColumnTypeAttributesMetadataPB = {
    val attributes = col.getTypeAttributes
    ColumnTypeAttributesMetadataPB
      .newBuilder()
      .setPrecision(attributes.getPrecision)
      .setScale(attributes.getScale)
      .setLength(attributes.getLength)
      .build()
  }

  private def getPartitionSchemaMetadata(table: KuduTable): PartitionSchemaMetadataPB = {
    val hashPartitions = getHashPartitionsMetadata(table)
    val rangePartitions = getRangePartitionMetadata(table)
    val rangeAndHashPartitions = getRangeAndHashPartitionsMetadata(table)
    PartitionSchemaMetadataPB
      .newBuilder()
      .addAllHashPartitions(hashPartitions.asJava)
      .setRangePartitions(rangePartitions)
      .addAllRangeAndHashPartitions(rangeAndHashPartitions.asJava)
      .build()
  }

  private def getRangeAndHashPartitionsMetadata(
      table: KuduTable): Seq[RangeAndHashPartitionMetadataPB] = {
    val tableSchema = table.getSchema
    val partitionSchema = table.getPartitionSchema
    val rangeColumnNames = partitionSchema.getRangeSchema.getColumnIds.asScala.map { id =>
      getColumnById(tableSchema, id).getName
    }
    partitionSchema.getRangesWithHashSchemas.asScala.map { rhs =>
      val hashSchemas = rhs.hashSchemas.asScala.map { hs =>
        val hashColumnNames = hs.getColumnIds.asScala.map { id =>
          getColumnById(tableSchema, id).getName
        }
        HashPartitionMetadataPB
          .newBuilder()
          .addAllColumnNames(hashColumnNames.asJava)
          .setNumBuckets(hs.getNumBuckets)
          .setSeed(hs.getSeed)
          .build()
      }
      val upperValues = getBoundValues(rhs.upperBound, rangeColumnNames.toSeq, tableSchema)
      val lowerValues = getBoundValues(rhs.lowerBound, rangeColumnNames.toSeq, tableSchema)
      val bounds = RangeBoundsMetadataPB
        .newBuilder()
        .addAllUpperBounds(upperValues.asJava)
        .addAllLowerBounds(lowerValues.asJava)
        .build()
      RangeAndHashPartitionMetadataPB
        .newBuilder()
        .setBounds(bounds)
        .addAllHashPartitions(hashSchemas.asJava)
        .build()
    }.toSeq
  }

  private def getHashPartitionsMetadata(table: KuduTable): Seq[HashPartitionMetadataPB] = {
    val tableSchema = table.getSchema
    val partitionSchema = table.getPartitionSchema
    partitionSchema.getHashBucketSchemas.asScala.map { hs =>
      val columnNames = hs.getColumnIds.asScala.map { id =>
        getColumnById(tableSchema, id).getName
      }
      HashPartitionMetadataPB
        .newBuilder()
        .addAllColumnNames(columnNames.asJava)
        .setNumBuckets(hs.getNumBuckets)
        .setSeed(hs.getSeed)
        .build()
    }.toSeq
  }

  private def getRangePartitionMetadata(table: KuduTable): RangePartitionMetadataPB = {
    val tableSchema = table.getSchema
    val partitionSchema = table.getPartitionSchema
    val columnNames = partitionSchema.getRangeSchema.getColumnIds.asScala.map { id =>
      getColumnById(tableSchema, id).getName
    }

    val bounds = table
      .getRangePartitionsWithTableHashSchema(table.getAsyncClient.getDefaultOperationTimeoutMs)
      .asScala
      .map { p =>
        val lowerValues =
          getBoundValues(p.getDecodedRangeKeyStart(table), columnNames.toSeq, tableSchema)
        val upperValues =
          getBoundValues(p.getDecodedRangeKeyEnd(table), columnNames.toSeq, tableSchema)
        RangeBoundsMetadataPB
          .newBuilder()
          .addAllUpperBounds(upperValues.asJava)
          .addAllLowerBounds(lowerValues.asJava)
          .build()
      }
    RangePartitionMetadataPB
      .newBuilder()
      .addAllColumnNames(columnNames.asJava)
      .addAllBounds(bounds.asJava)
      .build()
  }

  private def getColumnById(schema: Schema, colId: Int): ColumnSchema = {
    schema.getColumnByIndex(schema.getColumnIndex(colId))
  }

  private def getBoundValues(
      bound: PartialRow,
      columnNames: Seq[String],
      schema: Schema): Seq[ColumnValueMetadataPB] = {
    columnNames.filter(bound.isSet).map { col =>
      val colType = schema.getColumn(col).getType
      val value = getValue(bound, col, colType)
      ColumnValueMetadataPB
        .newBuilder()
        .setColumnName(col)
        .setValue(valueToString(value, colType))
        .build()
    }
  }

  private def getPartialRow(values: Seq[ColumnValueMetadataPB], schema: Schema): PartialRow = {
    val row = schema.newPartialRow()
    values.foreach { v =>
      val colType = schema.getColumn(v.getColumnName).getType
      addValue(valueFromString(v.getValue, colType), row, v.getColumnName, colType)
    }
    row
  }

  def getKuduSchema(metadata: TableMetadataPB): Schema = {
    var IsAutoIncrementingPresent = false
    metadata.getColumnsList.asScala.foreach { col =>
      if (col.getIsAutoIncrementing) {
        IsAutoIncrementingPresent = true
      }
    }
    val columns = new util.ArrayList[ColumnSchema]()
    val colIds = new util.ArrayList[Integer]()
    val toId = metadata.getColumnIdsMap.asScala
    metadata.getColumnsList.asScala.foreach { col =>
      if (!col.getIsAutoIncrementing) {
        val colType = Type.getTypeForName(col.getType)
        val builder = new ColumnSchemaBuilder(col.getName, colType)
          .nullable(col.getIsNullable)
          .encoding(Encoding.valueOf(col.getEncoding))
          .compressionAlgorithm(CompressionAlgorithm.valueOf(col.getCompression))
          .desiredBlockSize(col.getBlockSize)
          .comment(col.getComment)
        if (IsAutoIncrementingPresent) {
          builder.nonUniqueKey(col.getIsKey)
        } else {
          builder.key(col.getIsKey)
        }

        if (col.hasDefaultValue) {
          val value = valueFromString(col.getDefaultValue.getValue, colType)
          builder.defaultValue(value)
        }

        if (col.hasTypeAttributes) {
          val attributes = col.getTypeAttributes
          builder.typeAttributes(
            new ColumnTypeAttributesBuilder()
              .precision(attributes.getPrecision)
              .scale(attributes.getScale)
              .length(attributes.getLength)
              .build()
          )
        }
        colIds.add(toId(col.getName))
        columns.add(builder.build())
      }
    }
    new Schema(columns, colIds)
  }

  private def getValue(row: PartialRow, columnName: String, colType: Type): Any = {
    colType match {
      case Type.BOOL => row.getBoolean(columnName)
      case Type.INT8 => row.getByte(columnName)
      case Type.INT16 => row.getShort(columnName)
      case Type.INT32 => row.getInt(columnName)
      case Type.INT64 | Type.UNIXTIME_MICROS => row.getLong(columnName)
      case Type.FLOAT => row.getFloat(columnName)
      case Type.DOUBLE => row.getDouble(columnName)
      case Type.VARCHAR => row.getVarchar(columnName)
      case Type.STRING => row.getString(columnName)
      case Type.BINARY => row.getBinary(columnName)
      case Type.DECIMAL => row.getDecimal(columnName)
      case Type.DATE => row.getDate(columnName)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported column type: $colType")
    }
  }

  private def addValue(value: Any, row: PartialRow, columnName: String, colType: Type): Any = {
    colType match {
      case Type.BOOL => row.addBoolean(columnName, value.asInstanceOf[Boolean])
      case Type.INT8 => row.addByte(columnName, value.asInstanceOf[Byte])
      case Type.INT16 => row.addShort(columnName, value.asInstanceOf[Short])
      case Type.INT32 => row.addInt(columnName, value.asInstanceOf[Int])
      case Type.INT64 | Type.UNIXTIME_MICROS =>
        row.addLong(columnName, value.asInstanceOf[Long])
      case Type.FLOAT => row.addFloat(columnName, value.asInstanceOf[Float])
      case Type.DOUBLE => row.addDouble(columnName, value.asInstanceOf[Double])
      case Type.VARCHAR => row.addVarchar(columnName, value.asInstanceOf[String])
      case Type.STRING => row.addString(columnName, value.asInstanceOf[String])
      case Type.BINARY =>
        row.addBinary(columnName, value.asInstanceOf[Array[Byte]])
      case Type.DECIMAL =>
        row.addDecimal(columnName, value.asInstanceOf[BigDecimal])
      case Type.DATE => row.addDate(columnName, value.asInstanceOf[Date])
      case _ =>
        throw new IllegalArgumentException(s"Unsupported column type: $colType")
    }
  }

  /**
   * Returns the string value of serialized value according to the type of column.
   */
  private def valueToString(value: Any, colType: Type): String = {
    colType match {
      case Type.BOOL =>
        String.valueOf(value.asInstanceOf[Boolean])
      case Type.INT8 =>
        String.valueOf(value.asInstanceOf[Byte])
      case Type.INT16 =>
        String.valueOf(value.asInstanceOf[Short])
      case Type.INT32 =>
        String.valueOf(value.asInstanceOf[Int])
      case Type.INT64 | Type.UNIXTIME_MICROS =>
        String.valueOf(value.asInstanceOf[Long])
      case Type.FLOAT =>
        String.valueOf(value.asInstanceOf[Float])
      case Type.DOUBLE =>
        String.valueOf(value.asInstanceOf[Double])
      case Type.VARCHAR =>
        value.asInstanceOf[String]
      case Type.STRING =>
        value.asInstanceOf[String]
      case Type.BINARY =>
        Base64.encodeBase64String(value.asInstanceOf[Array[Byte]])
      case Type.DECIMAL =>
        value
          .asInstanceOf[BigDecimal]
          .toString // TODO: Explicitly control print format
      case Type.DATE =>
        value.asInstanceOf[Date].toString()
      case _ =>
        throw new IllegalArgumentException(s"Unsupported column type: $colType")
    }
  }

  private def valueFromString(value: String, colType: Type): Any = {
    colType match {
      case Type.BOOL => value.toBoolean
      case Type.INT8 => value.toByte
      case Type.INT16 => value.toShort
      case Type.INT32 => value.toInt
      case Type.INT64 | Type.UNIXTIME_MICROS => value.toLong
      case Type.FLOAT => value.toFloat
      case Type.DOUBLE => value.toDouble
      case Type.VARCHAR => value
      case Type.STRING => value
      case Type.BINARY => Base64.decodeBase64(value)
      case Type.DECIMAL => new BigDecimal(value) // TODO: Explicitly pass scale
      case Type.DATE => Date.valueOf(value)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported column type: $colType")
    }
  }

  def getCreateTableOptionsWithoutRangePartitions(
      metadata: TableMetadataPB,
      restoreOwner: Boolean): CreateTableOptions = {
    val options = new CreateTableOptions()
    if (restoreOwner) {
      options.setOwner(metadata.getTableOwner)
    }
    options.setComment(metadata.getTableComment)
    options.setNumReplicas(metadata.getNumReplicas)
    metadata.getPartitions.getHashPartitionsList.asScala.foreach { hp =>
      options
        .addHashPartitions(hp.getColumnNamesList, hp.getNumBuckets, hp.getSeed)
    }
    val rangePartitionColumns =
      metadata.getPartitions.getRangePartitions.getColumnNamesList
    options.setRangePartitionColumns(rangePartitionColumns)
    options
  }

  def getRangeBoundPartialRows(metadata: TableMetadataPB): Seq[(PartialRow, PartialRow)] = {
    val schema = getKuduSchema(metadata)
    metadata.getPartitions.getRangePartitions.getBoundsList.asScala.map { b =>
      val lower = getPartialRow(b.getLowerBoundsList.asScala.toSeq, schema)
      val upper = getPartialRow(b.getUpperBoundsList.asScala.toSeq, schema)
      (lower, upper)
    }.toSeq
  }

  def getRangeBoundsPartialRowsWithHashSchemas(
      metadata: TableMetadataPB): Seq[RangeWithHashSchema] = {
    val schema = getKuduSchema(metadata)
    metadata.getPartitions.getRangeAndHashPartitionsList.asScala.map { rhp =>
      val hashSchemas = rhp.getHashPartitionsList.asScala.map { hp =>
        val colIds = hp.getColumnNamesList.asScala.map { name =>
          new Integer(schema.getColumnIndex(schema.getColumnId(name)))
        }
        new HashBucketSchema(colIds.asJava, hp.getNumBuckets, hp.getSeed)
      }
      val lower = getPartialRow(rhp.getBounds.getLowerBoundsList.asScala.toSeq, schema)
      val upper = getPartialRow(rhp.getBounds.getUpperBoundsList.asScala.toSeq, schema)
      new RangeWithHashSchema(lower, upper, hashSchemas.asJava)
    }.toSeq
  }

  def getPartitionSchema(metadata: TableMetadataPB): PartitionSchema = {
    val colNameToId = metadata.getColumnIdsMap.asScala
    val schema = getKuduSchema(metadata)
    val rangeIds =
      metadata.getPartitions.getRangePartitions.getColumnNamesList.asScala.map(colNameToId)
    val rangeSchema = new RangeSchema(rangeIds.asJava)
    val hashSchemas = metadata.getPartitions.getHashPartitionsList.asScala.map { hp =>
      val colIds = hp.getColumnNamesList.asScala.map(colNameToId)
      new HashBucketSchema(colIds.asJava, hp.getNumBuckets, hp.getSeed)
    }
    val rangesWithHashSchemas =
      metadata.getPartitions.getRangeAndHashPartitionsList.asScala.map { rhp =>
        val rangeHashSchemas = rhp.getHashPartitionsList.asScala.map { hp =>
          val colIds = hp.getColumnNamesList.asScala.map(colNameToId)
          new HashBucketSchema(colIds.asJava, hp.getNumBuckets, hp.getSeed)
        }
        val lower = getPartialRow(rhp.getBounds.getLowerBoundsList.asScala.toSeq, schema)
        val upper = getPartialRow(rhp.getBounds.getUpperBoundsList.asScala.toSeq, schema)
        new RangeWithHashSchema(lower, upper, rangeHashSchemas.asJava)
      }
    new PartitionSchema(rangeSchema, hashSchemas.asJava, rangesWithHashSchemas.asJava, schema)
  }
}
