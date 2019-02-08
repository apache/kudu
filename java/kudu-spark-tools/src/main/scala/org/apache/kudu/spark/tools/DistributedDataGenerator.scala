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
package org.apache.kudu.spark.tools

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets

import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.client.PartialRow
import org.apache.kudu.client.SessionConfiguration
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu.KuduWriteOptions
import org.apache.kudu.spark.kudu.RowConverter
import org.apache.kudu.spark.kudu.SparkUtil
import org.apache.kudu.spark.tools.DistributedDataGeneratorOptions._
import org.apache.kudu.util.DataGenerator
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.JavaConverters._

case class GeneratorMetrics(rowsWritten: LongAccumulator, collisions: LongAccumulator)

object GeneratorMetrics {

  def apply(sc: SparkContext): GeneratorMetrics = {
    GeneratorMetrics(sc.longAccumulator("rows_written"), sc.longAccumulator("row_collisions"))
  }
}

object DistributedDataGenerator {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def run(options: DistributedDataGeneratorOptions, ss: SparkSession): Unit = {
    log.info(s"Running a DistributedDataGenerator with options: $options")
    val sc = ss.sparkContext
    val context = new KuduContext(options.masterAddresses, sc)
    val metrics = GeneratorMetrics(sc)

    // Generate the Inserts.
    var rdd = sc
      .parallelize(0 until options.numTasks, numSlices = options.numTasks)
      .mapPartitions(
        { taskNumIter =>
          // We know there is only 1 task per partition because numSlices = options.numTasks above.
          val taskNum = taskNumIter.next()
          val generator = new DataGenerator.DataGeneratorBuilder()
          // Add taskNum to the seed otherwise each task will try to generate the same rows.
            .random(new java.util.Random(options.seed + taskNum))
            .stringLength(options.stringLength)
            .binaryLength(options.binaryLength)
            .build()
          val table = context.syncClient.openTable(options.tableName)
          val schema = table.getSchema
          val numRows = options.numRows / options.numTasks
          val startRow: Long = numRows * taskNum
          new GeneratedRowIterator(generator, options.generatorType, schema, startRow, numRows)
        },
        true
      )

    if (options.repartition) {
      val table = context.syncClient.openTable(options.tableName)
      val sparkSchema = SparkUtil.sparkSchema(table.getSchema)
      rdd = context
        .repartitionRows(rdd, options.tableName, sparkSchema, KuduWriteOptions(ignoreNull = true))
    }

    // Write the rows to Kudu.
    // TODO: Use context.writeRows while still tracking inserts/collisions.
    rdd.foreachPartition { rows =>
      val kuduClient = context.syncClient
      val table = kuduClient.openTable(options.tableName)
      val kuduSchema = table.getSchema
      val sparkSchema = SparkUtil.sparkSchema(kuduSchema)
      val converter = new RowConverter(kuduSchema, sparkSchema, ignoreNull = true)

      val session = kuduClient.newSession()
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)

      var rowsWritten = 0
      rows.foreach { row =>
        val insert = table.newInsert()
        val partialRow = converter.toPartialRow(row)
        insert.setRow(partialRow)
        session.apply(insert)
        rowsWritten += 1
      }
      // Synchronously flush after the last record is written.
      session.flush()

      // Track the collisions.
      var collisions = 0
      for (error <- session.getPendingErrors.getRowErrors) {
        if (error.getErrorStatus.isAlreadyPresent) {
          // Because we can't check for collisions every time, but instead
          // only when the rows are flushed, we subtract any rows that may
          // have failed from the counter.
          rowsWritten -= 1
          collisions += 1
        } else {
          throw new RuntimeException("Kudu write error: " + error.getErrorStatus.toString)
        }
      }
      metrics.rowsWritten.add(rowsWritten)
      metrics.collisions.add(collisions)
      session.close()
    }

    log.info(s"Rows written: ${metrics.rowsWritten.value}")
    log.info(s"Collisions: ${metrics.collisions.value}")
  }

  /**
   * Entry point for testing. SparkContext is a singleton,
   * so tests must create and manage their own.
   */
  @InterfaceAudience.LimitedPrivate(Array("Test"))
  def testMain(args: Array[String], ss: SparkSession): Unit = {
    DistributedDataGeneratorOptions.parse(args) match {
      case None => throw new IllegalArgumentException("Could not parse arguments")
      case Some(config) => run(config, ss)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DistributedDataGenerator")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    testMain(args, ss)
  }
}

private class GeneratedRowIterator(
    generator: DataGenerator,
    generatorType: String,
    schema: Schema,
    startRow: Long,
    numRows: Long)
    extends Iterator[Row] {

  val sparkSchema = SparkUtil.sparkSchema(schema)
  // ignoreNull values so unset/defaulted rows can be passed through.
  val converter = new RowConverter(schema, sparkSchema, ignoreNull = true)

  var currentRow: Long = startRow
  var rowsGenerated: Long = 0

  override def hasNext: Boolean = rowsGenerated < numRows

  override def next(): Row = {
    if (rowsGenerated >= numRows) {
      throw new IllegalStateException("Already generated all of the rows.")
    }

    val partialRow = schema.newPartialRow()
    if (generatorType == SequentialGenerator) {
      setRow(partialRow, currentRow)
    } else if (generatorType == RandomGenerator) {
      generator.randomizeRow(partialRow)
    } else {
      throw new IllegalArgumentException(s"Generator type of $generatorType is unsupported")
    }
    currentRow += 1
    rowsGenerated += 1
    converter.toRow(partialRow)
  }

  /**
   * Sets all the columns in the passed row to the passed value.
   * TODO(ghenke): Consider failing when value doesn't fit into the type.
   */
  private def setRow(row: PartialRow, value: Long): Unit = {
    val schema = row.getSchema
    val columns = schema.getColumns.asScala
    columns.indices.foreach { i =>
      val col = columns(i)
      col.getType match {
        case Type.BOOL =>
          row.addBoolean(i, value % 2 == 1)
        case Type.INT8 =>
          row.addByte(i, value.toByte)
        case Type.INT16 =>
          row.addShort(i, value.toShort)
        case Type.INT32 =>
          row.addInt(i, value.toInt)
        case Type.INT64 =>
          row.addLong(i, value)
        case Type.UNIXTIME_MICROS =>
          row.addLong(i, value)
        case Type.FLOAT =>
          row.addFloat(i, value.toFloat)
        case Type.DOUBLE =>
          row.addDouble(i, value.toDouble)
        case Type.DECIMAL =>
          row.addDecimal(
            i,
            new BigDecimal(BigInteger.valueOf(value), col.getTypeAttributes.getScale))
        case Type.STRING =>
          row.addString(i, String.valueOf(value))
        case Type.BINARY =>
          val bytes: Array[Byte] = String.valueOf(value).getBytes(StandardCharsets.UTF_8)
          row.addBinary(i, bytes)
        case _ =>
          throw new UnsupportedOperationException("Unsupported type " + col.getType)
      }
    }
  }
}

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class DistributedDataGeneratorOptions(
    tableName: String,
    masterAddresses: String,
    generatorType: String = DistributedDataGeneratorOptions.DefaultGeneratorType,
    numRows: Long = DistributedDataGeneratorOptions.DefaultNumRows,
    numTasks: Int = DistributedDataGeneratorOptions.DefaultNumTasks,
    stringLength: Int = DistributedDataGeneratorOptions.DefaultStringLength,
    binaryLength: Int = DistributedDataGeneratorOptions.DefaultStringLength,
    seed: Long = System.currentTimeMillis(),
    repartition: Boolean = DistributedDataGeneratorOptions.DefaultRepartition)

@InterfaceAudience.Private
@InterfaceStability.Unstable
object DistributedDataGeneratorOptions {
  val DefaultNumRows: Long = 10000
  val DefaultNumTasks: Int = 1
  val DefaultStringLength: Int = 128
  val DefaultBinaryLength: Int = 128
  val RandomGenerator: String = "random"
  val SequentialGenerator: String = "sequential"
  val DefaultGeneratorType: String = SequentialGenerator
  val DefaultRepartition: Boolean = false

  private val parser: OptionParser[DistributedDataGeneratorOptions] =
    new OptionParser[DistributedDataGeneratorOptions]("LoadRandomData") {

      arg[String]("table-name")
        .action((v, o) => o.copy(tableName = v))
        .text("The table to load with random data")

      arg[String]("master-addresses")
        .action((v, o) => o.copy(masterAddresses = v))
        .text("Comma-separated addresses of Kudu masters")

      opt[String]("type")
        .action((v, o) => o.copy(generatorType = v))
        .text(s"The type of data generator. Must be one of 'random' or 'sequential'. " +
          s"Default: ${DefaultGeneratorType}")
        .optional()

      opt[Long]("num-rows")
        .action((v, o) => o.copy(numRows = v))
        .text(s"The total number of unique rows to generate. Default: ${DefaultNumRows}")
        .optional()

      opt[Int]("num-tasks")
        .action((v, o) => o.copy(numTasks = v))
        .text(s"The total number of Spark tasks to use when generating data. " +
          s"Default: ${DefaultNumTasks}")
        .optional()

      opt[Int]("string-length")
        .action((v, o) => o.copy(stringLength = v))
        .text(s"The length of generated string fields. Default: ${DefaultStringLength}")
        .optional()

      opt[Int]("binary-length")
        .action((v, o) => o.copy(binaryLength = v))
        .text(s"The length of generated binary fields. Default: ${DefaultBinaryLength}")
        .optional()

      opt[Long]("seed")
        .action((v, o) => o.copy(seed = v))
        .text(s"The seed to use in the random data generator. " +
          s"Default: `System.currentTimeMillis()`")

      opt[Boolean]("repartition")
        .action((v, o) => o.copy(repartition = v))
        .text(s"Repartition the data to ensure each spark task talks to a minimal " +
          s"set of tablet servers.")
    }

  def parse(args: Seq[String]): Option[DistributedDataGeneratorOptions] = {
    parser.parse(args, DistributedDataGeneratorOptions("", ""))
  }
}
