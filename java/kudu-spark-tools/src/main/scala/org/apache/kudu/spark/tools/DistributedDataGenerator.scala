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

import org.apache.kudu.Type
import org.apache.kudu.client.PartialRow
import org.apache.kudu.client.SessionConfiguration
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.tools.DistributedDataGeneratorOptions._
import org.apache.kudu.util.DataGenerator
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

  def generateRows(
      context: KuduContext,
      options: DistributedDataGeneratorOptions,
      taskNum: Int,
      metrics: GeneratorMetrics) {

    val kuduClient = context.syncClient
    val session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
    val kuduTable = kuduClient.openTable(options.tableName)

    val generator = new DataGenerator.DataGeneratorBuilder()
    // Add taskNum to the seed otherwise each task will try to generate the same rows.
      .random(new java.util.Random(options.seed + taskNum))
      .stringLength(options.stringLength)
      .binaryLength(options.binaryLength)
      .build()

    val rowsToWrite = options.numRows / options.numTasks
    var currentRow: Long = rowsToWrite * taskNum
    var rowsWritten: Long = 0
    while (rowsWritten < rowsToWrite) {
      val insert = kuduTable.newInsert()
      if (options.generatorType == SequentialGenerator) {
        setRow(insert.getRow, currentRow)
      } else if (options.generatorType == RandomGenerator) {
        generator.randomizeRow(insert.getRow)
      }
      session.apply(insert)

      // Synchronously flush on potentially the last iteration of the
      // loop, so we can check whether we need to retry any collisions.
      if (rowsWritten + 1 == rowsToWrite) session.flush()

      for (error <- session.getPendingErrors.getRowErrors) {
        if (error.getErrorStatus.isAlreadyPresent) {
          // Because we can't check for collisions every time, but instead
          // only when the rows are flushed, we subtract any rows that may
          // have failed from the counter.
          rowsWritten -= 1
          metrics.collisions.add(1)
        } else {
          throw new RuntimeException("Kudu write error: " + error.getErrorStatus.toString)
        }
      }
      currentRow += 1
      rowsWritten += 1
    }
    metrics.rowsWritten.add(rowsWritten)
    session.close()
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

  def run(options: DistributedDataGeneratorOptions, ss: SparkSession): Unit = {
    log.info(s"Running a DistributedDataGenerator with options: $options")
    val sc = ss.sparkContext
    val context = new KuduContext(options.masterAddresses, sc)
    val metrics = GeneratorMetrics(sc)
    sc.parallelize(0 until options.numTasks, numSlices = options.numTasks)
      .foreachPartition(taskNum => generateRows(context, options, taskNum.next(), metrics))
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
    seed: Long = System.currentTimeMillis())

@InterfaceAudience.Private
@InterfaceStability.Unstable
object DistributedDataGeneratorOptions {
  val DefaultNumRows: Long = 10000
  val DefaultNumTasks: Int = 1
  val DefaultStringLength: Int = 128
  val DefaultBinaryLength: Int = 128
  val RandomGenerator: String = "random"
  val SequentialGenerator: String = "sequential"
  val DefaultGeneratorType: String = RandomGenerator

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
        .text(s"The total number of Spark tasks to generate. Default: ${DefaultNumTasks}")
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
    }

  def parse(args: Seq[String]): Option[DistributedDataGeneratorOptions] = {
    parser.parse(args, DistributedDataGeneratorOptions("", ""))
  }
}
