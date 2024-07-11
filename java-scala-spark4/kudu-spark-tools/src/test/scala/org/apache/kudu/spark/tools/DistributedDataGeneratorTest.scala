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

import org.apache.kudu.Type
import org.apache.kudu.client.KuduPartitioner
import org.apache.kudu.spark.kudu.KuduTestSuite
import org.apache.kudu.test.RandomUtils
import org.apache.kudu.util.DecimalUtil
import org.apache.kudu.util.SchemaGenerator
import org.apache.kudu.spark.kudu.SparkListenerUtil.withJobTaskCounter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.junit.Test
import org.junit.Assert.assertEquals
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DistributedDataGeneratorTest extends KuduTestSuite {
  val log: Logger = LoggerFactory.getLogger(getClass)

  private val generator = new SchemaGenerator.SchemaGeneratorBuilder()
    .random(RandomUtils.getRandom)
    // These types don't have enough values to prevent collisions.
    .excludeTypes(Type.BOOL, Type.INT8)
    // Ensure decimals have enough values to prevent collisions.
    .precisionRange(DecimalUtil.MAX_DECIMAL32_PRECISION, DecimalUtil.MAX_DECIMAL_PRECISION)
    .build()

  private val randomTableName: String = "random-table"

  @Test
  def testGenerateRandomData() {
    val numRows = 100
    val args = Array(
      s"--num-rows=$numRows",
      "--num-tasks=10",
      "--type=random",
      randomTableName,
      harness.getMasterAddressesAsString)
    val (metrics, rdd) = runGeneratorTest(args)
    val (rowsWritten, collisions) = (metrics.rowsWritten.value, metrics.collisions.value)
    // Collisions may cause the number of rows written to be less than the number generated.
    assertEquals(rowsWritten, rdd.collect.length.toLong)
    assertEquals(numRows, rowsWritten + collisions)
  }

  @Test
  def testGenerateSequentialData() {
    val numRows = 100
    val args = Array(
      s"--num-rows=$numRows",
      "--num-tasks=10",
      "--type=sequential",
      randomTableName,
      harness.getMasterAddressesAsString)
    val (metrics, rdd) = runGeneratorTest(args)
    val (rowsWritten, collisions) = (metrics.rowsWritten.value, metrics.collisions.value)
    assertEquals(numRows.toLong, rowsWritten)
    assertEquals(numRows, rdd.collect.length)
    assertEquals(0L, collisions)
  }

  @Test
  def testRepartitionData() {
    val numRows = 100
    val args = Array(
      s"--num-rows=$numRows",
      "--num-tasks=10",
      "--type=sequential",
      "--repartition=true",
      randomTableName,
      harness.getMasterAddressesAsString)
    val (metrics, rdd) = runGeneratorTest(args)
    val (rowsWritten, collisions) = (metrics.rowsWritten.value, metrics.collisions.value)
    assertEquals(numRows.toLong, rowsWritten)
    assertEquals(numRows, rdd.collect.length)
    assertEquals(0L, collisions)
  }

  @Test
  def testNumTasks() {
    val numTasks = 8
    val numRows = 100
    val args = Array(
      s"--num-rows=$numRows",
      s"--num-tasks=$numTasks",
      randomTableName,
      harness.getMasterAddressesAsString)

    // count the number of tasks that end.
    val actualNumTasks = withJobTaskCounter(ss.sparkContext) { () =>
      runGeneratorTest(args)
    }
    assertEquals(numTasks, actualNumTasks)
  }

  @Test
  def testNumTasksRepartition(): Unit = {
    val numTasks = 8
    val numRows = 100
    val args = Array(
      s"--num-rows=$numRows",
      s"--num-tasks=$numTasks",
      "--repartition=true",
      randomTableName,
      harness.getMasterAddressesAsString)

    // count the number of tasks that end.
    val actualNumTasks = withJobTaskCounter(ss.sparkContext) { () =>
      runGeneratorTest(args)
    }

    val table = kuduContext.syncClient.openTable(randomTableName)
    val numPartitions = new KuduPartitioner.KuduPartitionerBuilder(table).build().numPartitions()

    // We expect the number of tasks to be equal to numTasks + numPartitions because numTasks tasks
    // are run to generate the data then we repartition the data to match the table partitioning
    // and numPartitions tasks load the data.
    assertEquals(numTasks + numPartitions, actualNumTasks)
  }

  def runGeneratorTest(args: Array[String]): (GeneratorMetrics, RDD[Row]) = {
    val schema = generator.randomSchema()
    val options = generator.randomCreateTableOptions(schema)
    kuduClient.createTable(randomTableName, schema, options)
    val metrics = DistributedDataGenerator.testMain(args, ss)
    (metrics, kuduContext.kuduRDD(ss.sparkContext, randomTableName))
  }
}
