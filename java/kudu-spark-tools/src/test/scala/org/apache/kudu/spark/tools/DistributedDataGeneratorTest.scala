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
import org.apache.kudu.spark.kudu.KuduTestSuite
import org.apache.kudu.test.RandomUtils
import org.apache.kudu.util.DecimalUtil
import org.apache.kudu.util.SchemaGenerator
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
    val rdd = runGeneratorTest(args)
    assertEquals(numRows, rdd.collect.length)
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
    val rdd = runGeneratorTest(args)
    assertEquals(numRows, rdd.collect.length)
  }

  def runGeneratorTest(args: Array[String]): RDD[Row] = {
    val schema = generator.randomSchema()
    val options = generator.randomCreateTableOptions(schema)
    kuduClient.createTable(randomTableName, schema, options)
    DistributedDataGenerator.testMain(args, ss)
    kuduContext.kuduRDD(ss.sparkContext, randomTableName)
  }
}
