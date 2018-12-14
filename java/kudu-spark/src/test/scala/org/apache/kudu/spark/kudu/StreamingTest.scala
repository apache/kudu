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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Before
import org.junit.Test

class StreamingTest extends KuduTestSuite {

  implicit var sqlContext: SQLContext = _
  var kuduOptions: Map[String, String] = _

  @Before
  def setUp(): Unit = {
    sqlContext = ss.sqlContext
    kuduOptions =
      Map("kudu.table" -> simpleTableName, "kudu.master" -> harness.getMasterAddressesAsString)
  }

  @Test
  def testKuduContextWithSparkStreaming() {
    val spark = ss
    import spark.implicits._
    val checkpointDir = java.nio.file.Files.createTempDirectory("spark_kudu")
    val input = MemoryStream[Int]
    val query = input
      .toDS()
      .map(v => (v + 1, v.toString))
      .toDF("key", "val")
      .writeStream
      .format("kudu")
      .option("kudu.master", harness.getMasterAddressesAsString)
      .option("kudu.table", simpleTableName)
      .option("checkpointLocation", checkpointDir.toFile.getCanonicalPath)
      .outputMode(OutputMode.Update)
      .start()

    def verifyOutput(expectedData: Seq[(Int, String)]): Unit = {
      val df = sqlContext.read.options(kuduOptions).format("kudu").load
      val actual = df.rdd
        .map { row =>
          (row.get(0), row.getString(1))
        }
        .collect()
        .toSet
      assert(actual === expectedData.toSet)
    }
    input.addData(1, 2, 3)
    query.processAllAvailable()
    verifyOutput(expectedData = Seq((2, "1"), (3, "2"), (4, "3")))
    query.stop()
  }
}
