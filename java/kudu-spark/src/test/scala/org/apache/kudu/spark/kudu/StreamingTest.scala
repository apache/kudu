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

import org.apache.kudu.test.KuduTestHarness.MasterServerConfig
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
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
      assertEquals(actual, expectedData.toSet)
    }
    input.addData(1, 2, 3)
    query.processAllAvailable()
    verifyOutput(expectedData = Seq((2, "1"), (3, "2"), (4, "3")))
    query.stop()
  }

  /**
   * Runs a streaming write to the Kudu table with the given extra options and
   * asserts that it fails with an error whose (possibly nested) message contains
   * `expectedMessage`.
   */
  private def assertStreamingWriteFails(
      extraOptions: Map[String, String],
      expectedMessage: String): Unit = {
    KuduClientCache.clearCacheForTests()
    val spark = ss
    import spark.implicits._
    val checkpointDir = java.nio.file.Files.createTempDirectory("spark_kudu")
    val input = MemoryStream[Int]
    input.addData(1, 2, 3)
    var query: StreamingQuery = null
    val exception =
      try {
        query = input
          .toDS()
          .map(v => (v + 1, v.toString))
          .toDF("key", "val")
          .writeStream
          .format("kudu")
          .option("kudu.master", harness.getMasterAddressesAsString)
          .option("kudu.table", simpleTableName)
          .option("checkpointLocation", checkpointDir.toFile.getCanonicalPath)
          .options(extraOptions)
          .outputMode(OutputMode.Update)
          .start()
        query.processAllAvailable()
        None
      } catch {
        case e: Throwable => Some(e)
      } finally {
        if (query != null) query.stop()
      }
    assertTrue("expected the streaming write to fail", exception.isDefined)
    val messages = Iterator
      .iterate(exception.get)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage))
      .mkString(" | ")
    assertTrue(s"unexpected exception: $messages", messages.contains(expectedMessage))
  }

  /**
   * Verifies that the KuduSink honors the "kudu.requireAuthentication" option.
   * Against an insecure cluster, requiring authentication must fail the write
   * rather than silently falling back to the default (no authentication).
   */
  @Test
  def testKuduRequireAuthenticationInsecureClusterStreaming() {
    assertStreamingWriteFails(
      Map("kudu.requireAuthentication" -> "true"),
      "client requires authentication, but server does not have Kerberos enabled")
  }

  /**
   * Verifies that the KuduSink honors the "kudu.encryptionPolicy" option.
   * Against a cluster with RPC encryption disabled, requiring encryption must
   * fail the write rather than silently falling back to the default (OPTIONAL).
   */
  @Test
  @MasterServerConfig(flags = Array("--rpc_encryption=disabled", "--rpc_authentication=disabled"))
  @TabletServerConfig(flags = Array("--rpc_encryption=disabled", "--rpc_authentication=disabled"))
  def testKuduRequireEncryptionInsecureClusterStreaming() {
    assertStreamingWriteFails(
      Map("kudu.encryptionPolicy" -> "required_remote"),
      "server does not support required TLS encryption")
  }
}
