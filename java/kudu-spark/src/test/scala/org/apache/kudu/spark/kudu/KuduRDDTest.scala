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

import scala.collection.JavaConverters._
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.junit.Assert.assertEquals
import org.junit.Test

class KuduRDDTest extends KuduTestSuite {

  @Test
  def testCollectRows() {
    insertRows(table, 100)
    val rdd = kuduContext.kuduRDD(ss.sparkContext, tableName, List("key"))
    assertEquals(100, rdd.collect().length)
    assertEquals(100L, rdd.asInstanceOf[KuduRDD].rowsRead.value)
  }

  @Test
  @TabletServerConfig(
    // Hard coded values because Scala doesn't handle array constants in annotations.
    flags = Array(
      "--scanner_ttl_ms=5000",
      "--scanner_gc_check_interval_us=500000" // 10% of the TTL.
    ))
  def testKeepAlive() {
    val rowCount = 500
    val shortScannerTtlMs = 5000

    // Create a simple table with a single partition.
    val tableName = "testKeepAlive"
    val tableSchema = {
      val columns = List(
        new ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
        new ColumnSchemaBuilder("val", Type.INT32).build()).asJava
      new Schema(columns)
    }
    val tableOptions = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)
    val table = kuduClient.createTable(tableName, tableSchema, tableOptions)

    val session = kuduClient.newSession()
    Range(0, rowCount).map { i =>
      val insert = table.newInsert
      val row = insert.getRow
      row.addInt(0, i)
      row.addInt(1, i)
      session.apply(insert)
    }
    session.flush()

    def processRDD(rdd: RDD[Row]): Unit = {
      // Ensure reading takes longer than the scanner ttl.
      var i = 0
      rdd.foreach { row =>
        // Sleep for half the ttl for the first few rows. This ensures
        // we are on the same tablet and will go past the ttl without
        // a new scan request. It also ensures a single row doesn't go
        // longer than the ttl.
        if (i < 5) {
          Thread.sleep(shortScannerTtlMs / 2) // Sleep for half the ttl.
          i = i + 1
        }
      }
    }

    // Test that a keepAlivePeriodMs less than the scanner ttl is successful.
    val goodRdd = kuduContext.kuduRDD(
      ss.sparkContext,
      tableName,
      List("key"),
      KuduReadOptions(
        batchSize = 100, // Set a small batch size so the first scan doesn't read all the rows.
        keepAlivePeriodMs = shortScannerTtlMs / 4)
    )
    processRDD(goodRdd)

    // Test that a keepAlivePeriodMs greater than the scanner ttl fails.
    val badRdd = kuduContext.kuduRDD(
      ss.sparkContext,
      tableName,
      List("key"),
      KuduReadOptions(
        batchSize = 100, // Set a small batch size so the first scan doesn't read all the rows.
        keepAlivePeriodMs = shortScannerTtlMs * 2)
    )
    try {
      processRDD(badRdd)
      fail("Should throw a scanner not found exception")
    } catch {
      case ex: SparkException =>
        assert(ex.getMessage.matches("(?s).*Scanner .* not found.*"))
    }
  }
}
