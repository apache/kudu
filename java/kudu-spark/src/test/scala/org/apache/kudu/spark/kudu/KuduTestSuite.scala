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

import java.math.BigDecimal
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import org.apache.spark.SparkConf
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.ColumnTypeAttributes.ColumnTypeAttributesBuilder
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client.MiniKuduCluster.MiniKuduClusterBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduTable
import org.apache.kudu.client.MiniKuduCluster
import org.apache.kudu.junit.RetryRule
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.util.DecimalUtil
import org.apache.spark.sql.SparkSession
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.scalatest.junit.JUnitSuite

// TODO (grant): Use BaseKuduTest for most of this.
trait KuduTestSuite extends JUnitSuite {
  var ss: SparkSession = _
  var miniCluster: MiniKuduCluster = _
  var kuduClient: KuduClient = _
  var table: KuduTable = _
  var kuduContext: KuduContext = _

  val tableName: String = "test"
  val simpleTableName: String = "simple-test"

  lazy val schema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchemaBuilder("c1_i", Type.INT32).build(),
      new ColumnSchemaBuilder("c2_s", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("c3_double", Type.DOUBLE).build(),
      new ColumnSchemaBuilder("c4_long", Type.INT64).build(),
      new ColumnSchemaBuilder("c5_bool", Type.BOOL).build(),
      new ColumnSchemaBuilder("c6_short", Type.INT16).build(),
      new ColumnSchemaBuilder("c7_float", Type.FLOAT).build(),
      new ColumnSchemaBuilder("c8_binary", Type.BINARY).build(),
      new ColumnSchemaBuilder("c9_unixtime_micros", Type.UNIXTIME_MICROS)
        .build(),
      new ColumnSchemaBuilder("c10_byte", Type.INT8).build(),
      new ColumnSchemaBuilder("c11_decimal32", Type.DECIMAL)
        .typeAttributes(
          new ColumnTypeAttributesBuilder()
            .precision(DecimalUtil.MAX_DECIMAL32_PRECISION)
            .build()
        )
        .build(),
      new ColumnSchemaBuilder("c12_decimal64", Type.DECIMAL)
        .typeAttributes(
          new ColumnTypeAttributesBuilder()
            .precision(DecimalUtil.MAX_DECIMAL64_PRECISION)
            .build()
        )
        .build(),
      new ColumnSchemaBuilder("c13_decimal128", Type.DECIMAL)
        .typeAttributes(
          new ColumnTypeAttributesBuilder()
            .precision(DecimalUtil.MAX_DECIMAL128_PRECISION)
            .build()
        )
        .build()
    )
    new Schema(columns.asJava)
  }

  lazy val simpleSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchemaBuilder("val", Type.STRING).nullable(true).build()).asJava
    new Schema(columns)
  }

  val tableOptions: CreateTableOptions = {
    val bottom = schema.newPartialRow() // Unbounded.
    val middle = schema.newPartialRow()
    middle.addInt("key", 50)
    val top = schema.newPartialRow() // Unbounded.

    new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .addRangePartition(bottom, middle)
      .addRangePartition(middle, top)
      .setNumReplicas(1)
  }

  val appID: String = new Date().toString + math
    .floor(math.random * 10E4)
    .toLong
    .toString

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", appID)

  // Add a rule to rerun tests. We use this with Gradle because it doesn't support
  // Surefire/Failsafe rerunFailingTestsCount like Maven does.
  @Rule
  def retryRule = new RetryRule()

  @Before
  def setUpBase(): Unit = {
    miniCluster = new MiniKuduClusterBuilder()
      .numMasterServers(1)
      .numTabletServers(1)
      .build()

    ss = SparkSession.builder().config(conf).getOrCreate()

    kuduClient = new KuduClientBuilder(miniCluster.getMasterAddressesAsString).build()

    kuduContext = new KuduContext(miniCluster.getMasterAddressesAsString, ss.sparkContext)

    table = kuduClient.createTable(tableName, schema, tableOptions)

    val simpleTableOptions = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)

    kuduClient.createTable(simpleTableName, simpleSchema, simpleTableOptions)
  }

  @After
  def tearDownBase() {
    if (kuduClient != null) kuduClient.shutdown()
    if (miniCluster != null) miniCluster.shutdown()
    if (ss != null) ss.stop()
  }

  def deleteRow(key: Int): Unit = {
    val kuduSession = kuduClient.newSession()
    val delete = table.newDelete()
    delete.getRow.addInt(0, key)
    kuduSession.apply(delete)
  }

  def insertRows(
      targetTable: KuduTable,
      rowCount: Integer): IndexedSeq[(Int, Int, String, Long)] = {
    val kuduSession = kuduClient.newSession()

    val rows = Range(0, rowCount).map { i =>
      val insert = targetTable.newInsert
      val row = insert.getRow
      row.addInt(0, i)
      row.addInt(1, i)
      row.addDouble(3, i.toDouble)
      row.addLong(4, i.toLong)
      row.addBoolean(5, i % 2 == 1)
      row.addShort(6, i.toShort)
      row.addFloat(7, i.toFloat)
      row.addBinary(8, s"bytes $i".getBytes())
      val ts = System.currentTimeMillis() * 1000
      row.addLong(9, ts)
      row.addByte(10, i.toByte)
      row.addDecimal(11, BigDecimal.valueOf(i))
      row.addDecimal(12, BigDecimal.valueOf(i))
      row.addDecimal(13, BigDecimal.valueOf(i))

      // Sprinkling some nulls so that queries see them.
      val s = if (i % 2 == 0) {
        row.addString(2, i.toString)
        i.toString
      } else {
        row.setNull(2)
        null
      }

      kuduSession.apply(insert)
      (i, i, s, ts)
    }
    rows
  }
}
