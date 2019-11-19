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
import scala.collection.immutable.IndexedSeq
import scala.util.control.NonFatal
import org.apache.spark.sql.SQLContext
import org.junit.Assert._
import org.scalatest.Matchers
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.spark.kudu.SparkListenerUtil.withJobTaskCounter
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.junit.Before
import org.junit.Test

class SparkSQLTest extends KuduTestSuite with Matchers {
  val rowCount = 10
  var sqlContext: SQLContext = _
  var rows: IndexedSeq[(Int, Int, String, Long)] = _
  var kuduOptions: Map[String, String] = _

  @Before
  def setUp(): Unit = {
    rows = insertRows(table, rowCount)

    sqlContext = ss.sqlContext

    kuduOptions =
      Map("kudu.table" -> tableName, "kudu.master" -> harness.getMasterAddressesAsString)

    sqlContext.read
      .options(kuduOptions)
      .format("kudu")
      .load()
      .createOrReplaceTempView(tableName)
  }

  @Test
  def testBasicSparkSQL() {
    val results = sqlContext.sql("SELECT * FROM " + tableName).collectAsList()
    assert(results.size() == rowCount)

    assert(results.get(1).isNullAt(2))
    assert(!results.get(0).isNullAt(2))
  }

  @Test
  def testBasicSparkSQLWithProjection() {
    val results = sqlContext.sql("SELECT key FROM " + tableName).collectAsList()
    assert(results.size() == rowCount)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(0))
  }

  @Test
  def testBasicSparkSQLWithPredicate() {
    val results = sqlContext
      .sql("SELECT key FROM " + tableName + " where key=1")
      .collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(1))

  }

  @Test
  def testBasicSparkSQLWithTwoPredicates() {
    val results = sqlContext
      .sql("SELECT key FROM " + tableName + " where key=2 and c2_s='2'")
      .collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(2))
  }

  @Test
  def testBasicSparkSQLWithInListPredicate() {
    val keys = Array(1, 5, 7)
    val results = sqlContext
      .sql(s"SELECT key FROM $tableName where key in (${keys.mkString(", ")})")
      .collectAsList()
    assert(results.size() == keys.length)
    keys.zipWithIndex.foreach {
      case (v, i) =>
        assert(results.get(i).size.equals(1))
        assert(results.get(i).getInt(0).equals(v))
    }
  }

  @Test
  def testBasicSparkSQLWithInListPredicateOnString() {
    val keys = Array(1, 4, 6)
    val results = sqlContext
      .sql(s"SELECT key FROM $tableName where c2_s in (${keys.mkString("'", "', '", "'")})")
      .collectAsList()
    assert(results.size() == keys.count(_ % 2 == 0))
    keys.filter(_ % 2 == 0).zipWithIndex.foreach {
      case (v, i) =>
        assert(results.get(i).size.equals(1))
        assert(results.get(i).getInt(0).equals(v))
    }
  }

  @Test
  def testBasicSparkSQLWithInListAndComparisonPredicate() {
    val keys = Array(1, 5, 7)
    val results = sqlContext
      .sql(s"SELECT key FROM $tableName where key>2 and key in (${keys.mkString(", ")})")
      .collectAsList()
    assert(results.size() == keys.count(_ > 2))
    keys.filter(_ > 2).zipWithIndex.foreach {
      case (v, i) =>
        assert(results.get(i).size.equals(1))
        assert(results.get(i).getInt(0).equals(v))
    }
  }

  @Test
  def testBasicSparkSQLWithTwoPredicatesNegative() {
    val results = sqlContext
      .sql("SELECT key FROM " + tableName + " where key=1 and c2_s='2'")
      .collectAsList()
    assert(results.size() == 0)
  }

  @Test
  def testBasicSparkSQLWithTwoPredicatesIncludingString() {
    val results = sqlContext
      .sql("SELECT key FROM " + tableName + " where c2_s='2'")
      .collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(2))
  }

  @Test
  def testBasicSparkSQLWithTwoPredicatesAndProjection() {
    val results = sqlContext
      .sql("SELECT key, c2_s FROM " + tableName + " where c2_s='2'")
      .collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(2))
    assert(results.get(0).getInt(0).equals(2))
    assert(results.get(0).getString(1).equals("2"))
  }

  @Test
  def testBasicSparkSQLWithTwoPredicatesGreaterThan() {
    val results = sqlContext
      .sql("SELECT key, c2_s FROM " + tableName + " where c2_s>='2'")
      .collectAsList()
    assert(results.size() == 4)
    assert(results.get(0).size.equals(2))
    assert(results.get(0).getInt(0).equals(2))
    assert(results.get(0).getString(1).equals("2"))
  }

  @Test
  def testSparkSQLStringStartsWithFilters() {
    // This test requires a special table.
    val testTableName = "startswith"
    val schema = new Schema(
      List(new ColumnSchemaBuilder("key", Type.STRING).key(true).build()).asJava)
    val tableOptions = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)
    val testTable = kuduClient.createTable(testTableName, schema, tableOptions)

    val kuduSession = kuduClient.newSession()
    val chars = List('a', 'b', '乕', Char.MaxValue, '\u0000')
    val keys = for {
      x <- chars
      y <- chars
      z <- chars
      w <- chars
    } yield Array(x, y, z, w).mkString
    keys.foreach { key =>
      val insert = testTable.newInsert
      val row = insert.getRow
      row.addString(0, key)
      kuduSession.apply(insert)
    }
    val options: Map[String, String] =
      Map("kudu.table" -> testTableName, "kudu.master" -> harness.getMasterAddressesAsString)
    sqlContext.read.options(options).format("kudu").load.createOrReplaceTempView(testTableName)

    val checkPrefixCount = { prefix: String =>
      val results = sqlContext.sql(s"SELECT key FROM $testTableName WHERE key LIKE '$prefix%'")
      assertEquals(keys.count(k => k.startsWith(prefix)), results.count())
    }
    // empty string
    checkPrefixCount("")
    // one character
    for (x <- chars) {
      checkPrefixCount(Array(x).mkString)
    }
    // all two character combos
    for {
      x <- chars
      y <- chars
    } {
      checkPrefixCount(Array(x, y).mkString)
    }
  }

  @Test
  def testSparkSQLIsNullPredicate() {
    var results = sqlContext
      .sql("SELECT key FROM " + tableName + " where c2_s IS NULL")
      .collectAsList()
    assert(results.size() == 5)

    results = sqlContext
      .sql("SELECT key FROM " + tableName + " where key IS NULL")
      .collectAsList()
    assert(results.isEmpty)
  }

  @Test
  def testSparkSQLIsNotNullPredicate() {
    var results = sqlContext
      .sql("SELECT key FROM " + tableName + " where c2_s IS NOT NULL")
      .collectAsList()
    assert(results.size() == 5)

    results = sqlContext
      .sql("SELECT key FROM " + tableName + " where key IS NOT NULL")
      .collectAsList()
    assert(results.size() == 10)
  }

  @Test
  def testSQLInsertInto() {
    val insertTable = "insertintotest"

    // read 0 rows just to get the schema
    val df = sqlContext.sql(s"SELECT * FROM $tableName LIMIT 0")
    kuduContext.createTable(
      insertTable,
      df.schema,
      Seq("key"),
      new CreateTableOptions()
        .setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))

    val newOptions: Map[String, String] =
      Map("kudu.table" -> insertTable, "kudu.master" -> harness.getMasterAddressesAsString)
    sqlContext.read
      .options(newOptions)
      .format("kudu")
      .load
      .createOrReplaceTempView(insertTable)

    sqlContext.sql(s"INSERT INTO TABLE $insertTable SELECT * FROM $tableName")
    val results =
      sqlContext.sql(s"SELECT key FROM $insertTable").collectAsList()
    assertEquals(10, results.size())
  }

  @Test
  def testSQLInsertOverwriteUnsupported() {
    val insertTable = "insertoverwritetest"

    // read 0 rows just to get the schema
    val df = sqlContext.sql(s"SELECT * FROM $tableName LIMIT 0")
    kuduContext.createTable(
      insertTable,
      df.schema,
      Seq("key"),
      new CreateTableOptions()
        .setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))

    val newOptions: Map[String, String] =
      Map("kudu.table" -> insertTable, "kudu.master" -> harness.getMasterAddressesAsString)
    sqlContext.read
      .options(newOptions)
      .format("kudu")
      .load
      .createOrReplaceTempView(insertTable)

    try {
      sqlContext.sql(s"INSERT OVERWRITE TABLE $insertTable SELECT * FROM $tableName")
      fail("insert overwrite should throw UnsupportedOperationException")
    } catch {
      case _: UnsupportedOperationException => // good
      case NonFatal(_) =>
        fail("insert overwrite should throw UnsupportedOperationException")
    }
  }

  @Test
  def testTableScanWithProjection() {
    assertEquals(10, sqlContext.sql(s"""SELECT key FROM $tableName""").count())
  }

  @Test
  def testTableScanWithProjectionAndPredicateDouble() {
    assertEquals(
      rows.count { case (_, i, _, _) => i > 5 },
      sqlContext
        .sql(s"""SELECT key, c3_double FROM $tableName where c3_double > "5.0"""")
        .count())
  }

  @Test
  def testTableScanWithProjectionAndPredicateLong() {
    assertEquals(
      rows.count { case (_, i, _, _) => i > 5 },
      sqlContext
        .sql(s"""SELECT key, c4_long FROM $tableName where c4_long > "5"""")
        .count())
  }

  @Test
  def testTableScanWithProjectionAndPredicateBool() {
    assertEquals(
      rows.count { case (_, i, _, _) => i % 2 == 0 },
      sqlContext
        .sql(s"""SELECT key, c5_bool FROM $tableName where c5_bool = true""")
        .count())
  }

  @Test
  def testTableScanWithProjectionAndPredicateShort() {
    assertEquals(
      rows.count { case (_, i, _, _) => i > 5 },
      sqlContext
        .sql(s"""SELECT key, c6_short FROM $tableName where c6_short > 5""")
        .count())

  }

  @Test
  def testTableScanWithProjectionAndPredicateFloat() {
    assertEquals(
      rows.count { case (_, i, _, _) => i > 5 },
      sqlContext
        .sql(s"""SELECT key, c7_float FROM $tableName where c7_float > 5""")
        .count())

  }

  @Test
  def testTableScanWithProjectionAndPredicateDecimal32() {
    assertEquals(
      rows.count { case (_, i, _, _) => i > 5 },
      sqlContext
        .sql(s"""SELECT key, c11_decimal32 FROM $tableName where c11_decimal32 > 5""")
        .count())
  }

  @Test
  def testTableScanWithProjectionAndPredicateDecimal64() {
    assertEquals(
      rows.count { case (_, i, _, _) => i > 5 },
      sqlContext
        .sql(s"""SELECT key, c12_decimal64 FROM $tableName where c12_decimal64 > 5""")
        .count())
  }

  @Test
  def testTableScanWithProjectionAndPredicateDecimal128() {
    assertEquals(
      rows.count { case (_, i, _, _) => i > 5 },
      sqlContext
        .sql(s"""SELECT key, c13_decimal128 FROM $tableName where c13_decimal128 > 5""")
        .count())
  }

  @Test
  def testTableScanWithProjectionAndPredicate() {
    assertEquals(
      rows.count { case (_, _, s, _) => s != null && s > "5" },
      sqlContext
        .sql(s"""SELECT key FROM $tableName where c2_s > "5"""")
        .count())

    assertEquals(
      rows.count { case (_, _, s, _) => s != null },
      sqlContext
        .sql(s"""SELECT key, c2_s FROM $tableName where c2_s IS NOT NULL""")
        .count())
  }

  @Test
  def testScanLocality() {
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.scanLocality" -> "closest_replica")

    val table = "scanLocalityTest"
    sqlContext.read.options(kuduOptions).format("kudu").load.createOrReplaceTempView(table)
    val results = sqlContext.sql(s"SELECT * FROM $table").collectAsList()
    assert(results.size() == rowCount)

    assert(!results.get(0).isNullAt(2))
    assert(results.get(1).isNullAt(2))
  }

  @Test
  def testTableNonFaultTolerantScan() {
    val results = sqlContext.sql(s"SELECT * FROM $tableName").collectAsList()
    assert(results.size() == rowCount)

    assert(!results.get(0).isNullAt(2))
    assert(results.get(1).isNullAt(2))
  }

  @Test
  def testTableFaultTolerantScan() {
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.faultTolerantScan" -> "true")

    val table = "faultTolerantScanTest"
    sqlContext.read.options(kuduOptions).format("kudu").load.createOrReplaceTempView(table)
    val results = sqlContext.sql(s"SELECT * FROM $table").collectAsList()
    assert(results.size() == rowCount)

    assert(!results.get(0).isNullAt(2))
    assert(results.get(1).isNullAt(2))
  }

  @Test
  @TabletServerConfig(
    flags = Array(
      "--flush_threshold_mb=1",
      "--flush_threshold_secs=1",
      // Disable rowset compact to prevent DRSs being merged because they are too small.
      "--enable_rowset_compaction=false"
    ))
  def testScanWithKeyRange() {
    upsertRowsWithRowDataSize(table, rowCount * 100, 32 * 1024)

    // Wait for mrs flushed
    Thread.sleep(5 * 1000)

    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.splitSizeBytes" -> "1024")

    // count the number of tasks that end.
    val actualNumTasks = withJobTaskCounter(ss.sparkContext) { () =>
      val t = "scanWithKeyRangeTest"
      sqlContext.read.options(kuduOptions).format("kudu").load.createOrReplaceTempView(t)
      val results = sqlContext.sql(s"SELECT * FROM $t").collectAsList()
      assertEquals(rowCount * 100, results.size())
    }
    assert(actualNumTasks > 2)
  }

  @Test
  @MasterServerConfig(
    flags = Array(
      "--mock_table_metrics_for_testing=true",
      "--on_disk_size_for_testing=1024",
      "--live_row_count_for_testing=100"
    ))
  def testJoinWithTableStatistics(): Unit = {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load

    // 1. Create two tables.
    val table1 = "table1"
    kuduContext.createTable(
      table1,
      df.schema,
      Seq("key"),
      new CreateTableOptions()
        .setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))
    val options1: Map[String, String] =
      Map("kudu.table" -> table1, "kudu.master" -> harness.getMasterAddressesAsString)
    df.write.options(options1).mode("append").format("kudu").save
    val df1 = sqlContext.read.options(options1).format("kudu").load
    df1.createOrReplaceTempView(table1)

    val table2 = "table2"
    kuduContext.createTable(
      table2,
      df.schema,
      Seq("key"),
      new CreateTableOptions()
        .setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))
    val options2: Map[String, String] =
      Map("kudu.table" -> table2, "kudu.master" -> harness.getMasterAddressesAsString)
    df.write.options(options2).mode("append").format("kudu").save
    val df2 = sqlContext.read.options(options2).format("kudu").load
    df2.createOrReplaceTempView(table2)

    // 2. Get the table statistics of each table and verify.
    val relation1 = kuduRelationFromDataFrame(df1)
    val relation2 = kuduRelationFromDataFrame(df2)
    assert(relation1.sizeInBytes == relation2.sizeInBytes)
    assert(relation1.sizeInBytes == 1024)

    // 3. Test join with table size should be able to broadcast.
    val sqlStr = s"SELECT * FROM $table1 JOIN $table2 ON $table1.key = $table2.key"
    val physical = sqlContext.sql(sqlStr).queryExecution.sparkPlan
    val operators = physical.collect {
      case j: BroadcastHashJoinExec => j
    }
    assert(operators.size == 1)

    // Verify result.
    val results = sqlContext.sql(sqlStr).collectAsList()
    assert(results.size() == rowCount)
  }
}
