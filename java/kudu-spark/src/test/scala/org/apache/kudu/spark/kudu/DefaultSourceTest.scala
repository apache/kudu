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

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.util.control.NonFatal

import com.google.common.collect.ImmutableList
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.{Schema, Type}

@RunWith(classOf[JUnitRunner])
class DefaultSourceTest extends FunSuite with TestContext with BeforeAndAfter with Matchers {

  test("timestamp conversion") {
    val epoch = new Timestamp(0)
    assertEquals(0, KuduRelation.timestampToMicros(epoch))
    assertEquals(epoch, KuduRelation.microsToTimestamp(0))

    val t1 = new Timestamp(0)
    t1.setNanos(123456000)
    assertEquals(123456, KuduRelation.timestampToMicros(t1))
    assertEquals(t1, KuduRelation.microsToTimestamp(123456))

    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    iso8601.setTimeZone(TimeZone.getTimeZone("UTC"))

    val t3 = new Timestamp(iso8601.parse("1923-12-01T00:44:36.876").getTime)
    t3.setNanos(876544000)
    assertEquals(-1454368523123456L, KuduRelation.timestampToMicros(t3))
    assertEquals(t3, KuduRelation.microsToTimestamp(-1454368523123456L))
  }

  val rowCount = 10
  var sqlContext : SQLContext = _
  var rows : IndexedSeq[(Int, Int, String, Long)] = _
  var kuduOptions : Map[String, String] = _

  before {
    rows = insertRows(rowCount)

    sqlContext = ss.sqlContext

    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> miniCluster.getMasterAddresses)

    sqlContext.read.options(kuduOptions).kudu.createOrReplaceTempView(tableName)
  }

  test("table creation") {
    val tableName = "testcreatetable"
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    val df = sqlContext.read.options(kuduOptions).kudu
    kuduContext.createTable(tableName, df.schema, Seq("key"),
                            new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
                                                    .setNumReplicas(1))
    kuduContext.insertRows(df, tableName)

    // now use new options to refer to the new table name
    val newOptions: Map[String, String] = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> miniCluster.getMasterAddresses)
    val checkDf = sqlContext.read.options(newOptions).kudu

    assert(checkDf.schema === df.schema)
    assertTrue(kuduContext.tableExists(tableName))
    assert(checkDf.count == 10)

    kuduContext.deleteTable(tableName)
    assertFalse(kuduContext.tableExists(tableName))
  }

  test("insertion") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val changedDF = df.limit(1).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)

    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))

    deleteRow(100)
  }

  test("insertion multiple") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val changedDF = df.limit(2).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)

    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))

    val collectedTwo = newDF.filter("key = 101").collect()
    assertEquals("abc", collectedTwo(0).getAs[String]("c2_s"))

    deleteRow(100)
    deleteRow(101)
  }

  test("insert ignore rows") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val baseDF = df.limit(1) // filter down to just the first row

    // change the c2 string to abc and insert
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    kuduContext.insertIgnoreRows(updateDF, tableName)

    // change the key and insert
    val insertDF = df.limit(1).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("def"))
    kuduContext.insertIgnoreRows(insertDF, tableName)

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("0", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // restore the original state of the table
    deleteRow(100)
  }

  test("upsert rows") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val baseDF = df.limit(1) // filter down to just the first row

    // change the c2 string to abc and update
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    kuduContext.upsertRows(updateDF, tableName)

    // change the key and insert
    val insertDF = df.limit(1).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("def"))
    kuduContext.upsertRows(insertDF, tableName)

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("abc", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // restore the original state of the table
    kuduContext.updateRows(baseDF.filter("key = 0").withColumn("c2_s", lit("0")), tableName)
    deleteRow(100)
  }

  test("delete rows") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val deleteDF = df.filter("key = 0").select("key")
    kuduContext.deleteRows(deleteDF, tableName)

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collectedDelete = newDF.filter("key = 0").collect()
    assertEquals(0, collectedDelete.length)

    // restore the original state of the table
    val insertDF = df.limit(1).filter("key = 0")
    kuduContext.insertRows(insertDF, tableName)
  }

  test("out of order selection") {
    val df = sqlContext.read.options(kuduOptions).kudu.select( "c2_s", "c1_i", "key")
    val collected = df.collect()
    assert(collected(0).getString(0).equals("0"))
  }

  test("table non fault tolerant scan") {
    val results = sqlContext.sql(s"SELECT * FROM $tableName").collectAsList()
    assert(results.size() == rowCount)

    assert(!results.get(0).isNullAt(2))
    assert(results.get(1).isNullAt(2))
  }

  test("table fault tolerant scan") {
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> miniCluster.getMasterAddresses,
      "kudu.faultTolerantScan" -> "true")

    val table = "faultTolerantScanTest"
    sqlContext.read.options(kuduOptions).kudu.createOrReplaceTempView(table)
    val results = sqlContext.sql(s"SELECT * FROM $table").collectAsList()
    assert(results.size() == rowCount)

    assert(!results.get(0).isNullAt(2))
    assert(results.get(1).isNullAt(2))
  }

  test("table scan with projection") {
    assertEquals(10, sqlContext.sql(s"""SELECT key FROM $tableName""").count())
  }

  test("table scan with projection and predicate double") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5 },
                 sqlContext.sql(s"""SELECT key, c3_double FROM $tableName where c3_double > "5.0"""").count())
  }

  test("table scan with projection and predicate long") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5 },
                 sqlContext.sql(s"""SELECT key, c4_long FROM $tableName where c4_long > "5"""").count())

  }
  test("table scan with projection and predicate bool") {
    assertEquals(rows.count { case (key, i, s, ts) => i % 2==0 },
                 sqlContext.sql(s"""SELECT key, c5_bool FROM $tableName where c5_bool = true""").count())

  }
  test("table scan with projection and predicate short") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5},
                 sqlContext.sql(s"""SELECT key, c6_short FROM $tableName where c6_short > 5""").count())

  }
  test("table scan with projection and predicate float") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5},
                 sqlContext.sql(s"""SELECT key, c7_float FROM $tableName where c7_float > 5""").count())

  }

  test("table scan with projection and predicate ") {
    assertEquals(rows.count { case (key, i, s, ts) => s != null && s > "5" },
      sqlContext.sql(s"""SELECT key FROM $tableName where c2_s > "5"""").count())

    assertEquals(rows.count { case (key, i, s, ts) => s != null },
      sqlContext.sql(s"""SELECT key, c2_s FROM $tableName where c2_s IS NOT NULL""").count())
  }


  test("Test basic SparkSQL") {
    val results = sqlContext.sql("SELECT * FROM " + tableName).collectAsList()
    assert(results.size() == rowCount)

    assert(results.get(1).isNullAt(2))
    assert(!results.get(0).isNullAt(2))
  }

  test("Test basic SparkSQL projection") {
    val results = sqlContext.sql("SELECT key FROM " + tableName).collectAsList()
    assert(results.size() == rowCount)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(0))
  }

  test("Test basic SparkSQL with predicate") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where key=1").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(1))

  }

  test("Test basic SparkSQL with two predicates") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where key=2 and c2_s='2'").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(2))
  }

  test("Test basic SparkSQL with in list predicate") {
    val keys = Array(1, 5, 7)
    val results = sqlContext.sql(s"SELECT key FROM $tableName where key in (${keys.mkString(", ")})").collectAsList()
    assert(results.size() == keys.length)
    keys.zipWithIndex.foreach { case (v, i) =>
      assert(results.get(i).size.equals(1))
      assert(results.get(i).getInt(0).equals(v))
    }
  }

  test("Test basic SparkSQL with in list predicate on string") {
    val keys = Array(1, 4, 6)
    val results = sqlContext.sql(s"SELECT key FROM $tableName where c2_s in (${keys.mkString("'", "', '", "'")})").collectAsList()
    assert(results.size() == keys.count(_ % 2 == 0))
    keys.filter(_ % 2 == 0).zipWithIndex.foreach { case (v, i) =>
      assert(results.get(i).size.equals(1))
      assert(results.get(i).getInt(0).equals(v))
    }
  }

  test("Test basic SparkSQL with in list and comparison predicate") {
    val keys = Array(1, 5, 7)
    val results = sqlContext.sql(s"SELECT key FROM $tableName where key>2 and key in (${keys.mkString(", ")})").collectAsList()
    assert(results.size() == keys.count(_>2))
    keys.filter(_>2).zipWithIndex.foreach { case (v, i) =>
      assert(results.get(i).size.equals(1))
      assert(results.get(i).getInt(0).equals(v))
    }
  }

  test("Test basic SparkSQL with two predicates negative") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where key=1 and c2_s='2'").collectAsList()
    assert(results.size() == 0)
  }

  test("Test basic SparkSQL with two predicates including string") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where c2_s='2'").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(2))
  }

  test("Test basic SparkSQL with two predicates and projection") {
    val results = sqlContext.sql("SELECT key, c2_s FROM " + tableName + " where c2_s='2'").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(2))
    assert(results.get(0).getInt(0).equals(2))
    assert(results.get(0).getString(1).equals("2"))
  }

  test("Test basic SparkSQL with two predicates greater than") {
    val results = sqlContext.sql("SELECT key, c2_s FROM " + tableName + " where c2_s>='2'").collectAsList()
    assert(results.size() == 4)
    assert(results.get(0).size.equals(2))
    assert(results.get(0).getInt(0).equals(2))
    assert(results.get(0).getString(1).equals("2"))
  }

  test("Test SparkSQL StringStartsWith filters") {
    // This test requires a special table.
    val testTableName = "startswith"
    val schema = new Schema(ImmutableList.of(
      new ColumnSchemaBuilder("key", Type.STRING).key(true).build()))
    val tableOptions = new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)
    val testTable = kuduClient.createTable(testTableName, schema, tableOptions)

    val kuduSession = kuduClient.newSession()
    val chars = List('a', 'b', '乕', Char.MaxValue, '\u0000')
    val keys = for (x <- chars; y <- chars; z <- chars; w <- chars) yield Array(x, y, z, w).mkString
    keys.foreach { key =>
      val insert = testTable.newInsert
      val row = insert.getRow
      val r = Array(1, 2, 3)
      row.addString(0, key)
      kuduSession.apply(insert)
    }
    val options: Map[String, String] = Map(
      "kudu.table" -> testTableName,
      "kudu.master" -> miniCluster.getMasterAddresses)
    sqlContext.read.options(options).kudu.createOrReplaceTempView(testTableName)

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
    for (x <- chars; y <- chars) {
      checkPrefixCount(Array(x, y).mkString)
    }
  }

  test("Test SparkSQL IS NULL predicate") {
    var results = sqlContext.sql("SELECT key FROM " + tableName + " where c2_s IS NULL").collectAsList()
    assert(results.size() == 5)

    results = sqlContext.sql("SELECT key FROM " + tableName + " where key IS NULL").collectAsList()
    assert(results.isEmpty())
  }

  test("Test SparkSQL IS NOT NULL predicate") {
    var results = sqlContext.sql("SELECT key FROM " + tableName + " where c2_s IS NOT NULL").collectAsList()
    assert(results.size() == 5)

    results = sqlContext.sql("SELECT key FROM " + tableName + " where key IS NOT NULL").collectAsList()
    assert(results.size() == 10)
  }

  test("Test SQL: insert into") {
    val insertTable = "insertintotest"

    // read 0 rows just to get the schema
    val df = sqlContext.sql(s"SELECT * FROM $tableName LIMIT 0")
    kuduContext.createTable(insertTable, df.schema, Seq("key"),
      new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))

    val newOptions: Map[String, String] = Map(
      "kudu.table" -> insertTable,
      "kudu.master" -> miniCluster.getMasterAddresses)
    sqlContext.read.options(newOptions).kudu.createOrReplaceTempView(insertTable)

    sqlContext.sql(s"INSERT INTO TABLE $insertTable SELECT * FROM $tableName")
    val results = sqlContext.sql(s"SELECT key FROM $insertTable").collectAsList()
    assertEquals(10, results.size())
  }

  test("Test SQL: insert overwrite unsupported") {
    val insertTable = "insertoverwritetest"

    // read 0 rows just to get the schema
    val df = sqlContext.sql(s"SELECT * FROM $tableName LIMIT 0")
    kuduContext.createTable(insertTable, df.schema, Seq("key"),
      new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))

    val newOptions: Map[String, String] = Map(
      "kudu.table" -> insertTable,
      "kudu.master" -> miniCluster.getMasterAddresses)
    sqlContext.read.options(newOptions).kudu.createOrReplaceTempView(insertTable)

    try {
      sqlContext.sql(s"INSERT OVERWRITE TABLE $insertTable SELECT * FROM $tableName")
      fail("insert overwrite should throw UnsupportedOperationException")
    } catch {
      case _: UnsupportedOperationException => // good
      case NonFatal(_) =>
        fail("insert overwrite should throw UnsupportedOperationException")
    }
  }

  test("Test write using DefaultSource") {
    val df = sqlContext.read.options(kuduOptions).kudu

    val newTable = "testwritedatasourcetable"
    kuduContext.createTable(newTable, df.schema, Seq("key"),
        new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
          .setNumReplicas(1))

    val newOptions: Map[String, String] = Map(
      "kudu.table" -> newTable,
      "kudu.master" -> miniCluster.getMasterAddresses)
    df.write.options(newOptions).mode("append").kudu

    val checkDf = sqlContext.read.options(newOptions).kudu
    assert(checkDf.schema === df.schema)
    assertTrue(kuduContext.tableExists(newTable))
    assert(checkDf.count == 10)
  }

  test("create relation with schema") {

    // user-supplied schema that is compatible with actual schema, but with the key at the end
    val userSchema: StructType = StructType(List(
      StructField("c4_long", DataTypes.LongType),
      StructField("key", DataTypes.IntegerType)
    ))

    val dfDefaultSchema = sqlContext.read.options(kuduOptions).kudu
    assertEquals(11, dfDefaultSchema.schema.fields.length)

    val dfWithUserSchema = sqlContext.read.options(kuduOptions).schema(userSchema).kudu
    assertEquals(2, dfWithUserSchema.schema.fields.length)

    dfWithUserSchema.limit(10).collect()
    assertTrue(dfWithUserSchema.columns.deep == Array("c4_long", "key").deep)
  }

  test("create relation with invalid schema") {

    // user-supplied schema that is NOT compatible with actual schema
    val userSchema: StructType = StructType(List(
      StructField("foo", DataTypes.LongType),
      StructField("bar", DataTypes.IntegerType)
    ))

    intercept[IllegalArgumentException] {
      sqlContext.read.options(kuduOptions).schema(userSchema).kudu
    }.getMessage should include ("Unknown column: foo")
  }

  // Verify that the propagated timestamp is properly updated inside
  // the same client.
  test("timestamp propagation") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val insertDF = df.limit(1)
                      .withColumn("key", df("key")
                      .plus(100))
                      .withColumn("c2_s", lit("abc"))

    // Initiate a write via KuduContext, and verify that the client should
    // have propagated timestamp.
    kuduContext.insertRows(insertDF, tableName)
    assert(kuduContext.syncClient.getLastPropagatedTimestamp > 0)
    var prevTimestamp = kuduContext.syncClient.getLastPropagatedTimestamp

    // Initiate a read via DataFrame, and verify that the client should
    // move the propagated timestamp further.
    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))
    assert(kuduContext.syncClient.getLastPropagatedTimestamp > prevTimestamp)
    prevTimestamp = kuduContext.syncClient.getLastPropagatedTimestamp

    // Initiate a read via KuduContext, and verify that the client should
    // move the propagated timestamp further.
    val rdd = kuduContext.kuduRDD(ss.sparkContext, tableName, List("key"))
    assert(rdd.collect.length == 11)
    assert(kuduContext.syncClient.getLastPropagatedTimestamp > prevTimestamp)
    prevTimestamp = kuduContext.syncClient.getLastPropagatedTimestamp

    // Initiate another write via KuduContext, and verify that the client should
    // move the propagated timestamp further.
    val updateDF = df.limit(1)
                     .withColumn("key", df("key")
                     .plus(100))
                     .withColumn("c2_s", lit("def"))
    kuduContext.insertIgnoreRows(updateDF, tableName)
    assert(kuduContext.syncClient.getLastPropagatedTimestamp > prevTimestamp)
  }
}
