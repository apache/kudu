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
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.Assert._
import org.scalatest.Matchers
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.test.RandomUtils
import org.apache.kudu.spark.kudu.SparkListenerUtil.withJobTaskCounter
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig
import org.junit.Before
import org.junit.Test

import scala.util.Random

class DefaultSourceTest extends KuduTestSuite with Matchers {
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

  /**
   * A simple test to verify the legacy package reader/writer
   * syntax still works. This should be removed when the
   * deprecated `kudu` methods are removed.
   */
  @Test
  def testPackageReaderAndWriter(): Unit = {
    val df = sqlContext.read.options(kuduOptions).kudu
    val baseDF = df.limit(1) // filter down to just the first row
    // change the c2 string to abc and update
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    updateDF.write.options(kuduOptions).mode("append").kudu

    val newDf = sqlContext.read.options(kuduOptions).kudu
    assertFalse(newDf.collect().isEmpty)
  }

  @Test
  def testTableCreation() {
    val tableName = "testcreatetable"
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    kuduContext.createTable(
      tableName,
      df.schema,
      Seq("key"),
      new CreateTableOptions()
        .setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))
    val insertsBefore = kuduContext.numInserts.value
    kuduContext.insertRows(df, tableName)
    assertEquals(insertsBefore + df.count(), kuduContext.numInserts.value)

    // Now use new options to refer to the new table name.
    val newOptions: Map[String, String] =
      Map("kudu.table" -> tableName, "kudu.master" -> harness.getMasterAddressesAsString)
    val checkDf = sqlContext.read.options(newOptions).format("kudu").load

    assert(checkDf.schema === df.schema)
    assertTrue(kuduContext.tableExists(tableName))
    assert(checkDf.count == 10)

    kuduContext.deleteTable(tableName)
    assertFalse(kuduContext.tableExists(tableName))
  }

  @Test
  def testTableCreationWithPartitioning() {
    val tableName = "testcreatepartitionedtable"
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    val df = sqlContext.read.options(kuduOptions).format("kudu").load

    val kuduSchema = kuduContext.createSchema(df.schema, Seq("key"))
    val lower = kuduSchema.newPartialRow()
    lower.addInt("key", 0)
    val upper = kuduSchema.newPartialRow()
    upper.addInt("key", Integer.MAX_VALUE)

    kuduContext.createTable(
      tableName,
      kuduSchema,
      new CreateTableOptions()
        .addHashPartitions(List("key").asJava, 2)
        .setRangePartitionColumns(List("key").asJava)
        .addRangePartition(lower, upper)
        .setNumReplicas(1)
    )
    val insertsBefore = kuduContext.numInserts.value
    kuduContext.insertRows(df, tableName)
    assertEquals(insertsBefore + df.count(), kuduContext.numInserts.value)

    // now use new options to refer to the new table name
    val newOptions: Map[String, String] =
      Map("kudu.table" -> tableName, "kudu.master" -> harness.getMasterAddressesAsString)
    val checkDf = sqlContext.read.options(newOptions).format("kudu").load

    assert(checkDf.schema === df.schema)
    assertTrue(kuduContext.tableExists(tableName))
    assert(checkDf.count == 10)

    kuduContext.deleteTable(tableName)
    assertFalse(kuduContext.tableExists(tableName))
  }

  @Test
  def testInsertion() {
    val insertsBefore = kuduContext.numInserts.value
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val changedDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)
    assertEquals(insertsBefore + changedDF.count(), kuduContext.numInserts.value)

    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))

    deleteRow(100)
  }

  @Test
  def testInsertionMultiple() {
    val insertsBefore = kuduContext.numInserts.value
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val changedDF = df
      .limit(2)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)
    assertEquals(insertsBefore + changedDF.count(), kuduContext.numInserts.value)

    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))

    val collectedTwo = newDF.filter("key = 101").collect()
    assertEquals("abc", collectedTwo(0).getAs[String]("c2_s"))

    deleteRow(100)
    deleteRow(101)
  }

  @Test
  def testInsertionIgnoreRows() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val baseDF = df.limit(1) // Filter down to just the first row.

    // change the c2 string to abc and insert
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    val kuduWriteOptions = KuduWriteOptions(ignoreDuplicateRowErrors = true)
    kuduContext.insertRows(updateDF, tableName, kuduWriteOptions)

    // Change the key and insert.
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    kuduContext.insertRows(insertDF, tableName, kuduWriteOptions)

    // Read the data back.
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("0", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // Restore the original state of the table.
    deleteRow(100)
  }

  @Test
  def testInsertIgnoreRowsUsingDefaultSource() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val baseDF = df.limit(1) // filter down to just the first row

    // change the c2 string to abc and insert
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    val newOptions: Map[String, String] = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.operation" -> "insert",
      "kudu.ignoreDuplicateRowErrors" -> "true")
    updateDF.write.options(newOptions).mode("append").format("kudu").save

    // change the key and insert
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    insertDF.write.options(newOptions).mode("append").format("kudu").save

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("0", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // restore the original state of the table
    deleteRow(100)
  }

  @Test
  def testInsertIgnoreRowsWriteOption() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val baseDF = df.limit(1) // filter down to just the first row

    // change the c2 string to abc and insert
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    val newOptions: Map[String, String] = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.operation" -> "insert-ignore")
    updateDF.write.options(newOptions).mode("append").format("kudu").save

    // change the key and insert
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    insertDF.write.options(newOptions).mode("append").format("kudu").save

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("0", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // restore the original state of the table
    deleteRow(100)
  }

  @Test
  def testInsertIgnoreRowsMethod() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val baseDF = df.limit(1) // filter down to just the first row

    // change the c2 string to abc and insert
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    kuduContext.insertIgnoreRows(updateDF, tableName)

    // change the key and insert
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    kuduContext.insertIgnoreRows(insertDF, tableName)

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("0", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // restore the original state of the table
    deleteRow(100)
  }

  @Test
  def testUpsertRows() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val baseDF = df.limit(1) // Filter down to just the first row.

    // Change the c2 string to abc and update.
    val upsertDF = baseDF.withColumn("c2_s", lit("abc"))
    kuduContext.upsertRows(upsertDF, tableName)

    // Change the key and insert.
    val upsertsBefore = kuduContext.numUpserts.value
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    kuduContext.upsertRows(insertDF, tableName)
    assertEquals(upsertsBefore + insertDF.count(), kuduContext.numUpserts.value)

    // Read the data back.
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("abc", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // Restore the original state of the table, and test the numUpdates metric.
    val updatesBefore = kuduContext.numUpdates.value
    val updateDF = baseDF.filter("key = 0").withColumn("c2_s", lit("0"))
    val updatesApplied = updateDF.count()
    kuduContext.updateRows(updateDF, tableName)
    assertEquals(updatesBefore + updatesApplied, kuduContext.numUpdates.value)
    deleteRow(100)
  }

  @Test
  def testUpsertRowsIgnoreNulls() {
    val nonNullDF =
      sqlContext.createDataFrame(Seq((0, "foo"))).toDF("key", "val")
    kuduContext.insertRows(nonNullDF, simpleTableName)

    val dataDF = sqlContext.read
      .options(
        Map("kudu.master" -> harness.getMasterAddressesAsString, "kudu.table" -> simpleTableName))
      .format("kudu")
      .load

    val nullDF = sqlContext
      .createDataFrame(Seq((0, null.asInstanceOf[String])))
      .toDF("key", "val")
    val ignoreNullOptions = KuduWriteOptions(ignoreNull = true)
    kuduContext.upsertRows(nullDF, simpleTableName, ignoreNullOptions)
    assert(dataDF.collect.toList === nonNullDF.collect.toList)

    val respectNullOptions = KuduWriteOptions(ignoreNull = false)
    kuduContext.updateRows(nonNullDF, simpleTableName)
    kuduContext.upsertRows(nullDF, simpleTableName, respectNullOptions)
    assert(dataDF.collect.toList === nullDF.collect.toList)

    kuduContext.updateRows(nonNullDF, simpleTableName)
    kuduContext.upsertRows(nullDF, simpleTableName)
    assert(dataDF.collect.toList === nullDF.collect.toList)

    val deleteDF = dataDF.filter("key = 0").select("key")
    kuduContext.deleteRows(deleteDF, simpleTableName)
  }

  @Test
  def testUpsertRowsIgnoreNullsUsingDefaultSource() {
    val nonNullDF =
      sqlContext.createDataFrame(Seq((0, "foo"))).toDF("key", "val")
    kuduContext.insertRows(nonNullDF, simpleTableName)

    val dataDF = sqlContext.read
      .options(
        Map("kudu.master" -> harness.getMasterAddressesAsString, "kudu.table" -> simpleTableName))
      .format("kudu")
      .load

    val nullDF = sqlContext
      .createDataFrame(Seq((0, null.asInstanceOf[String])))
      .toDF("key", "val")
    val options_0: Map[String, String] = Map(
      "kudu.table" -> simpleTableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.ignoreNull" -> "true")
    nullDF.write.options(options_0).mode("append").format("kudu").save
    assert(dataDF.collect.toList === nonNullDF.collect.toList)

    kuduContext.updateRows(nonNullDF, simpleTableName)
    val options_1: Map[String, String] =
      Map("kudu.table" -> simpleTableName, "kudu.master" -> harness.getMasterAddressesAsString)
    nullDF.write.options(options_1).mode("append").format("kudu").save
    assert(dataDF.collect.toList === nullDF.collect.toList)

    val deleteDF = dataDF.filter("key = 0").select("key")
    kuduContext.deleteRows(deleteDF, simpleTableName)
  }

  @Test
  def testRepartition(): Unit = {
    runRepartitionTest(false)
  }

  @Test
  def testRepartitionAndSort(): Unit = {
    runRepartitionTest(true)
  }

  def runRepartitionTest(repartitionSort: Boolean): Unit = {
    // Create a simple table with 2 range partitions split on the value 100.
    val tableName = "testRepartition"
    val splitValue = 100
    val split = simpleSchema.newPartialRow()
    split.addInt("key", splitValue)
    val options = new CreateTableOptions()
    options.setRangePartitionColumns(List("key").asJava)
    options.addSplitRow(split)
    val table = kuduClient.createTable(tableName, simpleSchema, options)

    val random = Random.javaRandomToRandom(RandomUtils.getRandom)
    val data = random.shuffle(
      Seq(
        Row.fromSeq(Seq(0, "0")),
        Row.fromSeq(Seq(25, "25")),
        Row.fromSeq(Seq(50, "50")),
        Row.fromSeq(Seq(75, "75")),
        Row.fromSeq(Seq(99, "99")),
        Row.fromSeq(Seq(100, "100")),
        Row.fromSeq(Seq(101, "101")),
        Row.fromSeq(Seq(125, "125")),
        Row.fromSeq(Seq(150, "150")),
        Row.fromSeq(Seq(175, "175")),
        Row.fromSeq(Seq(199, "199"))
      ))
    val dataRDD = ss.sparkContext.parallelize(data, numSlices = 2)
    val schema = SparkUtil.sparkSchema(table.getSchema)
    val dataDF = ss.sqlContext.createDataFrame(dataRDD, schema)

    // Capture the rows so we can validate the insert order.
    kuduContext.captureRows = true

    // Count the number of tasks that end.
    val actualNumTasks = withJobTaskCounter(ss.sparkContext) { () =>
      kuduContext.insertRows(
        dataDF,
        tableName,
        new KuduWriteOptions(repartition = true, repartitionSort = repartitionSort))
    }

    // 2 tasks from the parallelize call, and 2 from the repartitioning.
    assertEquals(4, actualNumTasks)
    val rows = kuduContext.rowsAccumulator.value.asScala
    assertEquals(data.size, rows.size)
    assertEquals(data.map(_.getInt(0)).sorted, rows.map(_.getInt(0)).sorted)

    // If repartitionSort is true, verify the rows were sorted while repartitioning.
    if (repartitionSort) {
      def isSorted(rows: Seq[Int]): Boolean = {
        rows.sliding(2).forall(p => (p.size == 1) || p.head < p.tail.head)
      }
      val (bottomRows, topRows) = rows.map(_.getInt(0)).partition(_ < splitValue)
      assertTrue(isSorted(bottomRows))
      assertTrue(isSorted(topRows))
    }
  }

  @Test
  def testDeleteRows() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val deleteDF = df.filter("key = 0").select("key")
    val deletesBefore = kuduContext.numDeletes.value
    val deletesApplied = deleteDF.count()
    kuduContext.deleteRows(deleteDF, tableName)
    assertEquals(deletesBefore + deletesApplied, kuduContext.numDeletes.value)

    // Read the data back.
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedDelete = newDF.filter("key = 0").collect()
    assertEquals(0, collectedDelete.length)

    // Restore the original state of the table.
    val insertDF = df.limit(1).filter("key = 0")
    kuduContext.insertRows(insertDF, tableName)
  }

  @Test
  def testOutOfOrderSelection() {
    val df =
      sqlContext.read.options(kuduOptions).format("kudu").load.select("c2_s", "c1_i", "key")
    val collected = df.collect()
    assert(collected(0).getString(0).equals("0"))
  }

  @Test
  def testWriteUsingDefaultSource() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load

    val newTable = "testwritedatasourcetable"
    kuduContext.createTable(
      newTable,
      df.schema,
      Seq("key"),
      new CreateTableOptions()
        .setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))

    val newOptions: Map[String, String] =
      Map("kudu.table" -> newTable, "kudu.master" -> harness.getMasterAddressesAsString)
    df.write.options(newOptions).mode("append").format("kudu").save

    val checkDf = sqlContext.read.options(newOptions).format("kudu").load
    assert(checkDf.schema === df.schema)
    assertTrue(kuduContext.tableExists(newTable))
    assert(checkDf.count == 10)
  }

  @Test
  def testCreateRelationWithSchema() {
    // user-supplied schema that is compatible with actual schema, but with the key at the end
    val userSchema: StructType = StructType(
      List(
        StructField("c4_long", DataTypes.LongType),
        StructField("key", DataTypes.IntegerType)
      ))

    val dfDefaultSchema = sqlContext.read.options(kuduOptions).format("kudu").load
    assertEquals(14, dfDefaultSchema.schema.fields.length)

    val dfWithUserSchema =
      sqlContext.read.options(kuduOptions).schema(userSchema).format("kudu").load
    assertEquals(2, dfWithUserSchema.schema.fields.length)

    dfWithUserSchema.limit(10).collect()
    assertTrue(dfWithUserSchema.columns.deep == Array("c4_long", "key").deep)
  }

  @Test
  def testCreateRelationWithInvalidSchema() {
    // user-supplied schema that is NOT compatible with actual schema
    val userSchema: StructType = StructType(
      List(
        StructField("foo", DataTypes.LongType),
        StructField("bar", DataTypes.IntegerType)
      ))

    intercept[IllegalArgumentException] {
      sqlContext.read.options(kuduOptions).schema(userSchema).format("kudu").load
    }.getMessage should include("Unknown column: foo")
  }

  // Verify that the propagated timestamp is properly updated inside
  // the same client.
  @Test
  def testTimestampPropagation() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val insertDF = df
      .limit(1)
      .withColumn(
        "key",
        df("key")
          .plus(100))
      .withColumn("c2_s", lit("abc"))

    // Initiate a write via KuduContext, and verify that the client should
    // have propagated timestamp.
    kuduContext.insertRows(insertDF, tableName)
    assert(kuduContext.syncClient.getLastPropagatedTimestamp > 0)
    var prevTimestamp = kuduContext.syncClient.getLastPropagatedTimestamp

    // Initiate a read via DataFrame, and verify that the client should
    // move the propagated timestamp further.
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
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
    val updateDF = df
      .limit(1)
      .withColumn(
        "key",
        df("key")
          .plus(100))
      .withColumn("c2_s", lit("def"))
    val kuduWriteOptions = KuduWriteOptions(ignoreDuplicateRowErrors = true)
    kuduContext.insertRows(updateDF, tableName, kuduWriteOptions)
    assert(kuduContext.syncClient.getLastPropagatedTimestamp > prevTimestamp)
  }

  /**
   * Verify that the kudu.scanRequestTimeoutMs parameter is parsed by the
   * DefaultSource and makes it into the KuduRelation as a configuration
   * parameter.
   */
  @Test
  def testScanRequestTimeoutPropagation() {
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.scanRequestTimeoutMs" -> "66666")
    val dataFrame = sqlContext.read.options(kuduOptions).format("kudu").load
    val kuduRelation = kuduRelationFromDataFrame(dataFrame)
    assert(kuduRelation.readOptions.scanRequestTimeoutMs == Some(66666))
  }

  @Test
  @MasterServerConfig(
    flags = Array(
      "--mock_table_metrics_for_testing=true",
      "--on_disk_size_for_testing=1024",
      "--live_row_count_for_testing=100"
    ))
  def testGetTableStatistics(): Unit = {
    val dataFrame = sqlContext.read.options(kuduOptions).format("kudu").load
    val kuduRelation = kuduRelationFromDataFrame(dataFrame)
    assert(kuduRelation.sizeInBytes == 1024)
  }
}
