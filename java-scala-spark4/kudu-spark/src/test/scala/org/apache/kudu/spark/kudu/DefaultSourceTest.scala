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

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.Assert._
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.test.KuduTestHarness
import org.apache.kudu.test.RandomUtils
import org.apache.kudu.spark.kudu.SparkListenerUtil.withJobTaskCounter
import org.apache.kudu.test.KuduTestHarness.EnableKerberos
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig
import org.junit.Before
import org.junit.Test
import org.scalatest.matchers.should.Matchers

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
   * A simple test to delete rows from an empty table.
   * First delete ignore rows from the empty table.
   * Next, insert data to the kudu table and delete all of them afterwards.
   * Finally, delete again from the empty table through deleteIngoreRows.
   */
  @Test
  def testDeleteRowsFromEmptyTable(): Unit = {
    val origDf = sqlContext.read.options(kuduOptions).format("kudu").load
    val tableName = "testEmptyTable"
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    kuduContext.createTable(
      tableName,
      origDf.schema,
      Seq("key"),
      new CreateTableOptions()
        .addHashPartitions(List("key").asJava, harness.getTabletServers.size() * 3))
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    // delete rows from the empty table.
    kuduContext.deleteIgnoreRows(df, tableName)
    // insert rows.
    val newKuduTableOptions =
      Map("kudu.table" -> tableName, "kudu.master" -> harness.getMasterAddressesAsString)
    val insertsBefore = kuduContext.numInserts.value.get(tableName)
    kuduContext.insertRows(df, tableName)
    assertEquals(insertsBefore + df.count(), kuduContext.numInserts.value.get(tableName))
    // delete all of the newly inserted rows.
    kuduContext.deleteRows(df, tableName)
    val newDf = sqlContext.read.options(newKuduTableOptions).format("kudu").load()
    assertEquals(newDf.collectAsList().size(), 0)
    // delete all of rows again, which is no hurt.
    kuduContext.deleteIgnoreRows(df, tableName)
  }

  /**
   * A simple test with two threads to delete the data from the same table.
   * After applying deleteIgnoreRows, there should be no errors even though
   * half of the delete operations will be applied on non-existing rows.
   */
  @Test
  def testDuplicateDelete(): Unit = {
    val totalRows = 10000
    val origDf = sqlContext.read.options(kuduOptions).format("kudu").load
    insertRows(table, totalRows, rowCount)
    val tableName = "testDuplicateDelete"
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    kuduContext.createTable(
      tableName,
      origDf.schema,
      Seq("key"),
      new CreateTableOptions()
        .addHashPartitions(List("key").asJava, harness.getTabletServers.size() * 3)
        .setNumReplicas(3))
    val newKuduTableOptions =
      Map("kudu.table" -> tableName, "kudu.master" -> harness.getMasterAddressesAsString)
    kuduContext.insertRows(origDf, tableName)
    val df = sqlContext.read.options(newKuduTableOptions).format("kudu").load()
    assertEquals(df.collectAsList().size(), totalRows + rowCount)
    val deleteThread1 = new Thread {
      override def run: Unit = {
        kuduContext.deleteIgnoreRows(df, tableName)
      }
    }
    val deleteThread2 = new Thread {
      override def run: Unit = {
        kuduContext.deleteIgnoreRows(df, tableName)
      }
    }
    deleteThread1.start()
    deleteThread2.start()
    deleteThread1.join(3000)
    deleteThread2.join(3000)
    val newDf = sqlContext.read.options(newKuduTableOptions).format("kudu").load()
    assertEquals(newDf.collectAsList().size(), 0)
  }

  /**
   * A simple test to verify the legacy package reader/writer
   * syntax still works. This should be removed when the
   * deprecated `kudu` methods are removed.
   */
  @Test @deprecated("Marked as deprecated to suppress warning", "")
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
    val insertsBefore = kuduContext.numInserts.value.get(tableName)
    kuduContext.insertRows(df, tableName)
    assertEquals(insertsBefore + df.count(), kuduContext.numInserts.value.get(tableName))

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
    val insertsBefore = kuduContext.numInserts.value.get(tableName)
    kuduContext.insertRows(df, tableName)
    assertEquals(insertsBefore + df.count(), kuduContext.numInserts.value.get(tableName))

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
    val insertsBefore = kuduContext.numInserts.value.get(tableName)
    println(s"insertsBefore: $insertsBefore")
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val changedDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)
    assertEquals(insertsBefore + changedDF.count(), kuduContext.numInserts.value.get(tableName))

    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))

    deleteRow(100)
  }

  @Test
  def testInsertionMultiple() {
    val insertsBefore = kuduContext.numInserts.value.get(tableName)
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val changedDF = df
      .limit(2)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)
    assertEquals(insertsBefore + changedDF.count(), kuduContext.numInserts.value.get(tableName))

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
      "kudu.operation" -> "insert_ignore")
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

  /**
   * Identical to the above test, but exercising the old session based insert ignore operations,
   * ensuring we functionally support the same semantics.
   * Also uses "insert-ignore" instead of "insert_ignore".
   */
  @Test
  @KuduTestHarness.MasterServerConfig(flags = Array("--master_support_ignore_operations=false"))
  def testLegacyInsertIgnoreRowsWriteOption() {
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

  @Test @deprecated("Marked as deprecated to suppress warning", "")
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
    val upsertsBefore = kuduContext.numUpserts.value.get(tableName)
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    kuduContext.upsertRows(insertDF, tableName)
    assertEquals(upsertsBefore + insertDF.count(), kuduContext.numUpserts.value.get(tableName))

    // Read the data back.
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("abc", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // Restore the original state of the table, and test the numUpdates metric.
    val updatesBefore = kuduContext.numUpdates.value.get(tableName)
    val updateDF = baseDF.filter("key = 0").withColumn("c2_s", lit("0"))
    val updatesApplied = updateDF.count()
    kuduContext.updateRows(updateDF, tableName)
    assertEquals(updatesBefore + updatesApplied, kuduContext.numUpdates.value.get(tableName))
    deleteRow(100)
  }

  @Test
  def testMultipleTableOperationCounts() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load

    val tableUpsertsBefore = kuduContext.numUpserts.value.get(tableName)
    val simpleTableUpsertsBefore = kuduContext.numUpserts.value.get(simpleTableName)

    // Change the key and insert.
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    kuduContext.upsertRows(insertDF, tableName)

    // insert new row to simple table
    val insertSimpleDF = sqlContext.createDataFrame(Seq((0, "foo"))).toDF("key", "val")
    kuduContext.upsertRows(insertSimpleDF, simpleTableName)

    assertEquals(tableUpsertsBefore + insertDF.count(), kuduContext.numUpserts.value.get(tableName))
    assertEquals(
      simpleTableUpsertsBefore + insertSimpleDF.count(),
      kuduContext.numUpserts.value.get(simpleTableName))

    // Restore the original state of the tables, and test the numDeletes metric.
    val deletesBefore = kuduContext.numDeletes.value.get(tableName)
    val simpleDeletesBefore = kuduContext.numDeletes.value.get(simpleTableName)
    kuduContext.deleteRows(insertDF, tableName)
    kuduContext.deleteRows(insertSimpleDF, simpleTableName)
    assertEquals(deletesBefore + insertDF.count(), kuduContext.numDeletes.value.get(tableName))
    assertEquals(
      simpleDeletesBefore + insertSimpleDF.count(),
      kuduContext.numDeletes.value.get(simpleTableName))
  }

  @Test
  def testWriteWithSink() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val baseDF = df.limit(1) // Filter down to just the first row.

    // Change the c2 string to abc and upsert.
    val upsertDF = baseDF.withColumn("c2_s", lit("abc"))
    upsertDF.write
      .format("kudu")
      .option("kudu.master", harness.getMasterAddressesAsString)
      .option("kudu.table", tableName)
      // Default kudu.operation is upsert.
      .mode(SaveMode.Append)
      .save()

    // Change the key and insert.
    val insertDF = df
      .limit(1)
      .withColumn("key", df("key").plus(100))
      .withColumn("c2_s", lit("def"))
    insertDF.write
      .format("kudu")
      .option("kudu.master", harness.getMasterAddressesAsString)
      .option("kudu.table", tableName)
      .option("kudu.operation", "insert")
      .mode(SaveMode.Append)
      .save()

    // Read the data back.
    val newDF = sqlContext.read.options(kuduOptions).format("kudu").load
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("abc", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))
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
      assertTrue(isSorted(bottomRows.toSeq))
      assertTrue(isSorted(topRows.toSeq))
    }
  }

  @Test
  def testDeleteRows() {
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val deleteDF = df.filter("key = 0").select("key")
    val deletesBefore = kuduContext.numDeletes.value.get(tableName)
    val deletesApplied = deleteDF.count()
    kuduContext.deleteRows(deleteDF, tableName)
    assertEquals(deletesBefore + deletesApplied, kuduContext.numDeletes.value.get(tableName))

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
  def testSchemaDrift() {
    val nonNullDF =
      sqlContext.createDataFrame(Seq((0, "foo"))).toDF("key", "val")
    kuduContext.insertRows(nonNullDF, simpleTableName)

    val tableOptions = Map(
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.table" -> simpleTableName
    )
    val df = sqlContext.read.options(tableOptions).format("kudu").load
    assertEquals(2, df.schema.fields.length)

    // Add a column not in the table schema by duplicating the val column.
    val newDf = df.withColumn("val2", col("val"))

    // Insert with handleSchemaDrift = false. Note that a new column was not created.
    kuduContext.upsertRows(newDf, simpleTableName, KuduWriteOptions(handleSchemaDrift = false))
    assertEquals(2, harness.getClient.openTable(simpleTableName).getSchema.getColumns.size())

    // Insert with handleSchemaDrift = true. Note that a new column was created.
    kuduContext.upsertRows(newDf, simpleTableName, KuduWriteOptions(handleSchemaDrift = true))
    assertEquals(3, harness.getClient.openTable(simpleTableName).getSchema.getColumns.size())

    val afterDf = sqlContext.read.options(tableOptions).format("kudu").load
    assertEquals(3, afterDf.schema.fields.length)
    assertEquals("val2", afterDf.schema.fieldNames.last)
    assertTrue(afterDf.collect().forall(r => r.getString(1) == r.getString(2)))
  }

  @Test
  def testInsertWrongType() {
    val nonNullDF =
      sqlContext.createDataFrame(Seq((0, "foo"))).toDF("key", "val")
    kuduContext.insertRows(nonNullDF, simpleTableName)

    val tableOptions = Map(
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.table" -> simpleTableName
    )
    val df = sqlContext.read.options(tableOptions).format("kudu").load
    // Convert the val column to a bytes instead of string.
    val toBytes = udf[Array[Byte], String](_.getBytes(StandardCharsets.UTF_8))
    val newDf = df
      .withColumn("valTmp", toBytes(col("val")))
      .drop("val")
      .withColumnRenamed("valTmp", "val")

    try {
      kuduContext.insertRows(newDf, simpleTableName, KuduWriteOptions())
    } catch {
      case e: SparkException =>
        assertTrue(e.getMessage.contains("val isn't [Type: binary], it's string"))
    }
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
    assertEquals(16, dfDefaultSchema.schema.fields.length)

    val dfWithUserSchema =
      sqlContext.read.options(kuduOptions).schema(userSchema).format("kudu").load
    assertEquals(2, dfWithUserSchema.schema.fields.length)

    dfWithUserSchema.limit(10).collect()
    assertTrue(dfWithUserSchema.columns.equals(Array("c4_long", "key")))
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
    assert(kuduRelation.readOptions.scanRequestTimeoutMs.contains(66666))
  }

  @Test
  def testSnapshotTimestampMsPropagation() {
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.snapshotTimestampMs" -> "86400000000")
    val dataFrameSnapshotTimestamp = sqlContext.read.options(kuduOptions).format("kudu").load
    val kuduRelationSnapshotTimestamp = kuduRelationFromDataFrame(dataFrameSnapshotTimestamp)

    kuduOptions =
      Map("kudu.table" -> tableName, "kudu.master" -> harness.getMasterAddressesAsString)
    val dataFrameNoneSnapshotTimestamp = sqlContext.read.options(kuduOptions).format("kudu").load
    val kuduRelationNoneSnapshotTimestamp = kuduRelationFromDataFrame(
      dataFrameNoneSnapshotTimestamp)
    assert(kuduRelationSnapshotTimestamp.readOptions.snapshotTimestampMs.contains(86400000000L))
    assert(kuduRelationNoneSnapshotTimestamp.readOptions.snapshotTimestampMs.isEmpty)
  }

  @Test
  def testReadDataFrameAtSnapshot() {
    insertRows(table, 100, 1)
    val timestamp = getLastPropagatedTimestampMs()
    insertRows(table, 100, 100)
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.snapshotTimestampMs" -> s"$timestamp")
    val dataFrameWithSnapshotTimestamp = sqlContext.read.options(kuduOptions).format("kudu").load
    kuduOptions =
      Map("kudu.table" -> tableName, "kudu.master" -> harness.getMasterAddressesAsString)
    val dataFrameWithoutSnapshotTimestamp = sqlContext.read.options(kuduOptions).format("kudu").load
    assertEquals(100, dataFrameWithSnapshotTimestamp.collect().length)
    assertEquals(200, dataFrameWithoutSnapshotTimestamp.collect().length)
  }

  @Test
  def testSnapshotTimestampBeyondMaxAge(): Unit = {
    val extraConfigs = new util.HashMap[String, String]()
    val tableName = "snapshot_test"
    extraConfigs.put("kudu.table.history_max_age_sec", "1");
    kuduClient.createTable(
      tableName,
      schema,
      tableOptions.setExtraConfigs(extraConfigs)
    )
    val timestamp = getLastPropagatedTimestampMs()
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.snapshotTimestampMs" -> s"$timestamp")
    insertRows(table, 100, 1)
    Thread.sleep(2000)
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val exception = intercept[Exception] {
      df.count()
    }
    assertTrue(
      exception.getMessage.contains(
        "snapshot scan end timestamp is earlier than the ancient history mark")
    )
  }

  @Test
  def testSnapshotTimestampBeyondCurrentTimestamp(): Unit = {
    val timestamp = getLastPropagatedTimestampMs() + 100000
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.snapshotTimestampMs" -> s"$timestamp")
    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    val exception = intercept[Exception] {
      df.count()
    }
    assertTrue(exception.getMessage.contains("cannot verify timestamp"))
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

  @Test
  @EnableKerberos(principal = "oryx")
  def testNonDefaultPrincipal(): Unit = {
    KuduClientCache.clearCacheForTests()
    val exception = intercept[Exception] {
      val df = sqlContext.read.options(kuduOptions).format("kudu").load
      df.count()
    }
    assertTrue(exception.getCause.getMessage.contains("this client is not authenticated"))

    KuduClientCache.clearCacheForTests()
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.saslProtocolName" -> "oryx"
    )

    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    assertEquals(rowCount, df.count())
  }

  @Test
  def testKuduRequireAuthenticationInsecureCluster(): Unit = {
    KuduClientCache.clearCacheForTests()
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.requireAuthentication" -> "true"
    )
    val exception = intercept[Exception] {
      val df = sqlContext.read.options(kuduOptions).format("kudu").load
      df.count
    }
    assertTrue(
      exception.getCause.getMessage
        .contains("client requires authentication, but server does not have Kerberos enabled"))
  }

  @Test
  @MasterServerConfig(flags = Array("--rpc_encryption=disabled", "--rpc_authentication=disabled"))
  @TabletServerConfig(flags = Array("--rpc_encryption=disabled", "--rpc_authentication=disabled"))
  def testKuduRequireEncryptionInsecureCluster(): Unit = {
    KuduClientCache.clearCacheForTests()
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.encryptionPolicy" -> "required_remote"
    )
    val exception = intercept[Exception] {
      val df = sqlContext.read.options(kuduOptions).format("kudu").load
      df.count
    }
    assertTrue(
      exception.getCause.getMessage.contains("server does not support required TLS encryption"))
  }

  @Test
  @EnableKerberos
  def testKuduRequireAuthenticationAndEncryptionSecureCluster(): Unit = {
    KuduClientCache.clearCacheForTests()
    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> harness.getMasterAddressesAsString,
      "kudu.encryptionPolicy" -> "required",
      "kudu.requireAuthentication" -> "true"
    )

    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    assertEquals(rowCount, df.count)
  }

  @Test
  @MasterServerConfig(flags = Array("--rpc_encryption=disabled", "--rpc_authentication=disabled"))
  @TabletServerConfig(flags = Array("--rpc_encryption=disabled", "--rpc_authentication=disabled"))
  def testKuduInsecureCluster(): Unit = {
    KuduClientCache.clearCacheForTests()

    val df = sqlContext.read.options(kuduOptions).format("kudu").load
    assertEquals(rowCount, df.count)
  }
}
