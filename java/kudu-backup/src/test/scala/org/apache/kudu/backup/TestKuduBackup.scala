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
package org.apache.kudu.backup

import java.nio.file.Files
import java.nio.file.Path
import java.util
import java.util.concurrent.TimeUnit

import com.google.common.base.Objects
import org.apache.commons.io.FileUtils
import org.apache.kudu.client.PartitionSchema.HashBucketSchema
import org.apache.kudu.client._
import org.apache.kudu.ColumnSchema
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.kudu.test.CapturingLogAppender
import org.apache.kudu.test.RandomUtils
import org.apache.kudu.util.DataGenerator.DataGeneratorBuilder
import org.apache.kudu.util.HybridTimeUtil
import org.apache.kudu.util.SchemaGenerator.SchemaGeneratorBuilder
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class TestKuduBackup extends KuduTestSuite {
  val log: Logger = LoggerFactory.getLogger(getClass)

  var random: util.Random = _

  @Before
  def setUp(): Unit = {
    random = RandomUtils.getRandom
  }

  @Test
  def testSimpleBackupAndRestore() {
    insertRows(table, 100) // Insert data into the default test table.

    backupAndRestore(Seq(tableName))

    val rdd =
      kuduContext.kuduRDD(ss.sparkContext, s"$tableName-restore", List("key"))
    assert(rdd.collect.length == 100)

    val tA = kuduClient.openTable(tableName)
    val tB = kuduClient.openTable(s"$tableName-restore")
    assertEquals(tA.getNumReplicas, tB.getNumReplicas)
    assertTrue(schemasMatch(tA.getSchema, tB.getSchema))
    assertTrue(partitionSchemasMatch(tA.getPartitionSchema, tB.getPartitionSchema))
  }

  // TODO (KUDU-2790): Add a thorough randomized test for full and incremental backup/restore.
  @Test
  def TestIncrementalBackupAndRestore() {
    insertRows(table, 100) // Insert data into the default test table.

    val rootDir = Files.createTempDirectory("backup")
    doBackup(rootDir, Seq(tableName)) // Full backup.
    insertRows(table, 100, 100) // Insert more data.
    doBackup(rootDir, Seq(tableName)) // Incremental backup.
    // Delete rows that span the full and incremental backup.
    Range(50, 150).foreach { i =>
      deleteRow(i)
    }
    doBackup(rootDir, Seq(tableName)) // Incremental backup.
    doRestore(rootDir, Seq(tableName)) // Restore all the backups.
    FileUtils.deleteDirectory(rootDir.toFile)

    val rdd =
      kuduContext.kuduRDD(ss.sparkContext, s"$tableName-restore", List("key"))
    assert(rdd.collect.length == 100)
  }

  @Test
  def TestForceIncrementalBackup() {
    insertRows(table, 100) // Insert data into the default test table.
    val beforeMs = getPropagatedTimestampMs
    insertRows(table, 100, 100) // Insert more data.

    val rootDir = Files.createTempDirectory("backup")

    // Capture the logs to validate job internals.
    val logs = new CapturingLogAppender()

    // Force an incremental backup without a full backup.
    // It will use a diff scan and won't check the existing dependency graph.
    val handle = logs.attach()
    doBackup(rootDir, Seq(tableName), fromMs = beforeMs) // Incremental backup.
    handle.close()

    assertTrue(Files.isDirectory(rootDir))
    assertEquals(1, rootDir.toFile.list().length)
    assertTrue(logs.getAppendedText.contains("fromMs was set"))
  }

  @Test
  def testSimpleBackupAndRestoreWithSpecialCharacters() {
    // Use an Impala-style table name to verify url encoding/decoding of the table name works.
    val impalaTableName = "impala::default.test"

    val tableOptions = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)

    kuduClient.createTable(impalaTableName, simpleSchema, tableOptions)

    backupAndRestore(Seq(impalaTableName))

    val rdd = kuduContext
      .kuduRDD(ss.sparkContext, s"$impalaTableName-restore", List("key"))
    // Only verifying the file contents could be read, the contents are expected to be empty.
    assert(rdd.isEmpty())
  }

  @Test
  def testRandomBackupAndRestore() {
    val table = createRandomTable()
    val tableName = table.getName
    loadRandomData(table)

    backupAndRestore(Seq(tableName))

    val backupRows = kuduContext.kuduRDD(ss.sparkContext, s"$tableName").collect
    val restoreRows =
      kuduContext.kuduRDD(ss.sparkContext, s"$tableName-restore").collect
    assertEquals(backupRows.length, restoreRows.length)

    val tA = kuduClient.openTable(tableName)
    val tB = kuduClient.openTable(s"$tableName-restore")
    assertEquals(tA.getNumReplicas, tB.getNumReplicas)
    assertTrue(schemasMatch(tA.getSchema, tB.getSchema))
    assertTrue(partitionSchemasMatch(tA.getPartitionSchema, tB.getPartitionSchema))
  }

  @Test
  def testBackupAndRestoreMultipleTables() {
    val numRows = 1
    val table1Name = "table1"
    val table2Name = "table2"

    val table1 = kuduClient.createTable(table1Name, schema, tableOptions)
    val table2 = kuduClient.createTable(table2Name, schema, tableOptions)

    insertRows(table1, numRows)
    insertRows(table2, numRows)

    backupAndRestore(Seq(table1Name, table2Name))

    val rdd1 =
      kuduContext.kuduRDD(ss.sparkContext, s"$table1Name-restore", List("key"))
    assertResult(numRows)(rdd1.count())

    val rdd2 =
      kuduContext.kuduRDD(ss.sparkContext, s"$table2Name-restore", List("key"))
    assertResult(numRows)(rdd2.count())
  }

  @Test
  def testBackupAndRestoreTableWithManyPartitions(): Unit = {
    val kNumPartitions = 100
    val tableName = "many-partitions-table"

    val options = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)

    // Add one range partition and create the table. Separate the range partition
    // from the ones added later so there's a bounded, non-covered range.
    val initialLower = schema.newPartialRow()
    initialLower.addInt("key", -5)
    val initialUpper = schema.newPartialRow()
    initialUpper.addInt("key", -4)
    options.addRangePartition(initialLower, initialUpper)
    val table = kuduClient.createTable(tableName, schema, options)

    // Add the rest of the partitions via alter.
    for (i <- 0 to kNumPartitions) {
      val alterOptions = new AlterTableOptions()
      val lower = schema.newPartialRow()
      lower.addInt("key", i)
      val upper = schema.newPartialRow()
      upper.addInt("key", i + 1)
      alterOptions.addRangePartition(lower, upper)
      kuduClient.alterTable(tableName, alterOptions)
    }

    // Insert some rows. Note that each row will go into a different range
    // partition, and the initial partition will be empty.
    insertRows(table, kNumPartitions)

    // Now backup and restore the table.
    backupAndRestore(Seq(tableName))
  }

  @Test
  def testBackupAndRestoreTableWithNoRangePartitions(): Unit = {
    val tableName = "only-hash-partitions-table"

    val options = new CreateTableOptions()
      .addHashPartitions(List("key").asJava, 2)
      .setNumReplicas(1)
    val table1 = kuduClient.createTable(tableName, schema, options)

    insertRows(table1, 100)

    backupAndRestore(Seq(tableName))
  }

  def schemasMatch(before: Schema, after: Schema): Boolean = {
    if (before eq after) return true
    if (before.getColumns.size != after.getColumns.size) return false
    (0 until before.getColumns.size).forall { i =>
      columnsMatch(before.getColumnByIndex(i), after.getColumnByIndex(i))
    }
  }

  def columnsMatch(before: ColumnSchema, after: ColumnSchema): Boolean = {
    if (before eq after) return true
    Objects.equal(before.getName, after.getName) &&
    Objects.equal(before.getType, after.getType) &&
    Objects.equal(before.isKey, after.isKey) &&
    Objects.equal(before.isNullable, after.isNullable) &&
    defaultValuesMatch(before.getDefaultValue, after.getDefaultValue) &&
    Objects.equal(before.getDesiredBlockSize, after.getDesiredBlockSize) &&
    Objects.equal(before.getEncoding, after.getEncoding) &&
    Objects
      .equal(before.getCompressionAlgorithm, after.getCompressionAlgorithm) &&
    Objects.equal(before.getTypeAttributes, after.getTypeAttributes)
  }

  // Special handling because default values can be a byte array which is not
  // handled by Guava's Objects.equals.
  // See https://github.com/google/guava/issues/1425
  def defaultValuesMatch(before: Any, after: Any): Boolean = {
    if (before.isInstanceOf[Array[Byte]] && after.isInstanceOf[Array[Byte]]) {
      util.Objects.deepEquals(before, after)
    } else {
      Objects.equal(before, after)
    }
  }

  def partitionSchemasMatch(before: PartitionSchema, after: PartitionSchema): Boolean = {
    if (before eq after) return true
    val beforeBuckets = before.getHashBucketSchemas.asScala
    val afterBuckets = after.getHashBucketSchemas.asScala
    if (beforeBuckets.size != afterBuckets.size) return false
    val hashBucketsMatch = (0 until beforeBuckets.size).forall { i =>
      HashBucketSchemasMatch(beforeBuckets(i), afterBuckets(i))
    }
    hashBucketsMatch &&
    Objects.equal(before.getRangeSchema.getColumnIds, after.getRangeSchema.getColumnIds)
  }

  def HashBucketSchemasMatch(before: HashBucketSchema, after: HashBucketSchema): Boolean = {
    if (before eq after) return true
    Objects.equal(before.getColumnIds, after.getColumnIds) &&
    Objects.equal(before.getNumBuckets, after.getNumBuckets) &&
    Objects.equal(before.getSeed, after.getSeed)
  }

  def createRandomTable(): KuduTable = {
    val columnCount = random.nextInt(50) + 1 // At least one column.
    val keyColumnCount = random.nextInt(columnCount) + 1 // At least one key.
    val schemaGenerator = new SchemaGeneratorBuilder()
      .random(random)
      .columnCount(columnCount)
      .keyColumnCount(keyColumnCount)
      .build()
    val schema = schemaGenerator.randomSchema()
    val options = schemaGenerator.randomCreateTableOptions(schema)
    options.setNumReplicas(1)
    val name = s"random-${System.currentTimeMillis()}"
    kuduClient.createTable(name, schema, options)
  }

  def loadRandomData(table: KuduTable): IndexedSeq[PartialRow] = {
    val kuduSession = kuduClient.newSession()
    val dataGenerator = new DataGeneratorBuilder()
      .random(random)
      .build()
    val rowCount = random.nextInt(200)
    (0 to rowCount).map { i =>
      val upsert = table.newUpsert()
      val row = upsert.getRow
      dataGenerator.randomizeRow(row)
      kuduSession.apply(upsert)
      row
    }
  }

  def backupAndRestore(tableNames: Seq[String]): Unit = {
    val rootDir = Files.createTempDirectory("backup")
    doBackup(rootDir, tableNames)
    doRestore(rootDir, tableNames)
    FileUtils.deleteDirectory(rootDir.toFile)
  }

  def doBackup(
      rootDir: Path,
      tableNames: Seq[String],
      fromMs: Long = BackupOptions.DefaultFromMS): Unit = {
    val nowMs = System.currentTimeMillis()

    // Log the timestamps to simplify flaky debugging.
    log.info(s"nowMs: ${System.currentTimeMillis()}")
    val hts = HybridTimeUtil.HTTimestampToPhysicalAndLogical(kuduClient.getLastPropagatedTimestamp)
    log.info(s"propagated physicalMicros: ${hts(0)}")
    log.info(s"propagated logical: ${hts(1)}")

    // Add one millisecond to our target snapshot time. This will ensure we read all of the records
    // in the backup and prevent flaky off-by-one errors. The underlying reason for adding 1 ms is
    // that we pass the timestamp in millisecond granularity but the snapshot time has microsecond
    // granularity. This means if the test runs fast enough that data is inserted with the same
    // millisecond value as nowMs (after truncating the micros) the records inserted in the
    // microseconds after truncation could be unread.
    val backupOptions = new BackupOptions(
      tables = tableNames,
      rootPath = rootDir.toUri.toString,
      kuduMasterAddresses = harness.getMasterAddressesAsString,
      toMs = nowMs + 1,
      fromMs = fromMs
    )
    KuduBackup.run(backupOptions, ss)
  }

  def doRestore(rootDir: Path, tableNames: Seq[String]): Unit = {
    val restoreOptions =
      new RestoreOptions(tableNames, rootDir.toUri.toString, harness.getMasterAddressesAsString)
    KuduRestore.run(restoreOptions, ss)
  }

  private def getPropagatedTimestampMs: Long = {
    val propagatedTimestamp = harness.getClient.getLastPropagatedTimestamp
    val physicalTimeMicros =
      HybridTimeUtil.HTTimestampToPhysicalAndLogical(propagatedTimestamp).head
    TimeUnit.MILLISECONDS.convert(physicalTimeMicros, TimeUnit.MICROSECONDS)
  }
}
