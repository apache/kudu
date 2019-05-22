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

import com.google.common.base.Objects
import org.apache.commons.io.FileUtils
import org.apache.kudu.client.PartitionSchema.HashBucketSchema
import org.apache.kudu.client._
import org.apache.kudu.ColumnSchema
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.spark.kudu._
import org.apache.kudu.test.CapturingLogAppender
import org.apache.kudu.test.RandomUtils
import org.apache.kudu.util.DataGenerator.DataGeneratorBuilder
import org.apache.kudu.util.HybridTimeUtil
import org.apache.kudu.util.SchemaGenerator.SchemaGeneratorBuilder
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.junit.Assert._
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class TestKuduBackup extends KuduTestSuite {
  val log: Logger = LoggerFactory.getLogger(getClass)

  var random: util.Random = _
  var rootDir: Path = _

  @Before
  def setUp(): Unit = {
    random = RandomUtils.getRandom
    rootDir = Files.createTempDirectory("backup")
  }

  @After
  def tearDown(): Unit = {
    FileUtils.deleteDirectory(rootDir.toFile)
  }

  @Test
  def testSimpleBackupAndRestore() {
    val rowCount = 100
    insertRows(table, rowCount) // Insert data into the default test table.

    // backup and restore.
    backupAndValidateTable(tableName, rowCount, false)
    restoreAndValidateTable(tableName, rowCount)

    // Validate the table schemas match.
    validateTablesMatch(tableName, s"$tableName-restore")
  }

  @Test
  def testSimpleIncrementalBackupAndRestore() {
    insertRows(table, 100) // Insert data into the default test table.

    // Run and validate initial backup.
    backupAndValidateTable(tableName, 100, false)

    // Insert more rows and validate incremental backup.
    insertRows(table, 100, 100) // Insert more data.
    backupAndValidateTable(tableName, 100, true)

    // Delete rows that span the full and incremental backup and validate incremental backup.
    Range(50, 150).foreach(deleteRow)
    backupAndValidateTable(tableName, 100, true)

    // Restore the backups and validate the end result.
    restoreAndValidateTable(tableName, 100)
  }

  @Test
  def testBackupAndRestoreWithNoRows(): Unit = {
    backupAndValidateTable(tableName, 0, false)
    backupAndValidateTable(tableName, 0, true)
    restoreAndValidateTable(tableName, 0)
    validateTablesMatch(tableName, s"$tableName-restore")
  }

  @Test
  def testBackupMissingTable(): Unit = {
    try {
      val options = createBackupOptions(Seq("missingTable"))
      doBackup(options)
      fail()
    } catch {
      case e: KuduException =>
        assertTrue(e.getMessage.contains("the table does not exist"))
    }
  }

  @Test
  def testRestoreWithNoBackup(): Unit = {
    try {
      val options = createRestoreOptions(Seq(tableName))
      doRestore(options)
      fail()
    } catch {
      case e: RuntimeException =>
        assertEquals(e.getMessage, s"No valid backups found for table: $tableName")
    }
  }

  @Test
  def testForceIncrementalBackup() {
    insertRows(table, 100) // Insert data into the default test table.
    Thread.sleep(1) // Ensure the previous insert is before beforeMs.
    // Set beforeMs so we can force an incremental at this time.
    val beforeMs = System.currentTimeMillis()
    Thread.sleep(1) // Ensure the next insert is after beforeMs.
    insertRows(table, 100, 100) // Insert more data.

    // Force an incremental backup without a full backup.
    // It will use a diff scan and won't check the existing dependency graph.
    val options = createBackupOptions(Seq(tableName), fromMs = beforeMs)
    val logs = captureLogs { () =>
      doBackup(options)
    }
    assertTrue(logs.contains("Performing an incremental backup, fromMs was set to"))
    validateBackup(options, 100, true)
  }

  @Test
  def testForceFullBackup() {
    insertRows(table, 100) // Insert data into the default test table.
    // Backup the table so the following backup should be an incremental.
    backupAndValidateTable(tableName, 100)
    insertRows(table, 100, 100) // Insert more data.

    // Force a full backup. It should contain all the rows.
    val options = createBackupOptions(Seq(tableName), forceFull = true)
    val logs = captureLogs { () =>
      doBackup(options)
    }
    assertTrue(logs.contains("Performing a full backup, forceFull was set to true"))
    validateBackup(options, 200, false)
  }

  @Test
  def testSimpleBackupAndRestoreWithSpecialCharacters() {
    // Use an Impala-style table name to verify url encoding/decoding of the table name works.
    val impalaTableName = "impala::default.test"

    val tableOptions = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)

    kuduClient.createTable(impalaTableName, simpleSchema, tableOptions)

    backupAndValidateTable(tableName, 0)
    restoreAndValidateTable(tableName, 0)
  }

  @Test
  def testRandomBackupAndRestore() {
    val table = createRandomTable()
    val tableName = table.getName
    val maxRows = 200
    val rows = loadRandomData(table)

    // Run a full backup.
    backupAndValidateTable(tableName, rows.length)

    // Run 1 to 5 incremental backups.
    val incrementalCount = random.nextInt(5) + 1
    (0 to incrementalCount).foreach { i =>
      val incrementalRows = loadRandomData(table, maxRows)
      backupAndValidateTable(tableName, incrementalRows.length, true)
    }

    doRestore(createRestoreOptions(Seq(tableName)))
    validateTablesMatch(tableName, s"$tableName-restore")
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

    doBackup(createBackupOptions(Seq(table1Name, table2Name)))
    doRestore(createRestoreOptions(Seq(table1Name, table2Name)))

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
    backupAndValidateTable(tableName, kNumPartitions)
    restoreAndValidateTable(tableName, kNumPartitions)
  }

  @Test
  def testBackupAndRestoreTableWithNoRangePartitions(): Unit = {
    val tableName = "only-hash-partitions-table"

    val options = new CreateTableOptions()
      .addHashPartitions(List("key").asJava, 2)
      .setNumReplicas(1)
    val table1 = kuduClient.createTable(tableName, schema, options)

    val rowCount = 100
    insertRows(table1, rowCount)

    backupAndValidateTable(tableName, rowCount)
    restoreAndValidateTable(tableName, rowCount)
  }

  @Test
  def testColumnAlterHandling(): Unit = {
    // Create a basic table.
    val tableName = "testColumnAlterHandling"
    val columns = List(
      new ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchemaBuilder("col_a", Type.STRING).build(),
      new ColumnSchemaBuilder("col_b", Type.STRING).build(),
      new ColumnSchemaBuilder("col_c", Type.STRING).build(),
      new ColumnSchemaBuilder("col_d", Type.STRING).build()
    )
    val schema = new Schema(columns.asJava)
    val options = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
    var table = kuduClient.createTable(tableName, schema, options)
    val session = kuduClient.newSession()

    // Insert some rows and take a full backup.
    Range(0, 10).foreach { i =>
      val insert = table.newInsert
      val row = insert.getRow
      row.addInt("key", i)
      row.addString("col_a", s"a$i")
      row.addString("col_b", s"b$i")
      row.addString("col_c", s"c$i")
      row.addString("col_d", s"d$i")
      session.apply(insert)
    }
    backupAndValidateTable(tableName, 10, false)

    // Rename col_a to col_1 and add a new col_a to ensure the column id's and defaults
    // work correctly. Also drop col_d and rename col_c to ensure collisions on renaming
    // columns don't occur when processing columns from left to right.
    kuduClient.alterTable(
      tableName,
      new AlterTableOptions()
        .renameColumn("col_a", "col_1")
        .addColumn(new ColumnSchemaBuilder("col_a", Type.STRING)
          .defaultValue("default")
          .build())
        .dropColumn("col_b")
        .dropColumn("col_d")
        .renameColumn("col_c", "col_d")
    )

    // Insert more rows and take an incremental backup
    table = kuduClient.openTable(tableName)
    Range(10, 20).foreach { i =>
      val insert = table.newInsert
      val row = insert.getRow
      row.addInt("key", i)
      row.addString("col_1", s"a$i")
      row.addString("col_d", s"c$i")
      session.apply(insert)
    }
    backupAndValidateTable(tableName, 10, true)

    // Restore the table and validate.
    doRestore(createRestoreOptions(Seq(tableName)))

    val restoreTable = kuduClient.openTable(s"$tableName-restore")
    val scanner = kuduClient.newScannerBuilder(restoreTable).build()
    val rows = scanner.asScala.toSeq

    // Validate there are still 20 rows.
    assertEquals(20, rows.length)
    // Validate col_b is dropped from all rows.
    assertTrue(rows.forall(!_.getSchema.hasColumn("col_b")))
    // Validate the existing and renamed columns have the expected set of values.
    val expectedSet = Range(0, 20).toSet
    assertEquals(expectedSet, rows.map(_.getInt("key")).toSet)
    assertEquals(expectedSet.map(i => s"a$i"), rows.map(_.getString("col_1")).toSet)
    assertEquals(expectedSet.map(i => s"c$i"), rows.map(_.getString("col_d")).toSet)
    // Validate the new col_a has all defaults.
    assertTrue(rows.forall(_.getString("col_a") == "default"))
  }

  @Test
  def testPartitionAlterHandling(): Unit = {
    // Create a basic table with 10 row range partitions covering 10 through 40.
    val tableName = "testColumnAlterHandling"
    val ten = createPartitionRow(10)
    val twenty = createPartitionRow(20)
    val thirty = createPartitionRow(30)
    val fourty = createPartitionRow(40)
    val options = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .addRangePartition(ten, twenty)
      .addRangePartition(twenty, thirty)
      .addRangePartition(thirty, fourty)
    var table = kuduClient.createTable(tableName, schema, options)

    // Fill the partitions with rows.
    insertRows(table, 30, 10)

    // Run a full backup on the table.
    backupAndValidateTable(tableName, 30, false)

    // Drop partition 10-20, drop and re-add partition 20-30, add partition 0-10 and 40-50.
    // (drops 20 total rows)
    val zero = createPartitionRow(0)
    val fifty = createPartitionRow(50)
    kuduClient.alterTable(
      tableName,
      new AlterTableOptions()
        .dropRangePartition(ten, twenty)
        .dropRangePartition(twenty, thirty)
        .addRangePartition(twenty, thirty)
        .addRangePartition(zero, ten)
        .addRangePartition(fourty, fifty)
    )

    // Add some rows back to the new partitions (adds 15 total rows)
    insertRows(table, 5, 0)
    insertRows(table, 5, 20)
    insertRows(table, 5, 40)

    // Run an incremental backup on the table.
    backupAndValidateTable(tableName, 15, true)

    // Restore the table and validate.
    doRestore(createRestoreOptions(Seq(tableName)))

    val restoreTable = kuduClient.openTable(s"$tableName-restore")
    val scanner = kuduClient.newScannerBuilder(restoreTable).build()
    val rows = scanner.asScala.toList.map(_.getInt("key")).sorted
    val expectedKeys =
      (Range(0, 5) ++ Range(20, 25) ++ Range(30, 40) ++ Range(40, 45)).toList.sorted

    assertEquals(25, rows.length)
    assertEquals(expectedKeys, rows)
  }

  @Test
  def testTableAlterHandling(): Unit = {
    // Create the initial table and load it with data.
    val tableName = "testTableAlterHandling"
    var table = kuduClient.createTable(tableName, schema, tableOptions)
    insertRows(table, 100)

    // Run and validate initial backup.
    backupAndValidateTable(tableName, 100, false)

    // Rename the table and insert more rows
    val newTableName = "impala::default.testTableAlterHandling"
    kuduClient.alterTable(tableName, new AlterTableOptions().renameTable(newTableName))
    table = kuduClient.openTable(newTableName)
    insertRows(table, 100, 100)

    // Run and validate an incremental backup.
    backupAndValidateTable(newTableName, 100, true)

    // Create a new table with the old name.
    val tableWithOldName = kuduClient.createTable(tableName, schema, tableOptions)
    insertRows(tableWithOldName, 50)

    // Backup the table with the old name.
    backupAndValidateTable(tableName, 50, false)

    // Restore the tables and check the row counts.
    restoreAndValidateTable(newTableName, 200)
    restoreAndValidateTable(tableName, 50)
  }

  @Test
  def testDeleteIgnore(): Unit = {
    insertRows(table, 100) // Insert data into the default test table.

    // Run and validate initial backup.
    backupAndValidateTable(tableName, 100, false)

    // Delete the rows and validate incremental backup.
    Range(0, 100).foreach(deleteRow)
    backupAndValidateTable(tableName, 100, true)

    // When restoring the table, delete half the rows after each job completes.
    // This will force delete rows to cause NotFound errors and allow validation
    // that they are correctly handled.
    val listener = new SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        val client = kuduContext.syncClient
        val table = client.openTable(s"$tableName-restore")
        val scanner = kuduContext.syncClient.newScannerBuilder(table).build()
        val session = client.newSession()
        scanner.asScala.foreach { rr =>
          if (rr.getInt("key") % 2 == 0) {
            val delete = table.newDelete()
            val row = delete.getRow
            row.addInt("key", rr.getInt("key"))
            session.apply(delete)
          }
        }
      }
    }
    ss.sparkContext.addSparkListener(listener)

    restoreAndValidateTable(tableName, 0)
  }

  def createPartitionRow(value: Int): PartialRow = {
    val row = schema.newPartialRow()
    row.addInt("key", value)
    row
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

  def loadRandomData(table: KuduTable, maxRows: Int = 200): IndexedSeq[PartialRow] = {
    val kuduSession = kuduClient.newSession()
    val dataGenerator = new DataGeneratorBuilder()
      .random(random)
      .build()
    val rowCount = random.nextInt(maxRows)
    (0 to rowCount).map { i =>
      val upsert = table.newUpsert()
      val row = upsert.getRow
      dataGenerator.randomizeRow(row)
      kuduSession.apply(upsert)
      row
    }
  }

  /**
   * A convenience method to create backup options for tests.
   *
   * We add one millisecond to our target snapshot time (toMs). This will ensure we read all of
   * the records in the backup and prevent flaky off-by-one errors. The underlying reason for
   * adding 1 ms is that we pass the timestamp in millisecond granularity but the snapshot time
   * has microsecond granularity. This means if the test runs fast enough that data is inserted
   * with the same millisecond value as nowMs (after truncating the micros) the records inserted
   * in the microseconds after truncation could be unread.
   */
  def createBackupOptions(
      tableNames: Seq[String],
      toMs: Long = System.currentTimeMillis() + 1,
      fromMs: Long = BackupOptions.DefaultFromMS,
      forceFull: Boolean = false): BackupOptions = {
    BackupOptions(
      rootPath = rootDir.toUri.toString,
      tables = tableNames,
      kuduMasterAddresses = harness.getMasterAddressesAsString,
      fromMs = fromMs,
      toMs = toMs,
      forceFull = forceFull
    )
  }

  /**
   * A convenience method to create backup options for tests.
   */
  def createRestoreOptions(
      tableNames: Seq[String],
      tableSuffix: String = "-restore"): RestoreOptions = {
    RestoreOptions(
      rootPath = rootDir.toUri.toString,
      tables = tableNames,
      kuduMasterAddresses = harness.getMasterAddressesAsString,
      tableSuffix = tableSuffix
    )
  }

  def backupAndValidateTable(
      tableName: String,
      expectedRowCount: Long,
      expectIncremental: Boolean = false) = {
    val options = createBackupOptions(Seq(tableName))
    // Run the backup.
    doBackup(options)
    validateBackup(options, expectedRowCount, expectIncremental)
  }

  def doBackup(options: BackupOptions): Unit = {
    // Log the timestamps to simplify flaky debugging.
    log.info(s"nowMs: ${System.currentTimeMillis()}")
    val hts = HybridTimeUtil.HTTimestampToPhysicalAndLogical(kuduClient.getLastPropagatedTimestamp)
    log.info(s"propagated physicalMicros: ${hts(0)}")
    log.info(s"propagated logical: ${hts(1)}")
    KuduBackup.run(options, ss)
  }

  def validateBackup(
      options: BackupOptions,
      expectedRowCount: Long,
      expectIncremental: Boolean): Unit = {
    val io = new BackupIO(ss.sparkContext.hadoopConfiguration, options.rootPath)
    val tableName = options.tables.head
    val table = harness.getClient.openTable(tableName)
    val backupPath = io.backupPath(table.getTableId, table.getName, options.toMs)
    val metadataPath = io.backupMetadataPath(backupPath)
    val metadata = io.readTableMetadata(metadataPath)

    // Verify the backup type.
    if (expectIncremental) {
      assertNotEquals(metadata.getFromMs, 0)
    } else {
      assertEquals(metadata.getFromMs, 0)
    }

    // Verify the output data.
    val schema = BackupUtils.dataSchema(table.getSchema, expectIncremental)
    val df = ss.sqlContext.read
      .format(metadata.getDataFormat)
      .schema(schema)
      .load(backupPath.toString)
    assertEquals(expectedRowCount, df.collect.length)
  }

  def restoreAndValidateTable(tableName: String, expectedRowCount: Long) = {
    val options = createRestoreOptions(Seq(tableName))
    doRestore(options)

    // Verify the table data.
    val rdd = kuduContext.kuduRDD(ss.sparkContext, s"$tableName-restore")
    assertEquals(rdd.collect.length, expectedRowCount)
  }

  def doRestore(options: RestoreOptions): Unit = {
    KuduRestore.run(options, ss)
  }

  def validateTablesMatch(tableA: String, tableB: String): Unit = {
    val tA = kuduClient.openTable(tableA)
    val tB = kuduClient.openTable(tableB)
    assertEquals(tA.getNumReplicas, tB.getNumReplicas)
    assertTrue(schemasMatch(tA.getSchema, tB.getSchema))
    assertTrue(partitionSchemasMatch(tA.getPartitionSchema, tB.getPartitionSchema))
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
    Objects.equal(before.getComment, after.getComment)
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

  /**
   * Captures the logs while the wrapped function runs and returns them as a String.
   */
  def captureLogs(f: () => Unit): String = {
    val logs = new CapturingLogAppender()
    val handle = logs.attach()
    try {
      f()
    } finally {
      handle.close()
    }
    logs.getAppendedText
  }
}
