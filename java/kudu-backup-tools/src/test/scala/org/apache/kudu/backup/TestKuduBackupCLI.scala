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

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.text.SimpleDateFormat

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.hadoop.conf.Configuration
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.backup.TableMetadata.MetadataVersion

class TestKuduBackupCLI {
  val log: Logger = LoggerFactory.getLogger(getClass)

  var rootDir: Path = _

  @Before
  def setUp(): Unit = {
    rootDir = Files.createTempDirectory("backupcli")
  }

  @After
  def tearDown(): Unit = {
    FileUtils.deleteDirectory(rootDir.toFile)
  }

  // Create dummy table metadata and write it to the test directory.
  private def createTableMetadata(
      io: BackupIO,
      tableName: String,
      fromMs: Long,
      toMs: Long): Unit = {
    // Create dummy table metadata with just enough information to be used to create a BackupGraph.
    val tableId = s"id_$tableName"
    val metadata = TableMetadataPB
      .newBuilder()
      .setVersion(MetadataVersion)
      .setFromMs(fromMs)
      .setToMs(toMs)
      .setTableName(tableName)
      .setTableId(tableId)
      .build()
    val backupPath = new HPath(io.tablePath(tableId, tableName), s"$toMs")
    val metadataPath = io.backupMetadataPath(backupPath)
    io.writeTableMetadata(metadata, metadataPath)
  }

  // Helper to write a standard collection of backup metadata useful for a few tests.
  private def createStandardTableMetadata(io: BackupIO): Unit = {
    Seq(
      // Two fulls and one incremental for 'taco' table.
      ("taco", 0, 100),
      ("taco", 0, 1000),
      ("taco", 100, 2000),
      // One full and two incrementals for 'pizza' table.
      ("pizza", 0, 200),
      ("pizza", 200, 400),
      ("pizza", 400, 600)
    ).foreach {
      case (tableName: String, fromMs: Int, toMs: Int) =>
        createTableMetadata(io, tableName, fromMs, toMs)
    }
  }

  // Helper to format the end time column, since its value depends on the timezone of the machine
  // where the tool is run.
  private def endTime(toMs: Long): String = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(toMs)
  }

  @Test
  def testListAllBackups(): Unit = {
    val io = new BackupIO(new Configuration(), rootDir.toUri.toString)
    createStandardTableMetadata(io)

    val options =
      BackupCLIOptions(Action.LIST_ALL, Format.CSV, Seq(), rootDir.toUri.toString)
    val stdout = new ByteArrayOutputStream
    Console.withOut(new PrintStream(stdout)) {
      assertEquals(0, KuduBackupCLI.run(options))
    }

    val headerString = KuduBackupCLI.HEADER.mkString(",")
    val expected = Seq(
      headerString,
      s"pizza,id_pizza,${endTime(200)},0,200,full",
      s"pizza,id_pizza,${endTime(400)},200,400,incremental",
      s"pizza,id_pizza,${endTime(600)},400,600,incremental",
      "",
      headerString,
      s"taco,id_taco,${endTime(100)},0,100,full",
      s"taco,id_taco,${endTime(1000)},0,1000,full",
      s"taco,id_taco,${endTime(2000)},100,2000,incremental"
    ).mkString("\n")
    assertEquals(expected, stdout.toString.trim)
  }

  @Test
  def testListLatestBackups(): Unit = {
    val io = new BackupIO(new Configuration(), rootDir.toUri.toString)
    createStandardTableMetadata(io)

    val options =
      BackupCLIOptions(Action.LIST_LATEST, Format.CSV, Seq(), rootDir.toUri.toString)
    val stdout = new ByteArrayOutputStream
    Console.withOut(new PrintStream(stdout)) {
      assertEquals(0, KuduBackupCLI.run(options))
    }

    val headerString = KuduBackupCLI.HEADER.mkString(",")
    val expected = Seq(
      headerString,
      s"pizza,id_pizza,${endTime(600)},400,600,incremental",
      s"taco,id_taco,${endTime(2000)},100,2000,incremental"
    ).mkString("\n")
    assertEquals(expected, stdout.toString.trim)
  }

  @Test
  def testListRestorePath(): Unit = {
    val io = new BackupIO(new Configuration(), rootDir.toUri.toString)
    createStandardTableMetadata(io)

    val options =
      BackupCLIOptions(Action.LIST_RESTORE_SEQUENCE, Format.CSV, Seq(), rootDir.toUri.toString)
    val stdout = new ByteArrayOutputStream
    Console.withOut(new PrintStream(stdout)) {
      assertEquals(0, KuduBackupCLI.run(options))
    }

    val headerString = KuduBackupCLI.HEADER.mkString(",")
    val expected = Seq(
      headerString,
      s"pizza,id_pizza,${endTime(200)},0,200,full",
      s"pizza,id_pizza,${endTime(400)},200,400,incremental",
      s"pizza,id_pizza,${endTime(600)},400,600,incremental",
      "",
      headerString,
      s"taco,id_taco,${endTime(100)},0,100,full",
      s"taco,id_taco,${endTime(2000)},100,2000,incremental"
    ).mkString("\n")
    assertEquals(expected, stdout.toString.trim)
  }

  @Test
  def testTableFilter(): Unit = {
    val io = new BackupIO(new Configuration(), rootDir.toUri.toString)
    createStandardTableMetadata(io)

    val options =
      BackupCLIOptions(Action.LIST_ALL, Format.CSV, Seq("taco"), rootDir.toUri.toString)
    val stdout = new ByteArrayOutputStream
    Console.withOut(new PrintStream(stdout)) {
      assertEquals(0, KuduBackupCLI.run(options))
    }

    val headerString = KuduBackupCLI.HEADER.mkString(",")
    val expected = Seq(
      headerString,
      s"taco,id_taco,${endTime(100)},0,100,full",
      s"taco,id_taco,${endTime(1000)},0,1000,full",
      s"taco,id_taco,${endTime(2000)},100,2000,incremental"
    ).mkString("\n")
    assertEquals(expected, stdout.toString.trim)
  }

  @Test
  def testMissingTable(): Unit = {
    val io = new BackupIO(new Configuration(), rootDir.toUri.toString)
    createStandardTableMetadata(io)

    val options =
      BackupCLIOptions(Action.LIST_ALL, Format.CSV, Seq("pizza", "nope"), rootDir.toUri.toString)
    val stdout = new ByteArrayOutputStream
    val stderr = new ByteArrayOutputStream
    Console.withOut(new PrintStream(stdout)) {
      Console.withErr(new PrintStream(stderr)) {
        assertEquals(1, KuduBackupCLI.run(options))
      }
    }

    val headerString = KuduBackupCLI.HEADER.mkString(",")
    val expected = Seq(
      headerString,
      s"pizza,id_pizza,${endTime(200)},0,200,full",
      s"pizza,id_pizza,${endTime(400)},200,400,incremental",
      s"pizza,id_pizza,${endTime(600)},400,600,incremental"
    ).mkString("\n")
    assertEquals(expected, stdout.toString.trim)

    assertEquals("No backups were found for 1 table(s):\nnope", stderr.toString.trim)
  }
}
