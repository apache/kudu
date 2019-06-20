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

import java.io.File
import java.nio.file.Files
import java.time.temporal.ChronoUnit
import java.time.Duration
import java.time.Instant

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class BaseTestKuduBackupCleaner {
  val log: Logger = LoggerFactory.getLogger(getClass)

  var rootPath: String = _

  // Return the epoch time in milliseconds that is 'secsBefore' seconds before 'current'.
  private def epochMillisBefore(current: Instant, secsBefore: Long): Long = {
    current.minus(Duration.of(secsBefore, ChronoUnit.SECONDS)).getEpochSecond * 1000
  }

  @Test
  def testBackupCleaner(): Unit = {
    val io = new BackupIO(new Configuration(), rootPath)
    val expirationAge = Duration.of(15, ChronoUnit.SECONDS)
    val now = Instant.now
    val tableName = "taco"

    val createPath = (ages: Array[Long]) => {
      for (i <- ages.indices) {
        val fromMs = if (i == 0) 0 else epochMillisBefore(now, ages(i - 1))
        val toMs = epochMillisBefore(now, ages(i))
        TestUtils.createTableMetadata(io, tableName, fromMs, toMs)
      }
    }

    // Create a graph of backups for a table incrementally. At first, there'll be one backup path,
    // path A, which must therefore be the restore path. All of its backups will be older than the
    // expiration age.
    val pathA: Array[Long] = Array(25, 21, 16)
    createPath(pathA)

    // Nothing should be cleaned up because all backups are on the restore path.
    val options = createOptions(rootPath, expirationAge, verbose = true)
    assertEquals(0, KuduBackupCleaner.run(options))

    val backupExists = (secsAgo: Long) => {
      val backupPath =
        new HPath(io.tablePath(s"id_$tableName", tableName), s"${epochMillisBefore(now, secsAgo)}")
      val metadataPath = io.backupMetadataPath(backupPath)
      println(s"checking existence of $metadataPath")
      io.fs.exists(metadataPath)
    }

    assertTrue(pathA.forall(backupExists(_)))

    // Now add a new backup path, path B, that ends at a later time and so is the new restore path.
    val pathB: Array[Long] = Array(20, 15, 10, 5, 1)
    createPath(pathB)

    // Add a backup with a from time of now - 20 and a to time of now - 18. The backup path that
    // ends in this backup is expired, but it forks from the restore path.
    TestUtils
      .createTableMetadata(io, tableName, epochMillisBefore(now, 20), epochMillisBefore(now, 18))

    // Running the cleaner should delete path A and the forked backup, but first do a dry run and
    // make sure nothing gets deleted.
    val dryRunOptions = createOptions(rootPath, expirationAge, dryRun = true)
    assertEquals(0, KuduBackupCleaner.run(dryRunOptions))

    assertTrue(pathA.forall(backupExists(_)))
    assertTrue(backupExists(18))
    assertTrue(pathB.forall(backupExists(_)))

    // After the cleaner runs, path A and the forked backup should be deleted and path B should remain.
    assertEquals(0, KuduBackupCleaner.run(options))
    assertTrue(pathA.forall(!backupExists(_)))
    assertTrue(!backupExists(18))
    assertTrue(pathB.forall(backupExists(_)))

    // Finally, add a third path which is not the restore path but which has backups that are old
    // enough to get deleted and backups that are too new to be deleted.
    val pathC: Array[Long] = Array(19, 14, 9, 4, 2)
    createPath(pathC)

    assertEquals(0, KuduBackupCleaner.run(options))
    assertTrue(pathA.forall(!backupExists(_)))
    assertTrue(pathB.forall(backupExists(_)))
    assertTrue(pathC.forall(backupExists(_)))
  }

  def createOptions(
      rootPath: String,
      expirationAge: Duration,
      tables: Seq[String] = Seq(),
      dryRun: Boolean = false,
      verbose: Boolean = false): BackupCLIOptions = {
    new BackupCLIOptions(
      rootPath,
      Mode.CLEAN,
      tables = tables,
      expirationAge = expirationAge,
      dryRun = dryRun,
      verbose = verbose)
  }
}

class LocalTestKuduBackupCleaner extends BaseTestKuduBackupCleaner {

  @Before
  def setUp(): Unit = {
    rootPath = Files.createTempDirectory("backupcli").toAbsolutePath.toString
  }

  @After
  def tearDown(): Unit = {
    FileUtils.deleteDirectory(new File(rootPath))
  }
}

class HDFSTestKuduBackupCleaner extends BaseTestKuduBackupCleaner {
  var baseDir: File = _

  @Before
  def setUp(): Unit = {
    baseDir = Files.createTempDirectory("hdfs-test").toFile.getAbsoluteFile

    // Create an HDFS mini-cluster.
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val hdfsCluster = new MiniDFSCluster.Builder(conf).build()

    // Set the root path to use the HDFS URI.
    rootPath = "hdfs://localhost:" + hdfsCluster.getNameNodePort + "/"
  }

  @After
  def tearDown(): Unit = {
    FileUtil.fullyDelete(baseDir)
  }
}
