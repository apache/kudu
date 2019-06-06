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

import com.google.common.collect.ImmutableList
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduTable
import org.apache.kudu.test.ClientTestUtil.getBasicSchema
import org.apache.kudu.test.KuduTestHarness
import org.junit.Assert._
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.annotation.meta.getter

class TestBackupGraph {
  val log: Logger = LoggerFactory.getLogger(getClass)

  var tableName: String = "TestBackupGraph"
  var table: KuduTable = _

  @(Rule @getter)
  val harness = new KuduTestHarness

  @Before
  def setUp(): Unit = {
    // Create the test table.
    val builder = new CreateTableOptions().setNumReplicas(3)
    builder.setRangePartitionColumns(ImmutableList.of("key"))
    table = harness.getClient.createTable(tableName, getBasicSchema, builder)
  }

  @Test
  def testSimpleBackupGraph() {
    val graph = new BackupGraph(table.getTableId)
    val full = createBackupVertex(table, 0, 1)
    graph.addBackup(full)

    // Validate a graph with only a single full backup.
    assertEquals(1, graph.fullBackups.size)
    assertEquals(1, graph.backupPaths.size)
    val fullPath = graph.backupPaths.head
    assertEquals("0", fullPath.pathString)

    // Validate a graph with a couple incremental backups.
    val inc1 = createBackupVertex(table, 1, 2)
    graph.addBackup(inc1)
    val inc2 = createBackupVertex(table, 2, 3)
    graph.addBackup(inc2)
    assertEquals(1, graph.fullBackups.size)
    assertEquals(1, graph.backupPaths.size)

    val incPath = graph.backupPaths.head
    assertEquals("0 -> 1 -> 2", incPath.pathString)
  }

  @Test
  def testForkingBackupGraph() {
    val graph = new BackupGraph(table.getTableId)
    val full = createBackupVertex(table, 0, 1)
    graph.addBackup(full)
    // Duplicate fromMs of 1 creates a fork in the graph.
    val inc1 = createBackupVertex(table, 1, 2)
    graph.addBackup(inc1)
    val inc2 = createBackupVertex(table, 1, 4)
    graph.addBackup(inc2)
    val inc3 = createBackupVertex(table, 2, 3)
    graph.addBackup(inc3)
    val inc4 = createBackupVertex(table, 4, 5)
    graph.addBackup(inc4)

    assertEquals(1, graph.fullBackups.size)
    assertEquals(2, graph.backupPaths.size)

    val path1 = graph.backupPaths.head
    assertEquals("0 -> 1 -> 2", path1.pathString)

    val path2 = graph.backupPaths.last
    assertEquals("0 -> 1 -> 4", path2.pathString)

    // Ensure the most recent incremental is used for a backup base and restore path.
    assertEquals(5, graph.backupBase.metadata.getToMs)
    assertEquals(5, graph.restorePath.toMs)
  }

  @Test
  def testMultiFullBackupGraph() {
    val graph = new BackupGraph(table.getTableId)
    val full1 = createBackupVertex(table, 0, 1)
    graph.addBackup(full1)
    val inc1 = createBackupVertex(table, 1, 2)
    graph.addBackup(inc1)
    val inc2 = createBackupVertex(table, 2, 4)
    graph.addBackup(inc2)

    // Add a second full backup.
    val full2 = createBackupVertex(table, 0, 4)
    graph.addBackup(full2)
    val inc3 = createBackupVertex(table, 4, 5)
    graph.addBackup(inc3)

    assertEquals(2, graph.fullBackups.size)
    assertEquals(2, graph.backupPaths.size)
    val path1 = graph.backupPaths.head
    assertEquals("0 -> 1 -> 2 -> 4", path1.pathString)

    val path2 = graph.backupPaths.last
    assertEquals("0 -> 4", path2.pathString)

    // Ensure the most recent incremental is used for a backup base and restore path.
    assertEquals(5, graph.backupBase.metadata.getToMs)
    assertEquals(5, graph.restorePath.toMs)
  }

  @Test
  def testFilterByTime() {
    val graph = new BackupGraph(table.getName)
    val full1 = createBackupVertex(table, 0, 1)
    graph.addBackup(full1)
    val inc1 = createBackupVertex(table, 1, 2)
    graph.addBackup(inc1)
    val inc2 = createBackupVertex(table, 2, 4)
    graph.addBackup(inc2)

    // Add a second full backup.
    val full2 = createBackupVertex(table, 0, 4)
    graph.addBackup(full2)
    val inc3 = createBackupVertex(table, 4, 5)
    graph.addBackup(inc3)

    val newGraph = graph.filterByTime(2)

    assertEquals(1, newGraph.fullBackups.size)
    assertEquals(1, newGraph.backupPaths.size)
  }

  private def createBackupVertex(table: KuduTable, fromMs: Long, toMs: Long): BackupNode = {
    val metadata = TableMetadata.getTableMetadata(table, fromMs, toMs, "parquet")
    BackupNode(null, metadata)
  }
}
