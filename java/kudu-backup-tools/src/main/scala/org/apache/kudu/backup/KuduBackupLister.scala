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

import java.text.SimpleDateFormat

import com.google.common.base.Preconditions
import org.apache.hadoop.conf.Configuration
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduBackupLister {

  // The header for all tables printed by the tool.
  val HEADER: Seq[String] =
    Seq("table name", "table id", "end time", "start timestamp", "end timestamp", "type")

  // Run the backup CLI tool with the given options. Like a command, returns 0 if successful, or
  // a nonzero error code.
  def run(options: BackupCLIOptions): Int = {
    Preconditions.checkArgument(options.mode == Mode.LIST)

    // Sort by table name for a consistent ordering (at least if there's no duplicate names).
    val sortedTables = options.tables.sorted

    val io: BackupIO = new BackupIO(new Configuration(), options.rootPath)
    val backupGraphs =
      if (sortedTables.isEmpty)
        io.readAllBackupGraphs()
      else
        io.readBackupGraphsByTableName(sortedTables)

    options.listType match {
      case ListType.LATEST => {
        val rows = backupGraphs.map(graph => rowForBackupNode(graph.restorePath.lastBackup))
        printTable(options.format, rows)
      }
      case ListType.RESTORE_SEQUENCE => {
        val tablesOfBackups =
          backupGraphs.map(_.restorePath.backups.map(node => rowForBackupNode(node)))
        tablesOfBackups.foreach(table => printTable(options.format, table))
      }
      case ListType.ALL => {
        val tablesOfBackups = backupGraphs.map(
          _.allBackups.sortBy(node => node.metadata.getToMs).map(node => rowForBackupNode(node)))
        tablesOfBackups.foreach(table => printTable(options.format, table))
      }
    }
    // Because of renames, one table name might map to multiple backup directories, so it's not
    // sufficient to check the size of 'options.tables' against the size of 'backupGraphs'.
    val foundTables = backupGraphs.map(graph => graph.backupBase.metadata.getTableName).toSet
    val notFoundTables = options.tables.filter(table => !foundTables.contains(table))
    if (notFoundTables.nonEmpty) {
      Console.err.println(s"No backups were found for ${notFoundTables.size} table(s):")
      notFoundTables.foreach(Console.err.println)
      return 1
    }
    0
  }

  private def rowForBackupNode(backup: BackupNode): Seq[String] = {
    val metadata = backup.metadata
    val tableName = metadata.getTableName
    val tableId = metadata.getTableId
    val fromMs = metadata.getFromMs
    val toMs = metadata.getToMs
    val toDatetime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(toMs)
    val backupType = if (fromMs == 0) "full" else "incremental"
    Seq(tableName, tableId, toDatetime, s"$fromMs", s"$toMs", backupType)
  }

  private def formatDsv(delimiter: String, table: Seq[Seq[String]]): String = {
    table.map(_.mkString(delimiter)).mkString("\n")
  }

  private def formatPrettyTable(table: Seq[Seq[String]]): String = {
    if (table.isEmpty) {
      return ""
    }
    // The width of a column is the width of largest cell, plus a padding of 2.
    val colWidths = table.transpose.map(_.map(_.length).max + 2)
    val rows = table.map { row =>
      (row, colWidths).zipped
        .map {
          // 1 space on left, then pad to (padding - 1) spaces.
          case (cell, width) => s" %-${width - 1}s".format(cell)
        }
        .mkString("|")
    }
    val separatorRow = colWidths.map("-" * _).mkString("+")
    (rows.head +: separatorRow +: rows.tail).mkString("\n")
  }

  private def printTable(format: Format.Value, rows: Seq[Seq[String]]): Unit = {
    if (rows.isEmpty) {
      return
    }
    val table = HEADER +: rows
    format match {
      case Format.PRETTY => {
        println(formatPrettyTable(table))
      }
      case Format.TSV => {
        println(formatDsv("\t", table))
      }
      case Format.CSV => {
        println(formatDsv(",", table))
      }
    }
    println() // Spacing after the table.
  }
}
