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

import org.apache.hadoop.conf.Configuration
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import scopt.OptionParser

// The possible backup CLI tool actions.
@InterfaceAudience.Private
@InterfaceStability.Unstable
object Action extends Enumeration {
  val LIST_LATEST, LIST_RESTORE_SEQUENCE, LIST_ALL = Value
}

// The possible backup CLI print formats.
@InterfaceAudience.Private
@InterfaceStability.Unstable
object Format extends Enumeration {
  val PRETTY, TSV, CSV = Value
}

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class BackupCLIOptions(
    action: Action.Value,
    format: Format.Value,
    tables: Seq[String],
    rootPath: String)

object BackupCLIOptions {

  val ProgramName: String =
    KuduBackupCLI.getClass.getCanonicalName.dropRight(1) // Remove trailing `$`

  val parser: OptionParser[BackupCLIOptions] =
    new OptionParser[BackupCLIOptions](ProgramName) {
      opt[String]("rootPath")
        .action((v, o) => o.copy(rootPath = v))
        .text("The root path to search for backups. Accepts any Hadoop compatible path.")
        .required()

      arg[String]("format")
        .validate(validateEnumeratedOption("format", Format.values.map(_.toString.toLowerCase)))
        .action((v, o) => o.copy(format = Format.withName(v.toUpperCase)))
        .text("The output format. One of 'pretty', 'tsv', 'csv'.")
        .optional()

      arg[String]("<action>")
        .validate(validateEnumeratedOption("action", Action.values.map(_.toString.toLowerCase)))
        .action((v, o) => o.copy(action = Action.withName(v.toUpperCase)))
        .text("The action to perform. One of 'list_latest', 'list_restore_sequence', 'list_all'.")

      arg[String]("<table>...")
        .unbounded()
        .action((v, o) => o.copy(tables = o.tables :+ v))
        .text("A list of tables about which to print backup information. Specifying no tables includes all tables.")
        .optional()

      help("help").text("Prints this usage text")
    }

  def validateEnumeratedOption(
      name: String,
      optionStrings: Iterable[String]): String => Either[String, Unit] =
    (v: String) => {
      if (optionStrings.exists(_.equalsIgnoreCase(v))) {
        Right(())
      } else {
        Left(s"$name must be one of ${optionStrings.mkString(", ")}: $v")
      }
    }

  def parse(args: Seq[String]): Option[BackupCLIOptions] = {
    parser.parse(args, BackupCLIOptions(null, Format.PRETTY, Seq(), null))
  }
}

@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduBackupCLI {

  // The header for all tables printed by the tool.
  val HEADER: Seq[String] =
    Seq("table name", "table id", "end time", "start timestamp", "end timestamp", "type")

  // Run the backup CLI tool with the given options. Like a command, returns 0 if successful, or
  // a nonzero error code.
  def run(options: BackupCLIOptions): Int = {
    // Sort by table name for a consistent ordering (at least if there's no duplicate names).
    val sortedTables = options.tables.sorted

    val io: BackupIO = new BackupIO(new Configuration(), options.rootPath)
    val backupGraphs =
      if (sortedTables.isEmpty)
        io.readAllBackupGraphs()
      else
        io.readBackupGraphsByTableName(sortedTables)

    options.action match {
      case Action.LIST_LATEST => {
        val rows = backupGraphs.map(graph => rowForBackupNode(graph.restorePath.lastBackup))
        printTable(options.format, rows)
      }
      case Action.LIST_RESTORE_SEQUENCE => {
        val tablesOfBackups =
          backupGraphs.map(_.restorePath.backups.map(node => rowForBackupNode(node)))
        tablesOfBackups.foreach(table => printTable(options.format, table))
      }
      case Action.LIST_ALL => {
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

  def main(args: Array[String]): Unit = {
    val options = BackupCLIOptions
      .parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))
    System.exit(run(options))
  }
}
