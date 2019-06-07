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

import java.time.Duration
import java.time.temporal.ChronoUnit

import org.apache.kudu.backup.BackupCLIOptions.DefaultDryRun
import org.apache.kudu.backup.BackupCLIOptions.DefaultExpirationAge
import org.apache.kudu.backup.BackupCLIOptions.DefaultFormat
import org.apache.kudu.backup.BackupCLIOptions.DefaultListType
import org.apache.kudu.backup.BackupCLIOptions.DefaultVerbose
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import scopt.OptionParser

object Mode extends Enumeration {
  val LIST, CLEAN = Value
}

// The possible backup CLI tool list types.
@InterfaceAudience.Private
@InterfaceStability.Unstable
object ListType extends Enumeration {
  val LATEST, RESTORE_SEQUENCE, ALL = Value
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
    rootPath: String,
    mode: Mode.Value,
    tables: Seq[String] = Seq(),
    listType: ListType.Value = DefaultListType,
    format: Format.Value = DefaultFormat,
    expirationAge: Duration = DefaultExpirationAge,
    dryRun: Boolean = DefaultDryRun,
    verbose: Boolean = DefaultVerbose)

object BackupCLIOptions {

  val DefaultDryRun: Boolean = false
  val DefaultExpirationAge: Duration = Duration.of(30, ChronoUnit.DAYS)
  val DefaultFormat: Format.Value = Format.PRETTY
  val DefaultListType: ListType.Value = ListType.LATEST
  val DefaultVerbose: Boolean = false

  val ProgramName: String = "kudu-backup-tools"

  val parser: OptionParser[BackupCLIOptions] =
    new OptionParser[BackupCLIOptions](ProgramName) {

      opt[String]("rootPath")
        .action((v, o) => o.copy(rootPath = v))
        .text("The root path to search for backups. Accepts any Hadoop compatible path.")
        .required()

      arg[String]("<table>...")
        .unbounded()
        .action((v, o) => o.copy(tables = o.tables :+ v))
        .text("A list of tables to be included. Specifying no tables includes all tables.")
        .optional()

      help("help").text("Prints this usage text")

      cmd("list")
        .action((_, c) => c.copy(mode = Mode.LIST))
        .text("List the backups in the rootPath.")
        .children(
          opt[String]("type")
            .action((v, o) => o.copy(listType = ListType.withName(v.toUpperCase)))
            .text("The type of listing to perform. One of 'latest', 'restore_sequence', 'all'. " +
              s"Default: ${DefaultListType.toString.toLowerCase()}")
            .validate(
              validateEnumeratedOption("type", ListType.values.map(_.toString.toLowerCase))),
          opt[String]("format")
            .action((v, o) => o.copy(format = Format.withName(v.toUpperCase)))
            .text(s"The output format. One of 'pretty', 'tsv', 'csv'. " +
              s"Default: ${DefaultFormat.toString.toLowerCase()}")
            .validate(validateEnumeratedOption("format", Format.values.map(_.toString.toLowerCase)))
            .optional()
        )

      cmd("clean")
        .action((_, c) => c.copy(mode = Mode.CLEAN))
        .text("Cleans up old backup data in the rootPath.")
        .children(
          opt[String]("expirationAgeDays")
            .action((v, o) => o.copy(expirationAge = Duration.of(v.toLong, ChronoUnit.DAYS)))
            .text("The age at which old backups should be expired. " +
              "Backups that are part of the current restore path are never expired. " +
              s"Default: ${DefaultExpirationAge.toDays} Days")
            .optional(),
          opt[Boolean]("dryRun")
            .action((v, o) => o.copy(dryRun = v))
            .text("Report on what backups will be deleted, but don't delete anything. " +
              s"Overrides --verbose. Default: $DefaultDryRun")
            .optional(),
          opt[Boolean]("verbose")
            .action((v, o) => o.copy(verbose = v))
            .text(s"Report on what backups are deleted. Default: $DefaultVerbose")
            .optional()
        )
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
    parser.parse(args, BackupCLIOptions(null, null))
  }
}

@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduBackupCLI {

  // Run the backup CLI tool with the given options.
  // Like a command, returns 0 if successful, or a nonzero error code.
  def run(options: BackupCLIOptions): Int = {
    options.mode match {
      case Mode.LIST => KuduBackupLister.run(options)
      case Mode.CLEAN => KuduBackupCleaner.run(options)
      case _ => throw new IllegalArgumentException("Arguments must come after the command")
    }
  }

  def main(args: Array[String]): Unit = {
    val options = BackupCLIOptions
      .parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))
    System.exit(run(options))
  }
}
