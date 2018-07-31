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

import java.net.InetAddress

import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import scopt.OptionParser

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class KuduRestoreOptions(
    tables: Seq[String],
    path: String,
    kuduMasterAddresses: String = InetAddress.getLocalHost.getCanonicalHostName,
    tableSuffix: String = KuduRestoreOptions.DefaultTableSuffix,
    createTables: Boolean = KuduRestoreOptions.DefaultCreateTables,
    metadataPath: String = "")

object KuduRestoreOptions {
  val DefaultTableSuffix: String = "-restore"
  val DefaultCreateTables: Boolean = true

  // TODO: clean up usage output.
  // TODO: timeout configurations.
  private val parser: OptionParser[KuduRestoreOptions] =
    new OptionParser[KuduRestoreOptions]("KuduRestore") {
      opt[String]("path")
        .action((v, o) => o.copy(path = v))
        .text("The root path to the backup data. Accepts any Spark compatible path.")
        .optional()

      opt[String]("kuduMasterAddresses")
        .action((v, o) => o.copy(kuduMasterAddresses = v))
        .text("Comma-separated addresses of Kudu masters.")
        .optional()

      opt[Boolean]("createTables")
        .action((v, o) => o.copy(createTables = v))
        .text("true to create tables during restore, false if they already exist.")
        .optional()

      opt[String]("tableSuffix")
        .action((v, o) => o.copy(tableSuffix = v))
        .text("The suffix to add to the restored table names. Only used when createTables is true.")
        .optional()

      opt[String]("metadataPath")
        .action((v, o) => o.copy(metadataPath = v))
        .text(
          "The root path to look for table metadata. This can be used to change the properties of " +
            "the tables created during restore. By default the backup path is used. " +
            "Only used when createTables is true.")
        .optional()

      arg[String]("<table>...")
        .unbounded()
        .action((v, o) => o.copy(tables = o.tables :+ v))
        .text("A list of tables to be restored.")
    }

  /**
   * Parses the passed arguments into Some[KuduRestoreOptions].
   *
   * If the arguments are bad, an error message is displayed
   * and None is returned.
   *
   * @param args The arguments to parse.
   * @return Some[KuduRestoreOptions] if parsing was successful, None if not.
   */
  def parse(args: Seq[String]): Option[KuduRestoreOptions] = {
    parser.parse(args, KuduRestoreOptions(Seq(), null))
  }
}
