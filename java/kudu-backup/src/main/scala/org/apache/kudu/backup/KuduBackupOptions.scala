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

import org.apache.kudu.client.AsyncKuduClient
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import scopt.OptionParser

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class KuduBackupOptions(
    tables: Seq[String],
    path: String,
    kuduMasterAddresses: String = InetAddress.getLocalHost.getCanonicalHostName,
    timestampMs: Long = System.currentTimeMillis(),
    format: String = KuduBackupOptions.DefaultFormat,
    scanBatchSize: Int = KuduBackupOptions.DefaultScanBatchSize,
    scanRequestTimeoutMs: Long = KuduBackupOptions.DefaultScanRequestTimeoutMs,
    scanPrefetching: Boolean = KuduBackupOptions.DefaultScanPrefetching,
    keepAlivePeriodMs: Long = KuduBackupOptions.defaultKeepAlivePeriodMs)

object KuduBackupOptions {
  val DefaultFormat: String = "parquet"
  val DefaultScanBatchSize: Int = 1024 * 1024 * 20 // 20 MiB
  val DefaultScanRequestTimeoutMs: Long =
    AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS // 30 seconds
  val DefaultScanPrefetching
    : Boolean = false // TODO: Add a test per KUDU-1260 and enable by default?
  val defaultKeepAlivePeriodMs: Long = 15000 // 25% of the default scanner ttl.

  // TODO: clean up usage output.
  // TODO: timeout configurations.
  private val parser: OptionParser[KuduBackupOptions] =
    new OptionParser[KuduBackupOptions]("KuduBackup") {
      opt[String]("path")
        .action((v, o) => o.copy(path = v))
        .text("The root path to output backup data. Accepts any Spark compatible path.")
        .optional()

      opt[String]("kuduMasterAddresses")
        .action((v, o) => o.copy(kuduMasterAddresses = v))
        .text("Comma-separated addresses of Kudu masters.")
        .optional()

      opt[Long]("timestampMs")
        .action((v, o) => o.copy(timestampMs = v))
        // TODO: Document the limitations based on cluster configuration (ex: ancient history watermark).
        .text("A UNIX timestamp in milliseconds since the epoch to execute scans at.")
        .optional()

      opt[String]("format")
        .action((v, o) => o.copy(format = v))
        .text("The file format to use when writing the data.")
        .optional()

      opt[Int]("scanBatchSize")
        .action((v, o) => o.copy(scanBatchSize = v))
        .text("The maximum number of bytes returned by the scanner, on each batch.")
        .optional()

      opt[Int]("scanRequestTimeoutMs")
        .action((v, o) => o.copy(scanRequestTimeoutMs = v))
        .text("Sets how long in milliseconds each scan request to a server can last.")
        .optional()

      opt[Unit]("scanPrefetching")
        .action((_, o) => o.copy(scanPrefetching = true))
        .text("An experimental flag to enable pre-fetching data.")
        .optional()

      opt[Long]("keepAlivePeriodMs")
        .action((v, o) => o.copy(keepAlivePeriodMs = v))
        .text(
          "Sets the period at which to send keep-alive requests to the tablet server to ensure" +
            " that scanners do not time out")
        .optional()

      arg[String]("<table>...")
        .unbounded()
        .action((v, o) => o.copy(tables = o.tables :+ v))
        .text("A list of tables to be backed up.")
    }

  /**
   * Parses the passed arguments into Some[KuduBackupOptions].
   *
   * If the arguments are bad, an error message is displayed
   * and None is returned.
   *
   * @param args The arguments to parse.
   * @return Some[KuduBackupOptions] if parsing was successful, None if not.
   */
  def parse(args: Seq[String]): Option[KuduBackupOptions] = {
    parser.parse(args, KuduBackupOptions(Seq(), null))
  }
}
