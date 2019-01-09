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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths

import com.google.protobuf.util.JsonFormat
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu.SparkUtil._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduBackup {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def run(options: KuduBackupOptions, session: SparkSession): Unit = {
    val context =
      new KuduContext(
        options.kuduMasterAddresses,
        session.sparkContext,
        // TODO: As a workaround for KUDU-1868 the socketReadTimeout is
        // matched to the scanRequestTimeout. Without this
        // "Invalid call sequence ID" errors can occur under heavy load.
        Some(options.scanRequestTimeoutMs)
      )
    val path = options.path
    log.info(s"Backing up to path: $path")

    // TODO: Make parallel so each table isn't process serially?
    options.tables.foreach { t =>
      val table = context.syncClient.openTable(t)
      val tablePath = Paths.get(path).resolve(URLEncoder.encode(t, "UTF-8"))

      val rdd = new KuduBackupRDD(table, options, context, session.sparkContext)
      val df =
        session.sqlContext.createDataFrame(rdd, sparkSchema(table.getSchema))
      // TODO: Prefix path with the time? Maybe a backup "name" parameter defaulted to something?
      // TODO: Take parameter for the SaveMode.
      val writer = df.write.mode(SaveMode.ErrorIfExists)
      // TODO: Restrict format option.
      // TODO: We need to cleanup partial output on failure otherwise.
      // retries of the entire job will fail because the file already exists.
      writer.format(options.format).save(tablePath.toString)

      val tableMetadata = TableMetadata.getTableMetadata(table, options)
      writeTableMetadata(tableMetadata, tablePath, session)
    }
  }

  private def writeTableMetadata(
      metadata: TableMetadataPB,
      path: Path,
      session: SparkSession): Unit = {
    val conf = session.sparkContext.hadoopConfiguration
    val hPath = new HPath(path.resolve(TableMetadata.MetadataFileName).toString)
    val fs = hPath.getFileSystem(conf)
    val out = fs.create(hPath, /* overwrite= */ false)
    val json = JsonFormat.printer().print(metadata)
    out.write(json.getBytes(StandardCharsets.UTF_8))
    out.flush()
    out.close()
  }

  def main(args: Array[String]): Unit = {
    val options = KuduBackupOptions
      .parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))

    val session = SparkSession
      .builder()
      .appName("Kudu Table Backup")
      .getOrCreate()

    run(options, session)
  }
}
