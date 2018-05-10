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

import java.io.InputStreamReader
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import com.google.common.io.CharStreams
import com.google.protobuf.util.JsonFormat
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.spark.kudu.{KuduContext, KuduWriteOptions}
import org.apache.spark.sql.SparkSession
import org.apache.yetus.audience.{InterfaceAudience, InterfaceStability}
import org.slf4j.{Logger, LoggerFactory}

@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduRestore {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def run(options: KuduRestoreOptions, session: SparkSession): Unit = {
    val context = new KuduContext(options.kuduMasterAddresses, session.sparkContext)
    val path = options.path
    log.info(s"Restoring from path: $path")

    // TODO: Make parallel so each table isn't processed serially.
    options.tables.foreach { t =>
      val tablePath = Paths.get(path).resolve(URLEncoder.encode(t, "UTF-8"))
      val metadataPath = getMetadataPath(t, options)
      val metadata = readTableMetadata(metadataPath, session)
      val restoreName = s"${metadata.getTableName}${options.tableSuffix}"
      val table =
        if (options.createTables) {
          // Read the metadata and generate a schema.
          val schema = TableMetadata.getKuduSchema(metadata)
          val createTableOptions = TableMetadata.getCreateTableOptions(metadata)
          context.createTable(restoreName, schema, createTableOptions)
        } else {
          context.syncClient.openTable(restoreName)
        }

      // TODO: Restrict format option.
      val df = session.sqlContext.read.format(metadata.getDataFormat).load(tablePath.toString)
      val writeOptions = new KuduWriteOptions(ignoreDuplicateRowErrors = false, ignoreNull = false)
      // TODO: Use client directly for more control?
      // (session timeout, consistency mode, flush interval, mutation buffer space)
      context.insertRows(df, restoreName, writeOptions)
    }
  }

  private def getMetadataPath(tableName: String, options: KuduRestoreOptions): Path = {
    val rootPath = if (options.metadataPath.isEmpty) options.path else options.metadataPath
    Paths.get(rootPath).resolve(tableName)
  }

  private def readTableMetadata(path: Path, session: SparkSession): TableMetadataPB = {
    val conf = session.sparkContext.hadoopConfiguration
    val hPath = new HPath(path.resolve(TableMetadata.MetadataFileName).toString)
    val fs = hPath.getFileSystem(conf)
    val in = new InputStreamReader(fs.open(hPath), StandardCharsets.UTF_8)
    val json = CharStreams.toString(in)
    in.close()
    val builder = TableMetadataPB.newBuilder()
    JsonFormat.parser().merge(json, builder)
    builder.build()
  }

  def main(args: Array[String]): Unit = {
    val options = KuduRestoreOptions.parse(args)
      .getOrElse(throw new IllegalArgumentException("could not parse the arguments"))

    val session = SparkSession.builder()
      .appName("Kudu Table Restore")
      .getOrCreate()

    run(options, session)
  }
}

