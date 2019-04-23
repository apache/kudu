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
import java.nio.file.Path
import java.nio.file.Paths

import com.google.common.io.CharStreams
import com.google.protobuf.util.JsonFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.kudu.Schema
import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.backup.SessionIO._
import org.apache.kudu.spark.kudu.SparkUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A class to encapsulate and centralize the logic for data layout and IO
 * of metadata and data of the backup and restore jobs.
 *
 * The default backup directory structure is:
 * /<rootPath>/<tableName>/<backup-id>/
 *   .kudu-metadata.json
 *   part-*.parquet
 *
 * In the above path the `/<rootPath>` can be used to distinguish separate backup groups.
 * The `<backup-id>` is currently the `toMs` time for the job.
 *
 * TODO: Should the tableName contain the table id?
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class SessionIO(val session: SparkSession, options: CommonOptions) {
  val log: Logger = LoggerFactory.getLogger(getClass)

  val conf: Configuration = session.sparkContext.hadoopConfiguration
  val rootHPath: HPath = new HPath(options.rootPath)
  val fs: FileSystem = rootHPath.getFileSystem(conf)

  /**
   * Returns the Spark schema for backup data based on the Kudu Schema.
   * Additionally handles adding the RowAction column for incremental backup/restore.
   * @return the Spark schema for backup data.
   */
  def dataSchema(schema: Schema, includeRowAction: Boolean = true): StructType = {
    var fields = SparkUtil.sparkSchema(schema).fields
    if (includeRowAction) {
      val changeTypeField = generateRowActionColumn(schema)
      fields = fields ++ Seq(changeTypeField)
    }
    StructType(fields)
  }

  /**
   * Generates a RowAction column and handles column name collisions.
   * The column name can vary because it's accessed positionally.
   */
  private def generateRowActionColumn(schema: Schema): StructField = {
    var columnName = "backup_row_action"
    // If the column already exists and we need to pick an alternate column name.
    while (schema.hasColumn(columnName)) {
      columnName += "_"
    }
    StructField(columnName, ByteType)
  }

  /**
   * @return the path to the table directory.
   */
  def tablePath(tableName: String): Path = {
    Paths.get(options.rootPath).resolve(URLEncoder.encode(tableName, "UTF-8"))
  }

  /**
   * @return the backup path for a table and time.
   */
  def backupPath(tableName: String, timestampMs: Long): Path = {
    tablePath(tableName).resolve(timestampMs.toString)
  }

  /**
   * @return the path to the metadata file within a backup path.
   */
  def backupMetadataPath(backupPath: Path): Path = {
    backupPath.resolve(MetadataFileName)
  }

  /**
   * Serializes the table metadata to Json and writes it to the metadata path.
   * @param tableMetadata the metadata to serialize.
   * @param metadataPath the path to write the metadata file too.
   */
  def writeTableMetadata(tableMetadata: TableMetadataPB, metadataPath: Path): Unit = {
    log.error(s"Writing metadata to $metadataPath")
    val hPath = new HPath(metadataPath.toString)
    val out = fs.create(hPath, /* overwrite= */ false)
    val json = JsonFormat.printer().print(tableMetadata)
    out.write(json.getBytes(StandardCharsets.UTF_8))
    out.flush()
    out.close()
  }

  /**
   * Reads an entire backup graph by reading all of the metadata files for the
   * given table. See [[BackupGraph]] for more details.
   * @param tableName the table to read a backup graph for.
   * @return the full backup graph.
   */
  def readBackupGraph(tableName: String): BackupGraph = {
    val backups = readTableBackups(tableName)
    val graph = new BackupGraph()
    backups.foreach {
      case (path, metadata) =>
        graph.addBackup(BackupNode(path, metadata))
    }
    graph
  }

  /**
   * Reads and returns all of the metadata for a given table.
   * @param tableName the table to read the metadata for.
   * @return a sequence of all the paths and metadata.
   */
  // TODO: Also use table-id to find backups.
  private def readTableBackups(tableName: String): Seq[(Path, TableMetadataPB)] = {
    val hPath = new HPath(tablePath(tableName).toString)
    val results = new mutable.ListBuffer[(Path, TableMetadataPB)]()
    if (fs.exists(hPath)) {
      val iter = fs.listLocatedStatus(hPath)
      while (iter.hasNext) {
        val file = iter.next()
        if (file.isDirectory) {
          val metadataHPath = new HPath(file.getPath, MetadataFileName)
          if (fs.exists(metadataHPath)) {
            val metadata = readTableMetadata(metadataHPath)
            results += ((Paths.get(file.getPath.toString), metadata))
          }
        }
      }
    }
    log.error(s"Found ${results.size} paths in ${hPath.toString}")
    results.toList
  }

  /**
   * Reads and deserializes the metadata file at the given path.
   * @param metadataPath the path to the metadata file.
   * @return the deserialized table metadata.
   */
  private def readTableMetadata(metadataPath: HPath): TableMetadataPB = {
    val in = new InputStreamReader(fs.open(metadataPath), StandardCharsets.UTF_8)
    val json = CharStreams.toString(in)
    in.close()
    val builder = TableMetadataPB.newBuilder()
    JsonFormat.parser().merge(json, builder)
    builder.build()
  }
}

object SessionIO {
  // The name of the metadata file within a backup directory.
  val MetadataFileName = ".kudu-metadata.json"
}
