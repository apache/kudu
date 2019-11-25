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

import com.google.common.io.CharStreams
import com.google.protobuf.util.JsonFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.backup.BackupIO._
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * A class to encapsulate and centralize the logic for data layout and IO
 * of metadata and data of the backup and restore jobs.
 *
 * The default backup directory structure is:
 * /<rootPath>/<tableId>-<tableName>/<backup-id>/
 *   .kudu-metadata.json
 *   part-*.parquet
 *
 * - rootPath: can be used to distinguish separate backup groups, jobs, or concerns.
 * - tableId: the unique internal ID of the table being backed up.
 * - tableName: the name of the table being backed up.
 * - backup-id: A way to uniquely identify/group the data for a single backup run.
 *   - Currently the `toMs` time for the job.
 * - .kudu-metadata.json: Contains all of the metadata to support recreating the table,
 *   linking backups by time, and handling data format changes.
 *   - Written last so that failed backups will not have a metadata file and will not be
 *     considered at restore time or backup linking time.
 * - part-*.parquet: The data files containing the tables data.
 *   - Incremental backups contain an additional “RowAction” byte column at the end.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class BackupIO(val conf: Configuration, rootPathStr: String) {
  val log: Logger = LoggerFactory.getLogger(getClass)

  val rootPath: Path = new Path(rootPathStr)
  val fs: FileSystem = rootPath.getFileSystem(conf)

  /**
   * Return the path to the table directory.
   */
  def tablePath(tableId: String, tableName: String): Path = {
    val encodedTableName = URLEncoder.encode(tableName, "UTF-8")
    val dirName = s"$tableId-$encodedTableName"
    new Path(rootPath, dirName)
  }

  /**
   * Return the backup path for a table and time.
   */
  def backupPath(tableId: String, tableName: String, timestampMs: Long): Path = {
    new Path(tablePath(tableId, tableName), timestampMs.toString)
  }

  /**
   * Return the path to the metadata file within a backup path.
   */
  def backupMetadataPath(backupPath: Path): Path = {
    new Path(backupPath, MetadataFileName)
  }

  /**
   * Serializes the table metadata to Json and writes it to the metadata path.
   */
  def writeTableMetadata(tableMetadata: TableMetadataPB, metadataPath: Path): Unit = {
    log.info(s"Writing metadata to $metadataPath")
    val out = fs.create(metadataPath, /* overwrite= */ false)
    val json = JsonFormat.printer().print(tableMetadata)
    out.write(json.getBytes(StandardCharsets.UTF_8))
    out.flush()
    out.close()
  }

  /**
   * Deletes the backup.
   */
  def deleteBackup(metadata: TableMetadataPB): Unit = {
    fs.delete(backupPath(metadata.getTableId, metadata.getTableName, metadata.getToMs), true)
  }

  /**
   * Reads all of the backup graphs.
   */
  def readAllBackupGraphs(): Seq[BackupGraph] = {
    buildBackupGraphs(listAllTableDirs(), System.currentTimeMillis())
  }

  /**
   * Reads all of the backup graphs for a given list of table names and a time filter.
   */
  def readBackupGraphsByTableName(
      tableNames: Seq[String],
      timeMs: Long = System.currentTimeMillis()): Seq[BackupGraph] = {
    // We also need to include the metadata from old table names.
    // To handle this we list all directories, get the IDs for the tableNames,
    // and then filter the directories by those IDs.
    val allDirs = listAllTableDirs()
    val encodedNames = tableNames.map(URLEncoder.encode(_, "UTF-8")).toSet
    val tableIds =
      allDirs.flatMap { dir =>
        val dirName = dir.getName
        val tableName = tableNameFromDirName(dirName)
        if (encodedNames.contains(tableName)) {
          Some(tableIdFromDirName(dirName))
        } else {
          None
        }
      }.toSet
    val dirs = allDirs.filter(dir => tableIds.contains(tableIdFromDirName(dir.getName)))
    buildBackupGraphs(dirs, timeMs)
  }

  /**
   * Reads all of the backup graphs for a given list of table IDs and a time filter.
   */
  def readBackupGraphsByTableId(
      tableIds: Seq[String],
      timeMs: Long = System.currentTimeMillis()): Seq[BackupGraph] = {
    val dirs = listTableIdDirs(tableIds)
    buildBackupGraphs(dirs, timeMs)
  }

  /**
   * Builds all of the backup graphs for a given list of directories by reading all of the
   * metadata files and inserting them into a backup graph for each table id.
   * See [[BackupGraph]] for more details.
   */
  private def buildBackupGraphs(dirs: Seq[Path], timeMs: Long): Seq[BackupGraph] = {
    // Read all the metadata and filter by timesMs.
    val metadata = dirs.flatMap(readTableBackups).filter(_._2.getToMs <= timeMs)
    // Group the metadata by the table ID and create a BackupGraph for each table ID.
    metadata
      .groupBy(_._2.getTableId)
      .map {
        case (tableId, pm) =>
          val graph = new BackupGraph(tableId)
          pm.foreach {
            case (path, metadata) =>
              graph.addBackup(BackupNode(path, metadata))
          }
          graph
      }
      .toList
  }

  /**
   * Return all of the table directories.
   */
  private def listAllTableDirs(): Seq[Path] = {
    listMatching(_ => true)
  }

  /**
   * Return the table directories for a given list of table IDs.
   */
  private def listTableIdDirs(tableIds: Seq[String]): Seq[Path] = {
    val idSet = tableIds.toSet
    listMatching { file =>
      val name = file.getPath.getName
      file.isDirectory && idSet.contains(tableIdFromDirName(name))
    }
  }

  private def tableIdFromDirName(dirName: String): String = {
    // Split to the left of "-" and keep the first half to get the table ID.
    dirName.splitAt(dirName.indexOf("-"))._1
  }

  private def tableNameFromDirName(dirName: String): String = {
    // Split to the right of "-" and keep the second half to get the table name.
    dirName.splitAt(dirName.indexOf("-") + 1)._2
  }

  /**
   * List all the files in the root directory and return the files that match
   * according to the passed function.
   */
  private def listMatching(fn: LocatedFileStatus => Boolean): Seq[Path] = {
    val results = new mutable.ListBuffer[Path]()
    if (fs.exists(rootPath)) {
      val iter = fs.listLocatedStatus(rootPath)
      while (iter.hasNext) {
        val file = iter.next()
        if (fn(file)) {
          results += file.getPath
        }
      }
    }
    results
  }

  /**
   * Reads and returns all of the metadata for a given table directory.
   */
  private def readTableBackups(tableDir: Path): Seq[(Path, TableMetadataPB)] = {
    val results = new mutable.ListBuffer[(Path, TableMetadataPB)]()
    val files = fs.listStatus(tableDir)
    files.foreach { file =>
      if (file.isDirectory) {
        val metadataPath = new Path(file.getPath, MetadataFileName)
        if (fs.exists(metadataPath)) {
          val metadata = readTableMetadata(metadataPath)
          results += ((file.getPath, metadata))
        }
      }
    }
    log.info(s"Found ${results.size} paths in ${tableDir.toString}")
    results.toList
  }

  /**
   * Reads and deserializes the metadata file at the given path.
   */
  def readTableMetadata(metadataPath: Path): TableMetadataPB = {
    val in = new InputStreamReader(fs.open(metadataPath), StandardCharsets.UTF_8)
    val json = CharStreams.toString(in)
    in.close()
    val builder = TableMetadataPB.newBuilder()
    JsonFormat.parser().merge(json, builder)
    builder.build()
  }
}

object BackupIO {
  // The name of the metadata file within a backup directory.
  val MetadataFileName = ".kudu-metadata.json"
}
