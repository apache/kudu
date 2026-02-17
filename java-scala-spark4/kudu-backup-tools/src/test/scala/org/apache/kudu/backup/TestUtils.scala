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

import org.apache.hadoop.fs.Path

import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.backup.TableMetadata.MetadataVersion

object TestUtils {

  // Create dummy table metadata and write it to the test directory.
  def createTableMetadata(io: BackupIO, tableName: String, fromMs: Long, toMs: Long): Unit = {
    // Create dummy table metadata with just enough information to be used to create a BackupGraph.
    val tableId = s"id_$tableName"
    val metadata = TableMetadataPB
      .newBuilder()
      .setVersion(MetadataVersion)
      .setFromMs(fromMs)
      .setToMs(toMs)
      .setTableName(tableName)
      .setTableId(tableId)
      .build()
    val backupPath = new Path(io.tablePath(tableId, tableName), s"$toMs")
    val metadataPath = io.backupMetadataPath(backupPath)
    io.writeTableMetadata(metadata, metadataPath)
  }
}
