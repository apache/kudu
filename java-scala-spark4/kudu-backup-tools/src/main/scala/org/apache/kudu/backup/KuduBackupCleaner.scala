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

import java.time.Instant

import com.google.common.base.Preconditions
import org.apache.hadoop.conf.Configuration
import org.apache.kudu.backup.Backup.TableMetadataPB

object KuduBackupCleaner {

  private def backupToShortString(metadata: TableMetadataPB): String = {
    s"name: ${metadata.getTableName}, id: ${metadata.getTableId}, fromMs: ${metadata.getFromMs}, " +
      s"toMs: ${metadata.getToMs}"
  }

  // Run the cleanup tool with the given options. Like a command, returns 0 if successful, or
  // a nonzero error code.
  def run(options: BackupCLIOptions): Int = {
    Preconditions.checkArgument(options.mode == Mode.CLEAN)

    // Delete the metadata for all backups that satisfy the following three conditions:
    // 1. The table name matches the provided names (does not apply if no names were specified).
    // 2. The backup is part of a path whose latest backup is older than the expiration age.
    // 3. The backup is not on the current restore path.
    // TODO(KUDU-2827): Consider dropped tables eligible for deletion once they reach a certain age.
    val io: BackupIO = new BackupIO(new Configuration(), options.rootPath)
    val backupGraphs =
      if (options.tables.isEmpty)
        io.readAllBackupGraphs()
      else
        io.readBackupGraphsByTableName(options.tables)
    val now = Instant.now()

    val tableNameSet = options.tables.toSet
    backupGraphs.foreach { graph =>
      val expiredPaths = graph.backupPaths.filter(path => {
        val lastBackupInstant = Instant.ofEpochSecond(path.lastBackup.metadata.getToMs / 1000)
        now.isAfter(lastBackupInstant.plus(options.expirationAge))
      })

      // The graph might be for a table that was once named a name in 'options.tables', but we only
      // want to clean up tables whose current name is in 'options.tables'.
      // TODO: This is temporary. It will change when pattern support is added.
      val currentTableName = graph.restorePath.tableName
      if (tableNameSet.isEmpty || tableNameSet.contains(currentTableName)) {
        // For each expired path, iterate over it from latest backup to earliest backup and delete
        // the backup, unless the backup-to-be-deleted is also part of the restore path. Deleting
        // from last to first in the path ensures that if the tool crashes partway through then a
        // prefix of the backup path is preserved and the tool can delete the rest of the eligible
        // backups next time it runs.
        val restoreSet = graph.restorePath.backups.toSet
        expiredPaths.foreach(path => {
          path.backups
            .filterNot(restoreSet.contains)
            .reverseMap(backup => {
              if (options.dryRun) {
                println(s"DRY RUN: Delete backup ${backupToShortString(backup.metadata)}")
              } else {
                if (options.verbose) {
                  println(s"Delete backup ${backupToShortString(backup.metadata)}")
                }
                // TODO(wdberkeley): Make this crash-consistent by handling backup directories
                //  with no metadata.
                io.deleteBackup(backup.metadata)
              }
            })
        })
      }
    }

    0
  }
}
