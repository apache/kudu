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

import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.SparkUtil
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object BackupUtils {

  /**
   * Returns the Spark schema for backup data based on the Kudu Schema.
   * Additionally handles adding the RowAction column for incremental backup/restore.
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

}
