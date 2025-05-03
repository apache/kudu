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

package org.apache.kudu.spark.kudu

import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

import org.apache.kudu.spark.kudu.KuduWriteOptions._

/**
 * KuduWriteOptions holds configuration of writes to Kudu tables.
 *
 * @param ignoreDuplicateRowErrors when inserting, ignore any new rows that
 *                                 have a primary key conflict with existing rows
 * @param ignoreNull update only non-Null columns if set true
 * @param repartition if set to true, the data will be repartitioned to match the
 *                   partitioning of the target Kudu table
 * @param repartitionSort if set to true, the data will also be sorted while being
 *                   repartitioned. This is only used if repartition is true.
 * @param handleSchemaDrift if set to true, when fields with names that are not in
 *                          the target Kudu table are encountered, the Kudu table
 *                          will be altered to include new columns for those fields.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
case class KuduWriteOptions(
    ignoreDuplicateRowErrors: Boolean = defaultIgnoreDuplicateRowErrors,
    ignoreNull: Boolean = defaultIgnoreNull,
    repartition: Boolean = defaultRepartition,
    repartitionSort: Boolean = defaultRepartitionSort,
    handleSchemaDrift: Boolean = defaultHandleSchemaDrift)

object KuduWriteOptions {
  val defaultIgnoreDuplicateRowErrors: Boolean = false
  val defaultIgnoreNull: Boolean = false
  val defaultRepartition: Boolean = false
  val defaultRepartitionSort: Boolean = true
  val defaultHandleSchemaDrift: Boolean = false
}
