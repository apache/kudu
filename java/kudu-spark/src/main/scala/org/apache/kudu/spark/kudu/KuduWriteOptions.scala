/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kudu.spark.kudu

import org.apache.yetus.audience.InterfaceStability

/**
 * KuduWriteOptions holds configuration of writes to Kudu tables.
 *
 * The instance of this class is passed to KuduContext write functions,
 * such as insertRows, deleteRows, upsertRows, and updateRows.
 *
 * @param ignoreDuplicateRowErrors when inserting, ignore any new rows that
 *                                 have a primary key conflict with existing rows
 * @param ignoreNull update only non-Null columns if set true
 */
@InterfaceStability.Unstable
class KuduWriteOptions(
    var ignoreDuplicateRowErrors: Boolean = false,
    var ignoreNull: Boolean = false)
    extends Serializable
