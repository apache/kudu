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
import org.apache.kudu.client.AsyncKuduClient
import org.apache.kudu.client.ReplicaSelection
import org.apache.kudu.spark.kudu.KuduReadOptions._

/**
 * KuduReadOptions holds configuration of reads to Kudu tables.
 *
 * @param batchSize Sets the maximum number of bytes returned by the scanner, on each batch.
 * @param scanLocality If true scan locality is enabled, so that the scan will
 *                     take place at the closest replica
 * @param faultTolerantScanner scanner type to be used. Fault tolerant if true,
 *                             otherwise, use non fault tolerant one
 * @param keepAlivePeriodMs The period at which to send keep-alive requests to the tablet
 *                          server to ensure that scanners do not time out
 * @param scanRequestTimeoutMs Maximum time allowed per scan request, in milliseconds
 * @param socketReadTimeoutMs This parameter is deprecated and has no effect
 * @param splitSizeBytes Sets the target number of bytes per spark task. If set, tablet's
 *                       primary key range will be split to generate uniform task sizes instead of
 *                       the default of 1 task per tablet.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
case class KuduReadOptions(
    batchSize: Int = defaultBatchSize,
    scanLocality: ReplicaSelection = defaultScanLocality,
    faultTolerantScanner: Boolean = defaultFaultTolerantScanner,
    keepAlivePeriodMs: Long = defaultKeepAlivePeriodMs,
    scanRequestTimeoutMs: Option[Long] = None,
    socketReadTimeoutMs: Option[Long] = None,
    splitSizeBytes: Option[Long] = None)

object KuduReadOptions {
  val defaultBatchSize: Int = 1024 * 1024 * 20 // TODO: Understand/doc this setting?
  val defaultScanLocality: ReplicaSelection = ReplicaSelection.CLOSEST_REPLICA
  val defaultFaultTolerantScanner: Boolean = false
  val defaultKeepAlivePeriodMs: Long = AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS
}
