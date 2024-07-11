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

package org.apache.kudu.client;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class GetTableStatisticsResponse extends KuduRpcResponse {

  private final long onDiskSize;
  private final long liveRowCount;


  /**
   * @param elapsedMillis Time in milliseconds since RPC creation to now
   * @param tsUUID the UUID of the tablet server that sent the response
   * @param onDiskSize the table's on disk size
   * @param liveRowCount the table's live row count
   */
  GetTableStatisticsResponse(long elapsedMillis,
                             String tsUUID,
                             long onDiskSize,
                             long liveRowCount) {
    super(elapsedMillis, tsUUID);
    this.onDiskSize = onDiskSize;
    this.liveRowCount = liveRowCount;
  }

  /**
   * Get the table's on disk size, this statistic is pre-replication.
   * @return Table's on disk size
   */
  public long getOnDiskSize() {
    return onDiskSize;
  }

  /**
   * Get the table's live row count, this statistic is pre-replication.
   * @return Table's live row count
   */
  public long getLiveRowCount() {
    return liveRowCount;
  }
}
