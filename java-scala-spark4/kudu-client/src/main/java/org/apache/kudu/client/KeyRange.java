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

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;

/*
 * Class used to represent primary key range in tablet.
 */
@InterfaceAudience.Private
class KeyRange {
  private byte[] primaryKeyStart;
  private byte[] primaryKeyEnd;
  private long dataSizeBytes;
  private LocatedTablet tablet;

  /**
   * Create a new key range [primaryKeyStart, primaryKeyEnd).
   * @param tablet the tablet which the key range belongs to, cannot be null
   * @param primaryKeyStart the encoded primary key where to start in the key range (inclusive)
   * @param primaryKeyEnd the encoded primary key where to stop in the key range (exclusive)
   * @param dataSizeBytes the estimated data size of the key range.
   */
  public KeyRange(LocatedTablet tablet,
                  byte[] primaryKeyStart,
                  byte[] primaryKeyEnd,
                  long dataSizeBytes) {
    Preconditions.checkNotNull(tablet);
    this.tablet = tablet;
    this.primaryKeyStart = primaryKeyStart;
    this.primaryKeyEnd = primaryKeyEnd;
    this.dataSizeBytes = dataSizeBytes;
  }

  /**
   * @return the start primary key
   */
  public byte[] getPrimaryKeyStart() {
    return primaryKeyStart;
  }

  /**
   * @return the end primary key
   */
  public byte[] getPrimaryKeyEnd() {
    return primaryKeyEnd;
  }

  /**
   * @return the located tablet
   */
  public LocatedTablet getTablet() {
    return tablet;
  }

  /**
   * @return the start partition key
   */
  public byte[] getPartitionKeyStart() {
    return tablet.getPartition().getPartitionKeyStart();
  }

  /**
   * @return the end partition key
   */
  public byte[] getPartitionKeyEnd() {
    return tablet.getPartition().getPartitionKeyEnd();
  }

  /**
   * @return the estimated data size of the key range
   */
  public long getDataSizeBytes() {
    return dataSizeBytes;
  }

  @Override
  public String toString() {
    return String.format("[%s, %s), %s, %s",
                         primaryKeyStart == null || primaryKeyStart.length == 0 ?
                             "<start>" : Bytes.hex(primaryKeyStart),
                         primaryKeyEnd == null || primaryKeyEnd.length == 0 ?
                             "<end>" : Bytes.hex(primaryKeyEnd),
                         String.valueOf(dataSizeBytes),
                         tablet.toString());
  }
}
