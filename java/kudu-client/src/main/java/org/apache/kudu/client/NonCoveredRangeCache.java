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

import com.google.common.base.Joiner;
import com.google.common.primitives.UnsignedBytes;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.kudu.annotations.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of the non-covered range partitions in a Kudu table.
 *
 * Currently entries are never invalidated from the cache.
 */
@ThreadSafe
@InterfaceAudience.Private
class NonCoveredRangeCache {
  private static final Logger LOG = LoggerFactory.getLogger(NonCoveredRangeCache.class);
  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

  private final ConcurrentNavigableMap<byte[], byte[]> nonCoveredRanges =
      new ConcurrentSkipListMap<>(COMPARATOR);

  /**
   * Retrieves a non-covered range from the cache.
   *
   * The pair contains the inclusive start partition key and the exclusive end
   * partition key containing the provided partition key. If there is no such
   * cached range, null is returned.
   *
   * @param partitionKey the partition key to lookup in the cache
   * @return the non covered range, or null
   */
  public Map.Entry<byte[], byte[]> getNonCoveredRange(byte[] partitionKey) {
    Map.Entry<byte[], byte[]> range = nonCoveredRanges.floorEntry(partitionKey);
    if (range == null ||
        (range.getValue().length != 0 && COMPARATOR.compare(partitionKey, range.getValue()) >= 0)) {
      return null;
    } else {
      return range;
    }
  }

  /**
   * Adds a non-covered range to the cache.
   *
   * @param startPartitionKey the inclusive start partition key of the non-covered range
   * @param endPartitionKey the exclusive end partition key of the non-covered range
   */
  public void addNonCoveredRange(byte[] startPartitionKey, byte[] endPartitionKey) {
    if (startPartitionKey == null || endPartitionKey == null) {
      throw new IllegalArgumentException("Non-covered partition range keys may not be null");
    }
    // Concurrent additions of the same non-covered range key are handled by
    // serializing puts through the concurrent map.
    if (nonCoveredRanges.put(startPartitionKey, endPartitionKey) == null) {
      LOG.info("Discovered non-covered partition range [{}, {})",
               Bytes.hex(startPartitionKey), Bytes.hex(endPartitionKey));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    boolean isFirst = true;
    for (Map.Entry<byte[], byte[]> range : nonCoveredRanges.entrySet()) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append('[');
      sb.append(range.getKey().length == 0 ? "<start>" : Bytes.hex(range.getKey()));
      sb.append(", ");
      sb.append(range.getValue().length == 0 ? "<end>" : Bytes.hex(range.getValue()));
      sb.append(')');
    }
    sb.append(']');
    return sb.toString();
  }
}
