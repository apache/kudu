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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A KuduPartitioner allows clients to determine the target partition of a
 * row without actually performing a write. The set of partitions is eagerly
 * fetched when the KuduPartitioner is constructed so that the actual partitioning
 * step can be performed synchronously without any network trips.
 *
 * NOTE: Because this operates on a metadata snapshot retrieved at construction
 * time, it will not reflect any metadata changes to the table that have occurred
 * since its creation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduPartitioner {
  private static final BytesKey EMPTY = new BytesKey(new byte[0]);
  private static final int NON_COVERED_RANGE_INDEX = -1;

  private final PartitionSchema partitionSchema;
  private final Map<String, Partition> tabletIdToPartition;
  private final NavigableMap<BytesKey, Integer> partitionByStartKey;
  private final int numPartitions;

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public KuduPartitioner(PartitionSchema partitionSchema,
                        Map<String, Partition> tabletIdToPartition) {
    // TODO(ghenke): Could also build a map of partition index to tablet ID which would
    //  be useful for identifying which tablet a given row would come from.
    NavigableMap<BytesKey, Integer> partitionByStartKey = new TreeMap<>();
    // Insert a sentinel for the beginning of the table, in case a user
    // queries for any row which falls before the first partition.
    partitionByStartKey.put(EMPTY, NON_COVERED_RANGE_INDEX);
    int index = 0;
    for (Map.Entry<String, Partition> entry : tabletIdToPartition.entrySet()) {
      BytesKey keyStart = new BytesKey(entry.getValue().partitionKeyStart);
      BytesKey keyEnd = new BytesKey(entry.getValue().partitionKeyEnd);
      partitionByStartKey.put(keyStart, index++);
      // Set the start of the next non-covered range to have the NON_COVERED_RANGE_INDEX.
      // As we process partitions, if a partition covers this range, the keyStart will be
      // equal to this keyEnd and the NON_COVERED_RANGE_INDEX will be replaced with the index
      // of that partition.
      partitionByStartKey.putIfAbsent(keyEnd, NON_COVERED_RANGE_INDEX);
    }
    this.partitionSchema = partitionSchema;
    this.tabletIdToPartition = tabletIdToPartition;
    this.partitionByStartKey = partitionByStartKey;
    this.numPartitions = tabletIdToPartition.size();
  }

  /**
   * @return the number of partitions known by this partitioner.
   */
  public int numPartitions() {
    return this.numPartitions;
  }

  /**
   * Determine if the given row falls into a valid partition.
   *
   * NOTE: The row must be constructed with a schema returned from the Kudu server.
   * ex: `KuduTable.getSchema().newPartialRow();`
   *
   * @param row The row to check.
   * @return true if the row falls into a valid partition.
   */
  public boolean isCovered(PartialRow row) {
    BytesKey partitionKey = new BytesKey(encodePartitionKey(row));
    // The greatest key that is less than or equal to the given key.
    Map.Entry<BytesKey, Integer> floor = partitionByStartKey.floorEntry(partitionKey);
    return floor.getValue() != NON_COVERED_RANGE_INDEX;
  }

  /**
   * Determine the partition index that the given row falls into.
   *
   * NOTE: The row must be constructed with a schema returned from the Kudu server.
   * ex: `KuduTable.getSchema().newPartialRow();`
   *
   * @param row The row to be partitioned.
   * @return The resulting partition index.
   *         The result will be less than numPartitions()
   * @throws NonCoveredRangeException if the row falls into a non-covered range.
   */
  public int partitionRow(PartialRow row) throws NonCoveredRangeException {
    BytesKey partitionKey = new BytesKey(encodePartitionKey(row));
    // The greatest key that is less than or equal to the given key.
    Map.Entry<BytesKey, Integer> floor = partitionByStartKey.floorEntry(partitionKey);
    if (floor.getValue() == NON_COVERED_RANGE_INDEX) {
      Map.Entry<BytesKey, Integer> ceiling = partitionByStartKey.ceilingEntry(partitionKey);
      throw new NonCoveredRangeException(floor.getKey().bytes, ceiling.getKey().bytes);
    }
    return floor.getValue();
  }

  private byte[] encodePartitionKey(PartialRow row) {
    // Column IDs are required to encode the partition key.
    Preconditions.checkArgument(row.getSchema().hasColumnIds(),
        "The row must be constructed with a schema returned from the server. " +
            "(ex: KuduTable.getSchema().newPartialRow();");
    return partitionSchema.encodePartitionKey(row);
  }

  /**
   * @return the internal map of tablet ID to Partition.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public Map<String, Partition> getTabletMap() {
    return tabletIdToPartition;
  }

  /**
   * A wrapper around a byte array that implements the Comparable interface
   * allowing it to be used as the key in map.
   */
  private static class BytesKey implements Comparable<BytesKey> {

    private final byte[] bytes;

    BytesKey(byte[] bytes) {
      this.bytes = bytes;
    }

    public boolean isEmpty() {
      return bytes.length == 0;
    }

    @Override
    public int compareTo(BytesKey other) {
      return Bytes.memcmp(this.bytes, other.bytes);
    }

    @Override
    public String toString() {
      return Bytes.hex(bytes);
    }
  }

  /**
   * A Builder class to build {@link KuduPartitioner}.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class KuduPartitionerBuilder {

    private final KuduTable table;
    private long timeoutMillis;

    public KuduPartitionerBuilder(KuduTable table) {
      this.table = table;
      this.timeoutMillis = table.getAsyncClient().getDefaultAdminOperationTimeoutMs();
    }

    /**
     * Set the timeout used for building the {@link KuduPartitioner}.
     * Defaults to the {@link AsyncKuduClient#getDefaultAdminOperationTimeoutMs()}.
     * @param timeoutMillis the timeout to set in milliseconds.
     */
    public KuduPartitionerBuilder buildTimeout(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    /**
     * Builds a {@link KuduPartitioner} using the passed configurations.
     * @return a new {@link KuduPartitioner}
     */
    public KuduPartitioner build() throws KuduException {
      final TimeoutTracker timeoutTracker = new TimeoutTracker();
      timeoutTracker.setTimeout(timeoutMillis);
      // Use a LinkedHashMap to maintain partition order.
      // This isn't strictly required, but it means that partitions with lower ranges
      // will have lower partition index since this map is processed in a for
      // loop when constructing the KuduPartitioner.
      LinkedHashMap<String, Partition> tabletIdToPartition = new LinkedHashMap<>();
      byte[] nextPartKey = EMPTY.bytes;
      while (true) {
        LocatedTablet tablet;
        try {
          tablet = KuduClient.joinAndHandleException(
              table.getAsyncClient().getTabletLocation(table,
                  nextPartKey, AsyncKuduClient.LookupType.LOWER_BOUND,
                  timeoutTracker.getMillisBeforeTimeout()));
        } catch (NonCoveredRangeException ncr) {
          // No more tablets
          break;
        }
        String tabletId = new String(tablet.getTabletId(), UTF_8);
        tabletIdToPartition.put(tabletId, tablet.getPartition());
        byte[] keyEnd = tablet.getPartition().partitionKeyEnd;
        if (keyEnd.length == 0) break;
        nextPartKey = keyEnd;
      }
      return new KuduPartitioner(table.getPartitionSchema(), tabletIdToPartition);
    }
  }
}
