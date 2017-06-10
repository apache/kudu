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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.annotations.InterfaceAudience;

/**
 * A cache of the tablet locations in a table, keyed by partition key. Entries
 * in the cache are either tablets or non-covered ranges.
 */
@ThreadSafe
@InterfaceAudience.Private
class TableLocationsCache {
  private static final Logger LOG = LoggerFactory.getLogger(TableLocationsCache.class);
  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

  @GuardedBy("rwl")
  private final NavigableMap<byte[], Entry> entries = new TreeMap<>(COMPARATOR);

  public Entry get(byte[] partitionKey) {

    if (partitionKey == null) {
      // Master lookup.
      rwl.readLock().lock();
      try {
        Preconditions.checkState(entries.size() <= 1);
        return entries.get(AsyncKuduClient.EMPTY_ARRAY);
      } finally {
        rwl.readLock().unlock();
      }

    }

    Map.Entry<byte[], Entry> entry;
    rwl.readLock().lock();
    try {
      entry = entries.floorEntry(partitionKey);
    } finally {
      rwl.readLock().unlock();
    }

    if (entry == null ||
        (entry.getValue().getUpperBoundPartitionKey().length > 0 &&
            Bytes.memcmp(partitionKey, entry.getValue().getUpperBoundPartitionKey()) >= 0) ||
        entry.getValue().isStale()) {
      return null;
    }
    return entry.getValue();
  }

  /**
   * Add tablet locations to the cache.
   *
   * Already known tablet locations will have their entry updated and deadline extended.
   *
   * @param tablets the discovered tablets to cache
   * @param requestPartitionKey the lookup partition key
   * @param requestedBatchSize the number of tablet locations requested from the master in the
   *                           original request
   * @param ttl the time in milliseconds that the tablets may be cached for
   */
  public void cacheTabletLocations(List<RemoteTablet> tablets,
                                   byte[] requestPartitionKey,
                                   int requestedBatchSize,
                                   long ttl) {
    long deadline = System.nanoTime() + ttl * TimeUnit.MILLISECONDS.toNanos(1);
    if (requestPartitionKey == null) {
      // Master lookup.
      Preconditions.checkArgument(tablets.size() == 1);
      Entry entry = Entry.tablet(tablets.get(0), TimeUnit.DAYS.toMillis(1));

      rwl.writeLock().lock();
      try {
        entries.clear();
        entries.put(AsyncKuduClient.EMPTY_ARRAY, entry);
      } finally {
        rwl.writeLock().unlock();
      }
      return;
    }

    List<Entry> newEntries = new ArrayList<>();

    if (tablets.isEmpty()) {
      // If there are no tablets in the response, then the table is empty. If
      // there were any tablets in the table they would have been returned, since
      // the master guarantees that if the partition key falls in a non-covered
      // range, the previous tablet will be returned, and we did not set an upper
      // bound partition key on the request.
      newEntries.add(Entry.nonCoveredRange(AsyncKuduClient.EMPTY_ARRAY,
                                           AsyncKuduClient.EMPTY_ARRAY,
                                           deadline));
    } else {
      // The comments below will reference the following diagram:
      //
      //   +---+   +---+---+
      //   |   |   |   |   |
      // A | B | C | D | E | F
      //   |   |   |   |   |
      //   +---+   +---+---+
      //
      // It depicts a tablet locations response from the master containing three
      // tablets: B, D and E. Three non-covered ranges are present: A, C, and F.
      // An RPC response containing B, D and E could occur if the lookup partition
      // key falls in A, B, or C, although the existence of A as an initial
      // non-covered range can only be inferred if the lookup partition key falls
      // in A.

      final byte[] firstLowerBound = tablets.get(0).getPartition().getPartitionKeyStart();

      if (Bytes.memcmp(requestPartitionKey, firstLowerBound) < 0) {
        // If the first tablet is past the requested partition key, then the
        // partition key falls in an initial non-covered range, such as A.
        newEntries.add(
            Entry.nonCoveredRange(AsyncKuduClient.EMPTY_ARRAY, firstLowerBound, deadline));
      }

      // lastUpperBound tracks the upper bound of the previously processed
      // entry, so that we can determine when we have found a non-covered range.
      byte[] lastUpperBound = firstLowerBound;

      for (RemoteTablet tablet : tablets) {
        final byte[] tabletLowerBound = tablet.getPartition().getPartitionKeyStart();
        final byte[] tabletUpperBound = tablet.getPartition().getPartitionKeyEnd();

        if (Bytes.memcmp(lastUpperBound, tabletLowerBound) < 0) {
          // There is a non-covered range between the previous tablet and this tablet.
          // This will discover C while processing the tablet location for D.
          newEntries.add(Entry.nonCoveredRange(lastUpperBound, tabletLowerBound, deadline));
        }
        lastUpperBound = tabletUpperBound;

        // Now add the tablet itself (such as B, D, or E).
        newEntries.add(Entry.tablet(tablet, deadline));
      }

      if (lastUpperBound.length > 0 &&
          tablets.size() < requestedBatchSize) {
        // There is a non-covered range between the last tablet and the end of the
        // partition key space, such as F.
        newEntries.add(
            Entry.nonCoveredRange(lastUpperBound, AsyncKuduClient.EMPTY_ARRAY, deadline));
      }
    }

    byte[] discoveredlowerBound = newEntries.get(0).getLowerBoundPartitionKey();
    byte[] discoveredUpperBound = newEntries.get(newEntries.size() - 1)
                                            .getUpperBoundPartitionKey();

    LOG.debug("Discovered table locations:\t{}", newEntries);

    rwl.writeLock().lock();
    try {
      // Remove all existing overlapping entries, and add the new entries.
      Map.Entry<byte[], Entry> floorEntry = entries.floorEntry(discoveredlowerBound);
      if (floorEntry != null &&
          Bytes.memcmp(requestPartitionKey,
                       floorEntry.getValue().getUpperBoundPartitionKey()) < 0) {
        discoveredlowerBound = floorEntry.getKey();
      }

      NavigableMap<byte[], Entry> overlappingEntries = entries.tailMap(discoveredlowerBound, true);
      if (discoveredUpperBound.length > 0) {
        overlappingEntries = overlappingEntries.headMap(discoveredUpperBound, false);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Existing table locations:\t\t{}", entries.values());
        LOG.trace("Removing table locations:\t\t{}", overlappingEntries.values());
      }
      overlappingEntries.clear();

      for (Entry entry : newEntries) {
        entries.put(entry.getLowerBoundPartitionKey(), entry);
      }
    } finally {
      rwl.writeLock().unlock();
    }
  }

  /**
   * Clears all non-covered range entries from the cache.
   */
  public void clearNonCoveredRangeEntries() {
    rwl.writeLock().lock();
    try {
      Iterator<Map.Entry<byte[], Entry>> it = entries.entrySet().iterator();
      while (it.hasNext()) {
        if (it.next().getValue().isNonCoveredRange()) {
          it.remove();
        }
      }
    } finally {
      rwl.writeLock().unlock();
    }
  }

  @Override
  public String toString() {
    return entries.values().toString();
  }

  /**
   * An entry in the meta cache. Represents either a non-covered range, or a tablet.
   */
  public static class Entry {
    /** The remote tablet, only set if this entry represents a tablet. */
    private final RemoteTablet tablet;
    /** The lower bound partition key, only set if this is a non-covered range. */
    private final byte[] lowerBoundPartitionKey;
    /** The upper bound partition key, only set if this is a non-covered range. */
    private final byte[] upperBoundPartitionKey;
    /** Deadline in ns relative the the System nanotime clock. */
    private final long deadline;

    private Entry(RemoteTablet tablet,
                  byte[] lowerBoundPartitionKey,
                  byte[] upperBoundPartitionKey,
                  long deadline) {
      this.tablet = tablet;
      this.lowerBoundPartitionKey = lowerBoundPartitionKey;
      this.upperBoundPartitionKey = upperBoundPartitionKey;
      this.deadline = deadline;
    }

    public static Entry nonCoveredRange(byte[] lowerBoundPartitionKey,
                                        byte[] upperBoundPartitionKey,
                                        long deadline) {
      return new Entry(null, lowerBoundPartitionKey, upperBoundPartitionKey, deadline);
    }

    public static Entry tablet(RemoteTablet tablet, long deadline) {
      return new Entry(tablet, null, null, deadline);
    }

    /**
     * @return {@code true} if this entry is a non-covered range.
     */
    public boolean isNonCoveredRange() {
      return tablet == null;
    }

    /**
     * @return the {@link RemoteTablet} for this tablet, or null
     * if this is a non-covered range.
     */
    public RemoteTablet getTablet() {
      return tablet;
    }

    public byte[] getLowerBoundPartitionKey() {
      return tablet == null ? lowerBoundPartitionKey : tablet.getPartition().getPartitionKeyStart();
    }

    public byte[] getUpperBoundPartitionKey() {
      return tablet == null ? upperBoundPartitionKey : tablet.getPartition().getPartitionKeyEnd();
    }

    private long ttl() {
      return TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
    }

    public boolean isStale() {
      return ttl() <= 0;
    }

    @Override
    public String toString() {
      if (isNonCoveredRange()) {
        return MoreObjects.toStringHelper("NonCoveredRange")
                          .add("lowerBoundPartitionKey", Bytes.hex(lowerBoundPartitionKey))
                          .add("upperBoundPartitionKey", Bytes.hex(upperBoundPartitionKey))
                          .add("ttl", ttl())
                          .toString();
      } else {
        return MoreObjects.toStringHelper("Tablet")
                          .add("lowerBoundPartitionKey", Bytes.hex(getLowerBoundPartitionKey()))
                          .add("upperBoundPartitionKey", Bytes.hex(getUpperBoundPartitionKey()))
                          .add("tablet-id", tablet.getTabletId())
                          .add("ttl", ttl())
                          .toString();
      }
    }
  }
}
