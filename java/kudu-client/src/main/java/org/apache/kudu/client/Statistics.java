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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

import com.google.common.collect.Sets;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A Statistics belongs to a specific AsyncKuduClient. It stores client-level
 * statistics including number of operations, number of bytes written, number of
 * rpcs. It is created along with the client's creation, and can be obtained through
 * AsyncKuduClient or KuduClient's getStatistics method. Once obtained, an instance
 * of this class can be used directly.
 * <p>
 * This class is thread-safe. The user can use it anywhere to get statistics of this
 * client.
 * <p>
 * The method {@link #toString} can be useful to get a dump of all the metrics aggregated
 * for all the tablets.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Statistics {
  private final ConcurrentHashMap<String, Statistics.TabletStatistics> stsMap =
      new ConcurrentHashMap<>();

  /**
   * The statistic enum to pass when querying.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum Statistic {
    /**
     * How many bytes have been written by this client. If one rpc fails, this
     * statistic won't be updated.
     */
    BYTES_WRITTEN(0),
    /**
     * How many operations have been sent to server and succeeded.
     */
    WRITE_OPS(1),
    /**
     * How many rpcs have been sent to server and succeeded. One rpc may contain
     * multiple operations.
     */
    WRITE_RPCS(2),
    /**
     * How many operations have been sent to server but failed.
     */
    OPS_ERRORS(3),
    /**
     * How many rpcs have been sent to server but failed.
     */
    RPC_ERRORS(4);

    Statistic(int idx) {
      this.idx = idx;
    }

    /**
     * Get index of this statistic.
     * @return index
     */
    int getIndex() {
      return this.idx;
    }

    private final int idx;
  }

  /**
   * Get the statistic count of this tablet.
   * If the specified tablet doesn't have statistics, 0 will be returned.
   * @param tabletId the tablet's id
   * @param statistic the statistic type to get
   * @return the value of the statistic
   */
  public long getTabletStatistic(String tabletId, Statistic statistic) {
    TabletStatistics tabletStatistics = stsMap.get(tabletId);
    if (tabletStatistics == null) {
      return 0;
    } else {
      return tabletStatistics.getStatistic(statistic);
    }
  }

  /**
   * Get the statistic count of this table.
   * @param tableName the table's name
   * @param statistic the statistic type to get
   * @return the value of the statistic
   */
  public long getTableStatistic(String tableName, Statistic statistic) {
    long stsResult = 0;
    for (TabletStatistics tabletStatistics : stsMap.values()) {
      if (!tabletStatistics.tableName.equals(tableName)) {
        continue;
      }
      stsResult += tabletStatistics.getStatistic(statistic);
    }
    return stsResult;
  }

  /**
   * Get the statistic count of the whole client.
   * @param statistic the statistic type to get
   * @return the value of the statistic
   */
  public long getClientStatistic(Statistic statistic) {
    long stsResult = 0;
    for (TabletStatistics tabletStatistics : stsMap.values()) {
      stsResult += tabletStatistics.getStatistic(statistic);
    }
    return stsResult;
  }

  /**
   * Get the set of tablets which have been written into by this client,
   * which have statistics information.
   * @return set of tablet ids
   */
  public Set<String> getTabletSet() {
    Set<String> tablets = Sets.newHashSet();
    // This cast forces the compiler to invoke Map.keySet() rather than
    // ConcurrentHashMap's override, which is critical because when this code
    // is built with JDK8, ConcurrentHashMap.keySet() introduces a dependency
    // on a Java 8 only API.
    //
    // Note: an alternative would be to always access stsMap as a Map, but that
    // just moves the problem to the putIfAbsent() call in getTabletStatistics(),
    // which is only a Map method in Java 8.
    //
    // See KUDU-2188 for details.
    for (String tablet : ((Map<String, Statistics.TabletStatistics>) stsMap).keySet()) {
      tablets.add(tablet);
    }
    return tablets;
  }

  /**
   * Get the set of tables which have been written into by this client,
   * which have statistics information.
   * @return set of table names
   */
  public Set<String> getTableSet() {
    Set<String> tables = Sets.newHashSet();
    for (TabletStatistics tabletStat : stsMap.values()) {
      tables.add(tabletStat.tableName);
    }
    return tables;
  }

  /**
   * Get table name of the given tablet id.
   * If the tablet has no statistics, null will be returned.
   * @param tabletId the tablet's id
   * @return table name
   */
  public String getTableName(String tabletId) {
    TabletStatistics tabletStatistics = stsMap.get(tabletId);
    if (tabletStatistics == null) {
      return null;
    } else {
      return tabletStatistics.tableName;
    }
  }

  /**
   * Get the TabletStatistics object for this specified tablet.
   * @param tableName the table's name
   * @param tabletId the tablet's id
   * @return a TabletStatistics object
   */
  Statistics.TabletStatistics getTabletStatistics(String tableName, String tabletId) {
    Statistics.TabletStatistics tabletStats = stsMap.get(tabletId);
    if (tabletStats == null) {
      Statistics.TabletStatistics newTabletStats = new Statistics.TabletStatistics(tableName,
          tabletId);
      tabletStats = stsMap.putIfAbsent(tabletId, newTabletStats);
      if (tabletStats == null) {
        tabletStats = newTabletStats;
      }
    }
    return tabletStats;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Current client statistics: ");
    buf.append("bytes written:");
    buf.append(getClientStatistic(Statistic.BYTES_WRITTEN));
    buf.append(", write rpcs:");
    buf.append(getClientStatistic(Statistic.WRITE_RPCS));
    buf.append(", rpc errors:");
    buf.append(getClientStatistic(Statistic.RPC_ERRORS));
    buf.append(", write operations:");
    buf.append(getClientStatistic(Statistic.WRITE_OPS));
    buf.append(", operation errors:");
    buf.append(getClientStatistic(Statistic.OPS_ERRORS));
    return buf.toString();
  }

  static class TabletStatistics {
    private final AtomicLongArray statistics;
    private final String tableName;
    private final String tabletId;

    TabletStatistics(String tableName, String tabletId) {
      this.tableName = tableName;
      this.tabletId = tabletId;
      this.statistics = new AtomicLongArray(Statistic.values().length);
    }

    void incrementStatistic(Statistic statistic, long count) {
      this.statistics.addAndGet(statistic.getIndex(), count);
    }

    long getStatistic(Statistic statistic) {
      return this.statistics.get(statistic.getIndex());
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("Table: ");
      buf.append(tableName);
      buf.append(", tablet:");
      buf.append(tabletId);
      buf.append(", bytes written:");
      buf.append(getStatistic(Statistic.BYTES_WRITTEN));
      buf.append(", write rpcs:");
      buf.append(getStatistic(Statistic.WRITE_RPCS));
      buf.append(", rpc errors:");
      buf.append(getStatistic(Statistic.RPC_ERRORS));
      buf.append(", write operations:");
      buf.append(getStatistic(Statistic.WRITE_OPS));
      buf.append(", operation errors:");
      buf.append(getStatistic(Statistic.OPS_ERRORS));
      return buf.toString();
    }
  }
}
