// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ListTabletServersResponse extends KuduRpcResponse {

  private final int tabletServersCount;
  private final List<String> tabletServersList;

  /**
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   * @param tabletServersCount How many tablet servers the master is reporting.
   * @param tabletServersList List of tablet servers.
   */
  ListTabletServersResponse(long ellapsedMillis, String tsUUID,
                            int tabletServersCount, List<String> tabletServersList) {
    super(ellapsedMillis, tsUUID);
    this.tabletServersCount = tabletServersCount;
    this.tabletServersList = tabletServersList;
  }

  /**
   * Get the count of tablet servers as reported by the master.
   * @return TS count.
   */
  public int getTabletServersCount() {
    return tabletServersCount;
  }

  /**
   * Get the list of tablet servers, as represented by their hostname.
   * @return List of hostnames, one per TS.
   */
  public List<String> getTabletServersList() {
    return tabletServersList;
  }
}
