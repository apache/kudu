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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;

/**
 * This class encapsulates the information regarding a tablet and its locations.
 * <p>
 * RemoteTablet's main function is to keep track of where the leader for this
 * tablet is. For example, an RPC might call {@link #getLeaderServerInfo()}, contact that TS, find
 * it's not the leader anymore, and then call {@link #demoteLeader(String)}.
 * <p>
 * A RemoteTablet's life is expected to be long in a cluster where roles aren't changing often,
 * and short when they do since the Kudu client will replace the RemoteTablet it caches with new
 * ones after getting tablet locations from the master.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RemoteTablet implements Comparable<RemoteTablet> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteTablet.class);

  private final String tableId;
  private final String tabletId;
  @GuardedBy("tabletServers")
  private final Map<String, ServerInfo> tabletServers;
  private final AtomicReference<List<LocatedTablet.Replica>> replicas =
      new AtomicReference(ImmutableList.of());
  private final Partition partition;

  @GuardedBy("tabletServers")
  private String leaderUuid;

  RemoteTablet(String tableId,
               Master.TabletLocationsPB tabletLocations,
               List<ServerInfo> serverInfos) {
    this.tabletId = tabletLocations.getTabletId().toStringUtf8();
    this.tableId = tableId;
    this.partition = ProtobufHelper.pbToPartition(tabletLocations.getPartition());
    this.tabletServers = new HashMap<>(serverInfos.size());

    for (ServerInfo serverInfo : serverInfos) {
      this.tabletServers.put(serverInfo.getUuid(), serverInfo);
    }

    ImmutableList.Builder<LocatedTablet.Replica> replicasBuilder = new ImmutableList.Builder<>();
    for (Master.TabletLocationsPB.ReplicaPB replica : tabletLocations.getReplicasList()) {
      String uuid = replica.getTsInfo().getPermanentUuid().toStringUtf8();
      replicasBuilder.add(new LocatedTablet.Replica(replica));
      if (replica.getRole().equals(Metadata.RaftPeerPB.Role.LEADER)) {
        leaderUuid = uuid;
      }
    }

    if (leaderUuid == null) {
      LOG.warn("No leader provided for tablet {}", getTabletId());
    }
    replicas.set(replicasBuilder.build());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(tabletId).append("@[");
    List<String> tsStrings;
    synchronized (tabletServers) {
      tsStrings = new ArrayList<>(tabletServers.size());
      for (ServerInfo e : tabletServers.values()) {
        String flag = e.getUuid().equals(leaderUuid) ? "[L]" : "";
        tsStrings.add(e.toString() + flag);
      }
    }
    // Sort so that we have a consistent iteration order regardless of
    // HashSet ordering.
    Collections.sort(tsStrings);
    sb.append(Joiner.on(',').join(tsStrings));
    sb.append(']');
    return sb.toString();
  }

  /**
   * Removes the passed tablet server from this tablet's list of tablet servers.
   * @param uuid a tablet server to remove from this cache
   * @return true if this method removed ts from the list, else false
   */
  boolean removeTabletClient(String uuid) {
    synchronized (tabletServers) {
      if (leaderUuid != null && leaderUuid.equals(uuid)) {
        leaderUuid = null;
      }
      if (tabletServers.remove(uuid) != null) {
        return true;
      }
      LOG.debug("tablet {} already removed ts {}, size left is {}",
          getTabletId(), uuid, tabletServers.size());
      return false;
    }
  }

  /**
   * Clears the leader UUID if the passed tablet server is the current leader.
   * If it is the current leader, then the next call to this tablet will have
   * to query the master to find the new leader.
   * @param uuid a tablet server that gave a sign that it isn't this tablet's leader
   */
  void demoteLeader(String uuid) {
    synchronized (tabletServers) {
      if (leaderUuid == null) {
        LOG.debug("{} couldn't be demoted as the leader for {}, there is no known leader",
            uuid, getTabletId());
        return;
      }

      if (leaderUuid.equals(uuid)) {
        leaderUuid = null;
        LOG.debug("{} was demoted as the leader for {}", uuid, getTabletId());
      } else {
        LOG.debug("{} wasn't the leader for {}, current leader is {}", uuid,
            getTabletId(), leaderUuid);
      }
    }
  }

  /**
   * Get the information on the tablet server that we think holds the leader replica for this
   * tablet.
   *
   * @return information on a tablet server that we think has the leader, else null
   */
  @Nullable
  ServerInfo getLeaderServerInfo() {
    synchronized (tabletServers) {
      return tabletServers.get(leaderUuid);
    }
  }

  /**
   * Get the information on the closest server. If none is closer than the others,
   * return the information on a randomly picked server.
   *
   * @return the information on the closest server, which might be any if none is closer, or null
   *   if this cache doesn't know any servers.
   */
  @Nullable
  ServerInfo getClosestServerInfo() {
    synchronized (tabletServers) {
      ServerInfo last = null;
      for (ServerInfo e : tabletServers.values()) {
        last = e;
        if (e.isLocal()) {
          return e;
        }
      }
      // TODO(KUDU-2348) this doesn't return a random server, but rather returns
      // whichever one's hashcode places it last. That might be the same
      // "random" choice across all clients, which is not so good.
      return last;
    }
  }

  /**
   * Helper function to centralize the calling of methods based on the passed replica selection
   * mechanism.
   *
   * @param replicaSelection replica selection mechanism to use
   * @return information on the server that matches the selection, can be null
   */
  @Nullable
  ServerInfo getReplicaSelectedServerInfo(ReplicaSelection replicaSelection) {
    switch (replicaSelection) {
      case LEADER_ONLY:
        return getLeaderServerInfo();
      case CLOSEST_REPLICA:
        return getClosestServerInfo();
      default:
        throw new RuntimeException("unknown replica selection mechanism " + replicaSelection);
    }
  }

  /**
   * Get replicas of this tablet. The returned list may not be mutated.
   *
   * @return the replicas of the tablet
   */
  List<LocatedTablet.Replica> getReplicas() {
    return replicas.get();
  }

  public String getTableId() {
    return tableId;
  }

  String getTabletId() {
    return tabletId;
  }

  public Partition getPartition() {
    return partition;
  }

  byte[] getTabletIdAsBytes() {
    return tabletId.getBytes(UTF_8);
  }

  @Override
  public int compareTo(RemoteTablet remoteTablet) {
    if (remoteTablet == null) {
      return 1;
    }

    return ComparisonChain.start()
        .compare(this.tableId, remoteTablet.tableId)
        .compare(this.partition, remoteTablet.partition).result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RemoteTablet that = (RemoteTablet) o;

    return this.compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableId, partition);
  }
}
