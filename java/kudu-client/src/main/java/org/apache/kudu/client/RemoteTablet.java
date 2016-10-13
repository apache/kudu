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

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import org.apache.kudu.Common;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class encapsulates the information regarding a tablet and its locations.
 * <p>
 * RemoteTablet's main function, once it is init()'d, is to keep track of where the leader for this
 * tablet is. For example, an RPC might call {@link #getLeaderConnection()}, contact that TS, find
 * it's not the leader anymore, and then call {@link #demoteLeader(TabletClient)}.
 * <p>
 * A RemoteTablet's life is expected to be long in a cluster where roles aren't changing often,
 * and short when they do since the Kudu client will replace the RemoteTablet it caches with new
 * ones after getting tablet locations from the master.
 * <p>
 * One particularity this class handles is {@link TabletClient} that disconnect due to their socket
 * read timeout being reached. Instead of removing them from {@link #tabletServers}, we instead
 * continue keeping track of them and if an RPC wants to use this tablet again, it'll notice that
 * the TabletClient returned by {@link #getLeaderConnection()} isn't alive, and will call
 * {@link #reconnectTabletClient(TabletClient)}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RemoteTablet implements Comparable<RemoteTablet> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteTablet.class);

  private static final int NO_LEADER_INDEX = -1;

  private final String tableId;
  private final Slice tabletId;
  @GuardedBy("tabletServers")
  private final ArrayList<TabletClient> tabletServers = new ArrayList<>();
  private final AtomicReference<List<LocatedTablet.Replica>> replicas =
      new AtomicReference(ImmutableList.of());
  private final Partition partition;
  private final ConnectionCache connectionCache;

  @GuardedBy("tabletServers")
  private int leaderIndex = NO_LEADER_INDEX;

  RemoteTablet(String tableId, Slice tabletId,
               Partition partition, ConnectionCache connectionCache) {
    this.tabletId = tabletId;
    this.tableId = tableId;
    this.partition = partition;
    this.connectionCache = connectionCache;
  }

  void init(Master.TabletLocationsPB tabletLocations) throws NonRecoverableException {

    synchronized (tabletServers) { // TODO not a fat lock with IP resolving in it
      tabletServers.clear();
      leaderIndex = NO_LEADER_INDEX;
      List<UnknownHostException> lookupExceptions =
          new ArrayList<>(tabletLocations.getReplicasCount());
      for (Master.TabletLocationsPB.ReplicaPB replica : tabletLocations.getReplicasList()) {

        List<Common.HostPortPB> addresses = replica.getTsInfo().getRpcAddressesList();
        if (addresses.isEmpty()) {
          LOG.warn("Tablet server for tablet " + getTabletIdAsString() + " doesn't have any " +
              "address");
          continue;
        }
        byte[] buf = Bytes.get(replica.getTsInfo().getPermanentUuid());
        String uuid = Bytes.getString(buf);
        // from meta_cache.cc
        // TODO: if the TS advertises multiple host/ports, pick the right one
        // based on some kind of policy. For now just use the first always.
        try {
          addTabletClient(uuid, addresses.get(0).getHost(), addresses.get(0).getPort(),
              replica.getRole().equals(Metadata.RaftPeerPB.Role.LEADER));
        } catch (UnknownHostException ex) {
          lookupExceptions.add(ex);
        }
      }

      if (leaderIndex == NO_LEADER_INDEX) {
        LOG.warn("No leader provided for tablet {}", getTabletIdAsString());
      }

      // If we found a tablet that doesn't contain a single location that we can resolve, there's
      // no point in retrying.
      if (!lookupExceptions.isEmpty() &&
          lookupExceptions.size() == tabletLocations.getReplicasCount()) {
        Status statusIOE = Status.IOError("Couldn't find any valid locations, exceptions: " +
            lookupExceptions);
        throw new NonRecoverableException(statusIOE);
      }

    }

    ImmutableList.Builder<LocatedTablet.Replica> replicasBuilder = new ImmutableList.Builder<>();
    for (Master.TabletLocationsPB.ReplicaPB replica : tabletLocations.getReplicasList()) {
      replicasBuilder.add(new LocatedTablet.Replica(replica));
    }
    replicas.set(replicasBuilder.build());
  }

  @GuardedBy("tabletServers")
  private void addTabletClient(String uuid, String host, int port, boolean isLeader)
      throws UnknownHostException {
    String ip = ConnectionCache.getIP(host);
    if (ip == null) {
      throw new UnknownHostException("Failed to resolve the IP of `" + host + "'");
    }
    TabletClient client = connectionCache.newClient(uuid, ip, port);

    tabletServers.add(client);
    if (isLeader) {
      leaderIndex = tabletServers.size() - 1;
    }
  }

  /**
   * Call this method when an existing TabletClient in this tablet's cache is found to be dead.
   * It removes the passed TS from this tablet's cache and replaces it with a new instance of
   * TabletClient. It will keep its leader status if it was already considered a leader.
   * If the passed TabletClient was already removed, then this is a no-op.
   * @param staleTs TS to reconnect to
   * @throws UnknownHostException if we can't resolve server's hostname
   */
  void reconnectTabletClient(TabletClient staleTs) throws UnknownHostException {
    assert (!staleTs.isAlive());

    synchronized (tabletServers) {
      int index = tabletServers.indexOf(staleTs);

      if (index == -1) {
        // Another thread already took care of it.
        return;
      }

      boolean wasLeader = index == leaderIndex;

      LOG.debug("Reconnecting to server {} for tablet {}. Was a leader? {}",
          staleTs.getUuid(), getTabletIdAsString(), wasLeader);

      boolean removed = removeTabletClient(staleTs);

      if (!removed) {
        LOG.debug("{} was already removed from tablet {}'s cache when reconnecting to it",
            staleTs.getUuid(), getTabletIdAsString());
      }

      addTabletClient(staleTs.getUuid(), staleTs.getHost(),
          staleTs.getPort(), wasLeader);
    }
  }

  @Override
  public String toString() {
    return getTabletIdAsString();
  }

  /**
   * Removes the passed TabletClient from this tablet's list of tablet servers. If it was the
   * leader, then we "promote" the next one unless it was the last one in the list.
   * @param ts a TabletClient that was disconnected
   * @return true if this method removed ts from the list, else false
   */
  boolean removeTabletClient(TabletClient ts) {
    synchronized (tabletServers) {
      // TODO unit test for this once we have the infra
      int index = tabletServers.indexOf(ts);
      if (index == -1) {
        return false; // we removed it already
      }

      tabletServers.remove(index);
      if (leaderIndex == index && leaderIndex == tabletServers.size()) {
        leaderIndex = NO_LEADER_INDEX;
      } else if (leaderIndex > index) {
        leaderIndex--; // leader moved down the list
      }

      return true;
      // TODO if we reach 0 TS, maybe we should remove ourselves?
    }
  }

  /**
   * Clears the leader index if the passed tablet server is the current leader.
   * If it is the current leader, then the next call to this tablet will have
   * to query the master to find the new leader.
   * @param ts a TabletClient that gave a sign that it isn't this tablet's leader
   */
  void demoteLeader(TabletClient ts) {
    synchronized (tabletServers) {
      int index = tabletServers.indexOf(ts);
      // If this TS was removed or we're already forcing a call to the master (meaning someone
      // else beat us to it), then we just noop.
      if (index == -1 || leaderIndex == NO_LEADER_INDEX) {
        LOG.debug("{} couldn't be demoted as the leader for {}",
            ts.getUuid(), getTabletIdAsString());
        return;
      }

      if (leaderIndex == index) {
        leaderIndex = NO_LEADER_INDEX;
        LOG.debug("{} was demoted as the leader for {}", ts.getUuid(), getTabletIdAsString());
      } else {
        LOG.debug("{} wasn't the leader for {}, current leader is at index {}", ts.getUuid(),
            getTabletIdAsString(), leaderIndex);
      }
    }
  }

  /**
   * Get the connection to the tablet server that we think holds the leader replica for this tablet.
   * @return a TabletClient that we think has the leader, else null
   */
  TabletClient getLeaderConnection() {
    synchronized (tabletServers) {
      if (tabletServers.isEmpty()) {
        return null;
      }
      if (leaderIndex == RemoteTablet.NO_LEADER_INDEX) {
        return null;
      } else {
        // and some reads.
        return tabletServers.get(leaderIndex);
      }
    }
  }

  /**
   * Gets the replicas of this tablet. The returned list may not be mutated.
   * @return the replicas of the tablet
   */
  List<LocatedTablet.Replica> getReplicas() {
    return replicas.get();
  }

  public String getTableId() {
    return tableId;
  }

  Slice getTabletId() {
    return tabletId;
  }

  public Partition getPartition() {
    return partition;
  }

  byte[] getTabletIdAsBytes() {
    return tabletId.getBytes();
  }

  String getTabletIdAsString() {
    return tabletId.toString(Charset.defaultCharset());
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RemoteTablet that = (RemoteTablet) o;

    return this.compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableId, partition);
  }

  static RemoteTablet createTabletFromPb(String tableId,
                                         Master.TabletLocationsPB tabletPb,
                                         ConnectionCache connectionCache)
      throws NonRecoverableException {
    Partition partition = ProtobufHelper.pbToPartition(tabletPb.getPartition());
    Slice tabletId = new Slice(tabletPb.getTabletId().toByteArray());
    RemoteTablet tablet = new RemoteTablet(tableId, tabletId, partition, connectionCache);
    tablet.init(tabletPb);
    return tablet;
  }
}
