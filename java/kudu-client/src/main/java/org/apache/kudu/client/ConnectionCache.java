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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

/**
 * The ConnectionCache is responsible for managing connections to Kudu masters and tablet servers.
 * There should only be one instance of ConnectionCache per Kudu client, and it should not be
 * shared between clients.
 * <p>
 * Disconnected instances of the {@link Connection} class are replaced in the cache with instances
 * when {@link #getConnection(ServerInfo)) method is called with the same destination. Since the map
 * is keyed by UUID of the server, it would require an ever-growing set of unique Kudu servers
 * to encounter memory issues.
 *
 * This class is thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class ConnectionCache {
  /** Security context to use for connection negotiation. */
  private final SecurityContext securityContext;

  /** Read timeout for connections (used by Netty's ReadTimeoutHandler) */
  private final long socketReadTimeoutMs;

  /** Timer to monitor read timeouts for connections (used by Netty's ReadTimeoutHandler) */
  private final HashedWheelTimer timer;

  /** Netty's channel factory to use by connections. */
  private final ClientSocketChannelFactory channelFactory;

  /** Synchronization primitive to guard access to the fields below. */
  private final ReentrantLock lock = new ReentrantLock();

  @GuardedBy("lock")
  private final HashMap<String, Connection> uuid2connection = new HashMap<>();

  /**
   * Create a new empty ConnectionCache given the specified parameters.
   */
  ConnectionCache(SecurityContext securityContext,
                  long socketReadTimeoutMs,
                  HashedWheelTimer timer,
                  ClientSocketChannelFactory channelFactory) {
    this.securityContext = securityContext;
    this.socketReadTimeoutMs = socketReadTimeoutMs;
    this.timer = timer;
    this.channelFactory = channelFactory;
  }

  /**
   * Get connection to the specified server. If no connection exists or the existing connection
   * is already disconnected, then create a new connection to the specified server. The newly
   * created connection is not negotiated until enqueuing the first RPC to the target server.
   *
   * @param serverInfo the server end-point to connect to
   * @return instance of this object with the specified destination
   */
  public Connection getConnection(final ServerInfo serverInfo) {
    Connection connection;

    lock.lock();
    try {
      // First try to find an existing connection.
      connection = uuid2connection.get(serverInfo.getUuid());
      if (connection == null || connection.isDisconnected()) {
        // If no valid connection is found, create a new one.
        connection = new Connection(serverInfo, securityContext,
            socketReadTimeoutMs, timer, channelFactory);
        uuid2connection.put(serverInfo.getUuid(), connection);
      }
    } finally {
      lock.unlock();
    }

    return connection;
  }

  /**
   * Asynchronously terminate every connection. This also cancels all the pending and in-flight
   * RPCs.
   */
  Deferred<ArrayList<Void>> disconnectEverything() {
    lock.lock();
    try {
      ArrayList<Deferred<Void>> deferreds = new ArrayList<>(uuid2connection.size());
      for (Connection c : uuid2connection.values()) {
        deferreds.add(c.shutdown());
      }
      return Deferred.group(deferreds);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Return a copy of the all-connections-list. This method is exposed only to allow
   * {@ref AsyncKuduClient} to forward it, so tests could get access to the underlying elements
   * of the cache.
   *
   * @return a copy of the list of all connections in the connection cache
   */
  @VisibleForTesting
  List<Connection> getConnectionListCopy() {
    lock.lock();
    try {
      return ImmutableList.copyOf(uuid2connection.values());
    } finally {
      lock.unlock();
    }
  }
}
