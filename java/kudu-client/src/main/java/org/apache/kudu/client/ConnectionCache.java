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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.HashMultimap;
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
 * Disconnected instances of the {@link Connection} class are replaced in the cache with new ones
 * when {@link #getConnection(ServerInfo, Connection.CredentialsPolicy)} method is called with the
 * same destination and matching credentials policy. Since the map is keyed by the address of the
 * target server, the theoretical maximum number of elements in the cache is twice the number of
 * all servers in the cluster (i.e. both masters and tablet servers). However, in practice it's
 * 2 * number of masters + number of tablet servers since tablet servers do not require connections
 * negotiated with primary credentials.
 *
 * This class is thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class ConnectionCache {

  /** Security context to use for connection negotiation. */
  private final SecurityContext securityContext;

  /** Timer to monitor read timeouts for connections (used by Netty's ReadTimeoutHandler) */
  private final HashedWheelTimer timer;

  /** Netty's channel factory to use by connections. */
  private final ClientSocketChannelFactory channelFactory;

  /**
   * Container mapping server IP/port into the established connection from the client to the
   * server. It may be up to two connections per server: one established with secondary
   * credentials (e.g. authn token), another with primary ones (e.g. Kerberos credentials).
   */
  @GuardedBy("connsByAddress")
  private final HashMultimap<InetSocketAddress, Connection> connsByAddress =
      HashMultimap.create();

  /** Create a new empty ConnectionCache given the specified parameters. */
  ConnectionCache(SecurityContext securityContext,
                  HashedWheelTimer timer,
                  ClientSocketChannelFactory channelFactory) {
    this.securityContext = securityContext;
    this.timer = timer;
    this.channelFactory = channelFactory;
  }

  /**
   * Get connection to the specified server. If no connection exists or the existing connection
   * is already disconnected, then create a new connection to the specified server. The newly
   * created connection is not negotiated until enqueuing the first RPC to the target server.
   *
   * @param serverInfo the server end-point to connect to
   * @param credentialsPolicy authentication credentials policy for the connection negotiation
   * @return instance of this object with the specified destination
   */
  public Connection getConnection(final ServerInfo serverInfo,
                                  Connection.CredentialsPolicy credentialsPolicy) {
    Connection result = null;
    synchronized (connsByAddress) {
      // Create and register a new connection object into the cache if one of the following is true:
      //
      //  * There isn't a registered connection to the specified destination.
      //
      //  * There is a connection to the specified destination, but it's in TERMINATED state.
      //    Such connections cannot be used again and should be recycled. The connection cache
      //    lazily removes such entries.
      //
      //  * A connection negotiated with primary credentials is requested but the only registered
      //    one does not have such property. In this case, the already existing connection
      //    (negotiated with secondary credentials, i.e. authn token) is kept in the cache and
      //    a new one is created to be open and negotiated with primary credentials. The newly
      //    created connection is put into the cache along with old one. We don't do anything
      //    special to the old connection to shut it down since it may be still in use. We rely
      //    on the server to close inactive connections in accordance with their TTL settings.
      //
      final Set<Connection> connections = connsByAddress.get(serverInfo.getResolvedAddress());
      Iterator<Connection> it = connections.iterator();
      while (it.hasNext()) {
        Connection c = it.next();
        if (c.isTerminated()) {
          // Lazy recycling of the terminated connections: removing them from the cache upon
          // an attempt to connect to the same destination again.
          it.remove();
          continue;
        }
        if (credentialsPolicy == Connection.CredentialsPolicy.ANY_CREDENTIALS ||
            credentialsPolicy == c.getCredentialsPolicy()) {
          // If the connection policy allows for using any credentials or the connection is
          // negotiated using the given credentials type, this is the connection we are looking for.
          result = c;
        }
      }
      if (result == null) {
        result = new Connection(serverInfo,
                                securityContext,
                                timer,
                                channelFactory,
                                credentialsPolicy);
        connections.add(result);
        // There can be at most 2 connections to the same destination: one with primary and another
        // with secondary credentials.
        assert connections.size() <= 2;
      }
    }

    return result;
  }

  /** Asynchronously terminate every connection. This cancels all the pending and in-flight RPCs. */
  Deferred<ArrayList<Void>> disconnectEverything() {
    synchronized (connsByAddress) {
      List<Deferred<Void>> deferreds = new ArrayList<>(connsByAddress.size());
      for (Connection c : connsByAddress.values()) {
        deferreds.add(c.shutdown());
      }
      return Deferred.group(deferreds);
    }
  }

  /**
   * Return a copy of the all-connections-list. This method is exposed only to allow
   * {@link AsyncKuduClient} to forward it, so tests could get access to the underlying elements
   * of the cache.
   *
   * @return a copy of the list of all connections in the connection cache
   */
  @InterfaceAudience.LimitedPrivate("Test")
  List<Connection> getConnectionListCopy() {
    synchronized (connsByAddress) {
      return ImmutableList.copyOf(connsByAddress.values());
    }
  }
}
