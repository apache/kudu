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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.SocketChannelConfig;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Common;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.NetUtil;

/**
 * The ConnectionCache is responsible for managing connections to masters and tablet servers.
 * There should only be one instance per Kudu client, and can <strong>not</strong> be shared between
 * clients.
 * <p>
 * {@link TabletClient}s are currently never removed from this cache. Since the map is keyed by
 * UUID, it would require an ever-growing set of unique tablet servers to encounter memory issues.
 * The reason for keeping disconnected connections in the cache is two-fold: 1) it makes
 * reconnecting easier since only UUIDs are passed around so we can use the dead TabletClient's
 * host and port to reconnect (see {@link #getLiveClient(String)}) and 2) having the dead
 * connection prevents tight looping when hitting "Connection refused"-type of errors.
 * <p>
 * This class is thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class ConnectionCache {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionCache.class);

  /**
   * Cache that maps UUIDs to the clients connected to them.
   * <p>
   * This isn't a {@link ConcurrentHashMap} because we want to do an atomic get-and-put,
   * {@code putIfAbsent} isn't a good fit for us since it requires creating
   * an object that may be "wasted" in case another thread wins the insertion
   * race, and we don't want to create unnecessary connections.
   */
  @GuardedBy("lock")
  private final HashMap<String, TabletClient> uuid2client = new HashMap<>();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.readLock();

  private final AsyncKuduClient kuduClient;

  /**
   * Create a new empty ConnectionCache that will used the passed client to create connections.
   * @param client a client that contains the information we need to create connections
   */
  ConnectionCache(AsyncKuduClient client) {
    this.kuduClient = client;
  }

  /**
   * Create a connection to a tablet server based on information provided by the master.
   * @param tsInfoPB master-provided information for the tablet server
   * @return an object that contains all the server's information
   * @throws UnknownHostException if we cannot resolve the tablet server's IP address
   */
  ServerInfo connectTS(Master.TSInfoPB tsInfoPB) throws UnknownHostException {
    List<Common.HostPortPB> addresses = tsInfoPB.getRpcAddressesList();
    String uuid = tsInfoPB.getPermanentUuid().toStringUtf8();
    if (addresses.isEmpty()) {
      LOG.warn("Received a tablet server with no addresses, UUID: {}", uuid);
      return null;
    }

    // from meta_cache.cc
    // TODO: if the TS advertises multiple host/ports, pick the right one
    // based on some kind of policy. For now just use the first always.
    HostAndPort hostPort = ProtobufHelper.hostAndPortFromPB(addresses.get(0));
    InetAddress inetAddress = NetUtil.getInetAddress(hostPort.getHost());
    if (inetAddress == null) {
      throw new UnknownHostException(
          "Failed to resolve the IP of `" + addresses.get(0).getHost() + "'");
    }
    return newClient(new ServerInfo(uuid, hostPort, inetAddress)).getServerInfo();
  }

  TabletClient newMasterClient(HostAndPort hostPort) {
    // We should pass a UUID here but we have a chicken and egg problem, we first need to
    // communicate with the masters to find out about them, and that's what we're trying to do.
    // The UUID is just used for logging and cache key, so instead we just use a constructed
    // string with the master host and port as.
    return newClient("master-" + hostPort.toString(), hostPort);
  }

  TabletClient newClient(String uuid, HostAndPort hostPort) {
    InetAddress inetAddress = NetUtil.getInetAddress(hostPort.getHost());
    if (inetAddress == null) {
      // TODO(todd): should we log the resolution failure? throw an exception?
      return null;
    }

    ServerInfo serverInfo = new ServerInfo(uuid, hostPort, inetAddress);
    return newClient(serverInfo);
  }

  TabletClient newClient(ServerInfo serverInfo) {
    TabletClient client;
    SocketChannel chan;

    writeLock.lock();
    try {
      client = uuid2client.get(serverInfo.getUuid());
      if (client != null && client.isAlive()) {
        return client;
      }
      final TabletClientPipeline pipeline = new TabletClientPipeline();
      client = pipeline.init(serverInfo);
      chan = this.kuduClient.getChannelFactory().newChannel(pipeline);
      uuid2client.put(serverInfo.getUuid(), client);
    } finally {
      writeLock.unlock();
    }

    final SocketChannelConfig config = chan.getConfig();
    config.setConnectTimeoutMillis(5000);
    config.setTcpNoDelay(true);
    // Unfortunately there is no way to override the keep-alive timeout in
    // Java since the JRE doesn't expose any way to call setsockopt() with
    // TCP_KEEPIDLE. And of course the default timeout is >2h. Sigh.
    config.setKeepAlive(true);
    chan.connect(new InetSocketAddress(serverInfo.getResolvedAddress(),
                                       serverInfo.getPort())); // Won't block.
    return client;
  }

  /**
   * Get a connection to a server for the given UUID. The returned connection can be down and its
   * state can be queried via {@link TabletClient#isAlive()}. To automatically get a client that's
   * gonna be re-connected automatically, use {@link #getLiveClient(String)}.
   * @param uuid server's identifier
   * @return a connection to a server, or null if the passed UUID isn't known
   */
  TabletClient getClient(String uuid) {
    readLock.lock();
    try {
      return uuid2client.get(uuid);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get a connection to a server for the given UUID. This method will automatically call
   * {@link #newClient(String, InetAddress, int)} if the cached connection is down.
   * @param uuid server's identifier
   * @return a connection to a server, or null if the passed UUID isn't known
   */
  TabletClient getLiveClient(String uuid) {
    TabletClient client = getClient(uuid);

    if (client == null) {
      return null;
    } else if (client.isAlive()) {
      return client;
    } else {
      return newClient(client.getServerInfo());
    }
  }

  /**
   * Asynchronously closes every socket, which will also cancel all the RPCs in flight.
   */
  Deferred<ArrayList<Void>> disconnectEverything() {
    readLock.lock();
    try {
      ArrayList<Deferred<Void>> deferreds = new ArrayList<>(uuid2client.size());
      for (TabletClient ts : uuid2client.values()) {
        deferreds.add(ts.shutdown());
      }
      return Deferred.group(deferreds);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * The list returned by this method can't be modified,
   * but calling certain methods on the returned TabletClients can have an effect. For example,
   * it's possible to forcefully shutdown a connection to a tablet server by calling {@link
   * TabletClient#shutdown()}.
   * @return copy of the current TabletClients list
   */
  List<TabletClient> getImmutableTabletClientsList() {
    readLock.lock();
    try {
      return ImmutableList.copyOf(uuid2client.values());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Queries all the cached connections if they are alive.
   * @return true if all the connections are down, else false
   */
  @VisibleForTesting
  boolean allConnectionsAreDead() {
    readLock.lock();
    try {
      for (TabletClient tserver : uuid2client.values()) {
        if (tserver.isAlive()) {
          return false;
        }
      }
    } finally {
      readLock.unlock();
    }
    return true;
  }

  private final class TabletClientPipeline extends DefaultChannelPipeline {
    TabletClient init(ServerInfo serverInfo) {
      super.addFirst("decode-frames", new LengthFieldBasedFrameDecoder(
          KuduRpc.MAX_RPC_SIZE,
          0, // length comes at offset 0
          4, // length prefix is 4 bytes long
          0, // no "length adjustment"
          4 /* strip the length prefix */));
      super.addLast("decode-inbound", new CallResponse.Decoder());
      super.addLast("encode-outbound", new RpcOutboundMessage.Encoder());
      AsyncKuduClient kuduClient = ConnectionCache.this.kuduClient;
      final TabletClient client = new TabletClient(kuduClient, serverInfo);
      if (kuduClient.getDefaultSocketReadTimeoutMs() > 0) {
        super.addLast("timeout-handler",
            new ReadTimeoutHandler(kuduClient.getTimer(),
                kuduClient.getDefaultSocketReadTimeoutMs(),
                TimeUnit.MILLISECONDS));
      }
      super.addLast("kudu-handler", client);

      return client;
    }
  }
}
