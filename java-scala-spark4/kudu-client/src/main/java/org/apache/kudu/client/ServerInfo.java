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
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.util.NetUtil;

/**
 * Container class for server information that never changes, like UUID and hostname.
 */
@InterfaceAudience.Private
public class ServerInfo {
  private final String uuid;
  private final HostAndPort hostPort;
  private final InetSocketAddress resolvedAddr;
  private final String location;
  private final boolean local;
  private static final ConcurrentHashMap<InetAddress, Boolean> isLocalAddressCache =
      new ConcurrentHashMap<>();

  /**
   * Constructor for all the fields. The intent is that there should only be one ServerInfo
   * instance per UUID the client is connected to.
   * @param uuid server's UUID
   * @param hostPort server's hostname and port
   * @param resolvedAddr resolved address used to check if the server is local
   * @param location the location assigned by the leader master, or an empty string if no location
   *                 is assigned
   */
  public ServerInfo(String uuid, HostAndPort hostPort, InetAddress resolvedAddr, String location) {
    Preconditions.checkNotNull(uuid);
    Preconditions.checkArgument(hostPort.getPort() > 0);
    Preconditions.checkNotNull(location);
    this.uuid = uuid;
    this.hostPort = hostPort;
    this.resolvedAddr = new InetSocketAddress(resolvedAddr, hostPort.getPort());
    this.location = location;
    this.local = isLocalAddressCache.computeIfAbsent(resolvedAddr,
        inetAddress -> NetUtil.isLocalAddress(resolvedAddr));
  }

  /**
   * Returns this server's uuid.
   * @return a string that contains this server's uuid
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Returns this server's canonical hostname.
   * @return a string that contains this server's canonical hostname
   */
  public String getAndCanonicalizeHostname() {
    try {
      return InetAddress.getByName(
          hostPort.getHost()).getCanonicalHostName().toLowerCase(Locale.ENGLISH);
    } catch (UnknownHostException e) {
      return hostPort.getHost();
    }
  }

  /**
   * Returns this server's hostname and port.
   * @return a HostAndPort that describes where this server can be reached.
   */
  public HostAndPort getHostAndPort() {
    return hostPort;
  }

  /**
   * Returns this server's port.
   * @return a port number that this server is bound to
   */
  public int getPort() {
    return hostPort.getPort();
  }

  /**
   * Returns this server's location. If no location is assigned, returns an empty string.
   * @return the server's location
   */
  public String getLocation() {
    return location;
  }

  /**
   * Returns true if the server is in the same location as 'location'.
   * @return true if the server is in 'location'.
   */
  public boolean inSameLocation(String loc) {
    Preconditions.checkNotNull(loc);
    return !loc.isEmpty() &&
           loc.equals(location);
  }

  /**
   * Returns if this server is on this client's host.
   * @return true if the server is local, else false
   */
  public boolean isLocal() {
    return local;
  }

  /**
   * @return the cached resolved address for this server
   */
  public InetSocketAddress getResolvedAddress() {
    return resolvedAddr;
  }

  @Override
  public String toString() {
    return uuid + "(" + hostPort + ")";
  }
}
