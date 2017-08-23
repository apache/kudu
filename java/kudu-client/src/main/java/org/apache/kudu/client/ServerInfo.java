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
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.util.NetUtil;

/**
 * Container class for server information that never changes, like UUID and hostname.
 */
@InterfaceAudience.Private
public class ServerInfo {
  private final String uuid;
  private final HostAndPort hostPort;
  private final InetAddress resolvedAddr;
  private final boolean local;
  private static final ConcurrentHashMap<InetAddress, Boolean> isLocalAddressCache =
      new ConcurrentHashMap<>();

  /**
   * Constructor for all the fields. The intent is that there should only be one ServerInfo
   * instance per UUID the client is connected to.
   * @param uuid server's UUID
   * @param hostPort server's hostname and port
   * @param resolvedAddr resolved address used to check if the server is local
   */
  public ServerInfo(String uuid, HostAndPort hostPort, InetAddress resolvedAddr) {
    Preconditions.checkNotNull(uuid);
    this.uuid = uuid;
    this.hostPort = hostPort;
    this.resolvedAddr = resolvedAddr;
    Boolean isLocal = isLocalAddressCache.get(resolvedAddr);
    if (isLocal == null) {
      isLocal = NetUtil.isLocalAddress(resolvedAddr);
      isLocalAddressCache.put(resolvedAddr, isLocal);
    }
    this.local = isLocal;
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
      return InetAddress.getByName(hostPort.getHost()).getCanonicalHostName().toLowerCase();
    } catch (UnknownHostException e) {
      return hostPort.getHost();
    }
  }

  /**
   * Returns this server's port.
   * @return a port number that this server is bound to
   */
  public int getPort() {
    return hostPort.getPort();
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
  public InetAddress getResolvedAddress() {
    return resolvedAddr;
  }
}
