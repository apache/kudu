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

import org.apache.kudu.annotations.InterfaceAudience;

/**
 * Container class for server information that never changes, like UUID and hostname.
 */
@InterfaceAudience.Private
public class ServerInfo {
  private final String uuid;
  private final String hostname;
  private final int port;
  private final boolean local;

  /**
   * Constructor for all the fields. The intent is that there should only be one ServerInfo
   * instance per UUID the client is connected to.
   * @param uuid server's UUID
   * @param hostname server's hostname, only one of them
   * @param port server's port
   * @param local if the server is hosted on the same machine where this client is running
   */
  public ServerInfo(String uuid, String hostname, int port, boolean local) {
    this.uuid = uuid;
    this.hostname = hostname;
    this.port = port;
    this.local = local;
  }

  /**
   * Returns this server's uuid.
   * @return a string that contains this server's uuid
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Returns this server's hostname. We might get many hostnames from the master for a single
   * TS, and this is the one we picked to connect to originally.
   * @return a string that contains this server's hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * Returns this server's port.
   * @return a port number that this server is bound to
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns if this server is on this client's host.
   * @return true if the server is local, else false
   */
  public boolean isLocal() {
    return local;
  }
}
