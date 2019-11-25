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

import com.google.common.base.Objects;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A simple wrapper around InetSocketAddress to prevent
 * accidentally introducing DNS lookups.
 *
 * The HostAndPort implementation in Guava is not used
 * because Guava is shaded and relocated in Kudu preventing
 * it from being used as a parameter or return value on
 * public methods. Additionally Guava's HostAndPort
 * implementation is marked as beta.
 */
@InterfaceAudience.Private
public class HostAndPort {

  private final InetSocketAddress address;

  public HostAndPort(String host, int port) {
    // Using createUnresolved ensures no lookups will occur.
    this.address = InetSocketAddress.createUnresolved(host, port);
  }

  public String getHost() {
    // Use getHostString to ensure no reverse lookup is done.
    return address.getHostString();
  }

  public int getPort() {
    return address.getPort();
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HostAndPort)) {
      return false;
    }
    HostAndPort that = (HostAndPort) o;
    return Objects.equal(address, that.address);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(address);
  }

  @Override
  public String toString() {
    return address.toString();
  }
}
