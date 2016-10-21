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
package org.apache.kudu.util;

import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.kudu.annotations.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Networking related methods.
 */
@InterfaceAudience.Private
public class NetUtil {

  private static final Logger LOG = LoggerFactory.getLogger(NetUtil.class);

  /**
   * Convert a list of {@link HostAndPort} objects to a comma separate string.
   * The inverse of {@link #parseStrings(String, int)}.
   *
   * @param hostsAndPorts A list of {@link HostAndPort} objects.
   * @return Comma separate list of "host:port" pairs.
   */
  public static String hostsAndPortsToString(List<HostAndPort> hostsAndPorts) {
    return Joiner.on(",").join(Lists.transform(hostsAndPorts, Functions.toStringFunction()));
  }

  /**
   * Parse a "host:port" pair into a {@link HostAndPort} object. If there is no
   * port specified in the string, then 'defaultPort' is used.
   *
   * @param addrString  A host or a "host:port" pair.
   * @param defaultPort Default port to use if no port is specified in addrString.
   * @return The HostAndPort object constructed from addrString.
   */
  public static HostAndPort parseString(String addrString, int defaultPort) {
    return addrString.indexOf(':') == -1 ? HostAndPort.fromParts(addrString, defaultPort) :
               HostAndPort.fromString(addrString);
  }

  /**
   * Parse a comma separated list of "host:port" pairs into a list of
   * {@link HostAndPort} objects. If no port is specified for an entry in
   * the comma separated list, then a default port is used.
   * The inverse of {@link #hostsAndPortsToString(List)}.
   *
   * @param commaSepAddrs The comma separated list of "host:port" pairs.
   * @param defaultPort   The default port to use if no port is specified.
   * @return A list of HostAndPort objects constructed from commaSepAddrs.
   */
  public static List<HostAndPort> parseStrings(final String commaSepAddrs, int defaultPort) {
    Iterable<String> addrStrings = Splitter.on(',').trimResults().split(commaSepAddrs);
    List<HostAndPort> hostsAndPorts = Lists.newArrayListWithCapacity(Iterables.size(addrStrings));
    for (String addrString : addrStrings) {
      HostAndPort hostAndPort = parseString(addrString, defaultPort);
      hostsAndPorts.add(hostAndPort);
    }
    return hostsAndPorts;
  }

  /**
   * Gets a hostname or an IP address and returns an InetAddress.
   * <p>
   * <strong>This method can block</strong> as there is no API for
   * asynchronous DNS resolution in the JDK.
   * @param host the hostname to resolve
   * @return an InetAddress for the given hostname,
   * or {@code null} if the address couldn't be resolved
   */
  public static InetAddress getInetAddress(final String host) {
    final long start = System.nanoTime();
    try {
      InetAddress ip = InetAddress.getByName(host);
      long latency = System.nanoTime() - start;
      if (latency > 500000/*ns*/ && LOG.isDebugEnabled()) {
        LOG.debug("Resolved IP of `{}' to {} in {}ns", host, ip, latency);
      } else if (latency >= 3000000/*ns*/) {
        LOG.warn("Slow DNS lookup! Resolved IP of `{}' to {} in {}ns", host, ip, latency);
      }
      return ip;
    } catch (UnknownHostException e) {
      LOG.error("Failed to resolve the IP of `{}' in {}ns", host, (System.nanoTime() - start));
      return null;
    }
  }

  /**
   * Given an InetAddress, checks to see if the address is a local address, by
   * comparing the address with all the interfaces on the node.
   * @param addr address to check if it is local node's address
   * @return true if the address corresponds to the local node
   */
  public static boolean isLocalAddress(InetAddress addr) {
    // Check if the address is any local or loopback.
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface.
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException e) {
      }
    }
    return local;
  }
}
