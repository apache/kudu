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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.client.HostAndPort;
import org.apache.kudu.test.junit.RetryRule;

/**
 * Test for {@link NetUtil}.
 */
public class TestNetUtil {

  @Rule
  public RetryRule retryRule = new RetryRule();

  /**
   * Tests parsing strings into {@link HostAndPort} objects with and without specifying
   * the port in the string.
   */
  @Test
  public void testParseString() {
    String stringWithPort = "1.2.3.4:1234";
    HostAndPort hostAndPortForStringWithPort = NetUtil.parseString(stringWithPort, 0);
    assertEquals(hostAndPortForStringWithPort.getHost(), "1.2.3.4");
    assertEquals(hostAndPortForStringWithPort.getPort(), 1234);

    String stringWithoutPort = "1.2.3.4";
    HostAndPort hostAndPortForStringWithoutPort = NetUtil.parseString(stringWithoutPort, 12345);
    assertEquals(hostAndPortForStringWithoutPort.getHost(), stringWithoutPort);
    assertEquals(hostAndPortForStringWithoutPort.getPort(), 12345);
  }

  /**
   * Tests parsing comma separated list of "host:port" pairs and hosts into a list of
   * {@link HostAndPort} objects.
   */
  @Test
  public void testParseStrings() {
    String testAddrs = "1.2.3.4.5,10.0.0.1:5555,127.0.0.1:7777";
    List<HostAndPort> hostsAndPorts = NetUtil.parseStrings(testAddrs, 3333);
    assertArrayEquals(hostsAndPorts.toArray(),
        new HostAndPort[] {
            new HostAndPort("1.2.3.4.5", 3333),
            new HostAndPort("10.0.0.1", 5555),
            new HostAndPort("127.0.0.1", 7777)}
    );
  }

  @Test
  public void testHostsAndPortsToString() {
    List<HostAndPort> hostsAndPorts = Arrays.asList(
        new HostAndPort("127.0.0.1", 1111),
        new HostAndPort("1.2.3.4.5", 0)
    );
    assertEquals(NetUtil.hostsAndPortsToString(hostsAndPorts), "127.0.0.1:1111,1.2.3.4.5:0");
  }

  @Test
  public void testLocal() throws Exception {
    assertTrue(NetUtil.isLocalAddress(NetUtil.getInetAddress("localhost")));
    assertTrue(NetUtil.isLocalAddress(NetUtil.getInetAddress("127.0.0.1")));
    assertTrue(NetUtil.isLocalAddress(InetAddress.getLocalHost()));
    assertFalse(NetUtil.isLocalAddress(NetUtil.getInetAddress("kudu.apache.org")));
  }
}
