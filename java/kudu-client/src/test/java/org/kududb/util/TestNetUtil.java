// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.util;

import com.google.common.net.HostAndPort;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link NetUtil}.
 */
public class TestNetUtil {

  /**
   * Tests parsing strings into {@link HostAndPort} objects with and without specifying
   * the port in the string.
   */
  @Test
  public void testParseString() {
    String aStringWithPort = "1.2.3.4:1234";
    HostAndPort hostAndPortForAStringWithPort = NetUtil.parseString(aStringWithPort, 0);
    assertEquals(hostAndPortForAStringWithPort.getHostText(), "1.2.3.4");
    assertEquals(hostAndPortForAStringWithPort.getPort(), 1234);

    String aStringWithoutPort = "1.2.3.4";
    HostAndPort hostAndPortForAStringWithoutPort = NetUtil.parseString(aStringWithoutPort, 12345);
    assertEquals(hostAndPortForAStringWithoutPort.getHostText(), aStringWithoutPort);
    assertEquals(hostAndPortForAStringWithoutPort.getPort(), 12345);
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
                         new HostAndPort[] { HostAndPort.fromParts("1.2.3.4.5", 3333),
                           HostAndPort.fromParts("10.0.0.1", 5555),
                           HostAndPort.fromParts("127.0.0.1", 7777) }
    );
  }

  @Test
  public void testHostsAndPortsToString() {
    List<HostAndPort> hostsAndPorts = Arrays.asList(
        HostAndPort.fromParts("127.0.0.1", 1111),
        HostAndPort.fromParts("1.2.3.4.5", 0)
    );
    assertEquals(NetUtil.hostsAndPortsToString(hostsAndPorts), "127.0.0.1:1111,1.2.3.4.5:0");
  }
}
