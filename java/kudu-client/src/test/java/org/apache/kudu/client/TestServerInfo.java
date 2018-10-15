/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.kudu.client;

import java.net.InetAddress;

import org.apache.kudu.test.cluster.FakeDNS;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;

public class TestServerInfo {
  /**
   * Test for KUDU-2103. Checks if the original hostnames is returned if unknown.
   */
  @Test
  public void testGetAndCanonicalizeUnknownHostname() throws Exception {
    installFakeDNS("master1.example.com", "server123.example.com", "10.1.2.3");

    ServerInfo serverInfo = new ServerInfo(
        "nevermind",
        new HostAndPort("master2.example.com", 12345),
        InetAddress.getByName("10.1.2.3"));

    Assert.assertEquals("master2.example.com", serverInfo.getAndCanonicalizeHostname());
  }

  /**
   * Test for KUDU-2103. Checks if the canonical hostname is returned instead
   * of the one it's set to.
   */
  @Test
  public void testGetAndCanonicalizeHostname() throws Exception {
    installFakeDNS("master1.example.com", "server123.example.com", "10.1.2.3");

    ServerInfo serverInfo = new ServerInfo(
        "abcdef", // uuid
        new HostAndPort("master1.example.com", 12345),
        InetAddress.getByName("10.1.2.3"));

    Assert.assertEquals("server123.example.com", serverInfo.getAndCanonicalizeHostname());
    Assert.assertEquals("abcdef(master1.example.com:12345)",  serverInfo.toString());
  }

  /**
   * Helper method to install FakeDNS with the expected values for the tests
   *
   * @param alias alias to be set for forward resolution
   * @param canonical canonical to be set for reverse resolution
   * @param ip IP both hostnames point to
   * @throws UnknownHostException if the "ip" is an unknown host
   */
  private void installFakeDNS(String alias, String canonical, String ip)
      throws UnknownHostException {
    FakeDNS fakeDNS = FakeDNS.getInstance();
    fakeDNS.install();
    InetAddress inetAddress = InetAddress.getByName(ip);
    fakeDNS.addForwardResolution(alias, inetAddress);
    fakeDNS.addForwardResolution(canonical, inetAddress);
    fakeDNS.addReverseResolution(inetAddress, canonical);
  }
}
