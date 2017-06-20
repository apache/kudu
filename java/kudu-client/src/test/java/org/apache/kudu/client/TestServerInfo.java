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

import com.google.common.net.HostAndPort;
import org.junit.Test;

public class TestServerInfo {
  /**
   * Test for KUDU-1982 Java client calls NetworkInterface.getByInetAddress too often.
   */
  @Test(timeout = 500)
  public void testConstructorNotSlow() throws Exception {
    String uuid = "nevermind";
    HostAndPort hap = HostAndPort.fromString("nevermind");
    // ip to force NetworkInterface.getByInetAddress call
    InetAddress ia = InetAddress.getByName("8.8.8.8");
    for (int i = 0; i < 100; ++i) {
      new ServerInfo(uuid, hap, ia);
    }
  }
}
