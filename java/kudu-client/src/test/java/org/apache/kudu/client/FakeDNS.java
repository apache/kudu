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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import sun.net.spi.nameservice.NameService;

/**
 * Fake DNS resolver which allows our tests to work well even though we use
 * strange loopback IP addresses (127.x.y.z) with no corresponding reverse
 * DNS.
 *
 * This overrides the reverse lookups for such IPs to return the same address
 * in String form.
 *
 * Without this class, reverse DNS lookups for such addresses often take
 * 5 seconds to return, causing timeouts and overall test slowness.
 *
 * In the future this class might also be extended to test more interesting
 * DNS-related scenarios.
 */
public class FakeDNS {
  static FakeDNS instance = new FakeDNS();

  @GuardedBy("this")
  private Map<String, InetAddress> forwardResolutions = new HashMap<>();

  @GuardedBy("this")
  private Map<InetAddress, String> reverseResolutions = new HashMap<>();

  /** whether the fake resolver has been installed */
  @GuardedBy("this")
  private boolean installed = false;

  private FakeDNS() {}
  public static FakeDNS getInstance() {
    return instance;
  }

  public synchronized void addForwardResolution(String hostname, InetAddress ip) {
    forwardResolutions.put(hostname, ip);
  }

  public synchronized void addReverseResolution(InetAddress ip, String hostname) {
    reverseResolutions.put(ip, hostname);
  }

  /**
   * Install the fake DNS resolver into the Java runtime.
   */
  public synchronized void install() {
    if (installed) return;
    try {
      Field field = InetAddress.class.getDeclaredField("nameServices");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<NameService> nameServices = (List<NameService>) field.get(null);
      nameServices.add(0, new NSImpl());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    installed = true;
  }

  private class NSImpl implements NameService {
    @Override
    public InetAddress[] lookupAllHostAddr(String host)
        throws UnknownHostException {

      InetAddress inetAddress = forwardResolutions.get(host);
      if (inetAddress != null) {
        return new InetAddress[]{inetAddress};
      }

      // JDK chains to the next provider automatically.
      throw new UnknownHostException();
    }

    @Override
    public String getHostByAddr(byte[] addr) throws UnknownHostException {
      if (addr[0] == 127) {
        return InetAddresses.toAddrString(InetAddress.getByAddress(addr));
      }

      String hostname = reverseResolutions.get(InetAddress.getByAddress(addr));
      if (hostname != null) {
        return hostname;
      }

      throw new UnknownHostException();
    }
  }
}
