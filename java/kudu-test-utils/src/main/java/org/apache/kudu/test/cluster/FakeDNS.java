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

package org.apache.kudu.test.cluster;

import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.net.InetAddresses;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

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
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FakeDNS {
  static FakeDNS instance = new FakeDNS();

  @GuardedBy("this")
  private Map<String, InetAddress> forwardResolutions = new HashMap<>();

  @GuardedBy("this")
  private Map<InetAddress, String> reverseResolutions = new HashMap<>();

  /** whether the fake resolver has been installed */
  @GuardedBy("this")
  private boolean installed = false;

  private FakeDNS() {
  }

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
    if (installed) {
      return;
    }
    try {
      try {
        // Override the NameService in Java 9 or later. On Java 18+ this class was
        // removed (replaced by java.net.spi.InetAddressResolver), so Class.forName
        // throws ClassNotFoundException and we fall through to the resolver path below.
        final Class<?> nameServiceInterface = Class.forName("java.net.InetAddress$NameService");
        Field field = InetAddress.class.getDeclaredField("nameService");
        // Get the default NameService to fallback to.
        Method method = InetAddress.class.getDeclaredMethod("createNameService");
        method.setAccessible(true);
        Object fallbackNameService = method.invoke(null);
        // Create a proxy instance to set on the InetAddress field which will handle
        // all NameService calls.
        Object proxy = Proxy.newProxyInstance(nameServiceInterface.getClassLoader(),
            new Class<?>[]{nameServiceInterface}, new NameServiceListener(fallbackNameService));
        field.setAccessible(true);
        field.set(InetAddress.class, proxy);
      } catch (ReflectiveOperationException | InaccessibleObjectException modernError) {
        // The Java 9+ NameService path is unavailable (class/field/method
        // missing) or its reflective access is blocked without --add-opens.
        // Narrowed to these expected failures so genuine programming errors
        // (NPE, ClassCastException, ...) surface instead of being silently
        // treated as "try the next JDK path".
        // Override InetAddress resolver in Java 25+.
        try {
          final Class<?> resolverInterface = Class.forName("java.net.spi.InetAddressResolver");
          Field resolverField = InetAddress.class.getDeclaredField("resolver");
          resolverField.setAccessible(true);
          // Ensure the resolver is initialized before we capture it.
          try {
            InetAddress.getByName("localhost");
          } catch (UnknownHostException e) {
            throw new AssertionError("localhost must resolve", e);
          }
          Object fallbackResolver = resolverField.get(InetAddress.class);
          if (fallbackResolver == null) {
            Field builtinResolverField = InetAddress.class.getDeclaredField("BUILTIN_RESOLVER");
            builtinResolverField.setAccessible(true);
            fallbackResolver = builtinResolverField.get(InetAddress.class);
          }
          Object proxy = Proxy.newProxyInstance(resolverInterface.getClassLoader(),
              new Class<?>[]{resolverInterface}, new NameServiceListener(fallbackResolver));
          resolverField.set(InetAddress.class, proxy);
        } catch (ReflectiveOperationException resolverError) {
          // Override the NameService in Java 8 or earlier.
          try {
            final Class<?> nameServiceInterface =
                Class.forName("sun.net.spi.nameservice.NameService");
            Field field = InetAddress.class.getDeclaredField("nameServices");
            // Get the default NameService to fallback to.
            Method method = InetAddress.class.getDeclaredMethod("createNSProvider", String.class);
            method.setAccessible(true);
            Object fallbackNameService = method.invoke(null, "default");
            // Create a proxy instance to set on the InetAddress field which will handle
            // all NameService calls.
            Object proxy = Proxy.newProxyInstance(nameServiceInterface.getClassLoader(),
                new Class<?>[]{nameServiceInterface}, new NameServiceListener(fallbackNameService));
            field.setAccessible(true);
            // Java 8 or earlier takes a list of NameServices
            field.set(InetAddress.class, Arrays.asList(proxy));
          } catch (ReflectiveOperationException legacyError) {
            // Surface the most relevant failure and preserve other attempts.
            resolverError.addSuppressed(modernError);
            legacyError.addSuppressed(resolverError);
            throw legacyError;
          }
        }
      }
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new RuntimeException(e);
    }
    installed = true;
  }

  /**
   * The NameService in all versions of Java has the same interface, so we
   * can use the same InvocationHandler as our proxy instance for both
   * java.net.InetAddress$NameService and sun.net.spi.nameservice.NameService.
   */
  private class NameServiceListener implements InvocationHandler {

    private final Object fallbackNameService;

    // Creates a NameServiceListener with a NameService implementation to
    // fallback to. The parameter is untyped so we can handle the NameService
    // type in all versions of Java with reflection.
    NameServiceListener(Object fallbackNameService) {
      this.fallbackNameService = fallbackNameService;
    }

    private InetAddress[] lookupAllHostAddr(String host) throws UnknownHostException {
      InetAddress inetAddress;
      synchronized (FakeDNS.this) {
        inetAddress = forwardResolutions.get(host);
      }
      if (inetAddress != null) {
        return new InetAddress[]{inetAddress};
      }

      try {
        Method method = fallbackNameService.getClass()
            .getDeclaredMethod("lookupAllHostAddr", String.class);
        method.setAccessible(true);
        return (InetAddress[]) method.invoke(fallbackNameService, host);
      } catch (NoSuchMethodException e) {
        try {
          Class<?> lookupPolicyClass =
              Class.forName("java.net.spi.InetAddressResolver$LookupPolicy");
          Method method = fallbackNameService.getClass()
              .getDeclaredMethod("lookupByName", String.class, lookupPolicyClass);
          method.setAccessible(true);
          Field policyField = InetAddress.class.getDeclaredField("PLATFORM_LOOKUP_POLICY");
          policyField.setAccessible(true);
          Object lookupPolicy = policyField.get(InetAddress.class);
          @SuppressWarnings("unchecked")
          Stream<InetAddress> stream = (Stream<InetAddress>) method.invoke(
              fallbackNameService, host, lookupPolicy);
          try (Stream<InetAddress> ignored = stream) {
            return stream.toArray(InetAddress[]::new);
          }
        } catch (ReflectiveOperationException nested) {
          Throwable cause = nested.getCause();
          if (cause instanceof UnknownHostException) {
            throw (UnknownHostException) cause;
          }
          throw new AssertionError("unexpected reflection issue", nested);
        }
      } catch (ReflectiveOperationException e) {
        Throwable cause = e.getCause();
        // Preserve the behavior of the former
        // Throwables.propagateIfPossible(cause, UnknownHostException.class):
        // rethrow UnknownHostException, RuntimeException, and Error unwrapped.
        if (cause instanceof UnknownHostException) {
          throw (UnknownHostException) cause;
        }
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new AssertionError("unexpected reflection issue", e);
      }
    }

    private String getHostByAddr(byte[] addr) throws UnknownHostException {
      if (addr[0] == 127) {
        return InetAddresses.toAddrString(InetAddress.getByAddress(addr));
      }

      String hostname;
      synchronized (FakeDNS.this) {
        hostname = reverseResolutions.get(InetAddress.getByAddress(addr));
      }
      if (hostname != null) {
        return hostname;
      }

      try {
        Method method = fallbackNameService.getClass()
            .getDeclaredMethod("getHostByAddr", byte[].class);
        method.setAccessible(true);
        return (String) method.invoke(fallbackNameService, (Object) addr);
      } catch (NoSuchMethodException e) {
        try {
          Method method = fallbackNameService.getClass()
              .getDeclaredMethod("lookupByAddress", byte[].class);
          method.setAccessible(true);
          return (String) method.invoke(fallbackNameService, (Object) addr);
        } catch (ReflectiveOperationException nested) {
          Throwable cause = nested.getCause();
          if (cause instanceof UnknownHostException) {
            throw (UnknownHostException) cause;
          }
          throw new AssertionError("unexpected reflection issue", nested);
        }
      } catch (ReflectiveOperationException e) {
        Throwable cause = e.getCause();
        // Preserve the behavior of the former
        // Throwables.propagateIfPossible(cause, UnknownHostException.class):
        // rethrow UnknownHostException, RuntimeException, and Error unwrapped.
        if (cause instanceof UnknownHostException) {
          throw (UnknownHostException) cause;
        }
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new AssertionError("unexpected reflection issue", e);
      }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
        case "lookupAllHostAddr":
          return lookupAllHostAddr((String) args[0]);
        case "lookupByName":
          return Arrays.stream(lookupAllHostAddr((String) args[0]));
        case "getHostByAddr":
        case "lookupByAddress":
          return getHostByAddr((byte[]) args[0]);
        default:
          throw new UnsupportedOperationException();
      }
    }
  }
}
