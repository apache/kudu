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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kudu.test.ClientTestUtil.createDefaultTable;
import static org.apache.kudu.test.ClientTestUtil.loadDefaultTable;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.apache.kudu.test.TempDirUtils;
import org.apache.kudu.test.cluster.KuduBinaryLocator;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;

// This is a class for Kudu RPC connection negotiation test scenarios targeting
// TLSv1.3. See TestNegotiator for pre-TLSv1.3 test scenarios.
public class TestNegotiationTLSv13 {

  static final String[] TLS13_CIPHERS = new String[]{
      "TLS_AES_128_GCM_SHA256",
      "TLS_AES_256_GCM_SHA384",
      "TLS_CHACHA20_POLY1305_SHA256",
  };

  private static final Logger LOG = LoggerFactory.getLogger(TestNegotiation.class);
  private static final String TABLE_NAME = "tls_v_1_3_test_table";
  private static final int NUM_ROWS = 10;

  private final MiniKuduClusterBuilder clusterBuilder;

  @Rule
  public KuduTestHarness harness;

  // Whether TLSv1.3 supported by both server and client side.
  private boolean isTLSv13Supported = false;

  // Check if TLSv1.3 is supported by the JVM.
  private static boolean isTLSv13SupportedByJVM() {
    // It seems some policy-related globals are initialized due to the
    // SSLContext.getInstance("TLSv1.3") call below, so server certificates
    // signed by 768-bit RSA keys aren't accepted later on when running test
    // scenarios due to default security policies. To work around that, override
    // the default security constraints the same way it's done
    // in the MiniKuduCluster's constructor.
    Security.setProperty("jdk.certpath.disabledAlgorithms", "MD2, RC4, MD5");
    Security.setProperty("jdk.tls.disabledAlgorithms", "SSLv3, RC4, MD5");
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, null, null);
      SSLEngine engine = ctx.createSSLEngine();
      engine.setUseClientMode(true);
      {
        Set<String> supported = Sets.newHashSet(engine.getSupportedCipherSuites());
        List<String> common = Lists.newArrayList();
        for (String c : TLS13_CIPHERS) {
          if (supported.contains(c)) {
            common.add(c);
          }
        }
        if (common.isEmpty()) {
          LOG.info("client side doesn't support TLSv1.3: no common ciphers");
          return false;
        }
      }
      {
        String[] enabled = engine.getEnabledProtocols();
        LOG.debug("enabled TLS protocols: {}", Joiner.on(' ').join(enabled));
        Set<String> supported = Sets.newHashSet(engine.getSupportedProtocols());
        LOG.debug("supported TLS protocols: {}", Joiner.on(' ').join(supported));
        if (!supported.contains("TLSv1.3")) {
          LOG.info("client side doesn't support TLSv1.3: unsupported protocol");
          return false;
        }
      }
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      LOG.info("client side doesn't support TLSv1.3", e);
      return false;
    }
    return true;
  }

  // Check if TLSv1.3 is supported by the Kudu server side.
  private static boolean isTLSv13SupportedByServerSide() {
    // Try to start kudu-master requiring TLSv1.3.  It will fail to start if
    // TLSv1.3 isn't supported either by the node's OpenSSL library or
    // by the build environment where the kudu-master binary was built.
    MiniKuduClusterBuilder b = new MiniKuduClusterBuilder()
        .numMasterServers(1)
        .numTabletServers(0)
        .addMasterServerFlag("--time_source=system_unsync")
        .addMasterServerFlag("--rpc_tls_min_protocol=TLSv1.3");
    try (MiniKuduCluster c = b.build()) {
      try {
        // A sanity check: make sure the started processes haven't crashed.
        // MiniKuduCluster does neither detect nor report properly on such
        // events otherwise.
        c.killAllMasterServers();
      } catch (IOException e) {
        LOG.error("unexpected exception:", e);
        fail("kudu-master didn't actually start");
        return false; // unreachable
      }
    } catch (IOException e) {
      LOG.info("server side doesn't support TLSv1.3", e);
      return false;
    }
    return true;
  }

  public TestNegotiationTLSv13() {
    clusterBuilder = new MiniKuduClusterBuilder()
        .numMasterServers(1)
        .numTabletServers(3)
        .enableKerberos();

    isTLSv13Supported = isTLSv13SupportedByJVM() && isTLSv13SupportedByServerSide();
    if (isTLSv13Supported) {
      // By the virtue of excluding all other protocols but TLSv1.3
      // from the list of available TLS protocols at the server side,
      // client and server will use TLSv1.3 to negotiate a connection.
      clusterBuilder.addMasterServerFlag("--rpc_tls_min_protocol=TLSv1.3");
      clusterBuilder.addTabletServerFlag("--rpc_tls_min_protocol=TLSv1.3");
    }

    harness = new KuduTestHarness(clusterBuilder);
  }

  /**
   * Make sure that Kudu Java client is able to negotiate RPC connections
   * protected by TLSv1.3 with Kudu servers. By the virtue of excluding all
   * other protocols but TLSv1.3 from the list of available TLS protocols
   * at the server side, this scenario verifies that Kudu Java client is able to
   * work with a secure Kudu cluster using TLSv1.3.
   *
   * Using the JUnit's terminology, this test scenario is conditionally run only
   * if both the client and the server sides support TLSv1.3.
   */
  @Test
  @MasterServerConfig(flags = {
      "--rpc-encryption=required",
      "--rpc_encrypt_loopback_connections",
      "--rpc-trace-negotiation",
  })
  @TabletServerConfig(flags = {
      "--rpc-encryption=required",
      "--rpc_encrypt_loopback_connections",
      "--rpc-trace-negotiation",
  })
  public void connectionNegotiation() throws Exception {
    assumeTrue("TLSv1.3 isn't supported by both sides", isTLSv13Supported);

    // Make sure Java client is able to communicate with Kudu masters and tablet
    // servers: create a table and write several rows into the table.
    {
      KuduClient c = harness.getClient();
      createDefaultTable(c, TABLE_NAME);
      loadDefaultTable(c, TABLE_NAME, NUM_ROWS);
    }

    // An extra sanity check: on successful negotiation the connection should be
    // considered 'private' once it's protected by TLS, so Kudu master must send
    // the client an authn token.
    {
      AsyncKuduClient c = harness.getAsyncClient();
      SecurityContext ctx = c.securityContext;
      assertNotNull(ctx.getAuthenticationToken());
    }
  }
}
