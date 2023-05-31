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

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.apache.kudu.test.junit.AssertHelpers.assertEventuallyTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.util.Arrays;

import com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.cluster.FakeDNS;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;
import org.apache.kudu.test.junit.AssertHelpers.BooleanExpression;

public class TestNegotiation {
  private static final MiniKuduClusterBuilder clusterBuilder =
      new MiniKuduClusterBuilder()
          .numMasterServers(1)
          .numTabletServers(0)
          .enableKerberos();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

  /**
   * Test that a non-Kerberized client will use SASL PLAIN to connect to a
   * Kerberized server which doesn't require authentication. Regression test for
   * KUDU-2121.
   */
  @Test
  @MasterServerConfig(flags = {
      "--rpc-authentication=optional",
      "--rpc-trace-negotiation",
      "--user-acl=*" })
  public void testSaslPlainFallback() throws Exception {
    FakeDNS.getInstance().install();

    CapturingLogAppender cla = new CapturingLogAppender();
    harness.kdestroy();
    harness.resetClients();
    try (Closeable c = cla.attach()) {
      assertTrue(harness.getClient().getTablesList().getTablesList().isEmpty());
    }
    assertTrue(cla.getAppendedText(),
               cla.getAppendedText().contains("Client requested to use mechanism: PLAIN"));
  }

  /**
   * When JWT is enabled on the server, a client with a valid JWT should be
   * able to connect using the provided JSON Web Token to authenticate to Kudu
   * servers (Kudu master in this particular case).
   *
   * In other words, when Kudu client has JWT and trusts the server's TLS
   * certificate, the client and the server should negotiate a connection using
   * the JSON Web Token provided by the client.
   */
  @Test
  @MasterServerConfig(flags = {
      "--enable-jwt-token-auth",
      "--rpc-trace-negotiation",
  })
  public void testJwtAuthnWithTrustedCert() throws Exception {
    FakeDNS.getInstance().install();
    CapturingLogAppender cla = new CapturingLogAppender();

    // The test harness might have the client already connected to the test
    // cluster.
    harness.resetClients();
    KuduClient client = harness.getClient();
    String jwt = harness.createJwtFor("account-id", "kudu", true);
    assertNotNull(jwt);
    client.jwt(jwt);

    waitForClusterCACert();
    final byte[] caCert = harness.getClusterCACertDer();
    assertNotEquals(0, caCert.length);
    client.trustedCertificates(Arrays.asList(ByteString.copyFrom(caCert)));

    try (Closeable c = cla.attach()) {
      // A simple call to make sure the client has connected to the cluster.
      // Success here assumes that the RPC connection to the Kudu server
      // has been successfully negotiated.
      assertFalse(client.tableExists("nonexistent"));
    }

    // Make sure the parties used JWT authn mechanism to negotiate the connection.
    assertTrue(cla.getAppendedText(),
               cla.getAppendedText().contains("Negotiated authn=JWT"));
  }

  @Test
  @MasterServerConfig(flags = {
      "--enable-jwt-token-auth",
      "--rpc-authentication=required",
      "--rpc-negotiation-timeout-ms=1500",
      "--rpc-trace-negotiation",
  })
  public void testJwtAuthnWithoutTrustedCert() throws Exception {
    FakeDNS.getInstance().install();
    CapturingLogAppender cla = new CapturingLogAppender();

    harness.kdestroy();
    harness.resetClients();

    // Create a special client with short timeout for RPCs. This is a bit funny,
    // but due to the way how ConnectToMaster is implemented in the Java client,
    // there isn't a simple way to stop the client to negotiate a connection
    // again and again, unless the overall RPC times out. A connection closure
    // upon negotiation failure is being interpreted as NetworkError, and that's
    // a recoverable exception, so the operation is retried again and again.
    //
    // For faster test runs, the RPC timeout is set lower than the RPC connection
    // negotiation timeout, while the latter is set lower than its default value
    // (see the MasterServerConfig for the test). However, to prevent test
    // flakiness, it's necessary to have at least connection negotiation attempt
    // before the RPC times out.
    AsyncKuduClient asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(
        harness.getMasterAddressesAsString())
        .defaultAdminOperationTimeoutMs(1000)
        .defaultOperationTimeoutMs(1000)
        .build();
    KuduClient client = asyncClient.syncClient();

    // Provide the client with a valid JWT.
    String jwt = harness.createJwtFor("account-id", "kudu", true);
    assertNotNull(jwt);
    client.jwt(jwt);

    try (Closeable c = cla.attach()) {
      // It doesn't matter what method to call here: ConnectToMaster should not
      // succeed, so the corresponding RPC won't be invoked anyway.
      assertFalse(client.tableExists("nonexistent"));
      fail();
    } catch (NonRecoverableException ex) {
      // Java client reports SERVICE_UNAVAILABLE in this case.
      //
      // TODO(aserbin): is this a bug? should it be fixed?
      assertTrue(ex.getStatus().isServiceUnavailable());
    }

    // Make sure the parties aren't using JWT authn mechanism to negotiate the
    // connection since the client shouldn't be willing to send its JWT to a
    // non-authenticated Kudu server. As of now, the parties are using the SASL
    // authn mechanism in current implementation, but that's not an invariant
    // to enforce, so it's not asserted here.
    assertFalse(cla.getAppendedText(), cla.getAppendedText().contains(
        "Negotiated authn=JWT"));
    assertTrue(cla.getAppendedText(), cla.getAppendedText().contains(
        "server requires authentication, but client does not have Kerberos credentials (tgt)."));
    assertTrue(cla.getAppendedText(), cla.getAppendedText().contains(
        "Authentication tokens were not used because no token is available]"));
  }

  /**
   * Try to authenticate with a valid JWT by mismatched account/principal name.
   * An RPC connection to the server will be established successfully, but
   * the client will fail to invoke the ConnectToMaster RPC because of
   * NotAuthorized error from the coarse-grain authz subsystem.
   */
  @Test
  @MasterServerConfig(flags = {
      "--enable-jwt-token-auth",
      "--rpc-trace-negotiation",
  })
  public void testValidJwtButWrongSubject() throws Exception {
    FakeDNS.getInstance().install();
    CapturingLogAppender cla = new CapturingLogAppender();

    // The test harness might have the client already connected to the test
    // cluster.
    harness.resetClients();
    KuduClient client = harness.getClient();
    String jwt = harness.createJwtFor("account-id", "interloper", true);
    assertNotNull(jwt);
    client.jwt(jwt);

    waitForClusterCACert();
    final byte[] caCert = harness.getClusterCACertDer();
    assertNotEquals(0, caCert.length);
    client.trustedCertificates(Arrays.asList(ByteString.copyFrom(caCert)));

    try (Closeable c = cla.attach()) {
      // It doesn't matter what method to call here: ConnectToMaster should not
      // succeed, so the corresponding RPC won't be invoked anyway.
      client.tableExists("nonexistent");
      fail();
    } catch (NonRecoverableException ex) {
      // That's a bit funny, but Java client reports SERVICE_UNAVAILABLE in this
      // case when failing to call a remote method due to NotAuthorized error
      // code returned by Kudu master.
      //
      // TODO(aserbin): is this a bug? should it be fixed?
      assertTrue(ex.getStatus().isServiceUnavailable());
      assertTrue(ex.getMessage().contains(
          "Not authorized: unauthorized access to method: ConnectToMaster"));
    }

    // Make sure the parties used JWT authn mechanism to successfully negotiate
    // the connection, even if the coarse-grained authz check rejected a remote
    // call of one of the API methods.
    assertTrue(cla.getAppendedText(),
        cla.getAppendedText().contains("Negotiated authn=JWT"));
  }

  /**
   * Try to authenticate with an invalid JWT. The connection negotiation
   * should fail because the server should not be able to verify the invalid JWT
   * that the client provided.
   */
  @Test
  @MasterServerConfig(flags = {
      "--enable-jwt-token-auth",
      "--rpc-negotiation-timeout-ms=1500",
      "--rpc-trace-negotiation",
  })
  public void testInvalidJwt() throws Exception {
    FakeDNS.getInstance().install();
    CapturingLogAppender cla = new CapturingLogAppender();

    // Create a special client with short timeout for RPCs. This is a bit funny,
    // but due to the way how ConnectToMaster is implemented in the Java client,
    // there isn't a simple way to stop the client to negotiate a connection
    // again and again, unless the overall RPC times out.
    //
    // For faster test runs, the RPC timeout is set lower than the RPC connection
    // negotiation timeout, while the latter is set lower than its default value
    // (see the MasterServerConfig for the test). However, to prevent test
    // flakiness, it's necessary to have at least connection negotiation attempt
    // before the RPC times out.
    //
    // TODO(aserbin): fix ConnectToMaster and stop negotiation attempts upon receiving NotAuthorized
    AsyncKuduClient asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(
        harness.getMasterAddressesAsString())
        .defaultAdminOperationTimeoutMs(1000)
        .defaultOperationTimeoutMs(1000)
        .build();
    KuduClient client = asyncClient.syncClient();

    String jwt = harness.createJwtFor("account-id", "kudu", false);
    assertNotNull(jwt);
    client.jwt(jwt);

    waitForClusterCACert();
    final byte[] caCert = harness.getClusterCACertDer();
    assertNotEquals(0, caCert.length);
    client.trustedCertificates(Arrays.asList(ByteString.copyFrom(caCert)));

    try (Closeable c = cla.attach()) {
      // It doesn't matter what method to call here: ConnectToMaster should not
      // succeed, so the corresponding RPC won't be invoked anyway.
      client.tableExists("nonexistent");
      fail();
    } catch (NonRecoverableException ex) {
      assertTrue(ex.getStatus().isTimedOut());
    }

    assertTrue(cla.getAppendedText(),cla.getAppendedText().contains(
        "Negotiated authn=JWT"));
    assertTrue(cla.getAppendedText(), cla.getAppendedText().contains(
        "Negotiation complete: Not authorized: Server connection negotiation failed"));
    assertTrue(cla.getAppendedText(), cla.getAppendedText().contains(
        "FATAL_INVALID_JWT: Not authorized: JWT verification failed: failed to verify signature"));
    assertTrue(cla.getAppendedText(), cla.getAppendedText().contains(
        "Unable to connect to master"));
    assertTrue(cla.getAppendedText(), cla.getAppendedText().contains(
        "connection closed"));
  }

  private void waitForClusterCACert() throws Exception {
    // It may take some time for the catalog manager to initialize
    // and have IPKI CA certificate ready.
    assertEventuallyTrue(
        "valid cluster IPKI CA certificate captured",
        new BooleanExpression() {
          @Override
          public boolean get() throws Exception {
            return harness.getClusterCACertDer().length != 0;
          }
        },
        10000/*timeoutMillis*/);
  }
}
