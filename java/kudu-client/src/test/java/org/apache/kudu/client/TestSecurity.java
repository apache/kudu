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

import static org.apache.kudu.util.AssertHelpers.assertEventuallyTrue;
import static org.apache.kudu.util.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.util.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.util.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.apache.kudu.client.Client.AuthenticationCredentialsPB;
import org.apache.kudu.client.MiniKuduCluster.MiniKuduClusterBuilder;
import org.apache.kudu.junit.RetryRule;
import org.apache.kudu.master.Master.ConnectToMasterResponsePB;
import org.apache.kudu.util.AssertHelpers;
import org.apache.kudu.util.AssertHelpers.BooleanExpression;
import org.apache.kudu.util.CapturingLogAppender;
import org.apache.kudu.util.SecurityUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.stumbleupon.async.Deferred;

public class TestSecurity {
  private static final String TABLE_NAME = "TestSecurity-table";
  private static final int TICKET_LIFETIME_SECS = 10;
  private static final int RENEWABLE_LIFETIME_SECS = 20;

  private final CapturingLogAppender cla = new CapturingLogAppender();
  private MiniKuduCluster miniCluster;
  private KuduClient client;

  private enum Option {
    LONG_LEADER_ELECTION,
    SHORT_TOKENS_AND_TICKETS,
    START_TSERVERS
  };

  private void startCluster(Set<Option> opts) throws IOException {
    MiniKuduClusterBuilder mcb = new MiniKuduClusterBuilder();
    mcb.enableKerberos();
    if (opts.contains(Option.LONG_LEADER_ELECTION)) {
      mcb.addMasterServerFlag("--leader_failure_max_missed_heartbeat_periods=10.0");
    }
    if (opts.contains(Option.SHORT_TOKENS_AND_TICKETS)) {
      mcb.addMasterServerFlag("--authn_token_validity_seconds=" + TICKET_LIFETIME_SECS)
         .kdcRenewLifetime(RENEWABLE_LIFETIME_SECS + "s")
         .kdcTicketLifetime(TICKET_LIFETIME_SECS + "s");
    }
    miniCluster = mcb.numMasterServers(3)
        .numTabletServers(opts.contains(Option.START_TSERVERS) ? 3 : 0)
        .build();
    miniCluster.kinit("test-admin");
    client = new KuduClient.KuduClientBuilder(miniCluster.getMasterAddressesAsString()).build();

    // TODO(todd): it seems that exportAuthenticationCredentials() doesn't properly retry
    // in the case that there is no leader, even though NoLeaderFoundException is a RecoverableException.
    // So, we have to use a hack of calling listTabletServers, which _does_ properly retry,
    // in order to wait for the masters to elect a leader.
    client.listTabletServers();
  }

  // Add a rule to rerun tests. We use this with Gradle because it doesn't support
  // Surefire/Failsafe rerunFailingTestsCount like Maven does.
  @Rule
  public RetryRule retryRule = new RetryRule();

  @Before
  public void setUp() {
    FakeDNS.getInstance().install();
  }

  @After
  public void tearDown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
  }

  /**
   * Create a KuduClient associated with the given Subject.
   */
  private KuduClient createClientFromSubject(Subject subject)
      throws PrivilegedActionException {
    return Subject.doAs(subject, new PrivilegedExceptionAction<KuduClient>() {
      @Override
      public KuduClient run() throws Exception {
        return createClient();
      }
    });
  }

  private KuduClient createClient() {
    return new KuduClient.KuduClientBuilder(miniCluster.getMasterAddressesAsString()).build();
  }

  private void checkClientCanReconnect(KuduClient client) throws IOException {
    // Cycle the masters to ensure that we have to re-connect and thus
    // re-negotiate an authenticated RPC connection. Without this step,
    // we'd just hang onto our existing authenticated connections which
    // would continue to work even though our credentials might have
    // expired (we only authenticate when a connection is negotiated, not
    // for each call).
    miniCluster.killAllMasterServers();
    miniCluster.startAllMasterServers();
    client.listTabletServers();
  }

  /**
   * Test that a client can export its authentication data and pass it to
   * a new client which has no Kerberos credentials, which will then
   * be able to authenticate to the masters and tablet servers using tokens.
   */
  @Test
  public void testImportExportAuthenticationCredentials() throws Exception {
    startCluster(ImmutableSet.of(Option.SHORT_TOKENS_AND_TICKETS,
        Option.START_TSERVERS));
    byte[] authnData = client.exportAuthenticationCredentials();
    assertNotNull(authnData);
    String oldTicketCache = System.getProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    System.clearProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    try {
      KuduClient newClient = new KuduClient.KuduClientBuilder(
          miniCluster.getMasterAddressesAsString()).build();

      // Test that a client with no credentials cannot list servers.
      try {
        newClient.listTabletServers();
        Assert.fail("should not have been able to connect to a secure cluster " +
            "with no credentials");
      } catch (NonRecoverableException e) {
        Assert.assertThat(e.getMessage(), CoreMatchers.containsString(
            "server requires authentication, but client does not have " +
            "Kerberos credentials (tgt). Authentication tokens were not used " +
            "because no token is available"));
      }

      // If we import the authentication data from the old authenticated client,
      // we should now be able to perform all of the normal client operations.
      newClient.importAuthenticationCredentials(authnData);
      KuduTable table = newClient.createTable(TABLE_NAME, getBasicSchema(),
          getBasicCreateTableOptions());
      KuduSession session = newClient.newSession();
      session.apply(createBasicSchemaInsert(table, 1));
      session.flush();
    } finally {
      // Restore ticket cache for other test cases.
      System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY, oldTicketCache);
    }
  }

  /**
   * Regression test for KUDU-2379: if the first usage of a client
   * is to export credentials, that should trigger a connection to the
   * cluster rather than returning empty credentials.
   */
  @Test(timeout=60000)
  public void testExportCredentialsBeforeAnyOtherAccess() throws IOException {
    startCluster(ImmutableSet.<Option>of());
    try (KuduClient c = createClient()) {
      AuthenticationCredentialsPB pb = AuthenticationCredentialsPB.parseFrom(
          c.exportAuthenticationCredentials());
      Assert.assertTrue(pb.hasAuthnToken());
      Assert.assertTrue(pb.getCaCertDersCount() > 0);
    }
  }

  /**
   * Test that if, for some reason, the client has a token but no CA certs, it
   * will emit an appropriate error message in the exception.
   */
  @Test
  public void testErrorMessageWithNoCaCert() throws Exception {
    startCluster(ImmutableSet.of(Option.SHORT_TOKENS_AND_TICKETS));
    byte[] authnData = client.exportAuthenticationCredentials();

    // Remove the CA certs from the credentials.
    authnData = AuthenticationCredentialsPB.parseFrom(authnData).toBuilder()
        .clearCaCertDers().build().toByteArray();

    String oldTicketCache = System.getProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    System.clearProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    try {
      KuduClient newClient = createClient();
      newClient.importAuthenticationCredentials(authnData);

      // We shouldn't be able to connect because we have no appropriate CA cert.
      try {
        newClient.listTabletServers();
        Assert.fail("should not have been able to connect to a secure cluster " +
            "with no credentials");
      } catch (NonRecoverableException e) {
        Assert.assertThat(e.getMessage(), CoreMatchers.containsString(
            "server requires authentication, but client does not have " +
            "Kerberos credentials (tgt). Authentication tokens were not used " +
            "because no TLS certificates are trusted by the client"));
      }
    } finally {
      // Restore ticket cache for other test cases.
      System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY, oldTicketCache);
    }
  }

  /**
   * Regression test for KUDU-2267 and KUDU-2319.
   *
   * A client with valid token but without valid Kerberos credentials
   * should be able to connect to all the masters.
   */
  @Test
  public void testKudu2267() throws Exception {
    startCluster(ImmutableSet.of(Option.SHORT_TOKENS_AND_TICKETS));
    byte[] authnData = client.exportAuthenticationCredentials();
    assertNotNull(authnData);
    String oldTicketCache = System.getProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    System.clearProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    try {
      final KuduClient newClient = createClient();
      newClient.importAuthenticationCredentials(authnData);

      // Try to connect to all the masters and assert there is no
      // authentication failures.
      assertEventuallyTrue("Not able to connect to all the masters",
          new BooleanExpression() {
            @Override
            public boolean get() throws Exception {
              ConnectToCluster connector = new ConnectToCluster(miniCluster.getMasterServers());
              List<Deferred<ConnectToMasterResponsePB>> deferreds =
                      connector.connectToMasters(newClient.asyncClient.getMasterTable(), null,
                      /* timeout = */50000,
                      Connection.CredentialsPolicy.ANY_CREDENTIALS);
              // Wait for all Deferreds are called back.
              for (Deferred<ConnectToMasterResponsePB> deferred : deferreds) {
                deferred.join();
              }
              List<Exception> s = connector.getExceptionsReceived();
              return s.size() == 0;
            }
      }, /* timeoutMillis = */50000);
    } finally {
      System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY, oldTicketCache);
    }
  }

  /**
   * Test that a client is able to connect to masters using valid tokens
   * after all masters were killed and restarted, and before a leader is
   * elected. Leader election time is configured to be long enough using
   * '--leader_failure_max_missed_heartbeat_periods'.
   */
  @Test
  public void testConnectToNonLeaderMasters() throws Exception {
    startCluster(ImmutableSet.of(Option.LONG_LEADER_ELECTION));
    System.err.println("=> started cluster");
    byte[] authnData = client.exportAuthenticationCredentials();
    System.err.println("=> exported auth");
    assertNotNull(authnData);
    String oldTicketCache = System.getProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    System.clearProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    try {
      KuduClient newClient = createClient();
      newClient.importAuthenticationCredentials(authnData);
      System.err.println("=> imported auth");

      miniCluster.killAllMasterServers();
      miniCluster.startAllMasterServers();
      newClient.listTabletServers();
      System.err.println("=> listTabletServers");
    } finally {
      System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY, oldTicketCache);
    }
  }

  /**
   * Test that, if our Kerberos credentials expire, that we will automatically
   * re-login from an available ticket cache.
   */
  @Test(timeout=300000)
  public void testRenewAndReacquireKeberosCredentials() throws Exception {
    startCluster(ImmutableSet.of(Option.SHORT_TOKENS_AND_TICKETS));
    Stopwatch timeSinceKinit = Stopwatch.createStarted();
    try (Closeable c = cla.attach()) {
      for (Stopwatch sw = Stopwatch.createStarted();
           sw.elapsed(TimeUnit.SECONDS) < RENEWABLE_LIFETIME_SECS * 2;) {
        if (timeSinceKinit.elapsed(TimeUnit.SECONDS) > TICKET_LIFETIME_SECS + 2) {
          // We have gotten past the initial lifetime and well into the renewable
          // lifetime. If we haven't failed yet, that means that Kudu
          // successfully renewed the ticket.
          //
          // We can now re-kinit to get a new ticket, to ensure that Kudu
          // will properly re-login from the on-disk cache when its in-memory
          // ticket is no longer renewable.
          miniCluster.kinit("test-admin");
          timeSinceKinit.reset().start();
        }
        Thread.sleep(1000);
        // Ensure that we don't use an authentication token to reconnect.
        client.asyncClient.securityContext.setAuthenticationToken(null);
        checkClientCanReconnect(client);
      }
    }
    Assert.assertThat(cla.getAppendedText(), CoreMatchers.containsString(
        "Successfully refreshed Kerberos credentials from ticket cache"));
  }

  /**
   * Test that, if the ticket cache is refreshed but contains a different principal
   * from the original one, we will not accept it.
   */
  @Test(timeout=300000)
  public void testDoNotSwitchPrincipalsInExistingClient() throws Exception {
    startCluster(ImmutableSet.of(Option.SHORT_TOKENS_AND_TICKETS));
    // Switch the ticket cache to a different user.
    miniCluster.kinit("test-user");
    try (Closeable c = cla.attach()) {
      // We should eventually fail to connect because the initial credentials will
      // have expired and the client should refuse to refresh credentials with a
      // different principal.
      assertEventualAuthenticationFailure(client,
          "server requires authentication, but " +
          "client Kerberos credentials (TGT) have expired");
    }
    Assert.assertThat(cla.getAppendedText(), CoreMatchers.containsString(
        "found that the new Kerberos principal test-user@KRBTEST.COM " +
        "did not match the original principal test-admin@KRBTEST.COM"));
  }

  private void assertEventualAuthenticationFailure(
      final KuduClient client,
      final String exceptionSubstring) throws Exception {
    AssertHelpers.assertEventuallyTrue("should eventually fail to connect",
        new BooleanExpression() {
          @Override
          public boolean get() throws Exception {
            Thread.sleep(3000);
            miniCluster.killAllMasterServers();
            miniCluster.startAllMasterServers();
            try {
              client.listTabletServers();
            } catch (Exception e) {
              if (e.toString().contains(exceptionSubstring)) {
                return true;
              }
              throw e;
            }
            return false;
          }
    }, 60000);
  }

  /**
   * Test that, if an externally-provided subject is used when the client
   * is created, the client will not attempt to refresh anything, and will
   * eventually fail with appropriate warnings in the log.
   */
  @Test(timeout=300000)
  public void testExternallyProvidedSubjectExpires() throws Exception {
    startCluster(ImmutableSet.of(Option.SHORT_TOKENS_AND_TICKETS));
    Subject subject = SecurityUtil.getSubjectFromTicketCacheOrNull();
    Assert.assertNotNull(subject);
    try (Closeable c = cla.attach()) {
      // Create a client attached to our own subject.
      KuduClient newClient = createClientFromSubject(subject);
      // It should not get auto-refreshed.
      try {
        assertEventualAuthenticationFailure(newClient,
            "server requires authentication, but " +
            "client Kerberos credentials (TGT) have expired");
      } finally {
        newClient.close();
      }
    }
    Assert.assertThat(cla.getAppendedText(), CoreMatchers.containsString(
        "Using caller-provided subject with Kerberos principal test-admin@KRBTEST.COM."));
    Assert.assertThat(cla.getAppendedText(), CoreMatchers.containsString(
        "Caller-provided Subject has a Kerberos ticket that is about to expire"));
  }

  /**
   * Test that, so long as we are periodically renewing a caller-provided Subject's
   * credentials, the client will continue to operate fine.
   *
   * This simulates the case of using the Kudu client from an application using
   * the UserGroupInformation class from Hadoop, which spawns a thread to
   * renew credentials from a keytab.
   */
  @Test(timeout=300000)
  public void testExternallyProvidedSubjectRefreshedExternally() throws Exception {
    startCluster(ImmutableSet.of(Option.SHORT_TOKENS_AND_TICKETS));

    Subject subject = SecurityUtil.getSubjectFromTicketCacheOrNull();
    Assert.assertNotNull(subject);
    try (Closeable c = cla.attach()) {
      // Create a client attached to our own subject.
      KuduClient newClient = createClientFromSubject(subject);
      // Run for longer than the renewable lifetime - this ensures that we
      // are indeed picking up the new credentials.
      for (Stopwatch sw = Stopwatch.createStarted();
          sw.elapsed(TimeUnit.SECONDS) < RENEWABLE_LIFETIME_SECS + 5;
          Thread.sleep(1000)) {
        miniCluster.kinit("test-admin");

        // Update the existing subject in-place by copying over the credentials from
        // a newly logged-in subject.
        Subject newSubject = SecurityUtil.getSubjectFromTicketCacheOrNull();
        subject.getPrivateCredentials().clear();
        subject.getPrivateCredentials().addAll(newSubject.getPrivateCredentials());
        // Ensure that we don't use an authentication token to reconnect.
        newClient.asyncClient.securityContext.setAuthenticationToken(null);
        checkClientCanReconnect(newClient);
      }
    }
    Assert.assertThat(cla.getAppendedText(), CoreMatchers.containsString(
        "Using caller-provided subject with Kerberos principal test-admin@KRBTEST.COM."));
  }
}
