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

import static org.junit.Assert.assertNotNull;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.kudu.util.SecurityUtil;

public class TestSecurity extends BaseKuduTest {

  private static final String TABLE_NAME = "TestSecurity-table";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    miniClusterBuilder.enableKerberos()
    .addMasterFlag("--rpc_trace_negotiation");

    BaseKuduTest.setUpBeforeClass();
  }

  /**
   * Test that a client can export its authentication data and pass it to
   * a new client which has no Kerberos credentials, which will then
   * be able to authenticate to the masters and tablet servers using tokens.
   */
  @Test
  public void testImportExportAuthenticationCredentials() throws Exception {
    byte[] authnData = client.exportAuthenticationCredentials().join();
    assertNotNull(authnData);
    String oldTicketCache = System.getProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    System.clearProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY);
    try {
      KuduClient newClient = new KuduClient.KuduClientBuilder(masterAddresses).build();

      // Test that a client with no credentials cannot list servers.
      try {
        newClient.listTabletServers();
        Assert.fail("should not have been able to connect to a secure cluster " +
            "with no credentials");
      } catch (NonRecoverableException e) {
        Assert.assertTrue(e.getMessage().contains("server requires authentication, " +
            "but client does not have Kerberos credentials available"));
      }

      // If we import the authentication data from the old authenticated client,
      // we should now be able to perform all of the normal client operations.
      newClient.importAuthenticationCredentials(authnData);
      KuduTable table = newClient.createTable(TABLE_NAME, basicSchema,
          getBasicCreateTableOptions());
      KuduSession session = newClient.newSession();
      session.apply(createBasicSchemaInsert(table, 1));
      session.flush();
    } finally {
      // Restore ticket cache for other test cases.
      System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY, oldTicketCache);
    }
  }
}
