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

import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;

/**
 * Tests that the 'real user' field of the security context is used for
 * SASL PLAIN negotiations, and is imported from the SecurityCredentialsPB.
 */
public class TestSecurityContextRealUser {
  private String tableName;

  private static final MiniKuduClusterBuilder clusterBuilder =
      KuduTestHarness.getBaseClusterBuilder()
      // This test requires a delicate setup. We enable Kerberos, make
      // authentication optional, and set the superuser ACL to test-admin so that
      // the external mini-cluster is able to connect to the master while creating
      // the cluster. The user ACL is scoped to a different user so that we can
      // test real user name propagation.
      .enableKerberos()
      .addMasterServerFlag("--user-acl=token-user")
      .addMasterServerFlag("--superuser-acl=test-admin")
      .addMasterServerFlag("--rpc-authentication=optional")
      .addMasterServerFlag("--rpc-trace-negotiation")
      .addTabletServerFlag("--user-acl=token-user")
      .addTabletServerFlag("--superuser-acl=test-admin")
      .addTabletServerFlag("--rpc-authentication=optional")
      .addTabletServerFlag("--rpc-trace-negotiation");

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

  @Before
  public void setTableName() {
    tableName = TestSecurityContextRealUser.class.getName() + "-" + System.currentTimeMillis();
  }

  @Test
  public void test() throws Exception {
    // Clear out the Kerberos credentials in the environment.
    harness.kdestroy();

    // Create a new client instance with the logged in user, and ensure that it
    // fails to connect (the logged in user is not in the user-acl).
    try (KuduClient client =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
      client.listTabletServers();
      fail();
    } catch (KuduException e) {
      // TODO(KUDU-2344): This should fail with NotAuthorized.
      assertTrue(e.getStatus().toString(), e.getStatus().isServiceUnavailable());
    }

    // Try again with a correct real user.
    try (KuduClient client =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
      Client.AuthenticationCredentialsPB credentials =
          Client.AuthenticationCredentialsPB.newBuilder().setRealUser("token-user").build();
      client.importAuthenticationCredentials(credentials.toByteArray());
      client.listTabletServers();

      // Smoke-test tserver connection by scanning a table.
      KuduTable table = client.createTable(tableName, getBasicSchema(),
                                           new CreateTableOptions().setRangePartitionColumns(
                                               new ArrayList<>()));
      assertEquals(0, scanTableToStrings(table).size());
    }
  }
}
