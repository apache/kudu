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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.apache.kudu.util.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.util.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that the 'real user' field of the security context is used for
 * SASL PLAIN negotiations, and is imported from the SecurityCredentialsPB.
 */
public class TestSecurityContextRealUser extends BaseKuduTest {
  private String tableName;

  @Override
  protected MiniKuduCluster.MiniKuduClusterBuilder getMiniClusterBuilder() {
    return super.getMiniClusterBuilder()
        // This test requires a delicate setup. We enable Kerberos, make
        // authentication optional, and set the superuser ACL to test-admin so that
        // the external mini-cluster is able to connect to the master while creating
        // the cluster. The user ACL is scoped to a different user so that we can
        // test real user name propagation.
        .enableKerberos()
        .addMasterFlag("--user-acl=token-user")
        .addMasterFlag("--superuser-acl=test-admin")
        .addMasterFlag("--rpc-authentication=optional")
        .addMasterFlag("--rpc-trace-negotiation")
        .addTserverFlag("--user-acl=token-user")
        .addTserverFlag("--superuser-acl=test-admin")
        .addTserverFlag("--rpc-authentication=optional")
        .addTserverFlag("--rpc-trace-negotiation");
  }

  @Before
  public void setTableName() {
    tableName = TestSecurityContextRealUser.class.getName() + "-" + System.currentTimeMillis();
  }

  @Test
  public void test() throws Exception {
    // Clear out the Kerberos credentials in the environment.
    miniCluster.kdestroy();

    // Create a new client instance with the logged in user, and ensure that it
    // fails to connect (the logged in user is not in the user-acl).
    try (KuduClient client =
             new KuduClient.KuduClientBuilder(miniCluster.getMasterAddresses()).build()) {
      client.listTabletServers();
      fail();
    } catch (KuduException e) {
      // TODO(KUDU-2344): This should fail with NotAuthorized.
      assertTrue(e.getStatus().toString(), e.getStatus().isServiceUnavailable());
    }

    // Try again with a correct real user.
    try (KuduClient client =
             new KuduClient.KuduClientBuilder(miniCluster.getMasterAddresses()).build()) {
      Client.AuthenticationCredentialsPB credentials =
          Client.AuthenticationCredentialsPB.newBuilder().setRealUser("token-user").build();
      client.importAuthenticationCredentials(credentials.toByteArray());
      client.listTabletServers();

      // Smoke-test tserver connection by scanning a table.
      KuduTable table = client.createTable(tableName, getBasicSchema(),
                                           new CreateTableOptions().setRangePartitionColumns(
                                               new ArrayList<String>()));
      assertEquals(0, scanTableToStrings(table).size());
    }
  }
}
