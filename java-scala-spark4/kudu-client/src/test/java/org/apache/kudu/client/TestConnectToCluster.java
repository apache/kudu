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

import static org.apache.kudu.consensus.Metadata.RaftPeerPB.Role.FOLLOWER;
import static org.apache.kudu.consensus.Metadata.RaftPeerPB.Role.LEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master.ConnectToMasterResponsePB;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.cluster.MiniKuduCluster;

public class TestConnectToCluster {

  private static final ImmutableList<HostAndPort> MASTERS = ImmutableList.of(
      new HostAndPort("0", 9000),
      new HostAndPort("1", 9000),
      new HostAndPort("2", 9000));

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  /**
   * Test that the client properly falls back to the old GetMasterRegistration
   * RPC when connecting to a master which does not support the new
   * ConnectToMaster RPC.
   */
  @Test(timeout = 60000)
  @MasterServerConfig(flags = { "--master_support_connect_to_master_rpc=0" })
  public void testFallbackConnectRpc() throws Exception {
    // Call some method which uses the master. This forces us to connect
    // and verifies that the fallback works.
    harness.getClient().listTabletServers();
  }

  /**
   * Test for KUDU-2200: if a cluster is running multiple masters, but
   * the user only specifies one of them in the connection string,
   * the resulting exception should clarify their error rather than
   * saying that no leader was found.
   */
  @Test(timeout = 60000)
  public void testConnectToOneOfManyMasters() throws Exception {
    int successes = 0;
    String[] masterAddrs = harness.getMasterAddressesAsString().split(",", -1);
    assertEquals(3, masterAddrs.length);
    for (String masterAddr : masterAddrs) {
      try (KuduClient c = new KuduClient.KuduClientBuilder(masterAddr).build()) {
        // Call some method which uses the master. This forces us to connect.
        c.listTabletServers();
        successes++;
      } catch (Exception e) {
        Assert.assertTrue("unexpected exception: " + e.toString(),
            e.toString().matches(
                ".*Client configured with 1 master\\(s\\) " +
                "\\(.+?\\) but cluster indicates it expects 3 master\\(s\\) " +
                "\\(.+?,.+?,.+?\\).*"));
        Assert.assertThat(Joiner.on("\n").join(e.getStackTrace()),
            CoreMatchers.containsString("testConnectToOneOfManyMasters"));
      }
    }

    // Typically, one of the connections will have succeeded. However, it's possible
    // that 0 succeeded in the case that the masters were slow at electing
    // themselves.
    Assert.assertTrue(successes <= 1);
  }


  /**
   * Unit test which checks that the ConnectToCluster aggregates the
   * responses from the different masters properly and returns the
   * response from the located leader.
   */
  @Test(timeout = 10000)
  public void testAggregateResponses() throws Exception {
    final NonRecoverableException reusableNRE = new NonRecoverableException(
        Status.RuntimeError(""));
    final RecoverableException reusableRE = new RecoverableException(
        Status.RuntimeError(""));
    final NoLeaderFoundException retryResponse =
        new NoLeaderFoundException(Status.RuntimeError(""));
    // We don't test for a particular good response, so as long as we pass something that's not an
    // exception to runTest() we're good.
    Object successResponse = new Object();

    // Success cases.

    // Normal case.
    runTest(
        makeCTMR(LEADER, MASTERS),
        makeCTMR(FOLLOWER, MASTERS),
        makeCTMR(FOLLOWER, MASTERS),
        successResponse);

    // Permutation works too.
    runTest(
        makeCTMR(FOLLOWER, MASTERS),
        makeCTMR(LEADER, MASTERS),
        makeCTMR(FOLLOWER, MASTERS),
        successResponse);

    // Multiple leaders, that's fine since it might be a TOCTOU situation, or one master
    // is confused. Raft handles this if the client then tries to do something that requires a
    // replication on the master-side.
    runTest(
        makeCTMR(LEADER, MASTERS),
        makeCTMR(LEADER, MASTERS),
        makeCTMR(FOLLOWER, MASTERS),
        successResponse);

    // Mixed bag, still works because there's a leader.
    runTest(
        reusableNRE,
        makeCTMR(FOLLOWER, MASTERS),
        makeCTMR(LEADER, MASTERS),
        successResponse);

    // All unreachable except one leader, still good.
    runTest(
        reusableNRE,
        reusableNRE,
        makeCTMR(LEADER, MASTERS),
        successResponse);

    // Permutation of the previous.
    runTest(
        reusableNRE,
        makeCTMR(LEADER, MASTERS),
        reusableNRE,
        successResponse);

    // Client try to connect three masters, but the cluster is configure with only one master.
    // If connect to a leader master, success.
    runTest(
        reusableNRE,
        reusableNRE,
        makeCTMR(LEADER, ImmutableList.of(MASTERS.get(0))),
        successResponse);

    // Retry cases.

    // Just followers means we retry.
    runTest(
        makeCTMR(FOLLOWER, MASTERS),
        makeCTMR(FOLLOWER, MASTERS),
        makeCTMR(FOLLOWER, MASTERS),
        retryResponse);

    // One NRE but we have responsive masters, retry.
    runTest(
        makeCTMR(FOLLOWER, MASTERS),
        makeCTMR(FOLLOWER, MASTERS),
        reusableNRE,
        retryResponse);

    // One good master but no leader, retry.
    runTest(
        reusableNRE,
        makeCTMR(FOLLOWER, MASTERS),
        reusableNRE,
        retryResponse);

    // Different case but same outcome.
    runTest(
        reusableRE,
        reusableNRE,
        makeCTMR(FOLLOWER, MASTERS),
        retryResponse);

    // All recoverable means retry.
    runTest(
        reusableRE,
        reusableRE,
        reusableRE,
        retryResponse);

    // Just one recoverable still means retry.
    runTest(
        reusableRE,
        reusableNRE,
        reusableNRE,
        retryResponse);

    // Client try to connect three masters, but the cluster is configure with only one master.
    // If the master hasn't become a leader, retry.
    runTest(
        reusableNRE,
        reusableNRE,
        makeCTMR(FOLLOWER, ImmutableList.of(MASTERS.get(0))),
        retryResponse);

    // Failure case.

    // Can't recover anything, give up.
    runTest(
        reusableNRE,
        reusableNRE,
        reusableNRE,
        reusableNRE);
  }

  private void runTest(Object response0,
                       Object response1,
                       Object response2,
                       Object expectedResponse) throws Exception {

    // Here we basically do what AsyncKuduClient would do, add all the callbacks and then we also
    // add the responses. We then check for the right response.

    ConnectToCluster grrm = new ConnectToCluster(MASTERS);

    Callback<Void, ConnectToMasterResponsePB> cb0 = grrm.callbackForNode(MASTERS.get(0));
    Callback<Void, ConnectToMasterResponsePB> cb1 = grrm.callbackForNode(MASTERS.get(1));
    Callback<Void, ConnectToMasterResponsePB> cb2 = grrm.callbackForNode(MASTERS.get(2));

    Callback<Void, Exception> eb0 = grrm.errbackForNode(MASTERS.get(0));
    Callback<Void, Exception> eb1 = grrm.errbackForNode(MASTERS.get(1));
    Callback<Void, Exception> eb2 = grrm.errbackForNode(MASTERS.get(2));

    callTheRightCallback(cb0, eb0, response0);
    callTheRightCallback(cb1, eb1, response1);
    callTheRightCallback(cb2, eb2, response2);

    try {
      grrm.getDeferred().join(); // Don't care about the response.
      if ((expectedResponse instanceof Exception)) {
        fail("Should not work " + expectedResponse.getClass());
      }
    } catch (Exception ex) {
      assertEquals(expectedResponse.getClass(), ex.getClass());
    }
  }

  // Helper method that determines if the callback or errback should be called.
  private static void callTheRightCallback(
      Callback<Void, ConnectToMasterResponsePB> cb,
      Callback<Void, Exception> eb,
      Object response) throws Exception {
    if (response instanceof Exception) {
      eb.call((Exception) response);
    } else {
      cb.call((ConnectToMasterResponsePB) response);
    }
  }

  // Helper method to make a ConnectToMasterResponsePB.
  private static ConnectToMasterResponsePB makeCTMR(Metadata.RaftPeerPB.Role role,
                                                    List<HostAndPort> masters) {
    ConnectToMasterResponsePB.Builder b = ConnectToMasterResponsePB.newBuilder();
    b.setRole(role);
    for (HostAndPort master : masters) {
      b.addMasterAddrs(ProtobufHelper.hostAndPortToPB(master));
    }
    return b.build();
  }
}
