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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.junit.Test;

import org.apache.kudu.WireProtocol;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;

public class TestGetMasterRegistrationReceived {

  private static final List<HostAndPort> MASTERS = ImmutableList.of(
      HostAndPort.fromParts("0", 9000),
      HostAndPort.fromParts("1", 9000),
      HostAndPort.fromParts("2", 9000));

  @Test(timeout = 10000)
  public void test() throws Exception {
    NonRecoverableException reusableNRE = new NonRecoverableException(
        Status.RuntimeError(""));
    RecoverableException reusableRE = new RecoverableException(
        Status.RuntimeError(""));
    NoLeaderFoundException retryResponse =
        new NoLeaderFoundException(Status.RuntimeError(""));
    // We don't test for a particular good response, so as long as we pass something that's not an
    // exception to runTest() we're good.
    Object successResponse = new Object();

    // Success cases.

    // Normal case.
    runTest(
        makeGMRR(LEADER),
        makeGMRR(FOLLOWER),
        makeGMRR(FOLLOWER),
        successResponse);

    // Permutation works too.
    runTest(
        makeGMRR(FOLLOWER),
        makeGMRR(LEADER),
        makeGMRR(FOLLOWER),
        successResponse);

    // Multiple leaders, that's fine since it might be a TOCTOU situation, or one master
    // is confused. Raft handles this if the client then tries to do something that requires a
    // replication on the master-side.
    runTest(
        makeGMRR(LEADER),
        makeGMRR(LEADER),
        makeGMRR(FOLLOWER),
        successResponse);

    // Mixed bag, still works because there's a leader.
    runTest(
        reusableNRE,
        makeGMRR(FOLLOWER),
        makeGMRR(LEADER),
        successResponse);

    // All unreachable except one leader, still good.
    runTest(
        reusableNRE,
        reusableNRE,
        makeGMRR(LEADER),
        successResponse);

    // Permutation of the previous.
    runTest(
        reusableNRE,
        makeGMRR(LEADER),
        reusableNRE,
        successResponse);

    // Retry cases.

    // Just followers means we retry.
    runTest(
        makeGMRR(FOLLOWER),
        makeGMRR(FOLLOWER),
        makeGMRR(FOLLOWER),
        retryResponse);

    // One NRE but we have responsive masters, retry.
    runTest(
        makeGMRR(FOLLOWER),
        makeGMRR(FOLLOWER),
        reusableNRE,
        retryResponse);

    // One good master but no leader, retry.
    runTest(
        reusableNRE,
        makeGMRR(FOLLOWER),
        reusableNRE,
        retryResponse);

    // Different case but same outcome.
    runTest(
        reusableRE,
        reusableNRE,
        makeGMRR(FOLLOWER),
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

    Deferred<Master.GetTableLocationsResponsePB> d = new Deferred<>();

    GetMasterRegistrationReceived grrm = new GetMasterRegistrationReceived(MASTERS, d);

    Callback<Void, GetMasterRegistrationResponse> cb0 = grrm.callbackForNode(MASTERS.get(0));
    Callback<Void, GetMasterRegistrationResponse> cb1 = grrm.callbackForNode(MASTERS.get(1));
    Callback<Void, GetMasterRegistrationResponse> cb2 = grrm.callbackForNode(MASTERS.get(2));

    Callback<Void, Exception> eb0 = grrm.errbackForNode(MASTERS.get(0));
    Callback<Void, Exception> eb1 = grrm.errbackForNode(MASTERS.get(1));
    Callback<Void, Exception> eb2 = grrm.errbackForNode(MASTERS.get(2));

    callTheRightCallback(cb0, eb0, response0);
    callTheRightCallback(cb1, eb1, response1);
    callTheRightCallback(cb2, eb2, response2);

    try {
      d.join(); // Don't care about the response.
      if ((expectedResponse instanceof Exception)) {
        fail("Should not work " + expectedResponse.getClass());
      } else {
        // ok
      }
    } catch (Exception ex) {
      assertEquals(expectedResponse.getClass(), ex.getClass());
    }
  }

  // Helper method that determines if the callback or errback should be called.
  private static void callTheRightCallback(
      Callback<Void, GetMasterRegistrationResponse> cb,
      Callback<Void, Exception> eb,
      Object response) throws Exception {
    if (response instanceof Exception) {
      eb.call((Exception) response);
    } else {
      cb.call((GetMasterRegistrationResponse) response);
    }
  }

  private static GetMasterRegistrationResponse makeGMRR(Metadata.RaftPeerPB.Role role) {
    return new GetMasterRegistrationResponse(0, "", role, null,
        WireProtocol.NodeInstancePB.getDefaultInstance());
  }
}
