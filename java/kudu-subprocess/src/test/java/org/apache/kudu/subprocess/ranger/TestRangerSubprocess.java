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

package org.apache.kudu.subprocess.ranger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.Any;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.kudu.ranger.Ranger.ActionPB;
import org.apache.kudu.ranger.Ranger.RangerRequestListPB;
import org.apache.kudu.ranger.Ranger.RangerRequestPB;
import org.apache.kudu.ranger.Ranger.RangerResponseListPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;
import org.apache.kudu.subprocess.SubprocessConfiguration;
import org.apache.kudu.subprocess.SubprocessExecutor;
import org.apache.kudu.subprocess.SubprocessTestUtil;
import org.apache.kudu.subprocess.ranger.authorization.RangerKuduAuthorizer;
import org.apache.kudu.test.junit.RetryRule;

/**
 * Tests for the ranger subprocess.
 */
public class TestRangerSubprocess extends SubprocessTestUtil {

  @Rule
  public RetryRule retryRule = new RetryRule();

  private static RangerRequestPB createRangerRequest(ActionPB action, String db,
                                                     String table, String col) {
    RangerRequestPB.Builder builder = RangerRequestPB.newBuilder();
    builder.setAction(action);
    builder.setDatabase(db);
    builder.setTable(table);
    builder.setColumn(col);
    return builder.build();
  }

  private static RangerRequestListPB createRangerRequestList(
      List<RangerRequestPB> requests, String user) {
    RangerRequestListPB.Builder builder = RangerRequestListPB.newBuilder();
    builder.addAllRequests(requests);
    builder.setUser(user);
    return builder.build();
  }

  private static SubprocessRequestPB createRangerSubprocessRequest(
      RangerRequestListPB request) {
    SubprocessRequestPB.Builder builder = SubprocessRequestPB.newBuilder();
    builder.setRequest(Any.pack(request));
    return builder.build();
  }

  @Before
  public void mockAuthorizer() {
    RangerProtocolHandler.authz = Mockito.mock(RangerKuduAuthorizer.class);
  }

  /**
   * Sends a list of Ranger request and verifies the response by mocking the authorization
   * decisions.
   */
  @Test
  public void testBasicRangerMessage() throws Exception {
    final String user = "Alice";
    final String db = "db";
    final String table = "table";
    final String col = "col";
    final RangerRequestPB updateRequest = createRangerRequest(ActionPB.UPDATE, db, table, col);
    final RangerRequestPB selectRequest = createRangerRequest(ActionPB.SELECT, db, table, col);
    final RangerRequestPB createRequest = createRangerRequest(ActionPB.CREATE, db, table, col);
    final List<RangerRequestPB> requests = new ArrayList<>();
    // Send multiple ranger requests in one message.
    requests.add(updateRequest);
    requests.add(selectRequest);
    requests.add(createRequest);
    final RangerRequestListPB requestList = createRangerRequestList(requests, user);
    final SubprocessRequestPB subprocessRequest = createRangerSubprocessRequest(requestList);

    // Mock the authorization results.
    List<RangerAccessResult> rangerResults = new ArrayList<>();
    final RangerAccessResult positiveResult = new RangerAccessResult(
        /* policyType= */1, "kudu",
        new RangerServiceDef(), new RangerAccessRequestImpl());
    positiveResult.setIsAllowed(true);
    final RangerAccessResult negativeResult = new RangerAccessResult(
        /* policyType= */1, "kudu",
        new RangerServiceDef(), new RangerAccessRequestImpl());
    negativeResult.setIsAllowed(false);
    rangerResults.add(positiveResult);
    rangerResults.add(negativeResult);
    rangerResults.add(positiveResult);
    Mockito.when(RangerProtocolHandler.authz.authorize(requestList))
           .thenReturn(rangerResults);

    SubprocessExecutor executor =
        setUpExecutorIO(NO_ERR, /*injectIOError*/false);
    sendRequestToPipe(subprocessRequest);
    // We expect the executor to time out since it is non cancelable
    // if no exception encountered.
    assertThrows(TimeoutException.class,
        () -> executor.run(new SubprocessConfiguration(NO_ARGS),
                           new RangerProtocolHandler(/* servicePrincipal= */"", /* keytab= */""),
                           TIMEOUT_MS));

    RangerResponseListPB resp = receiveResponse().getResponse().unpack(RangerResponseListPB.class);
    assertTrue(resp.getResponses(/* index= */0).getAllowed());
    assertFalse(resp.getResponses(/* index= */1).getAllowed());
    assertTrue(resp.getResponses(/* index= */2).getAllowed());
  }
}