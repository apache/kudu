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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.client.RpcTraceFrame.Action;
import org.apache.kudu.client.RpcTraceFrame.RpcTraceFrameBuilder;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

public class TestRpcTraces {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testLimit() {
    PingRequest ping = PingRequest.makeMasterPingRequest();

    ping.addTrace(getTrace());
    assertNotTruncated(ping);

    for (int i = 0; i < KuduRpc.MAX_TRACES_SIZE - 2; i++) {
      ping.addTrace(getTrace());
    }
    assertNotTruncated(ping);

    ping.addTrace(getTrace());
    assertNotTruncated(ping);

    ping.addTrace(getTrace());
    assertTruncateIsLast(ping);

    ping.addTrace(getTrace());
    assertTruncateIsLast(ping);
  }

  @Test
  public void testParentRpc() {
    PingRequest parent = PingRequest.makeMasterPingRequest();

    PingRequest daughter = PingRequest.makeMasterPingRequest();
    PingRequest son = PingRequest.makeMasterPingRequest();

    PingRequest sonsDaughter = PingRequest.makeMasterPingRequest();

    sonsDaughter.setParentRpc(son);
    son.setParentRpc(parent);
    daughter.setParentRpc(parent);

    // Son's daughter => son => parent.
    RpcTraceFrame trace = getTrace();
    sonsDaughter.addTrace(trace);
    assertSame(trace, son.getImmutableTraces().get(0));
    assertTrue(parent.getImmutableTraces().get(0) == trace);
    assertTrue(daughter.getImmutableTraces().isEmpty());

    // Son => parent.
    trace = getTrace();
    son.addTrace(trace);
    assertSame(trace, son.getImmutableTraces().get(1));
    assertSame(trace, parent.getImmutableTraces().get(1));
    assertTrue(daughter.getImmutableTraces().isEmpty());
    assertEquals(1, sonsDaughter.getImmutableTraces().size());

    // Daughter => parent.
    trace = getTrace();
    daughter.addTrace(trace);
    assertSame(trace, daughter.getImmutableTraces().get(0));
    assertSame(trace, parent.getImmutableTraces().get(2));
    assertEquals(2, son.getImmutableTraces().size());
    assertEquals(1, sonsDaughter.getImmutableTraces().size());

    // Parent alone.
    trace = getTrace();
    parent.addTrace(trace);
    assertSame(trace, parent.getImmutableTraces().get(3));
    assertEquals(1, daughter.getImmutableTraces().size());
    assertEquals(2, son.getImmutableTraces().size());
    assertEquals(1, sonsDaughter.getImmutableTraces().size());
  }

  @Test
  public void testTraceSummary() throws Exception {
    List<RpcTraceFrame> traces = new ArrayList<>();
    String emptySummary = RpcTraceFrame.getHumanReadableSummaryStringForTraces(traces);
    assertEquals("No traces", emptySummary);

    // Test a minimal frame with no server info or status.
    traces.add(new RpcTraceFrameBuilder("GetTableLocations", Action.QUERY_MASTER)
        .build());
    String summary1 = RpcTraceFrame.getHumanReadableSummaryStringForTraces(traces);
    assertTrue(summary1.contains("Sent(0)"));
    assertTrue(summary1.contains("Received(0)"));
    assertTrue(summary1.contains("Delayed(0)"));
    assertTrue(summary1.contains("MasterRefresh(1)"));
    assertTrue(summary1.contains("AuthRefresh(0)"));
    assertTrue(summary1.contains("Truncated: false"));
    assertFalse(summary1.contains("Sent:"));
    assertFalse(summary1.contains("Received:"));
    assertFalse(summary1.contains("Delayed:"));

    // Fake server info for building traces.
    ServerInfo serverInfo = new ServerInfo(
        "fake-uuid",
        new HostAndPort("test.com", 12345),
        InetAddress.getByName("10.1.2.3"),
        /*location=*/"");

    // Test a few sent and received messages.
    traces.add(new RpcTraceFrameBuilder("Batch", Action.SEND_TO_SERVER)
        .build());
    traces.add(new RpcTraceFrameBuilder("Batch", Action.SEND_TO_SERVER)
        .serverInfo(serverInfo)
        .build());
    traces.add(new RpcTraceFrameBuilder("Batch", Action.RECEIVE_FROM_SERVER)
        .build());
    traces.add(new RpcTraceFrameBuilder("Batch", Action.RECEIVE_FROM_SERVER)
        .serverInfo(serverInfo)
        .callStatus(Status.OK())
        .build());
    String summary2 = RpcTraceFrame.getHumanReadableSummaryStringForTraces(traces);
    assertTrue(summary2.contains("Sent(2)"));
    assertTrue(summary2.contains("Received(2)"));
    assertTrue(summary2.contains("Delayed(0)"));
    assertTrue(summary2.contains("MasterRefresh(1)"));
    assertTrue(summary2.contains("AuthRefresh(0)"));
    assertTrue(summary2.contains("Truncated: false"));
    assertTrue(summary2.contains("Sent: (UNKNOWN, [ Batch, 1 ]), (fake-uuid, [ Batch, 1 ])"));
    assertTrue(summary2.contains("Received: (UNKNOWN, [ UNKNOWN, 1 ]), (fake-uuid, [ OK, 1 ])"));
    assertFalse(summary2.contains("Delayed:"));

    // Test delayed messages including auth wait.
    traces.add(new RpcTraceFrameBuilder("Batch", Action.SLEEP_THEN_RETRY)
        .serverInfo(serverInfo)
        .build());
    traces.add(new RpcTraceFrameBuilder("Batch",
                                        Action.GET_NEW_AUTHENTICATION_TOKEN_THEN_RETRY)
        .serverInfo(serverInfo)
        .build());
    String summary3 = RpcTraceFrame.getHumanReadableSummaryStringForTraces(traces);
    assertTrue(summary3.contains("Sent(2)"));
    assertTrue(summary3.contains("Received(2)"));
    assertTrue(summary3.contains("Delayed(1)"));
    assertTrue(summary3.contains("MasterRefresh(1)"));
    assertTrue(summary3.contains("AuthRefresh(1)"));
    assertTrue(summary3.contains("Truncated: false"));
    assertFalse(summary2.contains("Delayed: (fake-uuid, [ Batch, 1 ])"));

    // Test truncation.
    traces.add(new RpcTraceFrameBuilder("Batch", Action.TRACE_TRUNCATED)
        .build());
    String summary4 = RpcTraceFrame.getHumanReadableSummaryStringForTraces(traces);
    assertTrue(summary4.contains("Truncated: true"));
  }

  private RpcTraceFrame getTrace() {
    return new RpcTraceFrameBuilder(
        "trace",
        Action.QUERY_MASTER) // Just a random action.
        .build();
  }

  private void assertNotTruncated(KuduRpc<?> rpc) {
    for (RpcTraceFrame trace : rpc.getImmutableTraces()) {
      assertNotEquals(Action.TRACE_TRUNCATED, trace.getAction());
    }
  }

  private void assertTruncateIsLast(KuduRpc<?> rpc) {
    List<RpcTraceFrame> traces = rpc.getImmutableTraces();
    assertEquals(KuduRpc.MAX_TRACES_SIZE + 1, traces.size());
    for (int i = 0; i < traces.size() - 1; i++) {
      assertNotEquals(Action.TRACE_TRUNCATED, traces.get(i).getAction());
    }
    assertEquals(Action.TRACE_TRUNCATED, traces.get(traces.size() - 1).getAction());
  }
}
