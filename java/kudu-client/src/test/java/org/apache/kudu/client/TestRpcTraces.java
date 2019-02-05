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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

public class TestRpcTraces {

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

  private RpcTraceFrame getTrace() {
    return new RpcTraceFrame.RpcTraceFrameBuilder(
        "trace",
        RpcTraceFrame.Action.QUERY_MASTER) // Just a random action.
        .build();
  }

  private void assertNotTruncated(KuduRpc<?> rpc) {
    for (RpcTraceFrame trace : rpc.getImmutableTraces()) {
      assertNotEquals(RpcTraceFrame.Action.TRACE_TRUNCATED, trace.getAction());
    }
  }

  private void assertTruncateIsLast(KuduRpc<?> rpc) {
    List<RpcTraceFrame> traces = rpc.getImmutableTraces();
    assertEquals(KuduRpc.MAX_TRACES_SIZE + 1, traces.size());
    for (int i = 0; i < traces.size() - 1; i++) {
      assertNotEquals(RpcTraceFrame.Action.TRACE_TRUNCATED, traces.get(i).getAction());
    }
    assertEquals(RpcTraceFrame.Action.TRACE_TRUNCATED, traces.get(traces.size() - 1).getAction());
  }
}
