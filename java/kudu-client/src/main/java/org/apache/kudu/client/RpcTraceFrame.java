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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Container class for traces. Most of its properties can be null, when they aren't set via the
 * builder. The timestamp is set automatically.
 */
@InterfaceAudience.Private
class RpcTraceFrame {
  enum Action {
    // Just before putting the RPC on the wire.
    SEND_TO_SERVER {
      @Override
      void appendToStringBuilder(RpcTraceFrame trace, StringBuilder sb) {
        sb.append(String.format("sending RPC to server %s",
                                trace.getServer().getUuid()));
      }
    },
    // Just after parsing the response from the server.
    RECEIVE_FROM_SERVER {
      @Override
      void appendToStringBuilder(RpcTraceFrame trace, StringBuilder sb) {
        sb.append(String.format("received response from server %s: %s",
                                trace.getServer().getUuid(),
                                trace.getStatus()));
      }
    },
    // Just before sleeping and then retrying.
    SLEEP_THEN_RETRY {
      @Override
      void appendToStringBuilder(RpcTraceFrame trace, StringBuilder sb) {
        sb.append(String.format("delaying RPC due to: %s", trace.getStatus()));
      }
    },
    // Waiting for a new authn token to re-send the request.
    GET_NEW_AUTHENTICATION_TOKEN_THEN_RETRY {
      @Override
      void appendToStringBuilder(RpcTraceFrame trace, StringBuilder sb) {
        sb.append("waiting for new authn token");
      }
    },
    // After having figured out that we don't know where the RPC is going,
    // before querying the master.
    QUERY_MASTER {
      @Override
      void appendToStringBuilder(RpcTraceFrame trace, StringBuilder sb) {
        sb.append("refreshing cache from master");
      }
    },
    // Once the trace becomes too large, will be the last trace object in the list.
    TRACE_TRUNCATED {
      @Override
      void appendToStringBuilder(RpcTraceFrame trace, StringBuilder sb) {
        sb.append(String.format("too many traces: truncated at %d traces",
                                KuduRpc.MAX_TRACES_SIZE));
      }
    };

    abstract void appendToStringBuilder(RpcTraceFrame trace, StringBuilder sb);
  }

  private final String rpcMethod;
  private final Action action;
  private final ServerInfo serverInfo;
  private final long timestampMs;
  private final Status callStatus;

  private RpcTraceFrame(String rpcMethod, Action action,
                        ServerInfo serverInfo, Status callStatus) {
    this.rpcMethod = rpcMethod;
    this.action = action;
    this.serverInfo = serverInfo;
    this.callStatus = callStatus;
    this.timestampMs = System.currentTimeMillis();
  }

  public String getRpcMethod() {
    return rpcMethod;
  }

  Action getAction() {
    return action;
  }

  ServerInfo getServer() {
    return serverInfo;
  }

  long getTimestampMs() {
    return timestampMs;
  }

  public Status getStatus() {
    return callStatus;
  }

  public static String getHumanReadableStringForTraces(List<RpcTraceFrame> traces) {
    String rootMethod;
    long baseTimestamp;
    if (traces.isEmpty()) {
      return "No traces";
    } else {
      RpcTraceFrame firstTrace = traces.get(0);
      rootMethod = firstTrace.getRpcMethod();
      baseTimestamp = firstTrace.getTimestampMs();
    }

    StringBuilder sb = new StringBuilder("Traces: ");
    for (int i = 0; i < traces.size(); i++) {
      RpcTraceFrame trace = traces.get(i);
      sb.append('[');
      sb.append(trace.getTimestampMs() - baseTimestamp);
      sb.append("ms] ");

      if (!rootMethod.equals(trace.getRpcMethod())) {
        sb.append(String.format("Sub RPC %s: ", trace.getRpcMethod()));
      }

      trace.getAction().appendToStringBuilder(trace, sb);

      if (i < traces.size() - 1) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  /**
   * Returns a String wih the trace summary in the following format:
   *
   *  Trace Summary(trace-duration ms): Sent(n), Received(n), Delayed(n), MasterRefresh(n),
   *  AuthRefresh(n), Truncated: ?
   *   Sent: (server-uuid, [ rpc-method, count ], ...), ...
   *   Received: (server-uuid, [ rpc-status, count ], ...), ...
   *   Delayed: (server-uuid, [ rpc-method, count ], ...), ...
   */
  public static String getHumanReadableSummaryStringForTraces(List<RpcTraceFrame> traces) {
    if (traces.isEmpty()) {
      return "No traces";
    }

    RpcTraceFrame firstTrace = traces.get(0);
    long baseTimestamp = firstTrace.getTimestampMs();

    // Table with Server UUID as the row, RPC Method as the column,
    // and count as the value for each send trace.
    Table<String, String, Long> sentTable = TreeBasedTable.create();
    long sentCount = 0;
    // Table with Server UUID as the row, RPC Status as the column,
    // and count as the value for each receive trace.
    Table<String, String, Long> receivedTable = TreeBasedTable.create();
    long receivedCount = 0;
    // Table with Server UUID as the row, RPC Method as the column,
    // and count as the value for each delay trace.
    Table<String, String, Long> delayedTable = TreeBasedTable.create();
    long delayedCount = 0;
    long masterRefreshCount = 0;
    long authRefreshCount = 0;
    boolean truncated = false;
    long maxTime = 0;

    for (RpcTraceFrame trace : traces) {
      String uuid = trace.getServer() == null ? "UNKNOWN" : trace.getServer().getUuid();
      String method = trace.getRpcMethod() == null ? "UNKNOWN" : trace.getRpcMethod();
      String status = trace.getStatus() == null ? "UNKNOWN" : trace.getStatus().getCodeName();
      switch (trace.getAction()) {
        case SEND_TO_SERVER: {
          long count = sentTable.contains(uuid, method) ? sentTable.get(uuid, method) : 0L;
          sentTable.put(uuid, method, count + 1);
          sentCount++;
          break;
        }
        case RECEIVE_FROM_SERVER: {
          long count = receivedTable.contains(uuid, status) ? receivedTable.get(uuid, status) : 0L;
          receivedTable.put(uuid, status, count + 1);
          receivedCount++;
          break;
        }
        case SLEEP_THEN_RETRY: {
          long count = delayedTable.contains(uuid, method) ? delayedTable.get(uuid, method) : 0L;
          delayedTable.put(uuid, method, count + 1);
          delayedCount++;
          break;
        }
        case QUERY_MASTER:
          masterRefreshCount++;
          break;
        case GET_NEW_AUTHENTICATION_TOKEN_THEN_RETRY:
          authRefreshCount++;
          break;
        case TRACE_TRUNCATED:
          truncated = true;
          break;
        default:
          throw new IllegalArgumentException("Unexpected action: " + trace.getAction());
      }
      maxTime = Long.max(maxTime, trace.getTimestampMs() - baseTimestamp);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("Trace Summary(%s ms): Sent(%s), Received(%s), Delayed(%s), " +
        "MasterRefresh(%s), AuthRefresh(%s), Truncated: %s",
        maxTime, sentCount, receivedCount, delayedCount, masterRefreshCount, authRefreshCount,
        truncated));
    if (!sentTable.isEmpty()) {
      sb.append(String.format("%n Sent: %s", tableToString(sentTable)));
    }
    if (!receivedTable.isEmpty()) {
      sb.append(String.format("%n Received: %s", tableToString(receivedTable)));
    }
    if (!delayedTable.isEmpty()) {
      sb.append(String.format("%n Delayed: %s", tableToString(delayedTable)));
    }
    return sb.toString();
  }

  /**
   * Returns a string representation of the table in the format of:
   *  (row, [ column, value ], ...), ..."
   */
  private static String tableToString(Table<String, String, Long> table) {
    StringBuilder sb = new StringBuilder();
    List<String> rowKeys = new ArrayList<>(table.rowKeySet());
    for (int i = 0; i < rowKeys.size(); i++) {
      String rowKey = rowKeys.get(i);
      sb.append("(").append(rowKey).append(", ");
      List<Map.Entry<String, Long>> columns = new ArrayList<>(table.row(rowKey).entrySet());
      for (int j = 0; j < columns.size(); j++) {
        Map.Entry<String, Long> column = columns.get(j);
        sb.append(String.format("[ %s, %s ]", column.getKey(), column.getValue()));
        if (j < columns.size() - 1) {
          sb.append(", ");
        }
      }

      sb.append(")");
      if (i < rowKeys.size() - 1) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("rpcMethod", rpcMethod)
        .add("timestampMs", timestampMs)
        .add("action", action)
        .add("serverInfo", serverInfo)
        .add("callStatus", callStatus)
        .toString();
  }

  /**
   * Builder class for trace frames. The only required parameters are set in the constructor.
   * Timestamp is set automatically.
   */
  static class RpcTraceFrameBuilder {
    private final String rpcMethod;
    private final Action action;
    private ServerInfo serverInfo;
    private Status callStatus;

    RpcTraceFrameBuilder(String rpcMethod, Action action) {
      this.rpcMethod = rpcMethod;
      this.action = action;
    }

    public RpcTraceFrameBuilder serverInfo(ServerInfo serverInfo) {
      this.serverInfo = serverInfo;
      return this;
    }

    public RpcTraceFrameBuilder callStatus(Status callStatus) {
      this.callStatus = callStatus;
      return this;
    }

    public RpcTraceFrame build() {
      return new RpcTraceFrame(rpcMethod, action, serverInfo, callStatus);
    }
  }
}
