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

import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;
import org.jboss.netty.util.Timer;

import org.apache.kudu.Common.KeyRangePB;
import org.apache.kudu.security.Token;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.Pair;

/*
 * RPC to split a tablet's primary key range into smaller ranges suitable for concurrent scanning.
 */
@InterfaceAudience.Private
class SplitKeyRangeRequest extends KuduRpc<SplitKeyRangeResponse> {

  private final byte[] startPrimaryKey;
  private final byte[] endPrimaryKey;
  private final byte[] partitionKey;
  private final long splitSizeBytes;

  /** The token with which to authorize this RPC. */
  private Token.SignedTokenPB authzToken;

  /**
   * Create a new RPC request
   * @param table table to lookup
   * @param startPrimaryKey the primary key to begin splitting at (inclusive), pass null to
   *                        start at the beginning
   * @param endPrimaryKey the primary key to stop splitting at (exclusive), pass null to
   *                      get all the key ranges
   * @param partitionKey the partition key of the tablet to find
   * @param splitSizeBytes the size of the data in each key range.
   *                       This is a hint: The tablet server may return the size of key range
   *                       larger or smaller than this value.
   * @param timer Timer to monitor RPC timeouts.
   * @param timeoutMillis the timeout of the request in milliseconds
   */
  SplitKeyRangeRequest(KuduTable table,
                       byte[] startPrimaryKey,
                       byte[] endPrimaryKey,
                       byte[] partitionKey,
                       long splitSizeBytes,
                       Timer timer,
                       long timeoutMillis) {
    super(table, timer, timeoutMillis);
    this.startPrimaryKey = startPrimaryKey;
    this.endPrimaryKey = endPrimaryKey;
    this.partitionKey = partitionKey;
    this.splitSizeBytes = splitSizeBytes;
  }

  @Override
  Message createRequestPB() {
    RemoteTablet tablet = super.getTablet();
    final Tserver.SplitKeyRangeRequestPB.Builder builder =
        Tserver.SplitKeyRangeRequestPB.newBuilder();
    builder.setTabletId(UnsafeByteOperations.unsafeWrap(tablet.getTabletIdAsBytes()));
    if (this.startPrimaryKey != null && this.startPrimaryKey.length > 0) {
      builder.setStartPrimaryKey(UnsafeByteOperations.unsafeWrap(startPrimaryKey));
    }
    if (this.endPrimaryKey != null && this.endPrimaryKey.length > 0) {
      builder.setStopPrimaryKey(UnsafeByteOperations.unsafeWrap(endPrimaryKey));
    }
    builder.setTargetChunkSizeBytes(splitSizeBytes);
    if (authzToken != null) {
      builder.setAuthzToken(authzToken);
    }

    return builder.build();
  }

  @Override
  boolean needsAuthzToken() {
    return true;
  }

  @Override
  void bindAuthzToken(Token.SignedTokenPB token) {
    authzToken = token;
  }

  @Override
  String serviceName() {
    return TABLET_SERVER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "SplitKeyRange";
  }

  @Override
  Pair<SplitKeyRangeResponse, Object> deserialize(CallResponse callResponse, String tsUuid) {
    final Tserver.SplitKeyRangeResponsePB.Builder respBuilder =
        Tserver.SplitKeyRangeResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);

    List<KeyRangePB> keyRanges = new ArrayList<>();
    for (KeyRangePB keyRange : respBuilder.getRangesList()) {
      keyRanges.add(keyRange);
    }

    SplitKeyRangeResponse response = new SplitKeyRangeResponse(
        timeoutTracker.getElapsedMillis(), tsUuid, keyRanges);
    return new Pair<SplitKeyRangeResponse, Object>(response,
      respBuilder.hasError() ? respBuilder.getError() : null);
  }

  @Override
  byte[] partitionKey() {
    return this.partitionKey;
  }
}
