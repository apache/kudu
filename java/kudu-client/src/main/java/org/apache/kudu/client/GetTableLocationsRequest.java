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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;

/**
 * Package-private RPC that can only go to a master.
 */
@InterfaceAudience.Private
class GetTableLocationsRequest extends KuduRpc<Master.GetTableLocationsResponsePB> {

  private final byte[] startPartitionKey;
  private final byte[] endKey;
  private final String tableId;
  private final int maxReturnedLocations;

  GetTableLocationsRequest(KuduTable table, byte[] startPartitionKey,
                           byte[] endPartitionKey, String tableId,
                           int maxReturnedLocations) {
    super(table);
    if (startPartitionKey != null && endPartitionKey != null &&
        Bytes.memcmp(startPartitionKey, endPartitionKey) > 0) {
      throw new IllegalArgumentException(
          "The start partition key must be smaller or equal to the end partition key");
    }
    this.startPartitionKey = startPartitionKey;
    this.endKey = endPartitionKey;
    this.tableId = tableId;
    this.maxReturnedLocations = maxReturnedLocations;
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "GetTableLocations";
  }

  @Override
  Pair<Master.GetTableLocationsResponsePB, Object> deserialize(
      final CallResponse callResponse, String tsUUID)
      throws KuduException {
    Master.GetTableLocationsResponsePB.Builder builder = Master.GetTableLocationsResponsePB
        .newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    Master.GetTableLocationsResponsePB resp = builder.build();
    return new Pair<Master.GetTableLocationsResponsePB, Object>(
        resp, builder.hasError() ? builder.getError() : null);
  }

  @Override
  Message createRequestPB() {
    final Master.GetTableLocationsRequestPB.Builder builder = Master
        .GetTableLocationsRequestPB.newBuilder();
    builder.setTable(Master.TableIdentifierPB.newBuilder()
        .setTableId(ByteString.copyFromUtf8(tableId)));
    if (startPartitionKey != null) {
      builder.setPartitionKeyStart(UnsafeByteOperations.unsafeWrap(startPartitionKey));
    }
    if (endKey != null) {
      builder.setPartitionKeyEnd(UnsafeByteOperations.unsafeWrap(endKey));
    }
    builder.setMaxReturnedLocations(maxReturnedLocations);
    return builder.build();
  }
}
