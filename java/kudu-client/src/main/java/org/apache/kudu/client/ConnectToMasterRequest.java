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

import static org.apache.kudu.consensus.Metadata.RaftPeerPB;
import static org.apache.kudu.master.Master.GetMasterRegistrationRequestPB;
import static org.apache.kudu.master.Master.GetMasterRegistrationResponsePB;
import static org.apache.kudu.master.Master.MasterErrorPB;

import java.util.Collection;
import java.util.Collections;

import com.google.protobuf.Message;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.master.Master.ConnectToMasterResponsePB;
import org.apache.kudu.master.Master.MasterFeatures;
import org.apache.kudu.util.Pair;

/**
 * Package-private RPC that can only go to master.
 */
@InterfaceAudience.Private
public class ConnectToMasterRequest extends KuduRpc<ConnectToClusterResponse> {
  /**
   * Kudu 1.2 and earlier use GetMasterRegistration to connect to the master.
   */
  private static final String GET_MASTER_REGISTRATION = "GetMasterRegistration";
  /**
   * Kudu 1.3 and later use a new ConnectToMaster RPC, which includes less
   * irrelevant information and also returns security-related items.
   */
  private static final String CONNECT_TO_MASTER = "ConnectToMaster";

  /**
   * We start by trying the new RPC, but if we fail, we'll retry this
   * RPC with the old RPC.
   */
  private String method = CONNECT_TO_MASTER;

  public ConnectToMasterRequest() {
    super(null); // no KuduTable
  }

  @Override
  Message createRequestPB() {
    return GetMasterRegistrationRequestPB.getDefaultInstance();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return method;
  }

  @Override
  Collection<Integer> getRequiredFeatures() {
    if (method == CONNECT_TO_MASTER) {
      return Collections.singleton(MasterFeatures.CONNECT_TO_MASTER.getNumber());
    }
    return Collections.emptySet();
  }

  @Override
  Pair<ConnectToClusterResponse, Object> deserialize(CallResponse callResponse,
                                                     String tsUUID) throws KuduException {
    if (method == CONNECT_TO_MASTER) {
      return deserializeNewRpc(callResponse, tsUUID);
    }
    return deserializeOldRpc(callResponse, tsUUID);
  }

  private Pair<ConnectToClusterResponse, Object> deserializeNewRpc(
      CallResponse callResponse, String tsUUID) {

    final ConnectToMasterResponsePB.Builder respBuilder =
        ConnectToMasterResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    RaftPeerPB.Role role = RaftPeerPB.Role.FOLLOWER;
    if (!respBuilder.hasError() || respBuilder.getError().getCode() !=
        MasterErrorPB.Code.CATALOG_MANAGER_NOT_INITIALIZED) {
      role = respBuilder.getRole();
    }
    ConnectToClusterResponse response = new ConnectToClusterResponse(
        deadlineTracker.getElapsedMillis(),
        tsUUID,
        role);
    return new Pair<ConnectToClusterResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }

  private Pair<ConnectToClusterResponse, Object> deserializeOldRpc(CallResponse callResponse,
      String tsUUID) throws KuduException {
    final GetMasterRegistrationResponsePB.Builder respBuilder =
        GetMasterRegistrationResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    RaftPeerPB.Role role = RaftPeerPB.Role.FOLLOWER;
    if (!respBuilder.hasError() || respBuilder.getError().getCode() !=
        MasterErrorPB.Code.CATALOG_MANAGER_NOT_INITIALIZED) {
      role = respBuilder.getRole();
    }
    ConnectToClusterResponse response = new ConnectToClusterResponse(
        deadlineTracker.getElapsedMillis(),
        tsUUID,
        role);
    return new Pair<ConnectToClusterResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }

  public void setUseOldMethod() {
    this.method = GET_MASTER_REGISTRATION;
  }
}
