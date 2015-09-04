// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import static org.kududb.consensus.Metadata.*;
import static org.kududb.master.Master.*;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Package-private RPC that can only go to master.
 */
@InterfaceAudience.Private
public class GetMasterRegistrationRequest extends KuduRpc<GetMasterRegistrationResponse> {
  private static final String GET_MASTER_REGISTRATION = "GetMasterRegistration";

  public GetMasterRegistrationRequest(KuduTable masterTable) {
    super(masterTable);
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final GetMasterRegistrationRequestPB.Builder builder =
        GetMasterRegistrationRequestPB.newBuilder();
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return GET_MASTER_REGISTRATION;
  }

  @Override
  Pair<GetMasterRegistrationResponse, Object> deserialize(CallResponse callResponse,
                                                          String tsUUID) throws Exception {
    final GetMasterRegistrationResponsePB.Builder respBuilder =
        GetMasterRegistrationResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    RaftPeerPB.Role role = RaftPeerPB.Role.FOLLOWER;
    if (!respBuilder.hasError() || respBuilder.getError().getCode() !=
        MasterErrorPB.Code.CATALOG_MANAGER_NOT_INITIALIZED) {
      role = respBuilder.getRole();
    }
    GetMasterRegistrationResponse response = new GetMasterRegistrationResponse(
        deadlineTracker.getElapsedMillis(),
        tsUUID,
        role,
        respBuilder.getRegistration(),
        respBuilder.getInstanceId());
    return new Pair<GetMasterRegistrationResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
