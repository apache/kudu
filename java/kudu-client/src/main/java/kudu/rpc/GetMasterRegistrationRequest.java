// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.Message;
import static kudu.master.Master.*;
import static kudu.metadata.Metadata.*;

import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Package-private RPC that can only go to master.
 */
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
    QuorumPeerPB.Role role = QuorumPeerPB.Role.FOLLOWER;
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
    return new Pair<GetMasterRegistrationResponse, Object>(response, respBuilder.getError());
  }
}
