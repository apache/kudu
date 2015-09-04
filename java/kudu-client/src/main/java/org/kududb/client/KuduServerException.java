// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.WireProtocol;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.rpc.RpcHeader;

/**
 * This class is used for errors sent in response to a RPC.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class KuduServerException extends KuduException {

  KuduServerException(String serverUuid, RpcHeader.ErrorStatusPB errorStatus) {
    this(serverUuid, errorStatus.getMessage(), errorStatus.getCode().toString(),
        errorStatus.getCode().getNumber(), null);
  }

  KuduServerException(String serverUuid, WireProtocol.AppStatusPB appStatus) {
    this(serverUuid, appStatus.getMessage(), appStatus.getCode().toString(),
        appStatus.getCode().getNumber(), null);
  }

  KuduServerException(String serverUuid, String message, String errorDesc,
                      int errCode, Throwable cause) {
    super("Server[" + serverUuid + "] "
        + errorDesc + "[code " + errCode + "]: "  + message, cause);
  }
}