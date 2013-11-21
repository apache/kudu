// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import kudu.WireProtocol;

/**
 * This class is used for errors sent in response to a RPC.
 */
public class KuduServerException extends KuduException {

  private final WireProtocol.AppStatusPB appStatus;
  private final int errCode;

  KuduServerException(WireProtocol.AppStatusPB appStatus, int errCode) {
    this(appStatus, errCode, null);
  }

  KuduServerException(WireProtocol.AppStatusPB appStatus, int errCode, Throwable cause) {
    super(appStatus.getMessage(), cause);
    this.appStatus = appStatus;
    this.errCode = errCode;
  }

  public int getErrCode() {
    return errCode;
  }
}