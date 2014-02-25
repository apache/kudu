// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import kudu.WireProtocol;

/**
 * This exception is thrown by Tablet Servers when something goes wrong processing a request.
 */
public class TabletServerErrorException extends KuduServerException {
  TabletServerErrorException(WireProtocol.AppStatusPB appStatus, int errCode) {
    super(appStatus, errCode);
  }
}
