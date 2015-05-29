// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.WireProtocol;
import org.kududb.rpc.RpcHeader;

/**
 * This exception is thrown by Tablet Servers when something goes wrong processing a request.
 */
@SuppressWarnings("serial")
public class TabletServerErrorException extends KuduServerException {

  TabletServerErrorException(WireProtocol.AppStatusPB appStatus) {
    super(appStatus);
  }

  TabletServerErrorException(RpcHeader.ErrorStatusPB errorStatus) {
    super(errorStatus);
  }
}

