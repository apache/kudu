// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.master.Master;
import org.kududb.rpc.RpcHeader;

/**
 * This exception is thrown when a Master responds to an RPC with an error message
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class MasterErrorException extends KuduServerException {

  MasterErrorException(String serverUuid, RpcHeader.ErrorStatusPB errorStatus) {
    super(serverUuid, errorStatus);
  }

  MasterErrorException(String serverUuid, Master.MasterErrorPB error) {
    super(serverUuid, error.getStatus());
  }
}
