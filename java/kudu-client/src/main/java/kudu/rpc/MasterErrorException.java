// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import kudu.master.Master;

/**
 * This exception is thrown when a Master responds to an RPC with an error message
 */
public class MasterErrorException extends KuduServerException {

  MasterErrorException(Master.MasterErrorPB error) {
    super(error.getStatus(), error.getCode().getNumber());
  }
}
