// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.annotations.InterfaceAudience;

/**
 * Base class for RPC responses.
 */
@InterfaceAudience.Private
abstract class KuduRpcResponse {
  private final long elapsedMillis;
  private final String tsUUID;

  /**
   * Constructor with information common to all RPCs.
   * @param elapsedMillis Time in milliseconds since RPC creation to now.
   * @param tsUUID A string that contains the UUID of the server that answered the RPC.
   */
  KuduRpcResponse(long elapsedMillis, String tsUUID) {
    this.elapsedMillis = elapsedMillis;
    this.tsUUID = tsUUID;
  }

  /**
   * Get the number of milliseconds elapsed since the RPC was created up to the moment when this
   * response was created.
   * @return Elapsed time in milliseconds.
   */
  public long getElapsedMillis() {
    return elapsedMillis;
  }

  /**
   * Get the identifier of the tablet server that sent the response.
   * @return A string containing a UUID.
   */
  public String getTsUUID() {
    return tsUUID;
  }
}
