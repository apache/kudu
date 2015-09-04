// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Response to a isAlterTableDone command to use to know if an alter table is currently running on
 * the specified table.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IsAlterTableDoneResponse extends KuduRpcResponse {

  private final boolean done;

  IsAlterTableDoneResponse(long elapsedMillis, String tsUUID, boolean done) {
    super(elapsedMillis, tsUUID);
    this.done = done;
  }

  /**
   * Tells if the table is done being altered or not.
   * @return whether the table alter is done
   */
  public boolean isDone() {
    return done;
  }
}
