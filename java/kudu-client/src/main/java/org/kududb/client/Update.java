// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Operation to update columns on an existing row
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Update extends Operation {

  Update(KuduTable table) {
    super(table);
  }

  @Override
  ChangeType getChangeType() {
    return ChangeType.UPDATE;
  }
}