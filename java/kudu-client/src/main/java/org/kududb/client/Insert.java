// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Represents a single row insert.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Insert extends Operation {

  Insert(KuduTable table) {
    super(table);
  }

  @Override
  ChangeType getChangeType() {
    return ChangeType.INSERT;
  }
}
