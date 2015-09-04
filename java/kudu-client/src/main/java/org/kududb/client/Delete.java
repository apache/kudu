// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Class of Operation for whole row removals.
 * Only columns which are part of the key can be set.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Delete extends Operation {

  Delete(KuduTable table) {
    super(table);
  }

  @Override
  ChangeType getChangeType() {
    return ChangeType.DELETE;
  }
}
