// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

/**
 * Operation to update columns on an existing row
 */
public class Update extends Operation {

  Update(KuduTable table) {
    super(table);
  }

  @Override
  ChangeType getChangeType() {
    return ChangeType.UPDATE;
  }
}