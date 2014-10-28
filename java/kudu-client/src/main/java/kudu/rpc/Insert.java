// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

/**
 * Represents a single row insert.
 */
public class Insert extends Operation {

  Insert(KuduTable table) {
    super(table);
  }

  @Override
  ChangeType getChangeType() {
    return ChangeType.INSERT;
  }
}
