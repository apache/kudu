// Copyright (c) 2013, Cloudera, inc.
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
