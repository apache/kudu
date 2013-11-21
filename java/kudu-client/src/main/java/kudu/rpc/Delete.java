// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import kudu.ColumnSchema;
import kudu.Type;

/**
 * Class of Operation for whole row removals.
 * Only columns which are part of the key can be set.
 */
public class Delete extends Operation {

  Delete(KuduTable table) {
    super(table);
  }

  @Override
  ChangeType getChangeType() {
    return ChangeType.DELETE;
  }

  @Override
  void checkColumn(ColumnSchema column, Type type) {
    super.checkColumn(column, type);
    if (!column.isKey())
      throw new IllegalArgumentException("Delete only accepts columns that arepart of the key");

  }
}
