// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import kudu.ColumnSchema;
import kudu.Schema;
import kudu.tserver.Tserver;

import java.util.ArrayList;

/**
 * Utility class to manage the predicates on a scanner. It automatically creates the start
 * and end row keys as the predicates are added.
 */
class ColumnRangePredicates {

  private final Schema schema;
  final ArrayList<Tserver.ColumnRangePredicatePB> predicates = new ArrayList<Tserver
      .ColumnRangePredicatePB>();
  private KeyEncoder startKeyEncoder = null;
  private KeyEncoder endKeyEncoder = null;
  private int lastColumnIndex = -1;

  ColumnRangePredicates(Schema schema) {
    this.schema = schema;
  }

  void addColumnRangePredicate(ColumnRangePredicate predicate) {
    ColumnSchema column = predicate.getColumn();
    int index = this.schema.getColumnIndex(column);
    if (column.isKey()) {
      if (lastColumnIndex >= index) {
        throw new IllegalArgumentException("The key columns must be added in order");
      }
      if (predicate.getLowerBound() != null) {
        if (this.startKeyEncoder == null) {
          this.startKeyEncoder = new KeyEncoder(this.schema);
        }
        byte[] key = predicate.getLowerBound();
        this.startKeyEncoder.addKey(key, 0, key.length, column, index);
      }
      if (predicate.getUpperBound() != null) {
        if (this.endKeyEncoder == null) {
          this.endKeyEncoder = new KeyEncoder(this.schema);
        }
        byte[] key = predicate.getUpperBound();
        this.endKeyEncoder.addKey(key, 0, key.length, column, index);
      }
    }
    this.predicates.add(predicate.pb.build());
    lastColumnIndex = index;
  }

  byte[] getStartKey() {
    return this.startKeyEncoder == null ? null : this.startKeyEncoder.extractByteArray();
  }

  byte[] getEndKey() {
    return this.endKeyEncoder == null ? null : this.endKeyEncoder.extractByteArray();
  }

  boolean hasStartKey() {
    return this.startKeyEncoder != null;
  }

  boolean hasEndKey() {
    return this.endKeyEncoder != null;
  }

}
