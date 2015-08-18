// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.collect.Lists;
import org.kududb.master.Master;

import java.util.List;

/**
 * This is a builder class for all the options that can be provided while creating a table.
 */
public class CreateTableBuilder {

  private Master.CreateTableRequestPB.Builder pb = Master.CreateTableRequestPB.newBuilder();
  private final List<PartialRow> splitRows = Lists.newArrayList();

  /**
   * Add a split point for the table. The table in the end will have splits + 1 tablets.
   * The row may be reused or modified safely after this call without changing the split point.
   * @param row a key row for the split point
   */
  public void addSplitRow(PartialRow row) {
    splitRows.add(new PartialRow(row));
  }

  /**
   * Sets the number of replicas that each tablet will have. If not specified, it defaults to 1
   * replica which isn't safe for production usage.
   * @param numReplicas the number of replicas to use
   */
  public void setNumReplicas(int numReplicas) {
    pb.setNumReplicas(numReplicas);
  }

  Master.CreateTableRequestPB.Builder getBuilder() {
    if (!splitRows.isEmpty()) {
      pb.setSplitRows(new Operation.OperationsEncoder().encodeSplitRows(splitRows));
    }
    return pb;
  }
}
