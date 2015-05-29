// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.ZeroCopyLiteralByteString;
import org.kududb.master.Master;

/**
 * This is a builder class for all the options that can be provided while creating a table.
 */
public class CreateTableBuilder {

  Master.CreateTableRequestPB.Builder pb = Master.CreateTableRequestPB.newBuilder();

  /**
   * Add a split point for the table. The table in the end will have splits + 1 tablets. The builder
   * is reset as part of this operation and can be reused.
   * @param builder a key builder for the split point
   */
  public void addSplitKey(KeyBuilder builder) {
    pb.addPreSplitKeys(ZeroCopyLiteralByteString.wrap(builder.extractByteArray()));
  }

  /**
   * Sets the number of replicas that each tablet will have. If not specified, it defaults to 1
   * replica which isn't safe for production usage.
   * @param numReplicas the number of replicas to use
   */
  public void setNumReplicas(int numReplicas) {
    pb.setNumReplicas(numReplicas);
  }
}
