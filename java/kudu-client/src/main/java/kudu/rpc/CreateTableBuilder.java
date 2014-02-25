// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.ByteString;
import kudu.master.Master;

/**
 * This is a builder class for all the options that can be provided while creating a table.
 */
public class CreateTableBuilder {

  Master.CreateTableRequestPB.Builder pb = Master.CreateTableRequestPB.newBuilder();

  /**
   * Add a split point for the table. The table in the end will have splits + 1 tablets.
   * @param key split point
   */
  public void addSplitKey(String key) {
    pb.addPreSplitKeys(ByteString.copyFromUtf8(key));
  }

  /**
   * Currently unsupported
   * @param numReplicas
   */
  public void setNumReplicas(int numReplicas) {
    pb.setNumReplicas(numReplicas);
  }
}
