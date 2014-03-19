// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.master.Master;

/**
 * This is a builder class for all the options that can be provided while creating a table.
 */
public class CreateTableBuilder {

  Master.CreateTableRequestPB.Builder pb = Master.CreateTableRequestPB.newBuilder();

  /**
   * Add a split point for the table. The table in the end will have splits + 1 tablets.
   * @param builder Key builder for the split point. The builder is reset as part of this
   *                operation and can be reused.
   */
  public void addSplitKey(KeyBuilder builder) {
    pb.addPreSplitKeys(ZeroCopyLiteralByteString.wrap(builder.extractByteArray()));
  }

  /**
   * Currently unsupported
   * @param numReplicas
   */
  public void setNumReplicas(int numReplicas) {
    pb.setNumReplicas(numReplicas);
  }
}
