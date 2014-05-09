// Copyright (c) 2014, Cloudera, inc.
package kudu.mapreduce;

import kudu.rpc.Bytes;
import kudu.rpc.Operation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A Writable used to wrap a Kudu row key so that it can be used as a MR key
 */
public class WritableRowKey implements WritableComparable<WritableRowKey> {

  private byte[] key;

  public WritableRowKey() {} // for Writable

  /**
   * Creates a Writable wrapper around the operation's key. The operation must already be
   * populated as the key will be extracted in this method.
   * @param op Operation to use to get the key.
   */
  public WritableRowKey(Operation op) {
    this.key = op.key();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.key.length);
    dataOutput.write(this.key);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.key = new byte[dataInput.readInt()];
    dataInput.readFully(this.key);
  }

  /**
   * Get the byte array representing the key.
   * @return A key
   */
  byte[] getKey() {
    return this.key;
  }

  @Override
  public int compareTo(WritableRowKey writableRowKey) {
    return Bytes.memcmp(key, writableRowKey.getKey());
  }
}
