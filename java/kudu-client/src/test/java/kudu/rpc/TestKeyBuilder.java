// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import kudu.ColumnSchema;
import kudu.Schema;
import kudu.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class TestKeyBuilder {

  /**
   * Testing single key, composite key, key overflow, but not the actual encoding since that's
   * what TestKeyEncoding is for.
   */
  @Test
  public void test() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>();
    columns.add(new ColumnSchema("a", Type.STRING, true));
    Schema simpleKeySchema = new Schema(columns);

    KeyBuilder builder = new KeyBuilder(simpleKeySchema);
    builder.addString("blah").extractByteArray();
    try {
      builder.addString("blah").addString("blah").extractByteArray();
      Assert.fail("Cannot have more key components than the schema has");
    } catch (IllegalArgumentException ex) {
      // expected, too many keys
    }

    columns.add(new ColumnSchema("b", Type.INT32, true));
    columns.add(new ColumnSchema("c", Type.INT8, true));
    Schema compositeKeySchema = new Schema(columns);

    builder = new KeyBuilder(compositeKeySchema);
    // partial row key
    builder.addString("blah").addInt(12).extractByteArray();
    // full
    builder.addString("blah").addInt(12).addByte((byte) 1).extractByteArray();

    try {
      builder.addString("blah").addInt(12).addByte((byte) 1).addString("blah").extractByteArray();
      Assert.fail("Cannot have more key components than the schema has");
    } catch (IllegalArgumentException ex) {
      // expected, too many keys
    }
  }
}
