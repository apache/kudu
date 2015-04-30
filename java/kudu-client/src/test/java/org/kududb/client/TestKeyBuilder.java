// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
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
    columns.add(new ColumnSchema.ColumnSchemaBuilder("a", Type.STRING)
        .key(true)
        .build());
    Schema simpleKeySchema = new Schema(columns);

    KeyBuilder builder = new KeyBuilder(simpleKeySchema);
    builder.addString("blah").extractByteArray();
    try {
      builder.addString("blah").addString("blah").extractByteArray();
      Assert.fail("Cannot have more key components than the schema has");
    } catch (IllegalArgumentException ex) {
      // expected, too many keys
    }

    columns.add(new ColumnSchema.ColumnSchemaBuilder("b", Type.INT32)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c", Type.INT8)
        .key(true)
        .build());
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
