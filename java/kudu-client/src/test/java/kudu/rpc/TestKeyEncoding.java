// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import static org.junit.Assert.*;

import kudu.ColumnSchema;
import kudu.Schema;
import kudu.Type;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestKeyEncoding {

  @Test
  public void test() {
    List<ColumnSchema> cols1 = new ArrayList<ColumnSchema>();
    cols1.add(new ColumnSchema("key", Type.STRING, true));
    Schema schemaOneString = new Schema(cols1);
    KuduTable table = new KuduTable(null, "one", schemaOneString);
    Insert oneKeyInsert = new Insert(table);
    oneKeyInsert.addString("key", "foo");
    assertTrue(Bytes.pretty(oneKeyInsert.key()) + " isn't foo", Bytes.equals(new byte[] {'f', 'o',
        'o'}, oneKeyInsert.key()));

    List<ColumnSchema> cols2 = new ArrayList<ColumnSchema>();
    cols2.add(new ColumnSchema("key", Type.STRING, true));
    cols2.add(new ColumnSchema("key2", Type.STRING, true));
    Schema schemaTwoString = new Schema(cols2);
    KuduTable table2 = new KuduTable(null, "two", schemaTwoString);
    Insert twoKeyInsert = new Insert(table2);
    twoKeyInsert.addString("key", "foo");
    twoKeyInsert.addString("key2", "bar");
    assertTrue(Bytes.pretty(twoKeyInsert.key()) + " isn't foo0x000x00bar",
        Bytes.equals(new byte[] {'f',
        'o', 'o', 0x00, 0x00, 'b', 'a', 'r'}, twoKeyInsert.key()));

    Insert twoKeyInsertWithNull = new Insert(table2);
    twoKeyInsertWithNull.addString("key", "xxx\0yyy");
    twoKeyInsertWithNull.addString("key2", "bar");
    assertTrue(Bytes.pretty(twoKeyInsertWithNull.key()) + " isn't " +
        "xxx0x000x01yyy0x000x00bar",
        Bytes.equals(new byte[] {'x', 'x', 'x', 0x00, 0x01, 'y', 'y', 'y', 0x00, 0x00, 'b', 'a',
            'r'},
            twoKeyInsertWithNull.key()));

    // The following tests test our assumptions on unsigned data types sorting from KeyEncoder
    byte four = 4;
    byte onHundredTwentyFour = -4;
    four = Bytes.xorLeftMostBit(four);
    onHundredTwentyFour = Bytes.xorLeftMostBit(onHundredTwentyFour);
    assertTrue(four < onHundredTwentyFour);

    byte[] threeHundred = Bytes.fromInt(300);
    byte[] reallyBigNumber = Bytes.fromInt(-300);
    threeHundred[0] = Bytes.xorLeftMostBit(threeHundred[0]);
    reallyBigNumber[3] = Bytes.xorLeftMostBit(reallyBigNumber[3]);
    assertTrue(Bytes.memcmp(threeHundred, reallyBigNumber) < 0);
  }
}
