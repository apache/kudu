// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.mapreduce;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.kududb.Schema;
import org.kududb.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TestKuduTableInputFormat extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestKuduTableInputFormat.class.getName() + "-" + System.currentTimeMillis();

  @Test
  public void test() throws Exception {
    createTable(TABLE_NAME, getBasicSchema(), new CreateTableBuilder());

    KuduTable table = openTable(TABLE_NAME);
    Schema schema = getBasicSchema();
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt(0, 1);
    row.addInt(1, 2);
    row.addInt(2, 3);
    row.addString(3, "a string");
    row.addBoolean(4, true);
    AsyncKuduSession session = client.newSession();
    session.apply(insert).join(DEFAULT_SLEEP);
    session.close().join(DEFAULT_SLEEP);

    // Test getting all the columns back
    RecordReader<NullWritable, RowResult> reader = createRecordReader("*", null);
    assertTrue(reader.nextKeyValue());
    assertEquals(5, reader.getCurrentValue().getColumnProjection().getColumnCount());
    assertFalse(reader.nextKeyValue());

    // Test getting two columns back
    reader = createRecordReader(schema.getColumnByIndex(3).getName() + "," +
        schema.getColumnByIndex(2).getName(), null);
    assertTrue(reader.nextKeyValue());
    assertEquals(2, reader.getCurrentValue().getColumnProjection().getColumnCount());
    assertEquals("a string", reader.getCurrentValue().getString(0));
    assertEquals(3, reader.getCurrentValue().getInt(1));
    try {
      reader.getCurrentValue().getString(2);
      fail("Should only be getting 2 columns back");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }

    // Test getting one column back
    reader = createRecordReader(schema.getColumnByIndex(1).getName(), null);
    assertTrue(reader.nextKeyValue());
    assertEquals(1, reader.getCurrentValue().getColumnProjection().getColumnCount());
    assertEquals(2, reader.getCurrentValue().getInt(0));
    try {
      reader.getCurrentValue().getString(1);
      fail("Should only be getting 1 column back");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }

    // Test getting empty rows back
    reader = createRecordReader("", null);
    assertTrue(reader.nextKeyValue());
    assertEquals(0, reader.getCurrentValue().getColumnProjection().getColumnCount());
    assertFalse(reader.nextKeyValue());

    // Test getting an unknown table, will not work
    try {
      createRecordReader("unknown", null);
      fail("Should not be able to scan a column that doesn't exist");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // Test using a predicate that filters the row out.
    ColumnRangePredicate pred1 = new ColumnRangePredicate(schema.getColumnByIndex(1));
    pred1.setLowerBound(3);
    reader = createRecordReader("*", Lists.newArrayList(pred1));
    assertFalse(reader.nextKeyValue());
  }

  private RecordReader<NullWritable, RowResult> createRecordReader(String columnProjection,
        List<ColumnRangePredicate> predicates) throws IOException, InterruptedException {
    KuduTableInputFormat input = new KuduTableInputFormat();
    Configuration conf = new Configuration();
    conf.set(KuduTableInputFormat.MASTER_ADDRESSES_KEY, getMasterAddresses());
    conf.set(KuduTableInputFormat.INPUT_TABLE_KEY, TABLE_NAME);
    if (columnProjection != null) {
      conf.set(KuduTableInputFormat.COLUMN_PROJECTION_KEY, columnProjection);
    }
    if (predicates != null) {
      String encodedPredicates = KuduTableMapReduceUtil.base64EncodePredicates(predicates);
      conf.set(KuduTableInputFormat.ENCODED_COLUMN_RANGE_PREDICATES_KEY, encodedPredicates);
    }
    input.setConf(conf);
    List<InputSplit> splits = input.getSplits(null);

    // We need to re-create the input format to reconnect the client.
    input = new KuduTableInputFormat();
    input.setConf(conf);
    RecordReader<NullWritable, RowResult> reader = input.createRecordReader(null, null);
    reader.initialize(Iterables.getOnlyElement(splits), null);
    return reader;
  }
}
