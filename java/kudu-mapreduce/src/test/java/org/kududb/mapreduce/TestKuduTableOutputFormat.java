// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.mapreduce;

import org.kududb.Schema;
import org.kududb.client.BaseKuduTest;
import org.kududb.client.CreateTableBuilder;
import org.kududb.client.Insert;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestKuduTableOutputFormat extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestKuduTableOutputFormat.class.getName() + "-" + System.currentTimeMillis();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
  }

  @Test
  public void test() throws Exception {
    createTable(TABLE_NAME, getBasicSchema(), new CreateTableBuilder());

    KuduTableOutputFormat output = new KuduTableOutputFormat();
    Configuration conf = new Configuration();
    conf.set(KuduTableOutputFormat.MASTER_QUORUM_KEY, getMasterQuorum());
    conf.set(KuduTableOutputFormat.OUTPUT_TABLE_KEY, TABLE_NAME);
    output.setConf(conf);

    String multitonKey = conf.get(KuduTableOutputFormat.MULTITON_KEY);
    KuduTable table = KuduTableOutputFormat.getKuduTable(multitonKey);
    assertNotNull(table);
    Schema schema = table.getSchema();

    Insert insert = table.newInsert();
    insert.addInt(schema.getColumn(0).getName(), 1);
    insert.addInt(schema.getColumn(1).getName(), 2);
    insert.addInt(schema.getColumn(2).getName(), 3);
    insert.addString(schema.getColumn(3).getName(), "a string");
    insert.addBoolean(schema.getColumn(4).getName(), true);

    RecordWriter<NullWritable, Operation> rw = output.getRecordWriter(null);
    rw.write(NullWritable.get(), insert);
    rw.close(null);
    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table, schema);
    assertEquals(1, countRowsInScan(builder.build()));
  }
}
