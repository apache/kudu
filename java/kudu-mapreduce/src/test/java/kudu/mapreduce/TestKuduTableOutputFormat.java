// Copyright (c) 2014, Cloudera, inc.
package kudu.mapreduce;

import kudu.Schema;
import kudu.rpc.BaseKuduTest;
import kudu.rpc.CreateTableBuilder;
import kudu.rpc.Insert;
import kudu.rpc.KuduScanner;
import kudu.rpc.KuduTable;
import kudu.rpc.Operation;
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
    conf.set(KuduTableOutputFormat.MASTER_ADDRESS_KEY, getMasterAddressAndPort());
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

    RecordWriter<NullWritable, Operation> rw = output.getRecordWriter(null);
    rw.write(NullWritable.get(), insert);
    rw.close(null);
    KuduScanner scanner = client.newScanner(table, schema);
    assertEquals(1, countRowsInScan(scanner));
  }
}
