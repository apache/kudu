// Copyright (c) 2014, Cloudera, inc.
package kudu.mapreduce;

import com.google.common.collect.Iterables;
import kudu.Schema;
import kudu.rpc.BaseKuduTest;
import kudu.rpc.CreateTableBuilder;
import kudu.rpc.Insert;
import kudu.rpc.KuduSession;
import kudu.rpc.KuduTable;
import kudu.rpc.RowResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestKuduTableInputFormat extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestKuduTableInputFormat.class.getName() + "-" + System.currentTimeMillis();

  @Test
  public void test() throws Exception {
    createTable(TABLE_NAME, getBasicSchema(), new CreateTableBuilder());

    KuduTableInputFormat input = new KuduTableInputFormat();
    Configuration conf = new Configuration();
    conf.set(KuduTableInputFormat.MASTER_ADDRESS_KEY, getMasterAddressAndPort());
    conf.set(KuduTableInputFormat.INPUT_TABLE_KEY, TABLE_NAME);
    input.setConf(conf);

    KuduTable table = openTable(TABLE_NAME);
    Schema schema = getBasicSchema();
    Insert insert = table.newInsert();
    insert.addInt(schema.getColumn(0).getName(), 1);
    insert.addInt(schema.getColumn(1).getName(), 2);
    insert.addInt(schema.getColumn(2).getName(), 3);
    insert.addString(schema.getColumn(3).getName(), "a string");
    KuduSession session = client.newSession();
    session.apply(insert).join(DEFAULT_SLEEP);
    session.close().join(DEFAULT_SLEEP);


    List<InputSplit> splits = input.getSplits(null);
    RecordReader<NullWritable, RowResult> reader = input.createRecordReader(null, null);
    reader.initialize(Iterables.getOnlyElement(splits), null);
    assertTrue(reader.nextKeyValue());
    assertFalse(reader.nextKeyValue());
  }
}
