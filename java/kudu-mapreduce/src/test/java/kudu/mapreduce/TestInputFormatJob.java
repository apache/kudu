// Copyright (c) 2014, Cloudera, inc.
package kudu.mapreduce;

import com.stumbleupon.async.Deferred;
import kudu.rpc.BaseKuduTest;
import kudu.rpc.CreateTableBuilder;
import kudu.rpc.Insert;
import kudu.rpc.KeyBuilder;
import kudu.rpc.KuduScanner;
import kudu.rpc.KuduSession;
import kudu.rpc.KuduTable;
import kudu.rpc.RowResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestInputFormatJob extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestInputFormatJob.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  /** Counter enumeration to count the actual rows. */
  private static enum Counters { ROWS }

  private static final int[] KEYS = new int[] {10, 20, 30};

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    CreateTableBuilder builder = new CreateTableBuilder();
    KeyBuilder keyBuilder = new KeyBuilder(basicSchema);
    for (int i : KEYS) {
      builder.addSplitKey(keyBuilder.addInt(i));
    }
    createTable(TABLE_NAME, basicSchema, builder);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      BaseKuduTest.tearDownAfterClass();
    } finally {
      HADOOP_UTIL.cleanup();
    }
  }

  @Test
  public void test() throws Exception {

    KuduSession session = client.newSession();
    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);

    // create a table with on empty tablet and 3 tablets of 3 rows each
    KuduTable table = openTable(TABLE_NAME);
    for (int key1 : KEYS) {
      for (int key2 = 1; key2 <= 3; key2++) {
        Insert insert = table.newInsert();
        insert.addInt(basicSchema.getColumn(0).getName(), key1 + key2);
        insert.addInt(basicSchema.getColumn(1).getName(), 1);
        insert.addInt(basicSchema.getColumn(2).getName(), 2);
        insert.addString(basicSchema.getColumn(3).getName(), "a string");
        session.apply(insert).join(DEFAULT_SLEEP);
      }
    }
    session.close().join(DEFAULT_SLEEP);

    Configuration conf = new Configuration();
    HADOOP_UTIL.setupAndGetTestDir(TestInputFormatJob.class.getName(), conf).getAbsolutePath();
    String jobName = TestInputFormatJob.class.getName();
    Job job = new Job(conf, jobName);

    Class mapperClass = TestMapperTableInput.class;
    job.setJarByClass(mapperClass);
    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
    KuduTableMapReduceUtil.initTableInputFormat(job, getMasterAddress() + ":" + getMasterPort(),
        TABLE_NAME,
        DEFAULT_SLEEP, false);

    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    assertEquals(9, job.getCounters().findCounter(Counters.ROWS).getValue());

    KuduScanner scanner = client.newScanner(table, basicSchema);
    assertEquals(9, countRowsInScan(scanner));
  }

  /**
   * Simple row counter and printer
   */
  static class TestMapperTableInput extends
      Mapper<NullWritable, RowResult, NullWritable, NullWritable> {

    @Override
    protected void map(NullWritable key, RowResult value, Context context) throws IOException,
        InterruptedException {
      context.getCounter(Counters.ROWS).increment(1);
      LOG.info(value.toStringLongFormat()); // useful to visual debugging
    }
  }

}
