// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.mapreduce;

import org.kududb.client.BaseKuduTest;
import org.kududb.client.CreateTableBuilder;
import org.kududb.client.Insert;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestOutputFormatJob extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestOutputFormatJob.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    createTable(TABLE_NAME, getBasicSchema(), new CreateTableBuilder());
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
  @SuppressWarnings("deprecation")
  public void test() throws Exception {
    Configuration conf = new Configuration();
    String testHome =
        HADOOP_UTIL.setupAndGetTestDir(TestOutputFormatJob.class.getName(), conf).getAbsolutePath();
    String jobName = TestOutputFormatJob.class.getName();
    Job job = new Job(conf, jobName);


    // Create a 2 lines input file
    File data = new File(testHome, "data.txt");
    writeDataFile(data);
    FileInputFormat.setInputPaths(job, data.toString());

    // Configure the job to map the file and write to kudu, without reducers
    Class<TestMapperTableOutput> mapperClass = TestMapperTableOutput.class;
    job.setJarByClass(mapperClass);
    job.setMapperClass(mapperClass);
    job.setInputFormatClass(TextInputFormat.class);
    job.setNumReduceTasks(0);
    new KuduTableMapReduceUtil.TableOutputFormatConfigurator(
        job,
        TABLE_NAME,
        getMasterQuorum())
        .operationTimeoutMs(DEFAULT_SLEEP)
        .addDependencies(false)
        .configure();

    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    // Make sure the data's there
    KuduTable table = openTable(TABLE_NAME);
    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table, getBasicSchema());
    assertEquals(2, countRowsInScan(builder.build()));
  }

  /**
   * Simple Mapper that writes one row per line, the key is the line number and the STRING column
   * is the data from that line
   */
  static class TestMapperTableOutput extends
      Mapper<LongWritable, Text, NullWritable, Operation> {

    private KuduTable table;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      Insert insert = table.newInsert();
      insert.addInt(table.getSchema().getColumn(0).getName(), (int) key.get());
      insert.addInt(table.getSchema().getColumn(1).getName(), 1);
      insert.addInt(table.getSchema().getColumn(2).getName(), 2);
      insert.addString(table.getSchema().getColumn(3).getName(), value.toString());
      insert.addBoolean(table.getSchema().getColumn(4).getName(), true);
      context.write(NullWritable.get(), insert);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      table = KuduTableMapReduceUtil.getTableFromContext(context);
    }
  }

  private void writeDataFile(File data) throws IOException {
    FileOutputStream fos = new FileOutputStream(data);
    fos.write("VALUE1\nVALUE2\n".getBytes());
    fos.close();
  }
}
