// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.mapreduce.util;

import kudu.ColumnSchema;
import kudu.mapreduce.CommandLineParser;
import kudu.mapreduce.HadoopTestingUtility;
import kudu.rpc.BaseKuduTest;
import kudu.rpc.CreateTableBuilder;
import kudu.rpc.KuduScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestImportCsv extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestImportCsv.class.getName() + "-" + System.currentTimeMillis();

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
  public void test() throws Exception {
    Configuration conf = new Configuration();
    String testHome =
        HADOOP_UTIL.setupAndGetTestDir(TestImportCsv.class.getName(), conf).getAbsolutePath();

    // Create a 2 lines input file
    File data = new File(testHome, "data.csv");
    writeCsvFile(data);

    StringBuilder sb = new StringBuilder();
    for (ColumnSchema col : basicSchema.getColumns()) {
      sb.append(col.getName());
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    String[] args = new String[] {
        "-D" + CommandLineParser.MASTER_ADDRESS_KEY + "=" + getMasterAddressAndPort(),
        sb.toString(), TABLE_NAME, data.toString()};

    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    Job job = ImportCsv.createSubmittableJob(parser.getConfiguration(), parser.getRemainingArgs());
    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    assertEquals(1, job.getCounters().findCounter(ImportCsv.Counters.BAD_LINES).getValue());

    KuduScanner scanner = client.newScanner(openTable(TABLE_NAME), basicSchema);
    assertEquals(4, countRowsInScan(scanner));
  }

  private void writeCsvFile(File data) throws IOException {
    FileOutputStream fos = new FileOutputStream(data);
    fos.write("1\t2\t3\tsome string\n".getBytes());
    fos.write("2\t4\t5\tsome more\n".getBytes());
    fos.write("3\twait this is not an int\t7\tbad row\n".getBytes());
    // Test that we can handle floating points, treating them as ints. This is a valid line:
    fos.write("4\t0.10\t8\twe have a double but that's fine\n".getBytes());
    fos.write("5\t9\t10\ttrailing separator isn't bad mkay?\t\n".getBytes());
    fos.close();
  }
}
