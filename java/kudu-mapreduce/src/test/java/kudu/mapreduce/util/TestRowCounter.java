// Copyright (c) 2014, Cloudera, inc.
package kudu.mapreduce.util;

import kudu.mapreduce.HadoopTestingUtility;
import kudu.rpc.BaseKuduTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRowCounter extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestRowCounter.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
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
    HADOOP_UTIL.setupAndGetTestDir(TestRowCounter.class.getName(), conf).getAbsolutePath();

    createFourTabletsTableWithNineRows(TABLE_NAME);

    String[] args = new String[] {TABLE_NAME, getMasterAddressAndPort()};
    Job job = RowCounter.createSubmittableJob(conf, args);
    assertTrue("Job did not end properly", job.waitForCompletion(true));

    assertEquals(9, job.getCounters().findCounter(RowCounter.Counters.ROWS).getValue());
  }
}
