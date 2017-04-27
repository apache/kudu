// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.kudu.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.BaseKuduTest;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.RowResult;

public class ITInputFormatJob extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(ITInputFormatJob.class);

  private static final String TABLE_NAME =
      ITInputFormatJob.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  /** Counter enumeration to count the actual rows. */
  private static enum Counters { ROWS }

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
  @SuppressWarnings("deprecation")
  public void test() throws Exception {

    createFourTabletsTableWithNineRows(TABLE_NAME);

    Configuration conf = new Configuration();
    HADOOP_UTIL.setupAndGetTestDir(ITInputFormatJob.class.getName(), conf).getAbsolutePath();

    createAndTestJob(conf, new ArrayList<KuduPredicate>(), 9);

    KuduPredicate pred1 = KuduPredicate.newComparisonPredicate(
        basicSchema.getColumnByIndex(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 20);
    createAndTestJob(conf, Lists.newArrayList(pred1), 6);

    KuduPredicate pred2 = KuduPredicate.newComparisonPredicate(
        basicSchema.getColumnByIndex(2), KuduPredicate.ComparisonOp.LESS_EQUAL, 1);
    createAndTestJob(conf, Lists.newArrayList(pred1, pred2), 2);
  }

  private void createAndTestJob(Configuration conf,
                                List<KuduPredicate> predicates, int expectedCount)
      throws Exception {
    String jobName = ITInputFormatJob.class.getName();
    Job job = new Job(conf, jobName);

    Class<TestMapperTableInput> mapperClass = TestMapperTableInput.class;
    job.setJarByClass(mapperClass);
    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
    KuduTableMapReduceUtil.TableInputFormatConfigurator configurator =
        new KuduTableMapReduceUtil.TableInputFormatConfigurator(
            job,
            TABLE_NAME,
            "*",
            getMasterAddresses())
            .operationTimeoutMs(DEFAULT_SLEEP)
            .addDependencies(false)
            .cacheBlocks(false)
            .isFaultTolerant(false);
    for (KuduPredicate predicate : predicates) {
      configurator.addPredicate(predicate);
    }
    configurator.configure();

    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    assertEquals(expectedCount, job.getCounters().findCounter(Counters.ROWS).getValue());
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
