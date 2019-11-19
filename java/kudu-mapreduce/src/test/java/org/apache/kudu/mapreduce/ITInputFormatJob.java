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

import static org.apache.kudu.test.ClientTestUtil.createFourTabletsTableWithNineRows;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kudu.Schema;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.RowResult;

public class ITInputFormatJob {
  private static final Logger LOG = LoggerFactory.getLogger(ITInputFormatJob.class);

  private static final String TABLE_NAME =
      ITInputFormatJob.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  /** Counter enumeration to count the actual rows. */
  private enum Counters { ROWS }

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @After
  public void tearDown() throws Exception {
    HADOOP_UTIL.cleanup();
  }

  @Test
  public void test() throws Exception {

    createFourTabletsTableWithNineRows(harness.getAsyncClient(), TABLE_NAME, DEFAULT_SLEEP);

    JobConf conf = new JobConf();
    HADOOP_UTIL.setupAndGetTestDir(ITInputFormatJob.class.getName(), conf);

    createAndTestJob(conf, new ArrayList<>(), 9);

    KuduPredicate pred1 = KuduPredicate.newComparisonPredicate(
        basicSchema.getColumnByIndex(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 20);
    createAndTestJob(conf, Lists.newArrayList(pred1), 6);

    KuduPredicate pred2 = KuduPredicate.newComparisonPredicate(
        basicSchema.getColumnByIndex(2), KuduPredicate.ComparisonOp.LESS_EQUAL, 1);
    createAndTestJob(conf, Lists.newArrayList(pred1, pred2), 2);
  }

  @SuppressWarnings("deprecation")
  private void createAndTestJob(JobConf conf,
                                List<KuduPredicate> predicates, int expectedCount)
      throws Exception {
    String jobName = ITInputFormatJob.class.getName();
    Job job = new Job(conf);
    job.setJobName(jobName);

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
            harness.getMasterAddressesAsString())
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
    protected void map(NullWritable key, RowResult value, Context context) {
      context.getCounter(Counters.ROWS).increment(1);
      LOG.info(value.toStringLongFormat()); // useful to visual debugging
    }
  }

}
