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

package org.apache.kudu.mapreduce.tools;

import static org.apache.kudu.test.ClientTestUtil.createFourTabletsTableWithNineRows;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.HadoopTestingUtility;

public class ITRowCounter {

  private static final String TABLE_NAME =
      ITRowCounter.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @After
  public void tearDown() throws Exception {
    HADOOP_UTIL.cleanup();
  }

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    HADOOP_UTIL.setupAndGetTestDir(ITRowCounter.class.getName(), conf).getAbsolutePath();

    createFourTabletsTableWithNineRows(harness.getAsyncClient(), TABLE_NAME, DEFAULT_SLEEP);

    String[] args = new String[] {
        "-D" + CommandLineParser.MASTER_ADDRESSES_KEY + "=" + harness.getMasterAddressesAsString(), TABLE_NAME};
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    Job job = RowCounter.createSubmittableJob(parser.getConfiguration(), parser.getRemainingArgs());
    assertTrue("Job did not end properly", job.waitForCompletion(true));

    assertEquals(9, job.getCounters().findCounter(RowCounter.Counters.ROWS).getValue());
  }
}
