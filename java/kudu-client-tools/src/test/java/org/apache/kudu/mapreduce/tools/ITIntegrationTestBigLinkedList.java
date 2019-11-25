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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.HadoopTestingUtility;
import org.apache.kudu.test.KuduTestHarness;

public class ITIntegrationTestBigLinkedList {

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
    HADOOP_UTIL.setupAndGetTestDir(
        ITIntegrationTestBigLinkedList.class.getName(),conf).getAbsolutePath();

    String[] args = new String[] {
        "-D" + CommandLineParser.MASTER_ADDRESSES_KEY + "=" + harness.getMasterAddressesAsString(),
        "Loop",
        "2", // Two iterations
        "1", // 1 mapper
        "2500", // 2.5k rows to insert
        "1", // 1 tablet
        "/tmp/itbll", // output dir
        "1", // 1 reduce
        "100", // create 100 columns
        "25", // wrap them together after 25 rows
        "0"
    };
    int ret = ToolRunner.run(new IntegrationTestBigLinkedList(), args);
    Assert.assertEquals(0, ret);

    args[2] = "1"; // Just one iteration this time
    args[10] = "5000"; // 2 * 2500 from previous run
    ret = ToolRunner.run(new IntegrationTestBigLinkedList(), args);
    Assert.assertEquals(0, ret);
  }
}
