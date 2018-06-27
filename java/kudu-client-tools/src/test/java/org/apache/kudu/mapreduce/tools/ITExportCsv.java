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

import static org.apache.kudu.util.ClientTestUtil.createFourTabletsTableWithNineRows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.After;
import org.junit.Test;

import org.apache.kudu.client.BaseKuduTest;
import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.HadoopTestingUtility;

public class ITExportCsv extends BaseKuduTest {

  private static final String TABLE_NAME =
    ITExportCsv.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  @After
  public void tearDown() throws Exception {
    HADOOP_UTIL.cleanup();
  }

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    String testHome =
      HADOOP_UTIL.setupAndGetTestDir(ITExportCsv.class.getName(), conf).getAbsolutePath();

    // create a table with on empty tablet and 3 tablets of 3 rows each.
    createFourTabletsTableWithNineRows(client, TABLE_NAME, DEFAULT_SLEEP);
    String[] args = new String[] {
      "-D" + CommandLineParser.MASTER_ADDRESSES_KEY + "=" + getMasterAddresses(),
      "*", TABLE_NAME, testHome + "/exportdata"};

    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    Job job = ExportCsv.createSubmittableJob(parser.getConfiguration(), parser.getRemainingArgs());
    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    String csvContent = readCsvFile(new File(testHome + "/exportdata/part-m-00001"));
    assertEquals(csvContent.split("\n").length,3);
    assertEquals(csvContent.split("\n", -1)[0].split("\t", -1)[3],"a string");
  }

  private String readCsvFile(File data) throws IOException {
    FileInputStream fos = new FileInputStream(data);
    return IOUtils.toString(fos, "UTF-8");
  }
}