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

import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.HadoopTestingUtility;

public class ITImportCsv {

  private static final String TABLE_NAME =
      ITImportCsv.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  private static Schema schema;

  static {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(4);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column1_i", Type.INT32)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column2_d", Type.DOUBLE)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column3_s", Type.STRING)
        .nullable(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column4_b", Type.BOOL)
        .build());
    schema = new Schema(columns);
  }

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    harness.getClient().createTable(TABLE_NAME, schema,
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key")));
  }

  @After
  public void tearDown() throws Exception {
    HADOOP_UTIL.cleanup();
  }

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    String testHome =
        HADOOP_UTIL.setupAndGetTestDir(ITImportCsv.class.getName(), conf).getAbsolutePath();

    // Create a 2 lines input file
    File data = new File(testHome, "data.csv");
    writeCsvFile(data);

    StringBuilder sb = new StringBuilder();
    for (ColumnSchema col : schema.getColumns()) {
      sb.append(col.getName());
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    String[] args = new String[] {
        "-D" + CommandLineParser.MASTER_ADDRESSES_KEY + "=" + harness.getMasterAddressesAsString(),
        sb.toString(), TABLE_NAME, data.toString()};

    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    Job job = ImportCsv.createSubmittableJob(parser.getConfiguration(), parser.getRemainingArgs());
    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    KuduTable openTable = harness.getClient().openTable(TABLE_NAME);
    assertEquals(1, job.getCounters().findCounter(ImportCsv.Counters.BAD_LINES).getValue());
    assertEquals(3, countRowsInScan(harness.getAsyncClient().newScannerBuilder(openTable).build()));
    assertEquals("INT32 key=1, INT32 column1_i=3, DOUBLE column2_d=2.3, STRING column3_s=some " +
      "string, BOOL column4_b=true", scanTableToStrings(openTable).get(0));
  }

  private void writeCsvFile(File data) throws IOException {
    FileOutputStream fos = new FileOutputStream(data);
    fos.write("1\t3\t2.3\tsome string\ttrue\n".getBytes(UTF_8));
    fos.write("2\t5\t4.5\tsome more\tfalse\n".getBytes(UTF_8));
    fos.write("3\t7\twait this is not a double\tbad row\ttrue\n".getBytes(UTF_8));
    fos.write("4\t9\t10\ttrailing separator isn't bad mkay?\ttrue\t\n".getBytes(UTF_8));
    fos.close();
  }
}
