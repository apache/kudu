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
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.HadoopTestingUtility;
import org.apache.kudu.test.KuduTestHarness;

public class ITImportParquetPreCheck {

  private static final String TABLE_NAME =
      ITImportParquet.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  private static Schema schema;

  static {
    ArrayList<ColumnSchema> columns = new ArrayList<>(4);
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

    // Create a 4 records parquet input file.
    Path data = new Path(testHome, "data.parquet");
    writeParquetFile(data,conf);

    String[] args = new String[] { "-D" + CommandLineParser.MASTER_ADDRESSES_KEY + "=" +
      harness.getMasterAddressesAsString(), TABLE_NAME, data.toString()};

    Throwable thrown = Assert.assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        Job job = ImportParquet.createSubmittableJob(parser.getConfiguration(),
                parser.getRemainingArgs());
        job.waitForCompletion(true);
      }
    });
    Assert.assertTrue(thrown.getMessage()
            .contains("The column column1_i does not exist in Parquet schema"));

    KuduTable openTable = harness.getClient().openTable(TABLE_NAME);
    assertEquals(0,
            countRowsInScan(harness.getAsyncClient().newScannerBuilder(openTable).build()));
  }

  @SuppressWarnings("deprecation")
  private void writeParquetFile(Path data,Configuration conf) throws IOException {
    MessageType schema = parseMessageType(
        "message test { " +
          "required int32 key; " +
          "required int32 column1_i_s; " +
          "required binary column2_d; " +
          "required binary column3_s; " +
          "required boolean column4_b; " +
          "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<>(data, new GroupWriteSupport(),
        UNCOMPRESSED, 1024, 1024, 512, true, false,
        ParquetProperties.WriterVersion.PARQUET_1_0, conf);

    writer.write(f.newGroup().append("key", 1)
        .append("column1_i_s", 292)
        .append("column2_d", "no type")
        .append("column3_s", "some string")
        .append("column4_b", true));
    writer.write(f.newGroup().append("key", 2)
        .append("column1_i_s", 23)
        .append("column2_d", "no type")
        .append("column3_s", "some more")
        .append("column4_b", false));
    writer.write(f.newGroup()
        .append("key", 3)
        .append("column1_i_s", 32)
        .append("column2_d", "no type")
        .append("column3_s", "some more and more")
        .append("column4_b", true));
    writer.write(f.newGroup()
        .append("key", 4)
        .append("column1_i_s", 22)
        .append("column2_d", "no type")
        .append("column3_s", "some more and alst")
        .append("column4_b", false));
    writer.close();
  }
}
