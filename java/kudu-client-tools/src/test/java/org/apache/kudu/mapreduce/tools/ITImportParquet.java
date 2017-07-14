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

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.kudu.client.KuduTable;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.BaseKuduTest;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.HadoopTestingUtility;

public class ITImportParquet extends BaseKuduTest {

  private static final String TABLE_NAME =
    ITImportParquet.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  private static Schema schema;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();

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

    createTable(TABLE_NAME, schema,
      new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key")));
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
      HADOOP_UTIL.setupAndGetTestDir(ITImportCsv.class.getName(), conf).getAbsolutePath();

    // Create a 4 records parquet input file.
    Path data = new Path(testHome, "data.parquet");
    writeParquetFile(data,conf);

    StringBuilder sb = new StringBuilder();
    for (ColumnSchema col : schema.getColumns()) {
      sb.append(col.getName());
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    String[] args = new String[] { "-D" + CommandLineParser.MASTER_ADDRESSES_KEY + "=" + getMasterAddresses(),
      TABLE_NAME, data.toString()};

    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    Job job = ImportParquet.createSubmittableJob(parser.getConfiguration(), parser.getRemainingArgs());
    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    KuduTable openTable = openTable(TABLE_NAME);
    assertEquals(4, countRowsInScan(
      client.newScannerBuilder(openTable).build()));
    assertEquals("INT32 key=1, INT32 column1_i=3, DOUBLE column2_d=2.3, STRING column3_s=some string, " +
      "BOOL column4_b=true",scanTableToStrings(openTable).get(0));
  }

  private void writeParquetFile(Path data,Configuration conf) throws IOException {
    MessageType schema = parseMessageType(
      "message test { "
        + "required int32 key; "
        + "required int32 column1_i; "
        + "required double column2_d; "
        + "required binary column3_s; "
        + "required boolean column4_b; "
        + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(data, new GroupWriteSupport(),
      UNCOMPRESSED, 1024, 1024, 512, true, false, ParquetProperties.WriterVersion.PARQUET_1_0, conf);

    writer.write(f.newGroup().append("key", 1).append("column1_i", 3).append("column2_d", 2.3)
        .append("column3_s", "some string").append("column4_b", true));
    writer.write(f.newGroup().append("key", 2).append("column1_i", 5).append("column2_d", 4.5)
        .append("column3_s", "some more").append("column4_b", false));
    writer.write(f.newGroup().append("key", 3).append("column1_i", 7).append("column2_d", 5.6)
        .append("column3_s", "some more and more").append("column4_b", true));
    writer.write(f.newGroup().append("key", 4).append("column1_i", 9).append("column2_d",10.9)
        .append("column3_s", "some more and alst").append("column4_b", false));
    writer.close();
  }
}