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

import static java.sql.Types.TIMESTAMP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.KuduTableMapReduceUtil;

/**
 * Map-only job that reads Apache Parquet files and inserts them into a single Kudu table.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ImportParquet extends Configured implements Tool {

  static final String NAME = "importparquet";
  static final String JOB_NAME_CONF_KEY = "importparquet.job.name";
  static final String PARQUET_INPUT_SCHEMA = "importparquet.input.schema";

  /**
   * Sets up the actual job.
   *
   * @param conf the current configuration
   * @param args the command line parameters
   * @return the newly created job
   * @throws java.io.IOException when setting up the job fails
   */
  @SuppressWarnings("deprecation")
  public static Job createSubmittableJob(Configuration conf, String[] args)
      throws IOException, ClassNotFoundException {

    final String tableName = args[0];
    Path inputDir = new Path(args[1]);

    List<Footer> footers = new ArrayList<Footer>();
    footers.addAll(ParquetFileReader.readFooters(conf, inputDir));

    MessageType schema = footers.get(0).getParquetMetadata().getFileMetaData().getSchema();
    GroupWriteSupport.setSchema(schema, conf);
    conf.set(PARQUET_INPUT_SCHEMA, schema.toString());

    String jobName = conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName);
    Job job = new Job(conf,jobName);
    job.setJarByClass(ImportParquet.class);
    job.setMapperClass(ImportParquetMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(ParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(job, ParquetReadSupport.class);
    ParquetInputFormat.setInputPaths(job, inputDir);

    CommandLineParser cmdLineParser = new CommandLineParser(conf);
    KuduClient client = cmdLineParser.getClient();
    KuduTable table = client.openTable(tableName);


    // Pre-flight checks of input parquet schema and table schema.
    for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
      if (schema.containsField(columnSchema.getName())) {
        if (!schema.getType(columnSchema.getName()).asPrimitiveType().getPrimitiveTypeName()
            .equals(getTypeName(columnSchema.getType()))) {
          throw new IllegalArgumentException("The column type " +
              getTypeName(columnSchema.getType()) + " does not exist in Parquet schema");
        }
      } else {
        throw new IllegalArgumentException("The column " + columnSchema.getName() +
            " does not exist in Parquet schema");
      }
    }
    // Kudu doesn't support Parquet's TIMESTAMP.
    Iterator<ColumnDescriptor> fields = schema.getColumns().iterator();
    while (fields.hasNext()) {
      if (fields.next().getType().equals(TIMESTAMP)) {
        throw new IllegalArgumentException("This " + fields.next().getType() +
          " Parquet type is not supported in Kudu");
      }
    }

    FileInputFormat.setInputPaths(job, inputDir);
    new KuduTableMapReduceUtil.TableOutputFormatConfiguratorWithCommandLineParser(
        job,
        tableName)
        .configure();
    return job;
  }

  private static PrimitiveType.PrimitiveTypeName getTypeName(Type type) {
    switch (type) {
      case BOOL:
        return PrimitiveType.PrimitiveTypeName.BOOLEAN;
      case INT8:
        return PrimitiveType.PrimitiveTypeName.INT32;
      case INT16:
        return PrimitiveType.PrimitiveTypeName.INT64;
      case INT32:
        return PrimitiveType.PrimitiveTypeName.INT32;
      case INT64:
        return PrimitiveType.PrimitiveTypeName.INT64;
      case STRING:
        return PrimitiveType.PrimitiveTypeName.BINARY;
      case FLOAT:
        return PrimitiveType.PrimitiveTypeName.FLOAT;
      case DOUBLE:
        return PrimitiveType.PrimitiveTypeName.DOUBLE;
      default:
        throw new IllegalArgumentException("Type " + type.getName() + " not recognized");
    }
  }

  /*
   * @param errorMsg error message. can be null
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    String usage =
        "Usage: " + NAME + "<table.name> <input.dir>\n\n" +
            "Imports the given input directory of Apache Parquet data into the specified table.\n" +
            "Other options that may be specified with -D include:\n" +
            "-D" + JOB_NAME_CONF_KEY + "=jobName - use the specified mapreduce job name for the" +
            "import.\n" + CommandLineParser.getHelpSnippet();

    System.err.println(usage);
  }

  @Override
  public int run(String[] otherArgs) throws Exception {
    if (otherArgs.length < 1) {
      usage("Wrong number of arguments: " + otherArgs.length);
      return -1;
    }
    Job job = createSubmittableJob(getConf(), otherArgs);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new ImportParquet(), args);
    System.exit(status);
  }
}
