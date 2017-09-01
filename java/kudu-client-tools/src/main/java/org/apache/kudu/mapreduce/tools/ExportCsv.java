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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.KuduTableInputFormat;
import org.apache.kudu.mapreduce.KuduTableMapReduceUtil;

/**
 * Map-only job that reads Kudu rows and writes them into a CSV file.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ExportCsv extends Configured implements Tool {

  static final String NAME = "exportcsv";
  static final String DEFAULT_SEPARATOR = "\t";
  static final String SEPARATOR_CONF_KEY = "exportcsv.separator";
  static final String JOB_NAME_CONF_KEY = "exportcsv.job.name";
  static final String COLUMNS_NAMES_KEY = "exportcsv.column.names";

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

    Class<ExportCsvMapper> mapperClass = ExportCsvMapper.class;
    conf.set(COLUMNS_NAMES_KEY, args[0]);
    String tableName = args[1];
    final Path outputDir = new Path(args[2]);

    String jobName = conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName);
    Job job = new Job(conf, jobName);
    job.setJarByClass(mapperClass);
    job.setInputFormatClass(KuduTableInputFormat.class);
    new KuduTableMapReduceUtil.TableInputFormatConfiguratorWithCommandLineParser(job, tableName,
      args[0]).configure();
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, outputDir);
    return job;
  }

  /*
   * @param errorMsg error message. can be null
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    String usage = "Usage: " + NAME + " <colAa,colB,colC> <table.name> <output.dir>\n\n" +
        "Exports the given table and columns into the specified output path.\n" +
        "The column names of the Kudu table must be specified in the form of \n" +
        "comma-separated column names.\n" +
        "Other options that may be specified with -D include:\n" +
        "'-D" + SEPARATOR_CONF_KEY + "=|' - eg separate on pipes instead of tabs\n" +
        "-D" + JOB_NAME_CONF_KEY + "=jobName - use the specified mapreduce job name for the" +
        "export.\n" + CommandLineParser.getHelpSnippet();
    System.err.println(usage);
  }

  @Override
  public int run(String[] otherArgs) throws Exception {
    if (otherArgs.length < 3) {
      usage("Wrong number of arguments: " + otherArgs.length);
      return -1;
    }
    Job job = createSubmittableJob(getConf(), otherArgs);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new ExportCsv(), args);
    System.exit(status);
  }
}
