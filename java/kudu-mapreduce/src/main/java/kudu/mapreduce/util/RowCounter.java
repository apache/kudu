// Copyright (c) 2014, Cloudera, inc.
package kudu.mapreduce.util;

import kudu.mapreduce.KuduTableMapReduceUtil;
import kudu.rpc.RowResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Map-only job that counts all the rows in the provided table.
 * TODO be able to provide a start and end key
 */
public class RowCounter extends Configured implements Tool {

  static final String NAME = "rowcounter";
  static final String OPERATION_TIMEOUT_MS_KEY = "rowcounter.operation.timeout.ms";
  static final String COLUMN_PROJECTION_KEY = "rowcounter.column.projection";

  /** Counter enumeration to count the actual rows. */
  public static enum Counters { ROWS }

  /**
   * Simple row counter
   */
  static class RowCounterMapper extends
      Mapper<NullWritable, RowResult, NullWritable, NullWritable> {

    @Override
    protected void map(NullWritable key, RowResult value, Context context) throws IOException,
        InterruptedException {
      context.getCounter(Counters.ROWS).increment(1);
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws java.io.IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
      throws IOException, ClassNotFoundException {

    long timeout = conf.getLong(OPERATION_TIMEOUT_MS_KEY, 10000);
    String columnProjection = conf.get(COLUMN_PROJECTION_KEY);

    Class mapperClass = RowCounterMapper.class;
    String tableName = args[0];
    String masterAddress = args[1];

    String jobName = NAME + "_" + tableName;
    Job job = new Job(conf, jobName);
    job.setJarByClass(mapperClass);
    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
    KuduTableMapReduceUtil.initTableInputFormat(job, masterAddress, tableName, timeout,
        columnProjection, true);
    return job;
  }

  /*
   * @param errorMsg Error message. Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    String usage =
        "Usage: " + NAME + " <table.name> <master.address>\n\n" +
            "Counts all the rows in the given table.\n" +
            "\n" +
            "Other options that may be specified with -D include:\n" +
            "  -D" + OPERATION_TIMEOUT_MS_KEY + "=10000 - how long this job waits for " +
            "Kudu operations.\n" +
            "  -D" + COLUMN_PROJECTION_KEY + "=a,b,c - comma-separated list of columns to read " +
            "as part of the row count. By default, none are read so that the count is as fast " +
            "as possible. When specifying columns that are keys, they must be at the beginning.\n";

    System.err.println(usage);
  }

  @Override
  public int run(String[] otherArgs) throws Exception {
    if (otherArgs.length != 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      return -1;
    }
    Job job = createSubmittableJob(getConf(), otherArgs);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new RowCounter(), args);
    System.exit(status);
  }
}
