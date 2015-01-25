// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.mapreduce;

import kudu.rpc.KuduClient;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility class that manages common configurations to all MR jobs. For example,
 * any job that uses {#kudu.mapreduce.KuduTableMapReduceUtil} to setup an input or output format
 * and that has parsed the command line arguments with
 * {@link org.apache.hadoop.util.GenericOptionsParser} can simply be passed:
 * <code>
 * -Dmaster.address=ADDRESS
 * </code>
 * in order to specify where the master is.
 * Use {@link CommandLineParser#getHelpSnippet()} to provide usage text for the configurations
 * managed by this class.
 */
public class CommandLineParser {
  private final Configuration conf;
  public static final String MASTER_QUORUM_KEY = "kudu.master.quorum";
  public static final String MASTER_QUORUM_DEFAULT = "127.0.0.1";
  public static final String OPERATION_TIMEOUT_MS_KEY = "kudu.operation.timeout.ms";
  public static final long OPERATION_TIMEOUT_MS_DEFAULT = 10000;
  public static final String NUM_REPLICAS_KEY = "kudu.num.replicas";
  public static final int NUM_REPLICAS_DEFAULT = 3;

  /**
   * Constructor that uses a Configuration that has already been through
   * {@link org.apache.hadoop.util.GenericOptionsParser}'s command line parsing.
   * @param conf the configuration from which job configurations will be extracted
   */
  public CommandLineParser(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the configured master's quorum.
   * @return a string that contains the passed quorum, or the default value
   */
  public String getMasterQuorum() {
    return conf.get(MASTER_QUORUM_KEY, MASTER_QUORUM_DEFAULT);
  }

  /**
   * Get the configured timeout.
   * @return a long that represents the passed timeout, or the default value
   */
  public long getOperationTimeoutMs() {
    return conf.getLong(OPERATION_TIMEOUT_MS_KEY, OPERATION_TIMEOUT_MS_DEFAULT);
  }

  /**
   * Get the number of replicas to use when configuring a new table.
   * @return an int that represents the passed number of replicas to use, or the default value.
   */
  public int getNumReplicas() {
    return conf.getInt(NUM_REPLICAS_KEY, NUM_REPLICAS_DEFAULT);
  }

  /**
   * Get a client connected to the configured Master.
   * @return a kudu client
   */
  public KuduClient getClient() {
    return KuduTableMapReduceUtil.connect(getMasterQuorum());
  }

  /**
   * This method returns a single multi-line string that contains the help snippet to append to
   * the tail of a usage() or help() type of method.
   * @return a string with all the available configurations and their defaults
   */
  public static String getHelpSnippet() {
    return "\nAdditionally, the following options are available:" +
      "  -D" + OPERATION_TIMEOUT_MS_KEY + "=TIME - how long this job waits for " +
          "Kudu operations, defaults to " + OPERATION_TIMEOUT_MS_DEFAULT + " \n"+
      "  -D" + MASTER_QUORUM_KEY + "=ADDRESSES - addresses to reach the Masters, " +
        "defaults to " + MASTER_QUORUM_DEFAULT + " which is usually wrong.\n" +
      "  -D " + NUM_REPLICAS_KEY + "=NUM - number of replicas to use when configuring a new " +
        "table, defaults to " + NUM_REPLICAS_DEFAULT;
  }
}
