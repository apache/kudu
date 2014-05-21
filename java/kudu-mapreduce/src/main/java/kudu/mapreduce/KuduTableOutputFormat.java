// Copyright (c) 2014, Cloudera, inc.
package kudu.mapreduce;

import com.stumbleupon.async.Deferred;
import kudu.rpc.KuduClient;
import kudu.rpc.KuduSession;
import kudu.rpc.KuduTable;
import kudu.rpc.Operation;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Use {@link kudu.mapreduce.KuduTableMapReduceUtil#initTableOutputFormat} to correctly setup
 * this output format, then {@link kudu.mapreduce.KuduTableMapReduceUtil#getTableFromContext} to
 * get a KuduTable.
 * @param <KEY>
 */
public class KuduTableOutputFormat<KEY> extends OutputFormat<KEY,
    Operation> implements Configurable {

  /** Job parameter that specifies the output table. */
  static final String OUTPUT_TABLE_KEY = "kudu.mapreduce.output.table";

  /** Job parameter that specifies where the master is */
  static final String MASTER_ADDRESS_KEY = "kudu.mapreduce.master.address";

  /** Job parameter that specifies how long we wait for operations to complete */
  static final String OPERATION_TIMEOUT_MS_KEY = "kudu.mapreduce.operation.timeout.ms";

  /**
   * Job parameter that specifies which key is to be used to reach the KuduTableOutputFormat
   * belonging to the caller
   */
  static final String MULTITON_KEY = "kudu.mapreduce.multitonkey";

  /**
   * This multiton is used so that the tasks using this output format/record writer can find
   * their KuduTable without having a direct dependency on this class,
   * with the additional complexity that the output format cannot be shared between threads.
   */
  private static final ConcurrentHashMap<String, KuduTableOutputFormat> MULTITON = new
      ConcurrentHashMap<String, KuduTableOutputFormat>();

  private Configuration conf = null;

  private KuduClient client;
  private KuduTable table;
  private KuduSession session;
  private long operationTimeoutMs;

  @Override
  public void setConf(Configuration entries) {
    this.conf = new Configuration(entries);

    String masterAddress = this.conf.get(MASTER_ADDRESS_KEY);
    String tableName = this.conf.get(OUTPUT_TABLE_KEY);
    this.operationTimeoutMs = this.conf.getLong(OPERATION_TIMEOUT_MS_KEY, 10000);

    this.client = KuduTableMapReduceUtil.connect(masterAddress);
    Deferred<Object> d = client.openTable(tableName);
    try {
      this.table = (KuduTable)d.join(this.operationTimeoutMs);
    } catch (Exception ex) {
      throw new RuntimeException("Could not obtain the table from the master, " +
          "is the master running and is this table created? tablename=" + tableName + " and " +
          "master address= " + masterAddress, ex);
    }
    this.session = client.newSession();
    this.session.setTimeoutMillis(this.operationTimeoutMs);
    String multitonKey = String.valueOf(Thread.currentThread().getId());
    assert(MULTITON.get(multitonKey) == null);
    MULTITON.put(multitonKey, this);
    entries.set(MULTITON_KEY, multitonKey);
  }

  public static KuduTable getKuduTable(String multitonKey) {
    return MULTITON.get(multitonKey).getKuduTable();
  }

  public KuduTable getKuduTable() {
    return this.table;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public RecordWriter<KEY, Operation> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new TableRecordWriter<KEY>(this.session, this.operationTimeoutMs);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException { }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws
      IOException, InterruptedException {
    return new KuduTableOutputCommitter();
  }

  protected static class TableRecordWriter<KEY>
      extends RecordWriter<KEY, Operation> {

    private final KuduSession session;
    private final long operationTimeoutMs;

    public TableRecordWriter(KuduSession session, long operationTimeoutMs) {
      this.session = session;
      this.operationTimeoutMs = operationTimeoutMs;
    }

    @Override
    public void write(KEY key, Operation operation) throws IOException, InterruptedException {
      Deferred<Object> d = session.apply(operation);
      try {
        d.join(operationTimeoutMs);
      } catch (Exception e) {
        throw new IOException("Could not apply the operation", e);
      }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException,
        InterruptedException {
      Deferred<Object> d = session.close();
      try {
        d.join(operationTimeoutMs);
      } catch (Exception e) {
        throw new IOException("Could not completely flush the operations when closing", e);
      }
    }
  }
}
