/**
 * Portions copyright (c) 2014 Cloudera, Inc.
 * Confidential Cloudera Information: Covered by NDA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package kudu.mapreduce;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import kudu.ColumnSchema;
import kudu.Common;
import kudu.Schema;
import kudu.metadata.Metadata;
import kudu.rpc.Bytes;
import kudu.rpc.DeadlineTracker;
import kudu.rpc.KuduClient;
import kudu.rpc.KuduScanner;
import kudu.rpc.KuduTable;
import kudu.rpc.LocatedTablet;
import kudu.rpc.RowResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.DNS;

import javax.naming.NamingException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This input format generates one split per tablet and the only location for each split is that
 * tablet's leader.
 * </p>
 *
 * <p>
 * Hadoop doesn't have the concept of "closing" the input format so in order to release the
 * resources we assume that once either {@link #getSplits(org.apache.hadoop.mapreduce.JobContext)}
 * or {@link kudu.mapreduce.KuduTableInputFormat.TableRecordReader#close()} have been called that
 * the object won't be used again and the KuduClient is shut down.
 * </p>
 */
public class KuduTableInputFormat extends InputFormat<NullWritable, RowResult>
    implements Configurable {

  private static final Log LOG = LogFactory.getLog(KuduTableInputFormat.class);

  private static final long SLEEP_TIME_FOR_RETRIES_MS = 1000;

  /** Job parameter that specifies the input table. */
  static final String INPUT_TABLE_KEY = "kudu.mapreduce.input.table";

  /** Job parameter that specifies where the masters are */
  static final String MASTER_QUORUM_KEY = "kudu.mapreduce.master.address";

  /** Job parameter that specifies how long we wait for operations to complete */
  static final String OPERATION_TIMEOUT_MS_KEY = "kudu.mapreduce.operation.timeout.ms";

  /** Job parameter that specifies the address for the name server */
  static final String NAME_SERVER_KEY = "kudu.mapreduce.name.server";

  /**
   * Job parameter that specifies the column projection as a comma-separated list of column names.
   * Not specifying this means using an empty projection.
   * Note: when specifying columns that are keys, they must be at the beginning.
   */
  static final String COLUMN_PROJECTION_KEY = "kudu.mapreduce.column.projection";

  /**
   * The reverse DNS lookup cache mapping: address from Kudu => hostname for Hadoop. This cache is
   * used in order to not do DNS lookups multiple times for each tablet server.
   */
  private final Map<String, String> reverseDNSCacheMap = new HashMap<String, String>();

  private Configuration conf;
  private KuduClient client;
  private KuduTable table;
  private long operationTimeoutMs;
  private String nameServer;
  private Schema querySchema;

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    try {
      if (table == null) {
        throw new IOException("No table was provided");
      }
      List<InputSplit> splits;
      DeadlineTracker deadline = new DeadlineTracker();
      deadline.setDeadline(operationTimeoutMs);
      // If the job is started while a leader election is running, we might not be able to find a
      // leader right away. We'll wait as long as the user is willing to wait with the operation
      // timeout, and once we've waited long enough we just start picking the first replica we see
      // for those tablets that don't have a leader. The client will later try to find the leader
      // and it might fail, in which case the task will get retried.
      retryloop:
      while (true) {
        List<LocatedTablet> locations;
        try {
          locations = table.getTabletsLocations(operationTimeoutMs);
        } catch (Exception e) {
          throw new IOException("Could not get the tablets locations", e);
        }

        if (locations.isEmpty()) {
          throw new IOException("The requested table has 0 tablets, cannot continue");
        }

        // For the moment we only pass the leader since that's who we read from.
        // If we've been trying to get a leader for each tablet for too long, we stop looping
        // and just finish with what we have.
        splits = new ArrayList<InputSplit>(locations.size());
        for (LocatedTablet locatedTablet : locations) {
          List<String> addresses = Lists.newArrayList();
          LocatedTablet.Replica replica = locatedTablet.getLeaderReplica();
          if (replica == null) {
            if (deadline.wouldSleepingTimeout(SLEEP_TIME_FOR_RETRIES_MS)) {
              LOG.debug("We ran out of retries, picking a non-leader replica for this tablet: " +
                  locatedTablet.toString());
              // We already checked it's not empty.
              replica = locatedTablet.getReplicas().get(0);
            } else {
              LOG.debug("Retrying creating the splits because this tablet is missing a leader: " +
                  locatedTablet.toString());
              Thread.sleep(SLEEP_TIME_FOR_RETRIES_MS);
              continue retryloop;
            }
          }
          addresses.add(reverseDNS(replica.getRpcHostPort()));
          String[] addressesArray = addresses.toArray(new String[addresses.size()]);
          TableSplit split = new TableSplit(locatedTablet.getStartKey(),
              locatedTablet.getEndKey(),
              addressesArray);
          splits.add(split);
        }
        return splits;
      }
    } finally {
      shutdownClient();
    }
  }

  private void shutdownClient() throws IOException {
    try {
      client.shutdown().join(operationTimeoutMs);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * This method might seem alien, but we do this in order to resolve the hostnames the same way
   * Hadoop does. This ensures we get locality if Kudu is running along MR/YARN.
   * @param address Address we got from the master
   * @return reverse DNS'd address
   */
  private String reverseDNS(Common.HostPortPB address) {
    String host = address.getHost();
    String location = this.reverseDNSCacheMap.get(host);
    if (location != null) {
      return location;
    }
    // The below InetSocketAddress creation does a name resolution.
    InetSocketAddress isa = new InetSocketAddress(host, address.getPort());
    if (isa.isUnresolved()) {
      LOG.warn("Failed address resolve for: " + isa);
    }
    InetAddress tabletInetAddress = isa.getAddress();
    try {
      location = domainNamePointerToHostName(
          DNS.reverseDns(tabletInetAddress, this.nameServer));
      this.reverseDNSCacheMap.put(host, location);
    } catch (NamingException e) {
      LOG.warn("Cannot resolve the host name for " + tabletInetAddress + " because of " + e);
      location = address.getHost();
    }
    return location;
  }

  @Override
  public RecordReader<NullWritable, RowResult> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new TableRecordReader();
  }

  @Override
  public void setConf(Configuration entries) {
    this.conf = new Configuration(entries);

    String tableName = conf.get(INPUT_TABLE_KEY);
    String masterQuorum = conf.get(MASTER_QUORUM_KEY);
    this.operationTimeoutMs = this.conf.getLong(OPERATION_TIMEOUT_MS_KEY, 10000);
    this.nameServer = conf.get(NAME_SERVER_KEY);

    this.client = KuduTableMapReduceUtil.connect(masterQuorum);
    Deferred<KuduTable> d = client.openTable(tableName);
    try {
      this.table = d.join(this.operationTimeoutMs);
    } catch (Exception ex) {
      throw new RuntimeException("Could not obtain the table from the master, " +
          "is the master running and is this table created? tablename=" + tableName + " and " +
          "master address= " + masterQuorum, ex);
    }

    String projectionConfig = conf.get(COLUMN_PROJECTION_KEY);
    Schema tableSchema = table.getSchema();
    if (projectionConfig == null || projectionConfig.equals("")) {
      this.querySchema = new Schema(new ArrayList<ColumnSchema>(0));
    } else {
      Iterable<String> columnProjection = Splitter.on(',').split(projectionConfig);
      List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
      for (String columnName : columnProjection) {
        ColumnSchema columnSchema = tableSchema.getColumn(columnName);
        if (columnSchema == null) {
          throw new IllegalArgumentException("Unkown column " + columnName);
        }
        columns.add(columnSchema);
      }
      this.querySchema = new Schema(columns);
    }
  }

  /**
   * Given a PTR string generated via reverse DNS lookup, return everything
   * except the trailing period. Example for host.example.com., return
   * host.example.com
   * @param dnPtr a domain name pointer (PTR) string.
   * @return Sanitized hostname with last period stripped off.
   *
   */
  private static String domainNamePointerToHostName(String dnPtr) {
    if (dnPtr == null)
      return null;
    return dnPtr.endsWith(".") ? dnPtr.substring(0, dnPtr.length() - 1) : dnPtr;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  static class TableSplit extends InputSplit implements Writable, Comparable<TableSplit> {

    private byte[] startKey;
    private byte[] endKey;
    private String[] locations;

    public TableSplit() { } // Writable

    public TableSplit(byte[] startKey, byte[] endKey, String[] locations) {
      this.startKey = startKey;
      this.endKey = endKey;
      this.locations = locations;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      // TODO Guesstimate a size
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return locations;
    }

    public byte[] getStartKey() {
      return startKey;
    }

    public byte[] getEndKey() {
      return endKey;
    }

    @Override
    public int compareTo(TableSplit tableSplit) {
      return Bytes.memcmp(startKey, tableSplit.getStartKey());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      Bytes.writeByteArray(dataOutput, startKey);
      Bytes.writeByteArray(dataOutput, endKey);
      dataOutput.writeInt(locations.length);
      for (String location : locations) {
        byte[] str = Bytes.fromString(location);
        Bytes.writeByteArray(dataOutput,str);
      }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      startKey = Bytes.readByteArray(dataInput);
      endKey = Bytes.readByteArray(dataInput);
      locations = new String[dataInput.readInt()];
      for (int i = 0; i < locations.length; i++) {
        byte[] str = Bytes.readByteArray(dataInput);
        locations[i] = Bytes.getString(str);
      }
    }

    @Override
    public int hashCode() {
      // We currently just care about the row key since we're within the same table
      return Objects.hashCode(startKey);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TableSplit that = (TableSplit) o;

      return this.compareTo(that) == 0;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("startKey=");
      sb.append(Bytes.pretty(startKey));
      sb.append(", endKey=");
      sb.append(Bytes.pretty(endKey));
      sb.append(" at ");
      sb.append("locations=");
      sb.append(Arrays.toString(locations));
      return sb.toString();
    }
  }

  class TableRecordReader extends RecordReader<NullWritable, RowResult> {

    private final NullWritable currentKey = NullWritable.get();
    private RowResult currentValue;
    private KuduScanner.RowResultIterator iterator;
    private KuduScanner scanner;
    private TableSplit split;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      if (!(inputSplit instanceof TableSplit)) {
        throw new IllegalArgumentException("TableSplit is the only accepted input split");
      }
      split = (TableSplit) inputSplit;
      scanner = client.newScanner(table, querySchema);
      scanner.setEncodedStartKey(split.getStartKey());
      scanner.setEncodedEndKey(split.getEndKey());

      // Calling this now to set iterator.
      tryRefreshIterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!iterator.hasNext()) {
        tryRefreshIterator();
        if (!iterator.hasNext()) {
          // Means we still have the same iterator, we're done
          return false;
        }
      }
      currentValue = iterator.next();
      return true;
    }

    /**
     * If the scanner has more rows, get a new iterator else don't do anything.
     * @throws IOException
     */
    private void tryRefreshIterator() throws IOException {
      if (!scanner.hasMoreRows()) {
        return;
      }
      try {
        iterator = scanner.nextRows().join(operationTimeoutMs);
      } catch (Exception e) {
        throw new IOException("Couldn't get scan data", e);
      }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public RowResult getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      // TODO Guesstimate progress
      return 0;
    }

    @Override
    public void close() throws IOException {
      try {
        scanner.close().join(operationTimeoutMs);
      } catch (Exception e) {
        throw new IOException(e);
      }
      shutdownClient();
    }
  }
}
