/**
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

package org.apache.kudu.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.net.util.Base64;
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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

/**
 * <p>
 * This input format generates one split per tablet and the only location for each split is that
 * tablet's leader.
 * </p>
 *
 * <p>
 * Hadoop doesn't have the concept of "closing" the input format so in order to release the
 * resources (mainly, the Kudu client) we assume that once either
 * {@link #getSplits(org.apache.hadoop.mapreduce.JobContext)}
 * or {@link KuduTableInputFormat.TableRecordReader#close()}
 * have been called that the object won't be used again and the AsyncKuduClient is shut down.
 *
 * To prevent a premature shutdown of the client, the KuduTableInputFormat and the
 * TableRecordReader both get their own client that they don't share.
 * </p>
 *
 * <p>
 * Default behavior of hadoop is to call {@link #getSplits(org.apache.hadoop.mapreduce.JobContext)}
 * in the MRAppMaster and for each inputSplit (in our case, Kudu tablet) will spawn one Mapper
 * with a TableRecordReader reading one Tablet.
 *
 * Therefore, total number of Kudu clients opened over the course of a MR application can be
 * estimated by (#Tablets +1). To reduce the number of concurrent open clients, it might be
 * advisable to restrict resources of the MR application or implement the
 * org.apache.hadoop.mapred.lib.CombineFileInputFormat over this InputFormat.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduTableInputFormat extends InputFormat<NullWritable, RowResult>
    implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(KuduTableInputFormat.class);

  /** Job parameter that specifies the input table. */
  static final String INPUT_TABLE_KEY = "kudu.mapreduce.input.table";

  /** Job parameter that specifies if the scanner should cache blocks or not (default: false). */
  static final String SCAN_CACHE_BLOCKS = "kudu.mapreduce.input.scan.cache.blocks";

  /**
   * Job parameter that specifies if the scanner should be fault tolerant
   * or not (default: false).
   */
  static final String FAULT_TOLERANT_SCAN = "kudu.mapreduce.input.fault.tolerant.scan";

  /** Job parameter that specifies where the masters are. */
  static final String MASTER_ADDRESSES_KEY = "kudu.mapreduce.master.address";

  /** Job parameter that specifies how long we wait for operations to complete (default: 10s). */
  static final String OPERATION_TIMEOUT_MS_KEY = "kudu.mapreduce.operation.timeout.ms";

  /** Job parameter that specifies the address for the name server. */
  static final String NAME_SERVER_KEY = "kudu.mapreduce.name.server";

  /** Job parameter that specifies the encoded column predicates (may be empty). */
  static final String ENCODED_PREDICATES_KEY =
      "kudu.mapreduce.encoded.predicates";

  /**
   * Job parameter that specifies the column projection as a comma-separated list of column names.
   *
   * Not specifying this at all (i.e. setting to null) or setting to the special string
   * '*' means to project all columns.
   *
   * Specifying the empty string means to project no columns (i.e just count the rows).
   */
  static final String COLUMN_PROJECTION_KEY = "kudu.mapreduce.column.projection";

  /**
   * The reverse DNS lookup cache mapping: address from Kudu => hostname for Hadoop. This cache is
   * used in order to not do DNS lookups multiple times for each tablet server.
   */
  private final Map<String, String> reverseDNSCacheMap = new HashMap<>();

  private Configuration conf;
  private KuduClient client;
  private KuduTable table;
  private long operationTimeoutMs;
  private String nameServer;
  private boolean cacheBlocks;
  private boolean isFaultTolerant;
  private List<String> projectedCols;
  private List<KuduPredicate> predicates;

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    try {
      if (table == null) {
        throw new IOException("No table was provided");
      }

      KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table)
                                                      .setProjectedColumnNames(projectedCols)
                                                      .cacheBlocks(cacheBlocks)
                                                      .setTimeout(operationTimeoutMs)
                                                      .setFaultTolerant(isFaultTolerant);
      for (KuduPredicate predicate : predicates) {
        tokenBuilder.addPredicate(predicate);
      }
      List<KuduScanToken> tokens = tokenBuilder.build();

      List<InputSplit> splits = new ArrayList<>(tokens.size());
      for (KuduScanToken token : tokens) {
        List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());
        for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
          locations.add(reverseDNS(replica.getRpcHost(), replica.getRpcPort()));
        }
        splits.add(new TableSplit(token, locations.toArray(new String[locations.size()])));
      }
      return splits;
    } finally {
      shutdownClient(client);
    }
  }

  /**
   * This method might seem alien, but we do this in order to resolve the hostnames the same way
   * Hadoop does. This ensures we get locality if Kudu is running along MR/YARN.
   * @param host hostname we got from the master
   * @param port port we got from the master
   * @return reverse DNS'd address
   */
  private String reverseDNS(String host, Integer port) {
    String location = this.reverseDNSCacheMap.get(host);
    if (location != null) {
      return location;
    }
    // The below InetSocketAddress creation does a name resolution.
    InetSocketAddress isa = new InetSocketAddress(host, port);
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
      location = host;
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
    String masterAddresses = conf.get(MASTER_ADDRESSES_KEY);
    this.client = buildKuduClient();
    this.nameServer = conf.get(NAME_SERVER_KEY);
    this.cacheBlocks = conf.getBoolean(SCAN_CACHE_BLOCKS, false);
    this.isFaultTolerant = conf.getBoolean(FAULT_TOLERANT_SCAN, false);

    try {
      this.table = client.openTable(tableName);
    } catch (Exception ex) {
      throw new RuntimeException("Could not obtain the table from the master, " +
          "is the master running and is this table created? tablename=" + tableName + " and " +
          "master address= " + masterAddresses, ex);
    }

    String projectionConfig = conf.get(COLUMN_PROJECTION_KEY);
    if (projectionConfig == null || projectionConfig.equals("*")) {
      this.projectedCols = null; // project the whole table
    } else if ("".equals(projectionConfig)) {
      this.projectedCols = new ArrayList<>();
    } else {
      this.projectedCols = Lists.newArrayList(Splitter.on(',').split(projectionConfig));

      // Verify that the column names are valid -- better to fail with an exception
      // before we submit the job.
      Schema tableSchema = table.getSchema();
      for (String columnName : projectedCols) {
        if (tableSchema.getColumn(columnName) == null) {
          throw new IllegalArgumentException("Unknown column " + columnName);
        }
      }
    }

    try {
      byte[] bytes = Base64.decodeBase64(conf.get(ENCODED_PREDICATES_KEY, ""));
      this.predicates = KuduPredicate.deserialize(table.getSchema(), bytes);
    } catch (IOException e) {
      throw new RuntimeException("unable to deserialize predicates from the configuration", e);
    }
  }

  private KuduClient buildKuduClient() {

    String masterAddresses = conf.get(MASTER_ADDRESSES_KEY);
    this.operationTimeoutMs = conf.getLong(OPERATION_TIMEOUT_MS_KEY,
        AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
    KuduClient kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
        .defaultOperationTimeoutMs(operationTimeoutMs)
        .build();
    KuduTableMapReduceUtil.importCredentialsFromCurrentSubject(kuduClient);
    return kuduClient;
  }

  private void shutdownClient(KuduClient kuduClient) throws IOException {
    try {
      kuduClient.shutdown();
    } catch (Exception e) {
      throw new IOException(e);
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
    if (dnPtr == null) {
      return null;
    }
    return dnPtr.endsWith(".") ? dnPtr.substring(0, dnPtr.length() - 1) : dnPtr;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  static class TableSplit extends InputSplit implements Writable, Comparable<TableSplit> {

    /** The scan token that the split will use to scan the Kudu table. */
    private byte[] scanToken;

    /** The start partition key of the scan. Used for sorting splits. */
    private byte[] partitionKey;

    /** Tablet server locations which host the tablet to be scanned. */
    private String[] locations;

    public TableSplit() { } // Writable

    public TableSplit(KuduScanToken token, String[] locations) throws IOException {
      this.scanToken = token.serialize();
      this.partitionKey = token.getTablet().getPartition().getPartitionKeyStart();
      this.locations = locations;
    }

    public byte[] getScanToken() {
      return scanToken;
    }

    public byte[] getPartitionKey() {
      return partitionKey;
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

    @Override
    public int compareTo(TableSplit other) {
      return UnsignedBytes.lexicographicalComparator().compare(partitionKey, other.partitionKey);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      Bytes.writeByteArray(dataOutput, scanToken);
      Bytes.writeByteArray(dataOutput, partitionKey);
      dataOutput.writeInt(locations.length);
      for (String location : locations) {
        byte[] str = Bytes.fromString(location);
        Bytes.writeByteArray(dataOutput, str);
      }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      scanToken = Bytes.readByteArray(dataInput);
      partitionKey = Bytes.readByteArray(dataInput);
      locations = new String[dataInput.readInt()];
      for (int i = 0; i < locations.length; i++) {
        byte[] str = Bytes.readByteArray(dataInput);
        locations[i] = Bytes.getString(str);
      }
    }

    @Override
    public int hashCode() {
      // We currently just care about the partition key since we're within the same table.
      return Arrays.hashCode(partitionKey);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TableSplit that = (TableSplit) o;

      return this.compareTo(that) == 0;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("TableSplit{");
      sb.append("partitionKey=");
      sb.append(Bytes.pretty(partitionKey));
      sb.append(", locations=");
      sb.append(Arrays.toString(locations));
      sb.append("}");
      return sb.toString();
    }
  }

  class TableRecordReader extends RecordReader<NullWritable, RowResult> {

    private final NullWritable currentKey = NullWritable.get();
    private RowResult currentValue;
    private RowResultIterator iterator;
    private KuduScanner scanner;
    private TableSplit split;
    private KuduClient kuduClient;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
      if (!(inputSplit instanceof TableSplit)) {
        throw new IllegalArgumentException("TableSplit is the only accepted input split");
      }

      split = (TableSplit) inputSplit;
      kuduClient = buildKuduClient();
      LOG.debug("Creating scanner for token: {}",
                KuduScanToken.stringifySerializedToken(split.getScanToken(), kuduClient));
      scanner = KuduScanToken.deserializeIntoScanner(split.getScanToken(), kuduClient);

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
        // scanner.nextRows() sometimes returns an empty RowResultIterator, but
        // scanner.hasMoreRows() returns still true, so we need to continue
        // iterating on the scanner until scanner.hasMoreRows() returns false.
        //
        // TODO (qqzhang) In future, the backend can guarantee that
        // TabletService.Scan() would not return the empty results, we need to
        // remove the loop.
        do {
          iterator = scanner.nextRows();
        } while (!iterator.hasNext() && scanner.hasMoreRows());
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
        scanner.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
      shutdownClient(kuduClient);
    }
  }
}
