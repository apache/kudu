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

package org.apache.kudu.hive.serde.input;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.DNS;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.hive.serde.HiveKuduBridgeUtils;
import org.apache.kudu.hive.serde.PartialRowWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduTableInputFormat implements InputFormat, Configurable {

  /**
   * Job parameter that specifies the input table.
   */
  private static final String INPUT_TABLE_KEY = "kudu.mapreduce.input.table";
  /**
   * Job parameter that specifies if the scanner should cache blocks or not (default: false).
   */
  private static final String SCAN_CACHE_BLOCKS = "kudu.mapreduce.input.scan.cache.blocks";
  /**
   * Job parameter that specifies if the scanner should be fault tolerant
   * or not (default: false).
   */
  private static final String FAULT_TOLERANT_SCAN = "kudu.mapreduce.input.fault.tolerant.scan";
  /**
   * Job parameter that specifies the address for the name server.
   */
  private static final String NAME_SERVER_KEY = "kudu.mapreduce.name.server";
  /**
   * Job parameter that specifies the encoded column predicates (may be empty).
   */
  private static final String ENCODED_PREDICATES_KEY =
      "kudu.mapreduce.encoded.predicates";
  /**
   * Job parameter that specifies the column projection as a comma-separated list of column names.
   *
   * Not specifying this at all (i.e. setting to null) or setting to the special string
   * '*' means to project all columns.
   *
   * Specifying the empty string means to project no columns (i.e just count the rows).
   */
  private static final String COLUMN_PROJECTION_KEY = "kudu.mapreduce.column.projection";
  private static final Logger LOG = LoggerFactory.getLogger(KuduTableInputFormat.class);
  /**
   * The reverse DNS lookup cache mapping: address from Kudu => hostname for Hadoop. This cache is
   * used in order to not do DNS lookups multiple times for each tablet server.
   */
  private final Map<String, String> reverseDNSCacheMap = new HashMap<>();

  private Configuration conf;
  private KuduClient client;
  private KuduTable table;
  private String nameServer;
  private boolean cacheBlocks;
  private boolean isFaultTolerant;
  private List<String> projectedCols;
  private List<KuduPredicate> predicates;

  /**
   * Given a PTR string generated via reverse DNS lookup, return everything
   * except the trailing period. Example for host.example.com., return
   * host.example.com
   *
   * @param dnPtr a domain name pointer (PTR) string.
   * @return Sanitized hostname with last period stripped off.
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

  @Override
  public void setConf(Configuration jobConf) {
    this.conf = new Configuration(jobConf);
    this.client = HiveKuduBridgeUtils.getKuduClient(jobConf);

    this.nameServer = conf.get(NAME_SERVER_KEY);
    this.cacheBlocks = conf.getBoolean(SCAN_CACHE_BLOCKS, false);
    this.isFaultTolerant = conf.getBoolean(FAULT_TOLERANT_SCAN, false);

    String tableName = conf.get(INPUT_TABLE_KEY);
    try {
      this.table = client.openTable(tableName);
    } catch (KuduException ex) {
      throw new RuntimeException("Could not obtain the table from the master, " +
          "is the master running and is this table created? tablename=" + tableName);
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

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    try {
      if (table == null) {
        throw new IOException("No table was provided");
      }

      KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table)
          .setProjectedColumnNames(projectedCols)
          .cacheBlocks(cacheBlocks)
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
        splits.add(new KuduTableSplit(token, locations.toArray(new String[0])));
      }
      return splits.toArray(new InputSplit[0]);
    } finally {
      client.shutdown();
    }
  }

  @Override
  public RecordReader<NullWritable, PartialRowWritable> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    return new KuduRecordReader(split, job);
  }

  /**
   * This method might seem alien, but we do this in order to resolve the hostnames the same way
   * Hadoop does. This ensures we get locality if Kudu is running along MR/YARN.
   *
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


}
