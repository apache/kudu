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

package org.apache.kudu.client;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Client.ScanTokenPB;
import org.apache.kudu.security.Token;
import org.apache.kudu.util.NetUtil;
import org.apache.kudu.util.Pair;

/**
 * A scan token describes a partial scan of a Kudu table limited to a single
 * contiguous physical location. Using the {@link KuduScanTokenBuilder}, clients can
 * describe the desired scan, including predicates, bounds, timestamps, and
 * caching, and receive back a collection of scan tokens.
 *
 * Each scan token may be separately turned into a scanner using
 * {@link #intoScanner}, with each scanner responsible for a disjoint section
 * of the table.
 *
 * Scan tokens may be serialized using the {@link #serialize} method and
 * deserialized back into a scanner using the {@link #deserializeIntoScanner}
 * method. This allows use cases such as generating scan tokens in the planner
 * component of a query engine, then sending the tokens to execution nodes based
 * on locality, and then instantiating the scanners on those nodes.
 *
 * Scan token locality information can be inspected using the {@link #getTablet}
 * method.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class KuduScanToken implements Comparable<KuduScanToken> {
  private final LocatedTablet tablet;
  private final ScanTokenPB message;

  private KuduScanToken(LocatedTablet tablet, ScanTokenPB message) {
    this.tablet = tablet;
    this.message = message;
  }

  /**
   * Returns the tablet which the scanner created from this token will access.
   * @return the located tablet
   */
  public LocatedTablet getTablet() {
    return tablet;
  }

  /**
   * Creates a {@link KuduScanner} from this scan token.
   * @param client a Kudu client for the cluster
   * @return a scanner for the scan token
   */
  public KuduScanner intoScanner(KuduClient client) throws Exception {
    return pbIntoScanner(message, client);
  }

  /**
   * Serializes this {@code KuduScanToken} into a byte array.
   * @return the serialized scan token
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    return serialize(message);
  }

  /**
   * Serializes a {@code KuduScanToken} into a byte array.
   * @return the serialized scan token
   * @throws IOException
   */
  @InterfaceAudience.LimitedPrivate("Test")
  static byte[] serialize(ScanTokenPB message) throws IOException {
    byte[] buf = new byte[message.getSerializedSize()];
    CodedOutputStream cos = CodedOutputStream.newInstance(buf);
    message.writeTo(cos);
    cos.flush();
    return buf;
  }

  /**
   * Deserializes a {@code KuduScanToken} into a {@link KuduScanner}.
   * @param buf a byte array containing the serialized scan token.
   * @param client a Kudu client for the cluster
   * @return a scanner for the serialized scan token
   */
  public static KuduScanner deserializeIntoScanner(byte[] buf, KuduClient client)
      throws IOException {
    return pbIntoScanner(ScanTokenPB.parseFrom(CodedInputStream.newInstance(buf)), client);
  }

  /**
   * Formats the serialized token for debug printing.
   *
   * @param buf the serialized token
   * @param client a Kudu client for the cluster to which the token belongs
   * @return a debug string
   */
  public static String stringifySerializedToken(byte[] buf, KuduClient client) throws IOException {
    ScanTokenPB token = ScanTokenPB.parseFrom(CodedInputStream.newInstance(buf));
    KuduTable table = getKuduTable(token, client);

    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper("ScanToken")
                                                   .add("table-name", table.getName());
    helper.add("table-id", table.getTableId());

    if (token.hasLowerBoundPrimaryKey() && !token.getLowerBoundPrimaryKey().isEmpty()) {
      helper.add("lower-bound-primary-key",
                 KeyEncoder.decodePrimaryKey(table.getSchema(),
                                             token.getLowerBoundPrimaryKey().toByteArray())
                           .stringifyRowKey());
    }

    if (token.hasUpperBoundPrimaryKey() && !token.getUpperBoundPrimaryKey().isEmpty()) {
      helper.add("upper-bound-primary-key",
                 KeyEncoder.decodePrimaryKey(table.getSchema(),
                                             token.getUpperBoundPrimaryKey().toByteArray())
                           .stringifyRowKey());
    }

    helper.addValue(KeyEncoder.formatPartitionKeyRange(table.getSchema(),
                                                       table.getPartitionSchema(),
                                                       token.getLowerBoundPartitionKey()
                                                           .toByteArray(),
                                                       token.getUpperBoundPartitionKey()
                                                           .toByteArray()));

    return helper.toString();
  }

  private static List<Integer> computeProjectedColumnIndexesForScanner(ScanTokenPB message,
                                                                       Schema schema) {
    if (message.getProjectedColumnIdxCount() != 0) {
      return message.getProjectedColumnIdxList();
    }

    List<Integer> columns = new ArrayList<>(message.getProjectedColumnsCount());
    for (Common.ColumnSchemaPB colSchemaFromPb : message.getProjectedColumnsList()) {
      int colIdx = colSchemaFromPb.hasId() && schema.hasColumnIds() ?
          schema.getColumnIndex(colSchemaFromPb.getId()) :
          schema.getColumnIndex(colSchemaFromPb.getName());
      ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
      if (colSchemaFromPb.getType() !=
          colSchema.getType().getDataType(colSchema.getTypeAttributes())) {
        throw new IllegalStateException(String.format(
            "invalid type %s for column '%s' in scan token, expected: %s",
            colSchemaFromPb.getType().name(), colSchemaFromPb.getName(),
            colSchema.getType().name()));
      }
      if (colSchemaFromPb.getIsNullable() != colSchema.isNullable()) {
        throw new IllegalStateException(String.format(
            "invalid nullability for column '%s' in scan token, expected: %s",
            colSchemaFromPb.getName(), colSchema.isNullable() ? "NULLABLE" : "NOT NULL"));
      }
      columns.add(colIdx);
    }
    return columns;
  }

  @SuppressWarnings("deprecation")
  private static KuduScanner pbIntoScanner(ScanTokenPB message,
                                           KuduClient client) throws KuduException {
    Preconditions.checkArgument(
        !message.getFeatureFlagsList().contains(ScanTokenPB.Feature.Unknown),
        "Scan token requires an unsupported feature. This Kudu client must be updated.");

    // Use the table metadata from the scan token if it exists,
    // otherwise call OpenTable to get the metadata from the master.
    KuduTable table = getKuduTable(message, client);

    // Prime the client tablet location cache if no entry is already present.
    if (message.hasTabletMetadata()) {
      Client.TabletMetadataPB tabletMetadata = message.getTabletMetadata();
      Partition partition =
          ProtobufHelper.pbToPartition(tabletMetadata.getPartition());
      if (client.asyncClient.getTableLocationEntry(table.getTableId(),
          partition.partitionKeyStart) == null) {
        TableLocationsCache tableLocationsCache =
            client.asyncClient.getOrCreateTableLocationsCache(table.getTableId());

        List<LocatedTablet.Replica> replicas = new ArrayList<>();
        for (Client.TabletMetadataPB.ReplicaMetadataPB replicaMetadataPB :
            tabletMetadata.getReplicasList()) {
          Client.ServerMetadataPB server =
              tabletMetadata.getTabletServers(replicaMetadataPB.getTsIdx());
          LocatedTablet.Replica replica = new LocatedTablet.Replica(
              server.getRpcAddresses(0).getHost(),
              server.getRpcAddresses(0).getPort(),
              replicaMetadataPB.getRole(), replicaMetadataPB.getDimensionLabel());
          replicas.add(replica);
        }

        List<ServerInfo> servers = new ArrayList<>();
        for (Client.ServerMetadataPB serverMetadataPB : tabletMetadata.getTabletServersList()) {
          HostAndPort hostPort =
              ProtobufHelper.hostAndPortFromPB(serverMetadataPB.getRpcAddresses(0));
          final InetAddress inetAddress = NetUtil.getInetAddress(hostPort.getHost());
          ServerInfo serverInfo = new ServerInfo(serverMetadataPB.getUuid().toString(),
              hostPort, inetAddress, serverMetadataPB.getLocation());
          servers.add(serverInfo);
        }

        RemoteTablet remoteTablet = new RemoteTablet(table.getTableId(),
            tabletMetadata.getTabletId(), partition, replicas, servers);

        tableLocationsCache.cacheTabletLocations(Collections.singletonList(remoteTablet),
            partition.partitionKeyStart, 1, tabletMetadata.getTtlMillis());
      }
    }

    if (message.hasAuthzToken()) {
      client.asyncClient.getAuthzTokenCache().put(table.getTableId(), message.getAuthzToken());
    }

    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);

    builder.setProjectedColumnIndexes(
        computeProjectedColumnIndexesForScanner(message, table.getSchema()));

    for (Common.ColumnPredicatePB pred : message.getColumnPredicatesList()) {
      builder.addPredicate(KuduPredicate.fromPB(table.getSchema(), pred));
    }

    if (message.hasLowerBoundPrimaryKey()) {
      builder.lowerBoundRaw(message.getLowerBoundPrimaryKey().toByteArray());
    }
    if (message.hasUpperBoundPrimaryKey()) {
      builder.exclusiveUpperBoundRaw(message.getUpperBoundPrimaryKey().toByteArray());
    }

    if (message.hasLowerBoundPartitionKey()) {
      builder.lowerBoundPartitionKeyRaw(message.getLowerBoundPartitionKey().toByteArray());
    }
    if (message.hasUpperBoundPartitionKey()) {
      builder.exclusiveUpperBoundPartitionKeyRaw(message.getUpperBoundPartitionKey().toByteArray());
    }

    if (message.hasLimit()) {
      builder.limit(message.getLimit());
    }

    if (message.hasReadMode()) {
      switch (message.getReadMode()) {
        case READ_AT_SNAPSHOT: {
          builder.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
          if (message.hasSnapTimestamp()) {
            builder.snapshotTimestampRaw(message.getSnapTimestamp());
          }
          // Set the diff scan timestamps if they are set.
          if (message.hasSnapStartTimestamp()) {
            builder.diffScan(message.getSnapStartTimestamp(), message.getSnapTimestamp());
          }
          break;
        }
        case READ_LATEST: {
          builder.readMode(AsyncKuduScanner.ReadMode.READ_LATEST);
          break;
        }
        case READ_YOUR_WRITES: {
          builder.readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES);
          break;
        }
        default: throw new IllegalArgumentException("unknown read mode");
      }
    }

    if (message.hasReplicaSelection()) {
      switch (message.getReplicaSelection()) {
        case LEADER_ONLY: {
          builder.replicaSelection(ReplicaSelection.LEADER_ONLY);
          break;
        }
        case CLOSEST_REPLICA: {
          builder.replicaSelection(ReplicaSelection.CLOSEST_REPLICA);
          break;
        }
        default: throw new IllegalArgumentException("unknown replica selection policy");
      }
    }

    if (message.hasPropagatedTimestamp() &&
        message.getPropagatedTimestamp() != AsyncKuduClient.NO_TIMESTAMP) {
      client.updateLastPropagatedTimestamp(message.getPropagatedTimestamp());
    }

    if (message.hasCacheBlocks()) {
      builder.cacheBlocks(message.getCacheBlocks());
    }

    if (message.hasFaultTolerant()) {
      builder.setFaultTolerant(message.getFaultTolerant());
    }

    if (message.hasBatchSizeBytes()) {
      builder.batchSizeBytes(message.getBatchSizeBytes());
    }

    if (message.hasScanRequestTimeoutMs()) {
      builder.scanRequestTimeout(message.getScanRequestTimeoutMs());
    }

    if (message.hasKeepAlivePeriodMs()) {
      builder.keepAlivePeriodMs(message.getKeepAlivePeriodMs());
    }

    return builder.build();
  }

  private static KuduTable getKuduTable(ScanTokenPB message,
                                        KuduClient client) throws KuduException {
    // Use the table metadata from the scan token if it exists,
    // otherwise call OpenTable to get the metadata from the master.
    if (message.hasTableMetadata()) {
      Client.TableMetadataPB tableMetadata = message.getTableMetadata();
      Schema schema = ProtobufHelper.pbToSchema(tableMetadata.getSchema());
      PartitionSchema partitionSchema =
          ProtobufHelper.pbToPartitionSchema(tableMetadata.getPartitionSchema(), schema);
      return new KuduTable(client.asyncClient, tableMetadata.getTableName(),
          tableMetadata.getTableId(), schema, partitionSchema,
          tableMetadata.getNumReplicas(), tableMetadata.getExtraConfigsMap());
    } else if (message.hasTableId()) {
      return client.openTableById(message.getTableId());
    } else {
      return client.openTable(message.getTableName());
    }
  }

  @Override
  public int compareTo(KuduScanToken other) {
    if (message.hasTableId() && other.message.hasTableId()) {
      if (!message.getTableId().equals(other.message.getTableId())) {
        throw new IllegalArgumentException("Scan tokens from different tables may not be compared");
      }
    } else if (!message.getTableName().equals(other.message.getTableName())) {
      throw new IllegalArgumentException("Scan tokens from different tables may not be compared");
    }

    return tablet.getPartition().compareTo(other.getTablet().getPartition());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KuduScanToken)) {
      return false;
    }
    KuduScanToken that = (KuduScanToken) o;
    return compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tablet, message);
  }

  /**
   * Builds a sequence of scan tokens.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static class KuduScanTokenBuilder
      extends AbstractKuduScannerBuilder<KuduScanTokenBuilder, List<KuduScanToken>> {

    private static final int DEFAULT_SPLIT_SIZE_BYTES = -1;

    private long timeout;

    // By default, a scan token is created for each tablet to be scanned.
    private long splitSizeBytes = DEFAULT_SPLIT_SIZE_BYTES;

    private boolean includeTableMetadata = true;
    private boolean includeTabletMetadata = true;

    KuduScanTokenBuilder(AsyncKuduClient client, KuduTable table) {
      super(client, table);
      timeout = client.getDefaultOperationTimeoutMs();
    }

    /**
     * Sets a timeout value to use when building the list of scan tokens. If
     * unset, the client operation timeout will be used.
     * @param timeoutMs the timeout in milliseconds.
     */
    public KuduScanTokenBuilder setTimeout(long timeoutMs) {
      timeout = timeoutMs;
      return this;
    }

    /**
     * Sets the data size of key range. It is used to split tablet's primary key range
     * into smaller ranges. The split doesn't change the layout of the tablet. This is a hint:
     * The tablet server may return the size of key range larger or smaller than this value.
     * If unset or <= 0, the key range includes all the data of the tablet.
     * @param splitSizeBytes the data size of key range.
     */
    public KuduScanTokenBuilder setSplitSizeBytes(long splitSizeBytes) {
      this.splitSizeBytes = splitSizeBytes;
      return this;
    }

    /**
     * If the table metadata is included on the scan token a GetTableSchema
     * RPC call to the master can be avoided when deserializing each scan token
     * into a scanner.
     * @param includeMetadata true, if table metadata should be included.
     */
    public KuduScanTokenBuilder includeTableMetadata(boolean includeMetadata) {
      this.includeTableMetadata = includeMetadata;
      return this;
    }

    /**
     * If the tablet metadata is included on the scan token a GetTableLocations
     * RPC call to the master can be avoided when scanning with a scanner constructed
     * from a scan token.
     * @param includeMetadata true, if tablet metadata should be included.
     */
    public KuduScanTokenBuilder includeTabletMetadata(boolean includeMetadata) {
      this.includeTabletMetadata = includeMetadata;
      return this;
    }

    @Override
    public List<KuduScanToken> build() {
      if (lowerBoundPartitionKey.length != 0 ||
          upperBoundPartitionKey.length != 0) {
        throw new IllegalArgumentException(
            "Partition key bounds may not be set on KuduScanTokenBuilder");
      }

      // If the scan is short-circuitable, then return no tokens.
      for (KuduPredicate predicate : predicates.values()) {
        if (predicate.getType() == KuduPredicate.PredicateType.NONE) {
          return new ArrayList<>();
        }
      }

      Client.ScanTokenPB.Builder proto = Client.ScanTokenPB.newBuilder();

      if (includeTableMetadata) {
        // Set the table metadata so that a call to the master is not needed when
        // deserializing the token into a scanner.
        Client.TableMetadataPB tableMetadataPB = Client.TableMetadataPB.newBuilder()
            .setTableId(table.getTableId())
            .setTableName(table.getName())
            .setNumReplicas(table.getNumReplicas())
            .setSchema(ProtobufHelper.schemaToPb(table.getSchema()))
            .setPartitionSchema(ProtobufHelper.partitionSchemaToPb(table.getPartitionSchema()))
            .putAllExtraConfigs(table.getExtraConfig())
            .build();
        proto.setTableMetadata(tableMetadataPB);

        // Only include the authz token if the table metadata is included.
        // It is returned in the required GetTableSchema request otherwise.
        Token.SignedTokenPB authzToken = client.getAuthzToken(table.getTableId());
        if (authzToken != null) {
          proto.setAuthzToken(authzToken);
        }
      } else {
        // If we add the table metadata, we don't need to set the old table id
        // and table name. It is expected that the creation and use of a scan token
        // will be on the same or compatible versions.
        proto.setTableId(table.getTableId());
        proto.setTableName(table.getName());
      }

      // Map the column names or indices to actual columns in the table schema.
      // If the user did not set either projection, then scan all columns.
      Schema schema = table.getSchema();
      if (includeTableMetadata) {
        // If the table metadata is included, then the column indexes can be
        // used instead of duplicating the ColumnSchemaPBs in the serialized
        // scan token.
        if (projectedColumnNames != null) {
          for (String columnName : projectedColumnNames) {
            proto.addProjectedColumnIdx(schema.getColumnIndex(columnName));
          }
        } else if (projectedColumnIndexes != null) {
          proto.addAllProjectedColumnIdx(projectedColumnIndexes);
        } else {
          List<Integer> indexes = IntStream.range(0, schema.getColumnCount())
              .boxed().collect(Collectors.toList());
          proto.addAllProjectedColumnIdx(indexes);
        }
      } else {
        if (projectedColumnNames != null) {
          for (String columnName : projectedColumnNames) {
            ColumnSchema columnSchema = schema.getColumn(columnName);
            Preconditions.checkArgument(columnSchema != null,
                "unknown column i%s", columnName);
            ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(),
                schema.hasColumnIds() ? schema.getColumnId(columnName) : -1,
                columnSchema);
          }
        } else if (projectedColumnIndexes != null) {
          for (int columnIdx : projectedColumnIndexes) {
            ColumnSchema columnSchema = schema.getColumnByIndex(columnIdx);
            Preconditions.checkArgument(columnSchema != null,
                "unknown column index %s", columnIdx);
            ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(),
                schema.hasColumnIds() ?
                    schema.getColumnId(columnSchema.getName()) :
                    -1,
                columnSchema);
          }
        } else {
          for (ColumnSchema column : schema.getColumns()) {
            ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(),
                schema.hasColumnIds() ?
                    schema.getColumnId(column.getName()) :
                    -1,
                column);
          }
        }
      }

      for (KuduPredicate predicate : predicates.values()) {
        proto.addColumnPredicates(predicate.toPB());
      }

      if (lowerBoundPrimaryKey.length > 0) {
        proto.setLowerBoundPrimaryKey(UnsafeByteOperations.unsafeWrap(lowerBoundPrimaryKey));
      }
      if (upperBoundPrimaryKey.length > 0) {
        proto.setUpperBoundPrimaryKey(UnsafeByteOperations.unsafeWrap(upperBoundPrimaryKey));
      }

      proto.setLimit(limit);
      proto.setReadMode(readMode.pbVersion());

      if (replicaSelection == ReplicaSelection.LEADER_ONLY) {
        proto.setReplicaSelection(Common.ReplicaSelection.LEADER_ONLY);
      } else if (replicaSelection == ReplicaSelection.CLOSEST_REPLICA) {
        proto.setReplicaSelection(Common.ReplicaSelection.CLOSEST_REPLICA);
      }

      // If the last propagated timestamp is set send it with the scan.
      if (table.getAsyncClient().getLastPropagatedTimestamp() != AsyncKuduClient.NO_TIMESTAMP) {
        proto.setPropagatedTimestamp(client.getLastPropagatedTimestamp());
      }

      // If the mode is set to read on snapshot set the snapshot timestamps.
      if (readMode == AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT) {
        if (htTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
          proto.setSnapTimestamp(htTimestamp);
        }
        if (startTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
          proto.setSnapStartTimestamp(startTimestamp);
        }
      }

      proto.setCacheBlocks(cacheBlocks);
      proto.setFaultTolerant(isFaultTolerant);
      proto.setBatchSizeBytes(batchSizeBytes);
      proto.setScanRequestTimeoutMs(scanRequestTimeout);
      proto.setKeepAlivePeriodMs(keepAlivePeriodMs);

      try {
        PartitionPruner pruner = PartitionPruner.create(this);
        List<KeyRange> keyRanges = new ArrayList<>();
        while (pruner.hasMorePartitionKeyRanges()) {
          Pair<byte[], byte[]> partitionRange = pruner.nextPartitionKeyRange();
          List<KeyRange> newKeyRanges = client.getTableKeyRanges(
              table,
              proto.getLowerBoundPrimaryKey().toByteArray(),
              proto.getUpperBoundPrimaryKey().toByteArray(),
              partitionRange.getFirst().length == 0 ? null : partitionRange.getFirst(),
              partitionRange.getSecond().length == 0 ? null : partitionRange.getSecond(),
              AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
              splitSizeBytes,
              timeout).join();

          if (newKeyRanges.isEmpty()) {
            pruner.removePartitionKeyRange(partitionRange.getSecond());
          } else {
            pruner.removePartitionKeyRange(newKeyRanges.get(newKeyRanges.size() - 1)
                                                       .getPartitionKeyEnd());
          }
          keyRanges.addAll(newKeyRanges);
        }

        List<KuduScanToken> tokens = new ArrayList<>(keyRanges.size());
        for (KeyRange keyRange : keyRanges) {
          Client.ScanTokenPB.Builder builder = proto.clone();
          builder.setLowerBoundPartitionKey(
              UnsafeByteOperations.unsafeWrap(keyRange.getPartitionKeyStart()));
          builder.setUpperBoundPartitionKey(
              UnsafeByteOperations.unsafeWrap(keyRange.getPartitionKeyEnd()));
          byte[] primaryKeyStart = keyRange.getPrimaryKeyStart();
          if (primaryKeyStart != null && primaryKeyStart.length > 0) {
            builder.setLowerBoundPrimaryKey(UnsafeByteOperations.unsafeWrap(primaryKeyStart));
          }
          byte[] primaryKeyEnd = keyRange.getPrimaryKeyEnd();
          if (primaryKeyEnd != null && primaryKeyEnd.length > 0) {
            builder.setUpperBoundPrimaryKey(UnsafeByteOperations.unsafeWrap(primaryKeyEnd));
          }

          LocatedTablet tablet = keyRange.getTablet();

          // Set the tablet metadata so that a call to the master is not needed to
          // locate the tablet to scan when opening the scanner.
          if (includeTabletMetadata) {
            TableLocationsCache.Entry entry = client.getTableLocationEntry(table.getTableId(),
                tablet.getPartition().partitionKeyStart);
            if (entry != null && !entry.isNonCoveredRange() && !entry.isStale()) {
              RemoteTablet remoteTablet = entry.getTablet();

              // Build the list of server metadata.
              List<Client.ServerMetadataPB> servers = new ArrayList<>();
              Map<HostAndPort, Integer> serverIndexMap = new HashMap<>();
              List<ServerInfo> tabletServers = remoteTablet.getTabletServersCopy();
              for (int i = 0; i < tabletServers.size(); i++) {
                ServerInfo serverInfo = tabletServers.get(i);
                Client.ServerMetadataPB serverMetadataPB =
                    Client.ServerMetadataPB.newBuilder()
                        .setUuid(ByteString.copyFromUtf8(serverInfo.getUuid()))
                        .addRpcAddresses(
                            ProtobufHelper.hostAndPortToPB(serverInfo.getHostAndPort()))
                        .setLocation(serverInfo.getLocation())
                      .build();
                servers.add(serverMetadataPB);
                serverIndexMap.put(serverInfo.getHostAndPort(), i);
              }

              // Build the list of replica metadata.
              List<Client.TabletMetadataPB.ReplicaMetadataPB> replicas = new ArrayList<>();
              for (LocatedTablet.Replica replica : remoteTablet.getReplicas()) {
                Integer serverIndex = serverIndexMap.get(
                    new HostAndPort(replica.getRpcHost(), replica.getRpcPort()));
                Client.TabletMetadataPB.ReplicaMetadataPB.Builder tabletMetadataBuilder =
                    Client.TabletMetadataPB.ReplicaMetadataPB.newBuilder()
                        .setRole(replica.getRoleAsEnum())
                        .setTsIdx(serverIndex);
                if (replica.getDimensionLabel() != null) {
                  tabletMetadataBuilder.setDimensionLabel(replica.getDimensionLabel());
                }
                replicas.add(tabletMetadataBuilder.build());
              }

              // Build the tablet metadata and add it to the token.
              Client.TabletMetadataPB tabletMetadataPB = Client.TabletMetadataPB.newBuilder()
                  .setTabletId(remoteTablet.getTabletId())
                  .setPartition(ProtobufHelper.partitionToPb(remoteTablet.getPartition()))
                  .addAllReplicas(replicas)
                  .addAllTabletServers(servers)
                  .setTtlMillis(entry.ttl())
                  .build();
              builder.setTabletMetadata(tabletMetadataPB);
            }
          }

          tokens.add(new KuduScanToken(keyRange.getTablet(), builder.build()));
        }
        return tokens;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
