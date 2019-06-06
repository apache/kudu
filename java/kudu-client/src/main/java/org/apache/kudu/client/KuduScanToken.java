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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Client.ScanTokenPB;
import org.apache.kudu.client.ProtobufHelper.SchemaPBConversionFlags;
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
    KuduTable table = token.hasTableId() ? client.openTableById(token.getTableId()) :
                                           client.openTable(token.getTableName());

    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper("ScanToken")
                                                   .add("table-name", token.getTableName());

    if (token.hasTableId()) {
      helper.add("table-id", token.getTableId());
    }

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
    List<Integer> columns = new ArrayList<>(message.getProjectedColumnsCount());
    for (Common.ColumnSchemaPB colSchemaFromPb : message.getProjectedColumnsList()) {
      int colIdx = colSchemaFromPb.hasId() && schema.hasColumnIds() ?
          schema.getColumnIndex(colSchemaFromPb.getId()) :
          schema.getColumnIndex(colSchemaFromPb.getName());
      ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
      if (colSchemaFromPb.getType() != colSchema.getType().getDataType(colSchema.getTypeAttributes())) {
        throw new IllegalStateException(String.format(
            "invalid type %s for column '%s' in scan token, expected: %s",
            colSchemaFromPb.getType().name(), colSchemaFromPb.getName(), colSchema.getType().name()));
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

  private static KuduScanner pbIntoScanner(ScanTokenPB message,
                                           KuduClient client) throws KuduException {
    Preconditions.checkArgument(
        !message.getFeatureFlagsList().contains(ScanTokenPB.Feature.Unknown),
        "Scan token requires an unsupported feature. This Kudu client must be updated.");

    KuduTable table = message.hasTableId() ? client.openTableById(message.getTableId()) :
                                             client.openTable(message.getTableName());
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

      proto.setTableId(table.getTableId());
      proto.setTableName(table.getName());

      // Map the column names or indices to actual columns in the table schema.
      // If the user did not set either projection, then scan all columns.
      Schema schema = table.getSchema();
      if (projectedColumnNames != null) {
        for (String columnName : projectedColumnNames) {
          ColumnSchema columnSchema = schema.getColumn(columnName);
          Preconditions.checkArgument(columnSchema != null, "unknown column i%s", columnName);
          ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(),
                                    schema.hasColumnIds() ? schema.getColumnId(columnName) : -1,
                                    columnSchema);
        }
      } else if (projectedColumnIndexes != null) {
        for (int columnIdx : projectedColumnIndexes) {
          ColumnSchema columnSchema = schema.getColumnByIndex(columnIdx);
          Preconditions.checkArgument(columnSchema != null, "unknown column index %s", columnIdx);
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
          tokens.add(new KuduScanToken(keyRange.getTablet(), builder.build()));
        }
        return tokens;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
