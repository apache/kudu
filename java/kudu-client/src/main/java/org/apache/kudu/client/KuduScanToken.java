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
import java.util.List;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.client.Client.ScanTokenPB;
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
    KuduTable table = client.openTable(token.getTableName());

    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper("ScanToken")
                                                   .add("table", token.getTableName());

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

  private static KuduScanner pbIntoScanner(ScanTokenPB message,
                                           KuduClient client) throws KuduException {
    Preconditions.checkArgument(
        !message.getFeatureFlagsList().contains(ScanTokenPB.Feature.Unknown),
        "Scan token requires an unsupported feature. This Kudu client must be updated.");

    KuduTable table = client.openTable(message.getTableName());
    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);

    List<Integer> columns = new ArrayList<>(message.getProjectedColumnsCount());
    for (Common.ColumnSchemaPB column : message.getProjectedColumnsList()) {
      int columnIdx = table.getSchema().getColumnIndex(column.getName());
      ColumnSchema schema = table.getSchema().getColumnByIndex(columnIdx);
      if (column.getType() != schema.getType().getDataType()) {
        throw new IllegalStateException(String.format(
            "invalid type %s for column '%s' in scan token, expected: %s",
            column.getType().name(), column.getName(), schema.getType().name()));
      }
      if (column.getIsNullable() != schema.isNullable()) {
        throw new IllegalStateException(String.format(
            "invalid nullability for column '%s' in scan token, expected: %s",
            column.getName(), column.getIsNullable() ? "NULLABLE" : "NOT NULL"));

      }

      columns.add(columnIdx);
    }
    builder.setProjectedColumnIndexes(columns);

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

    if (message.hasFaultTolerant()) {
      // TODO(KUDU-1040)
    }

    if (message.hasReadMode()) {
      switch (message.getReadMode()) {
        case READ_AT_SNAPSHOT: {
          builder.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
          if (message.hasSnapTimestamp()) {
            builder.snapshotTimestampRaw(message.getSnapTimestamp());
          }
          break;
        }
        case READ_LATEST: {
          builder.readMode(AsyncKuduScanner.ReadMode.READ_LATEST);
          break;
        }
        default: throw new IllegalArgumentException("unknown read mode");
      }
    }

    if (message.hasPropagatedTimestamp()) {
      // TODO (KUDU-1411)
    }

    if (message.hasCacheBlocks()) {
      builder.cacheBlocks(message.getCacheBlocks());
    }

    if (message.hasFaultTolerant()) {
      builder.setFaultTolerant(message.getFaultTolerant());
    }

    return builder.build();
  }

  @Override
  public int compareTo(KuduScanToken other) {
    if (!message.getTableName().equals(other.message.getTableName())) {
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

    private long timeout;

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
          return ImmutableList.of();
        }
      }

      Client.ScanTokenPB.Builder proto = Client.ScanTokenPB.newBuilder();

      proto.setTableName(table.getName());

      // Map the column names or indices to actual columns in the table schema.
      // If the user did not set either projection, then scan all columns.
      if (projectedColumnNames != null) {
        for (String columnName : projectedColumnNames) {
          ColumnSchema columnSchema = table.getSchema().getColumn(columnName);
          Preconditions.checkArgument(columnSchema != null, "unknown column i%s", columnName);
          ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(), columnSchema);
        }
      } else if (projectedColumnIndexes != null) {
        for (int columnIdx : projectedColumnIndexes) {
          ColumnSchema columnSchema = table.getSchema().getColumnByIndex(columnIdx);
          Preconditions.checkArgument(columnSchema != null, "unknown column index %s", columnIdx);
          ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(), columnSchema);
        }
      } else {
        for (ColumnSchema column : table.getSchema().getColumns()) {
          ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(), column);
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

      // If the last propagated timestamp is set send it with the scan.
      if (table.getAsyncClient().getLastPropagatedTimestamp() != AsyncKuduClient.NO_TIMESTAMP) {
        proto.setPropagatedTimestamp(client.getLastPropagatedTimestamp());
      }

      // If the mode is set to read on snapshot set the snapshot timestamp.
      if (readMode == AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT &&
          htTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
        proto.setSnapTimestamp(htTimestamp);
      }

      proto.setCacheBlocks(cacheBlocks);
      proto.setFaultTolerant(isFaultTolerant);

      try {
        PartitionPruner pruner = PartitionPruner.create(this);
        List<LocatedTablet> tablets = new ArrayList<>();
        while (pruner.hasMorePartitionKeyRanges()) {
          Pair<byte[], byte[]> partitionRange = pruner.nextPartitionKeyRange();
          List<LocatedTablet> newTablets = table.getTabletsLocations(
              partitionRange.getFirst().length == 0 ? null : partitionRange.getFirst(),
              partitionRange.getSecond().length == 0 ? null : partitionRange.getSecond(),
              timeout);

          if (newTablets.isEmpty()) {
            pruner.removePartitionKeyRange(partitionRange.getSecond());
          } else {
            pruner.removePartitionKeyRange(newTablets.get(newTablets.size() - 1)
                                                     .getPartition()
                                                     .getPartitionKeyEnd());
          }
          tablets.addAll(newTablets);
        }

        List<KuduScanToken> tokens = new ArrayList<>(tablets.size());
        for (LocatedTablet tablet : tablets) {
          Client.ScanTokenPB.Builder builder = proto.clone();
          builder.setLowerBoundPartitionKey(
              UnsafeByteOperations.unsafeWrap(tablet.getPartition().getPartitionKeyStart()));
          builder.setUpperBoundPartitionKey(
              UnsafeByteOperations.unsafeWrap(tablet.getPartition().getPartitionKeyEnd()));
          tokens.add(new KuduScanToken(tablet, builder.build()));
        }
        return tokens;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
