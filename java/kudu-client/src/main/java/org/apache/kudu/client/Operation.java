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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.RowOperations.RowOperationsPB;
import org.apache.kudu.Schema;
import org.apache.kudu.WireProtocol.AppStatusPB.ErrorCode;
import org.apache.kudu.client.ProtobufHelper.SchemaPBConversionFlags;
import org.apache.kudu.client.Statistics.Statistic;
import org.apache.kudu.client.Statistics.TabletStatistics;
import org.apache.kudu.security.Token;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.Pair;

/**
 * Base class for the RPCs that related to WriteRequestPB. It contains almost all the logic
 * and knows how to serialize its child classes.
 *
 * TODO(todd): this should not extend KuduRpc. Rather, we should make single-operation writes
 * just use a Batch instance with a single operation in it.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Operation extends KuduRpc<OperationResponse> {
  /**
   * This size will be set when serialize is called. It stands for the size of the row in this
   * operation.
   */
  private long rowOperationSizeBytes = 0;

  enum ChangeType {
    INSERT((byte)RowOperationsPB.Type.INSERT.getNumber()),
    UPDATE((byte)RowOperationsPB.Type.UPDATE.getNumber()),
    DELETE((byte)RowOperationsPB.Type.DELETE.getNumber()),
    SPLIT_ROWS((byte)RowOperationsPB.Type.SPLIT_ROW.getNumber()),
    UPSERT((byte)RowOperationsPB.Type.UPSERT.getNumber()),
    RANGE_LOWER_BOUND((byte) RowOperationsPB.Type.RANGE_LOWER_BOUND.getNumber()),
    RANGE_UPPER_BOUND((byte) RowOperationsPB.Type.RANGE_UPPER_BOUND.getNumber()),
    EXCLUSIVE_RANGE_LOWER_BOUND(
        (byte) RowOperationsPB.Type.EXCLUSIVE_RANGE_LOWER_BOUND.getNumber()),
    INCLUSIVE_RANGE_UPPER_BOUND(
        (byte) RowOperationsPB.Type.INCLUSIVE_RANGE_UPPER_BOUND.getNumber()),
    INSERT_IGNORE((byte) RowOperationsPB.Type.INSERT_IGNORE.getNumber()),
    UPDATE_IGNORE((byte) RowOperationsPB.Type.UPDATE_IGNORE.getNumber()),
    DELETE_IGNORE((byte) RowOperationsPB.Type.DELETE_IGNORE.getNumber());

    ChangeType(byte encodedByte) {
      this.encodedByte = encodedByte;
    }

    byte toEncodedByte() {
      return encodedByte;
    }

    /** The byte used to encode this in a RowOperationsPB */
    private final byte encodedByte;
  }

  static final String METHOD = "Write";

  private PartialRow row;

  private Token.SignedTokenPB authzToken;

  /** See {@link SessionConfiguration#setIgnoreAllDuplicateRows(boolean)} */
  boolean ignoreAllDuplicateRows = false;
  /** See {@link SessionConfiguration#setIgnoreAllNotFoundRows(boolean)} */
  boolean ignoreAllNotFoundRows = false;
  /**
   * Transaction identifier for the generated WriteRequestPB. Applicable only
   * if set to a valid value.
   */
  long txnId = AsyncKuduClient.INVALID_TXN_ID;

  /**
   * Package-private constructor. Subclasses need to be instantiated via AsyncKuduSession
   * @param table table with the schema to use for this operation
   */
  Operation(KuduTable table) {
    super(table, null, 0);
    this.row = table.getSchema().newPartialRow();
  }

  /**
   * Reset the timeout of this batch.
   *
   * TODO(wdberkeley): The fact we have to do this is a sign an Operation should not subclass
   * KuduRpc.
   *
   * @param timeoutMillis the new timeout of the batch in milliseconds
   */
  void resetTimeoutMillis(Timer timer, long timeoutMillis) {
    timeoutTracker.reset();
    timeoutTracker.setTimeout(timeoutMillis);
    if (timeoutTask != null) {
      timeoutTask.cancel();
    }
    timeoutTask = AsyncKuduClient.newTimeout(timer, new RpcTimeoutTask(), timeoutMillis);
  }

  /** See {@link SessionConfiguration#setIgnoreAllDuplicateRows(boolean)} */
  void setIgnoreAllDuplicateRows(boolean ignoreAllDuplicateRows) {
    this.ignoreAllDuplicateRows = ignoreAllDuplicateRows;
  }

  /** See {@link SessionConfiguration#setIgnoreAllNotFoundRows(boolean)} */
  void setIgnoreAllNotFoundRows(boolean ignoreAllNotFoundRows) {
    this.ignoreAllNotFoundRows = ignoreAllNotFoundRows;
  }

  /**
   * Set transaction identifier for this operation. If set, the transaction
   * identifier is propagated into the generated WriteRequestPB.
   *
   * @param txnId transaction identifier to set
   */
  void setTxnId(long txnId) {
    this.txnId = txnId;
  }

  /**
   * Classes extending Operation need to have a specific ChangeType
   * @return Operation's ChangeType
   */
  abstract ChangeType getChangeType();

  /**
   * Returns the size in bytes of this operation's row after serialization.
   * @return size in bytes
   * @throws IllegalStateException thrown if this RPC hasn't been serialized eg sent to a TS
   */
  long getRowOperationSizeBytes() {
    if (this.rowOperationSizeBytes == 0) {
      throw new IllegalStateException("This row hasn't been serialized yet");
    }
    return this.rowOperationSizeBytes;
  }

  @Override
  String serviceName() {
    return TABLET_SERVER_SERVICE_NAME;
  }

  @Override
  String method() {
    return METHOD;
  }

  @Override
  boolean needsAuthzToken() {
    return true;
  }

  @Override
  void bindAuthzToken(Token.SignedTokenPB token) {
    authzToken = token;
  }

  @Override
  Message createRequestPB() {
    final Tserver.WriteRequestPB.Builder builder =
        createAndFillWriteRequestPB(ImmutableList.of(this));
    this.rowOperationSizeBytes = (long)builder.getRowOperations().getRows().size() +
        (long)builder.getRowOperations().getIndirectData().size();
    builder.setTabletId(UnsafeByteOperations.unsafeWrap(getTablet().getTabletIdAsBytes()));
    builder.setExternalConsistencyMode(this.externalConsistencyMode.pbVersion());
    if (this.propagatedTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
      builder.setPropagatedTimestamp(this.propagatedTimestamp);
    }
    if (authzToken != null) {
      builder.setAuthzToken(authzToken);
    }
    if (this.txnId != AsyncKuduClient.INVALID_TXN_ID) {
      builder.setTxnId(this.txnId);
    }
    return builder.build();
  }

  @Override
  Pair<OperationResponse, Object> deserialize(CallResponse callResponse,
                                              String tsUUID) throws KuduException {
    Tserver.WriteResponsePB.Builder builder = Tserver.WriteResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    Tserver.WriteResponsePB.PerRowErrorPB error = null;
    if (builder.getPerRowErrorsCount() != 0) {
      error = builder.getPerRowErrors(0);
      ErrorCode errorCode = error.getError().getCode();
      if ((ignoreAllDuplicateRows && errorCode == ErrorCode.ALREADY_PRESENT) ||
          (ignoreAllNotFoundRows && errorCode == ErrorCode.NOT_FOUND)) {
        error = null;
      }
    }
    Tserver.ResourceMetricsPB metricsPB = builder.hasResourceMetrics() ?
            builder.getResourceMetrics() : null;
    OperationResponse response = new OperationResponse(timeoutTracker.getElapsedMillis(),
                                                       tsUUID,
                                                       builder.getTimestamp(),
                                                       this,
                                                       error,
                                                       metricsPB);
    return new Pair<>(response, builder.hasError() ? builder.getError() : null);
  }

  @Override
  public byte[] partitionKey() {
    return this.getTable().getPartitionSchema().encodePartitionKey(row);
  }

  @Override
  boolean isRequestTracked() {
    return true;
  }

  /**
   * Get the underlying row to modify.
   * @return a partial row that will be sent with this Operation
   */
  public PartialRow getRow() {
    return this.row;
  }

  /**
   * Set the underlying row.
   *
   * Note: The schema of the underlying row and the table must be equal by reference.
   * To ensure they are equal, create the partial row from the table's schema.
   *
   * <pre>{@code
   *   KuduTable table = client.openTable("my-table");
   *   PartialRow row = table.getSchema().newPartialRow();
   *   ...
   *   Operation op = table.newInsert();
   *   op.setRow(row);
   * }</pre>
   *
   * @param row the row to set
   */
  public void setRow(PartialRow row) {
    Preconditions.checkArgument(row.getSchema() == table.getSchema(),
        "The row's schema must be equal by reference to the table schema");
    this.row = row;
  }

  @Override
  void updateStatistics(Statistics statistics, OperationResponse response) {
    String tabletId = this.getTablet().getTabletId();
    String tableName = this.getTable().getName();
    TabletStatistics tabletStatistics = statistics.getTabletStatistics(tableName, tabletId);
    if (response == null) {
      tabletStatistics.incrementStatistic(Statistic.OPS_ERRORS, 1);
      tabletStatistics.incrementStatistic(Statistic.RPC_ERRORS, 1);
      return;
    }
    tabletStatistics.incrementStatistic(Statistic.WRITE_RPCS, 1);
    if (response.hasRowError()) {
      // If ignoreAllDuplicateRows is set, the already_present exception will be
      // discarded and wont't be recorded here
      tabletStatistics.incrementStatistic(Statistic.OPS_ERRORS, 1);
    } else {
      tabletStatistics.incrementStatistic(Statistic.WRITE_OPS, 1);
    }
    tabletStatistics.incrementStatistic(Statistic.BYTES_WRITTEN, getRowOperationSizeBytes());
  }

  /**
   * Helper method that puts a list of Operations together into a WriteRequestPB.
   * @param operations The list of ops to put together in a WriteRequestPB
   * @return A fully constructed WriteRequestPB containing the passed rows, or
   *         null if no rows were passed.
   */
  static Tserver.WriteRequestPB.Builder createAndFillWriteRequestPB(List<Operation> operations) {
    if (operations == null || operations.isEmpty()) {
      return null;
    }
    Schema schema = operations.get(0).table.getSchema();
    RowOperationsPB rowOps = new OperationsEncoder().encodeOperations(operations);
    if (rowOps == null) {
      return null;
    }

    Tserver.WriteRequestPB.Builder requestBuilder = Tserver.WriteRequestPB.newBuilder();
    requestBuilder.setSchema(ProtobufHelper.schemaToPb(schema,
        EnumSet.of(SchemaPBConversionFlags.SCHEMA_PB_WITHOUT_COMMENT,
                   SchemaPBConversionFlags.SCHEMA_PB_WITHOUT_ID)));
    requestBuilder.setRowOperations(rowOps);
    return requestBuilder;
  }

  static class OperationsEncoder {
    private Schema schema;
    private ByteBuffer rows;
    // We're filling this list as we go through the operations in encodeRow() and at the same time
    // compute the total size, which will be used to right-size the array in toPB().
    private List<ByteBuffer> indirect;
    private long indirectWrittenBytes;

    /**
     * Initializes the state of the encoder based on the schema and number of operations to encode.
     *
     * @param schema the schema of the table which the operations belong to.
     * @param numOperations the number of operations.
     */
    private void init(Schema schema, int numOperations) {
      this.schema = schema;

      // Set up the encoded data.
      // Estimate a maximum size for the data. This is conservative, but avoids
      // having to loop through all the operations twice.
      final int columnBitSetSize = Bytes.getBitSetSize(schema.getColumnCount());
      int sizePerRow = 1 /* for the op type */ + schema.getRowSize() + columnBitSetSize;
      if (schema.hasNullableColumns()) {
        // nullsBitSet is the same size as the columnBitSet
        sizePerRow += columnBitSetSize;
      }

      // TODO: would be more efficient to use a buffer which "chains" smaller allocations
      // instead of a doubling buffer like BAOS.
      this.rows = ByteBuffer.allocate(sizePerRow * numOperations)
                            .order(ByteOrder.LITTLE_ENDIAN);
      this.indirect = new ArrayList<>(schema.getVarLengthColumnCount() * numOperations);
    }

    /**
     * Builds the row operations protobuf message with encoded operations.
     * @return the row operations protobuf message.
     */
    private RowOperationsPB toPB() {
      RowOperationsPB.Builder rowOpsBuilder = RowOperationsPB.newBuilder();

      // TODO: we could avoid a copy here by using an implementation that allows
      // zero-copy on a slice of an array.
      rows.limit(rows.position());
      rows.flip();
      rowOpsBuilder.setRows(ByteString.copyFrom(rows));
      if (indirect.size() > 0) {
        // TODO: same as above, we could avoid a copy here by using an implementation that allows
        // zero-copy on a slice of an array.
        byte[] indirectData = new byte[(int)indirectWrittenBytes];
        int offset = 0;
        for (ByteBuffer bb : indirect) {
          int bbSize = bb.remaining();
          bb.get(indirectData, offset, bbSize);
          offset += bbSize;
        }
        rowOpsBuilder.setIndirectData(UnsafeByteOperations.unsafeWrap(indirectData));
      }
      return rowOpsBuilder.build();
    }

    private void encodeRow(PartialRow row, ChangeType type) {
      int columnCount = row.getSchema().getColumnCount();
      BitSet columnsBitSet = row.getColumnsBitSet();
      BitSet nullsBitSet = row.getNullsBitSet();

      // If this is a DELETE operation only the key columns should to be set.
      if (type == ChangeType.DELETE || type == ChangeType.DELETE_IGNORE) {
        columnCount = row.getSchema().getPrimaryKeyColumnCount();
        // Clear the bits indicating any non-key fields are set.
        columnsBitSet.clear(schema.getPrimaryKeyColumnCount(), columnsBitSet.size());
        if (schema.hasNullableColumns()) {
          nullsBitSet.clear(schema.getPrimaryKeyColumnCount(), nullsBitSet.size());
        }
      }

      rows.put(type.toEncodedByte());
      rows.put(Bytes.fromBitSet(columnsBitSet, schema.getColumnCount()));
      if (schema.hasNullableColumns()) {
        rows.put(Bytes.fromBitSet(nullsBitSet, schema.getColumnCount()));
      }

      byte[] rowData = row.getRowAlloc();
      int currentRowOffset = 0;
      for (int colIdx = 0; colIdx < columnCount; colIdx++) {
        ColumnSchema col = schema.getColumnByIndex(colIdx);
        // Keys should always be specified, maybe check?
        if (row.isSet(colIdx) && !row.isSetToNull(colIdx)) {
          if (col.getType().isFixedSize()) {
            rows.put(rowData, currentRowOffset, col.getTypeSize());
          } else {
            ByteBuffer varLengthData = row.getVarLengthData().get(colIdx);
            varLengthData.reset();
            rows.putLong(indirectWrittenBytes);
            int bbSize = varLengthData.remaining();
            rows.putLong(bbSize);
            indirect.add(varLengthData);
            indirectWrittenBytes += bbSize;
          }
        }
        currentRowOffset += col.getTypeSize();
      }
    }

    public RowOperationsPB encodeOperations(List<Operation> operations) {
      if (operations == null || operations.isEmpty()) {
        return null;
      }
      init(operations.get(0).table.getSchema(), operations.size());
      for (Operation operation : operations) {
        encodeRow(operation.row, operation.getChangeType());
      }
      return toPB();
    }

    public RowOperationsPB encodeRangePartitions(
        List<RangePartition> rangePartitions,
        List<PartialRow> splitRows) {

      if (splitRows.isEmpty() && rangePartitions.isEmpty()) {
        return null;
      }

      Schema schema = splitRows.isEmpty() ? rangePartitions.get(0).getLowerBound().getSchema()
                                          : splitRows.get(0).getSchema();
      init(schema, splitRows.size() + 2 * rangePartitions.size());

      for (PartialRow row : splitRows) {
        encodeRow(row, ChangeType.SPLIT_ROWS);
      }

      for (RangePartition partition : rangePartitions) {
        encodeRow(partition.getLowerBound(),
            partition.getLowerBoundType() == RangePartitionBound.INCLUSIVE_BOUND ?
                ChangeType.RANGE_LOWER_BOUND :
                ChangeType.EXCLUSIVE_RANGE_LOWER_BOUND);
        encodeRow(partition.getUpperBound(),
            partition.getUpperBoundType() == RangePartitionBound.EXCLUSIVE_BOUND ?
                ChangeType.RANGE_UPPER_BOUND :
                ChangeType.INCLUSIVE_RANGE_UPPER_BOUND);
      }

      return toPB();
    }

    public RowOperationsPB encodeLowerAndUpperBounds(PartialRow lowerBound,
                                                     PartialRow upperBound,
                                                     RangePartitionBound lowerBoundType,
                                                     RangePartitionBound upperBoundType) {
      init(lowerBound.getSchema(), 2);
      encodeRow(lowerBound,
                lowerBoundType == RangePartitionBound.INCLUSIVE_BOUND ?
                    ChangeType.RANGE_LOWER_BOUND :
                    ChangeType.EXCLUSIVE_RANGE_LOWER_BOUND);
      encodeRow(upperBound,
                upperBoundType == RangePartitionBound.EXCLUSIVE_BOUND ?
                    ChangeType.RANGE_UPPER_BOUND :
                    ChangeType.INCLUSIVE_RANGE_UPPER_BOUND);
      return toPB();
    }
  }

  static class OperationsDecoder {
    private Schema schema;
    private int columnBitmapSize;
    private int columnCount;

    /**
     * Utility method to initialize internals of the OperationsDecoder class.
     *
     * @param schema table's schema used for encoding/decoding of the data
     */
    private void init(Schema schema) {
      this.schema = schema;
      this.columnCount = schema.getColumnCount();
      this.columnBitmapSize = Bytes.getBitSetSize(this.columnCount);
    }

    /**
     * Decode range partitions from the 'pb' message, assuming the data has been
     * encoded using the table schema in 'schema'.
     *
     * @param pb the encoded data
     * @param schema the table schema to use for decoding
     * @return a list of PangePartition objects with corresponding bounds
     */
    public List<RangePartition> decodeRangePartitions(
        RowOperationsPB pb, Schema schema) {
      if (pb == null) {
        return null;
      }
      if (!pb.hasRows()) {
        throw new IllegalArgumentException("row operation PB lacks 'rows' field");
      }

      init(schema);

      ByteBuffer rowsBuf = ByteBuffer.wrap(pb.getRows().toByteArray());
      rowsBuf.order(ByteOrder.LITTLE_ENDIAN);
      final byte[] indirectData = pb.getIndirectData().toByteArray();

      List<Pair<PartialRow, RowOperationsPB.Type>> decodedBounds =
          new ArrayList<>();
      while (rowsBuf.hasRemaining()) {
        decodedBounds.add(decodeBound(rowsBuf, indirectData));
      }

      if (decodedBounds.size() % 2 != 0) {
        throw new IllegalArgumentException(
            "unexpected odd number of range partition bounds");
      }

      List<RangePartition> result = new ArrayList<>();
      for (int i = 0; i < decodedBounds.size(); i += 2) {
        Pair<PartialRow, RowOperationsPB.Type> lower = decodedBounds.get(i);
        Pair<PartialRow, RowOperationsPB.Type> upper = decodedBounds.get(i + 1);

        RangePartitionBound lowerType;
        if (lower.getSecond() == RowOperationsPB.Type.EXCLUSIVE_RANGE_LOWER_BOUND) {
          lowerType = RangePartitionBound.EXCLUSIVE_BOUND;
        } else if (lower.getSecond() == RowOperationsPB.Type.RANGE_LOWER_BOUND) {
          lowerType = RangePartitionBound.INCLUSIVE_BOUND;
        } else {
          throw new IllegalArgumentException(String.format(
              "%s: unexpected bound type for the lower bound", lower.getSecond().toString()));
        }

        RangePartitionBound upperType;
        if (upper.getSecond() == RowOperationsPB.Type.INCLUSIVE_RANGE_UPPER_BOUND) {
          upperType = RangePartitionBound.INCLUSIVE_BOUND;
        } else if (upper.getSecond() == RowOperationsPB.Type.RANGE_UPPER_BOUND) {
          upperType = RangePartitionBound.EXCLUSIVE_BOUND;
        } else {
          throw new IllegalArgumentException(String.format(
              "%s: unexpected bound type for the upper bound", upper.getSecond().toString()));
        }

        result.add(new RangePartition(lower.getFirst(), upper.getFirst(), lowerType, upperType));
      }
      return result;
    }

    /**
     * Decode lower and upper range bounds encoded with the RowOperationsPB
     * conventions.
     *
     * @param rowsBuf byte buffer wrapping RowOperationsPB.rows
     * @param indirectData byte array of the RowOperationsPB.indirect_data field
     * @return a pair: decoded bound as PartialRow and the type of the bound
     */
    public Pair<PartialRow, RowOperationsPB.Type> decodeBound(
        ByteBuffer rowsBuf, byte[] indirectData) {
      RowOperationsPB.Type opType;
      final byte opTypeEncoded = rowsBuf.get();
      switch (opTypeEncoded) {
        case RowOperationsPB.Type.EXCLUSIVE_RANGE_LOWER_BOUND_VALUE:
          opType = RowOperationsPB.Type.EXCLUSIVE_RANGE_LOWER_BOUND;
          break;
        case RowOperationsPB.Type.RANGE_LOWER_BOUND_VALUE:
          opType = RowOperationsPB.Type.RANGE_LOWER_BOUND;
          break;
        case RowOperationsPB.Type.INCLUSIVE_RANGE_UPPER_BOUND_VALUE:
          opType = RowOperationsPB.Type.INCLUSIVE_RANGE_UPPER_BOUND;
          break;
        case RowOperationsPB.Type.RANGE_UPPER_BOUND_VALUE:
          opType = RowOperationsPB.Type.RANGE_UPPER_BOUND;
          break;
        default:
          throw new IllegalArgumentException(String.format(
              "%d: unexpected operation type", opTypeEncoded));
      }

      // Read the 'isset' column bitmap.
      byte[] columnsBitArray = new byte[columnBitmapSize];
      rowsBuf.get(columnsBitArray);
      BitSet columnsBitSet = Bytes.toBitSet(columnsBitArray, 0, 8 * columnBitmapSize);

      // If present, read the 'null' column bitmap.
      BitSet nullsBitSet = null;
      if (schema.hasNullableColumns()) {
        byte[] columnsNullArray = new byte[columnBitmapSize];
        rowsBuf.get(columnsNullArray);
        nullsBitSet = Bytes.toBitSet(columnsNullArray, 0, 8 * columnBitmapSize);
      }

      // Construct the PartialRow object to contain the boundary as decoded.
      PartialRow resultRow = schema.newPartialRow();
      for (int i = 0; i < columnsBitSet.size(); ++i) {
        // Read the columns which has been set to a non-null value.
        if (columnsBitSet.get(i) && (nullsBitSet == null || !nullsBitSet.get(i))) {
          final ColumnSchema columnSchema = schema.getColumnByIndex(i);
          if (columnSchema.getType().isFixedSize()) {
            // The data for fixed-size types is read from the 'rowsBuf' buffer.
            byte[] columnData = new byte[columnSchema.getTypeSize()];
            rowsBuf.get(columnData);
            resultRow.setRaw(i, columnData);
          } else {
            // The data for variable-size types is read from the 'indirectData'
            // byte array.
            final int indirectOffset = (int)rowsBuf.getLong();
            final int indirectSize = (int)rowsBuf.getLong();

            ByteBuffer auxBuf = ByteBuffer.wrap(
                indirectData, indirectOffset, indirectSize);
            auxBuf.order(ByteOrder.LITTLE_ENDIAN);

            byte[] columnData = new byte[indirectSize];
            auxBuf.get(columnData);
            resultRow.setRaw(i, columnData);
          }
        }

      }
      return new Pair<PartialRow, RowOperationsPB.Type>(resultRow, opType);
    }
  }
}
