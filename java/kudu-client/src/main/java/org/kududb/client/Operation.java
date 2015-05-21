// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;

import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;

import kudu.WireProtocol.RowOperationsPB;
import kudu.tserver.Tserver;

import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Base class for the RPCs that related to WriteRequestPB. It contains almost all the logic
 * and knows how to serialize its child classes.
 * Each Operation is backed by an Arena where all the cells (except strings) are written. The
 * strings are kept in a List.
 */
public abstract class Operation extends KuduRpc<OperationResponse> implements KuduRpc.HasKey {

  // Number given by the session when apply()'d for the first time. Necessary to retain operations
  // in their original order even after tablet lookup.
  private long sequenceNumber = -1;

  enum ChangeType {
    INSERT((byte)RowOperationsPB.Type.INSERT.getNumber()),
    UPDATE((byte)RowOperationsPB.Type.UPDATE.getNumber()),
    DELETE((byte)RowOperationsPB.Type.DELETE.getNumber());

    ChangeType(byte encodedByte) {
      this.encodedByte = encodedByte;
    }

    byte toEncodedByte() {
      return encodedByte;
    }

    /** The byte used to encode this in a RowOperationsPB */
    private byte encodedByte;
  }

  static final String METHOD = "Write";
  final Schema schema;

  // String data, post UTF8-encoding.
  final List<byte[]> strings;
  final int rowSize;
  final byte[] rowAlloc;
  final BitSet columnsBitSet;
  final BitSet nullsBitSet;

  /**
   * Package-private constructor. Subclasses need to be instantiated via AsyncKuduSession
   * @param table table with the schema to use for this operation
   */
  Operation(KuduTable table) {
    super(table);
    this.schema = table.getSchema();
    this.columnsBitSet = new BitSet(this.schema.getColumnCount());
    this.nullsBitSet = schema.hasNullableColumns() ?
        new BitSet(this.schema.getColumnCount()) : null;
    this.rowSize = schema.getRowSize();
    this.rowAlloc = new byte[this.rowSize];
    strings = Lists.newArrayListWithCapacity(schema.getStringCount());
  }

  /**
   * Classes extending Operation need to have a specific ChangeType
   * @return Operation's ChangeType
   */
  abstract ChangeType getChangeType();

  /**
   * Add a boolean for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addBoolean(String columnName, boolean val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.BOOL);
    rowAlloc[getPositionInRowAllocAndSetBitSet(col)] = (byte) (val ? 1 : 0);
  }

  /**
   * Add a byte for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addByte(String columnName, byte val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.INT8);
    rowAlloc[getPositionInRowAllocAndSetBitSet(col)] = val;
  }

  /**
   * Add a short for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addShort(String columnName, short val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.INT16);
    Bytes.setShort(rowAlloc, val, getPositionInRowAllocAndSetBitSet(col));
  }

  /**
   * Add an int for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addInt(String columnName, int val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.INT32);
    Bytes.setInt(rowAlloc, val, getPositionInRowAllocAndSetBitSet(col));
  }

  /**
   * Add an long for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addLong(String columnName, long val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.INT64);
    Bytes.setLong(rowAlloc, val, getPositionInRowAllocAndSetBitSet(col));
  }

  /**
   * Add an float for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addFloat(String columnName, float val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.FLOAT);
    Bytes.setFloat(rowAlloc, val, getPositionInRowAllocAndSetBitSet(col));
  }

  /**
   * Add an double for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addDouble(String columnName, double val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.DOUBLE);
    Bytes.setDouble(rowAlloc, val, getPositionInRowAllocAndSetBitSet(col));
  }

  /**
   * Add a String for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addString(String columnName, String val) {
    addStringUtf8(columnName, Bytes.fromString(val));
  }

  /**
   * Add a String for the specified value, encoded as UTF8.
   * Note that the provided value must not be mutated after this.
   */
  public void addStringUtf8(String columnName, byte[] val) {
    // TODO: use Utf8.isWellFormed from Guava 16 to verify that
    // the user isn't putting in any garbage data.
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.STRING);
    int stringsIdx = strings.size();
    strings.add(val);
    // Set the bit and set the usage bit
    int pos = getPositionInRowAllocAndSetBitSet(col);

    // For now, just store the index of the string.
    // Later, we'll replace this with the offset within the wire buffer
    // before we send it.
    Bytes.setLong(rowAlloc, stringsIdx, pos);
    Bytes.setLong(rowAlloc, val.length, pos + Longs.BYTES);
  }

  /**
   * Set the specified column to null
   * @param columnName Name of the column
   * @throws IllegalArgumentException if the column doesn't exist or cannot be set to null
   */
  public void setNull(String columnName) {
    assert nullsBitSet != null;
    ColumnSchema col = schema.getColumn(columnName);
    checkColumnExists(col);
    if (!col.isNullable()) {
      throw new IllegalArgumentException(col.getName() + " cannot be set to null");
    }
    int idx = schema.getColumns().indexOf(col);
    columnsBitSet.set(idx);
    nullsBitSet.set(idx);
  }

  /**
   * Verifies if the column exists and belongs to the specified type
   * It also does some internal accounting
   * @param column column the user wants to set
   * @param type type we expect
   * @throws IllegalArgumentException if the column or type was invalid
   */
  void checkColumn(ColumnSchema column, Type type) {
    checkColumnExists(column);
    if (!column.getType().equals(type))
      throw new IllegalArgumentException(column.getName() +
          " isn't " + type.getName() + ", it's " + column.getType().getName());
  }

  /**
   * @param column column the user wants to set
   * @throws IllegalArgumentException if the column doesn't exist
   */
  void checkColumnExists(ColumnSchema column) {
    if (column == null)
      throw new IllegalArgumentException("Column name isn't present in the table's schema");
  }

  /**
   * Gives the column's location in the byte array and marks it as set
   * @param column column to get the position for and mark as set
   * @return the offset in rowAlloc for the column
   */
  int getPositionInRowAllocAndSetBitSet(ColumnSchema column) {
    int idx = schema.getColumns().indexOf(column);
    columnsBitSet.set(idx);
    return schema.getColumnOffset(idx);
  }

  /**
   * Tells if the specified column was set by the user
   * @param column column's index in the schema
   * @return true if it was set, else false
   */
  boolean isSet(int column) {
    return this.columnsBitSet.get(column);
  }

  /**
   * Tells if the specified column was set to null by the user
   * @param column column's index in the schema
   * @return true if it was set, else false
   */
  boolean isSetToNull(int column) {
    if (this.nullsBitSet == null) {
      return false;
    }
    return this.nullsBitSet.get(column);
  }

  /**
   * Sets the sequence number used when batching operations. Should only be called once.
   * @param sequenceNumber a new sequence number
   */
  void setSequenceNumber(long sequenceNumber) {
    assert (this.sequenceNumber == -1);
    this.sequenceNumber = sequenceNumber;
  }

  /**
   * Returns the sequence number given to this operation.
   * @return a long representing the sequence number given to this operation after it was applied,
   * can be -1 if it wasn't set
   */
  long getSequenceNumber() {
    return this.sequenceNumber;
  }

  @Override
  String serviceName() { return TABLET_SERVER_SERVICE_NAME; }

  @Override
  String method() {
    return METHOD;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Tserver.WriteRequestPB.Builder builder = createAndFillWriteRequestPB(this);
    builder.setTabletId(ZeroCopyLiteralByteString.wrap(getTablet().getTabletIdAsBytes()));
    builder.setExternalConsistencyMode(this.externalConsistencyMode.pbVersion());
    if (this.propagatedTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
      builder.setPropagatedTimestamp(this.propagatedTimestamp);
    }
    return toChannelBuffer(header, builder.build());
  }

  @Override
  Pair<OperationResponse, Object> deserialize(CallResponse callResponse,
                                              String tsUUID) throws Exception {
    Tserver.WriteResponsePB.Builder builder = Tserver.WriteResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    Tserver.WriteResponsePB.PerRowErrorPB error = null;
    if (builder.getPerRowErrorsCount() != 0) {
      error = builder.getPerRowErrors(0);
    }
    OperationResponse response = new OperationResponse(deadlineTracker.getElapsedMillis(), tsUUID,
        builder.getTimestamp(), this, error);
    return new Pair<OperationResponse, Object>(response, builder.getError());
  }

  @Override
  public byte[] key() {
    int seenStrings = 0;
    KeyEncoder keyEncoder = new KeyEncoder(this.schema);
    for (int i = 0; i < this.schema.getKeysCount(); i++) {
      ColumnSchema column = this.schema.getColumn(i);
      if (column.getType() == Type.STRING) {
        byte[] string = this.strings.get(seenStrings);
        seenStrings++;
        keyEncoder.addKey(string, 0, string.length, column, i);
      } else {
        keyEncoder.addKey(this.rowAlloc, this.schema.getColumnOffset(i), column.getType().getSize(),
            column, i);
      }
    }
    // TODO we might want to cache the key
    return keyEncoder.extractByteArray();
  }

  /**
   * Helper method that puts a list of Operations together into a WriteRequestPB.
   * @param operations The list of ops to put together in a WriteRequestPB
   * @return A fully constructed WriteRequestPB containing the passed rows, or
   *         null if no rows were passed.
   */
  static Tserver.WriteRequestPB.Builder createAndFillWriteRequestPB(Operation... operations) {
    if (operations == null || operations.length == 0) return null;

    Schema schema = operations[0].schema;


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
    ByteBuffer rows = ByteBuffer.allocate(sizePerRow * operations.length)
        .order(ByteOrder.LITTLE_ENDIAN);
    ByteArrayOutputStream indirect = new ByteArrayOutputStream();

    for (Operation operation : operations) {
      assert operation.schema == schema;

      rows.put(operation.getChangeType().toEncodedByte());
      rows.put(Bytes.fromBitSet(operation.columnsBitSet, schema.getColumnCount()));
      if (schema.hasNullableColumns()) {
        rows.put(Bytes.fromBitSet(operation.nullsBitSet, schema.getColumnCount()));
      }
      int colIdx = 0;
      byte[] rowData = operation.rowAlloc;
      int currentRowOffset = 0;
      for (ColumnSchema col : operation.schema.getColumns()) {
        // Keys should always be specified, maybe check?
        if (operation.isSet(colIdx) && !operation.isSetToNull(colIdx)) {
          if (col.getType() == Type.STRING) {
            int stringIndex = (int)Bytes.getLong(rowData, currentRowOffset);
            byte[] string = operation.strings.get(stringIndex);
            assert string.length == Bytes.getLong(rowData, currentRowOffset + Longs.BYTES);
            rows.putLong(indirect.size());
            rows.putLong(string.length);
            try {
              indirect.write(string);
            } catch (IOException e) {
              throw new AssertionError(e); // cannot occur
            }
          } else {
            // This is for cols other than strings
            rows.put(rowData, currentRowOffset, col.getType().getSize());
          }
        }
        currentRowOffset += col.getType().getSize();
        colIdx++;
      }
    }

    // Actually build the protobuf.
    RowOperationsPB.Builder rowOpsBuilder = RowOperationsPB.newBuilder();

    // TODO: we could implement a ZeroCopy approach here by subclassing LiteralByteString.
    // We have ZeroCopyLiteralByteString, but that only supports an entire array. Here
    // we've only partially filled in rows.array(), so we have to make the extra copy.
    rows.limit(rows.position());
    rows.flip();
    rowOpsBuilder.setRows(ByteString.copyFrom(rows));
    if (indirect.size() > 0) {
      // TODO: same as above, we could avoid a copy here by using an implementation that allows
      // zero-copy on a slice of an array.
      rowOpsBuilder.setIndirectData(ZeroCopyLiteralByteString.wrap(indirect.toByteArray()));
    }

    Tserver.WriteRequestPB.Builder requestBuilder = Tserver.WriteRequestPB.newBuilder();
    requestBuilder.setSchema(ProtobufHelper.schemaToPb(schema));
    requestBuilder.setRowOperations(rowOpsBuilder);
    return requestBuilder;
  }
}
