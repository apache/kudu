// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.ColumnSchema;
import kudu.Schema;
import kudu.Type;
import kudu.WireProtocol;
import kudu.tserver.Tserver;
import kudu.util.Arena;
import kudu.util.Slice;
import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Base class for the RPCs that related to WriteRequestPB. It contains almost all the logic
 * and knows how to serialize its child classes.
 * Each Operation is backed by an Arena where all the cells (except strings) are written. The
 * strings are kept in a List.
 */
public abstract class Operation extends KuduRpc {

  enum ChangeType {
    INSERT, // in C++ this is "uninitialized" but we'll never send it so overload
    UPDATE,
    DELETE
  }

  final static String METHOD = "Write";
  final Schema schema;
  final List<Slice> strings;
  final int rowSize;
  final Arena arena;
  final byte[] rowAlloc;
  final int offset;
  final BitSet columnsBitSet;

  // The following attributes are used to speed up serialization
  int stringsSize;
  // This is used for update/delete that have strings in their key
  int keyOnlyStringSize;
  // Size of the RowChangeList. Starts at one for the ChangeType's ordinal, only used for update/delete.
  int encodedSize = 1;
  int partialRowSize = 0;

  /**
   * Package-private constructor. Subclasses need to be instantiated via KuduSession
   * @param table table with the schema to use for this operation
   */
  Operation(KuduTable table) {
    super(table);
    this.schema = table.getSchema();
    this.columnsBitSet = new BitSet(this.schema.getColumnCount());
    this.rowSize = schema.getRowSize();
    this.arena = new Arena();
    Arena.Allocation alloc = this.arena.allocateBytes(this.rowSize);
    this.rowAlloc = alloc.getData();
    this.offset = alloc.getOffset();
    strings = new ArrayList<Slice>(schema.getStringCount());
  }

  /**
   * Classes extending Operation need to have a specific ChangeType
   * @return Operation's ChangeType
   */
  abstract ChangeType getChangeType();

  /**
   * Add an unsigned byte for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addUnsignedByte(String columnName, short val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.UINT8);
    // TODO
  }

  /**
   * Add an unsigned short for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addUnsignedShort(String columnName, int val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.UINT16);
    // TODO
  }

  /**
   * Add an unsigned int for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addUnsignedInt(String columnName, long val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.UINT32);
    // TODO
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
   * Add a String for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addString(String columnName, String val) {
    ColumnSchema col = this.schema.getColumn(columnName);
    checkColumn(col, Type.STRING);
    int size = val.length();
    Arena.Allocation alloc = arena.allocateBytes(size);
    // TODO Should we even bother? Just keep the strings around?
    System.arraycopy(val.getBytes(), 0, alloc.getData(), alloc.getOffset(), size);
    this.strings.add(new Slice(alloc.getData(), alloc.getOffset(), size));
    this.stringsSize += size;
    // special accounting for string row keys in updates or deletes
    if (col.isKey()) {
      keyOnlyStringSize += size;
    } else {
      encodedSize += CodedOutputStream.computeRawVarint32Size(size);
      encodedSize += size;
    }
    // Set the bit and set the usage bit
    getPositionInRowAllocAndSetBitSet(col);
  }

  /**
   * Verifies if the column exists and belongs to the specified type
   * It also does some internal accounting
   * @param column column the user wants to set
   * @param type type we expect
   * @throws IllegalArgumentException if the column or type was invalid
   */
  void checkColumn(ColumnSchema column, Type type) {
    if (column == null)
      throw new IllegalArgumentException("Column name isn't present in the table's schema");
    if (!column.getType().equals(type))
      throw new IllegalArgumentException(column.getName() +
          "'s isn't " + type.getName() + ", it's " + column.getType().getName());
    if (!column.isKey()) {
      encodedSize++; // col id
      if (!column.getType().equals(Type.STRING)) {
        encodedSize += column.getType().getSize(); // strings have their own processing in addString
      }
    }
  }

  /**
   * Gives the column's location in the byte array and marks it as set
   * @param column column to get the position for and mark as set
   * @return the offset in rowAlloc for the column
   */
  int getPositionInRowAllocAndSetBitSet(ColumnSchema column) {
    int idx = schema.getColumns().indexOf(column);
    columnsBitSet.set(idx);
    partialRowSize += column.getType().getSize();
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

  @Override
  String method() {
    return METHOD;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Tserver.WriteRequestPB.Builder builder = createAndFillWriteRequestPB(this);
    builder.setTabletId(ByteString.copyFrom(getTablet().getBytes()));
    return toChannelBuffer(header, builder.build());
  }

  @Override
  Object deserialize(ChannelBuffer buf) {
    Tserver.WriteResponsePB.Builder builder = Tserver.WriteResponsePB.newBuilder();
    readProtobuf(buf, builder);
    return builder.build();
  }

  /**
   * Helper method that puts a list of Operations together into a WriteRequestPB.
   * @param operations The list of ops to put together in a WriteRequestPB
   * @return A fully constructed WriteRequestPB containing the passed rows, or
   *         null if no rows were passed.
   */
  static Tserver.WriteRequestPB.Builder createAndFillWriteRequestPB(Operation... operations) {
    if (operations == null || operations.length == 0) return null;

    int insertRowsSize = 0;
    int insertIndirectSize = 0;
    List<Operation> inserts = null;
    int deleteCount = 0;
    int updateCount = 0;
    int updateIndirectSize = 0;
    int updateEncodedDataSize = 0;
    Schema schema = operations[0].schema;
    int columnBitSetSize = getSizeOfColumnBitSet(schema.getColumnCount());
    // get the counts to right-size the buffers
    for (Operation operation : operations) {
      switch (operation.getChangeType()) {
        case INSERT:
          if (inserts == null) {
            inserts = new ArrayList<Operation>(operations.length / 3);
          }
          inserts.add(operation);
          insertRowsSize += operation.partialRowSize + columnBitSetSize;
          insertIndirectSize += operation.stringsSize;
          break;
        case UPDATE:
          updateCount++;
          updateEncodedDataSize += operation.encodedSize;
          updateIndirectSize += operation.keyOnlyStringSize;
          break;
        case DELETE:
          deleteCount++;
          updateEncodedDataSize += operation.encodedSize;
          updateIndirectSize += operation.keyOnlyStringSize;
          break;
        default:
          throw new IllegalArgumentException("This operation has an unexpected type: "
              + operation);
      }
    }
    int updatePlusDeleteCount = updateCount + deleteCount;
    boolean hasUpdateOrDelete = !(deleteCount == 0 && updateCount == 0);
    Tserver.WriteRequestPB.Builder requestBuilder = Tserver.WriteRequestPB.newBuilder();

    Schema keyProjection = schema.getRowKeyProjection();
    int keySize = keyProjection.getRowSize();
    ByteBuffer toUpdateRows = hasUpdateOrDelete ?
        ByteBuffer.allocate(updatePlusDeleteCount * keySize).order(ByteOrder.LITTLE_ENDIAN) : null;
    ByteBuffer encodedMutations = hasUpdateOrDelete ? ByteBuffer.allocate((updatePlusDeleteCount
        * 4) + updateEncodedDataSize).order(ByteOrder.LITTLE_ENDIAN) : null;
    ByteBuffer updateIndirectData = hasUpdateOrDelete ? ByteBuffer.allocate(updateIndirectSize)
        .order(ByteOrder.LITTLE_ENDIAN) : null;
    for (Operation operation : operations) {
      if (operation.getChangeType().equals(ChangeType.INSERT)) {
        continue;
      }

      // Set the size for this op in the encoded buffer plus the change type
      encodedMutations.putInt(operation.encodedSize);
      encodedMutations.put((byte) operation.getChangeType().ordinal());
      int currentRowOffset = operation.offset;
      byte[] rowData = operation.rowAlloc;
      int stringsIndex = 0;
      int colIdx = 0;
      for (ColumnSchema col : operation.schema.getColumns()) {
        // Keys should always be specified, maybe check?
        if (!operation.isSet(colIdx)) {
          // code also at the end of the for, just have a big if?
          currentRowOffset += col.getType().getSize();
          colIdx++;
          continue;
        }

        ByteBuffer specificBuffer = col.isKey() ? toUpdateRows : encodedMutations;
        if (!col.isKey()) {
          Bytes.putVarInt32(specificBuffer, colIdx);
        }

        if (col.getType() == Type.STRING) {
          Slice slice = operation.strings.get(stringsIndex);
          if (col.isKey()) {
            updateIndirectData.put(slice.getRawArray(), slice.getRawOffset(), slice.length());
            specificBuffer.putLong(updateIndirectData.position() - slice.length());
            specificBuffer.putLong(slice.length());
          } else {
            // for an non-key update we directly put the string in the data buffer
            Bytes.putVarInt32(specificBuffer, slice.length());
            specificBuffer.put(slice.getRawArray(), slice.getRawOffset(), slice.length());
          }
          stringsIndex++;
        } else {
          // This is for cols other than strings
          specificBuffer.put(rowData, currentRowOffset, col.getType().getSize());
        }
        currentRowOffset += col.getType().getSize();
        colIdx++;
      }
    }

    if (inserts != null) {
      requestBuilder.setToInsertRows(
          createPartialRowsPB(inserts, insertRowsSize, insertIndirectSize));
      requestBuilder.setSchema(ProtobufHelper.schemaToPb(schema));
    }
    if (hasUpdateOrDelete) {
      assert !toUpdateRows.hasRemaining();
      assert !encodedMutations.hasRemaining();
      assert !updateIndirectData.hasRemaining();
      WireProtocol.RowwiseRowBlockPB.Builder updateRowsBuilder =
          WireProtocol.RowwiseRowBlockPB.newBuilder();
      updateRowsBuilder.setNumRows(updatePlusDeleteCount);
      updateRowsBuilder.addAllSchema(ProtobufHelper.schemaToListPb(schema));
      updateRowsBuilder.setNumKeyColumns(schema.getKeysCount());
      updateRowsBuilder.setRows(ZeroCopyLiteralByteString.wrap(toUpdateRows.array()));
      updateRowsBuilder.setIndirectData(ZeroCopyLiteralByteString.wrap(updateIndirectData.array()));
      requestBuilder.setToMutateRowKeys(updateRowsBuilder.build());
      requestBuilder.setEncodedMutations(ZeroCopyLiteralByteString.wrap(encodedMutations.array()));
    }
    return requestBuilder;
  }

  // TODO replace the previous with this one once partial rows are in and remove the comments
  static Tserver.WriteRequestPB.Builder createAndFillWriteRequestPBPartialRows(Operation...
                                                                                   operations) {
    if (operations == null || operations.length == 0) return null;

    Schema schema = operations[0].schema;
    Tserver.WriteRequestPB.Builder requestBuilder = Tserver.WriteRequestPB.newBuilder();

    List<Operation> inserts = null;
    List<Operation> updates = null;
    List<Operation> deletes = null;
    int insertRowsSize = 0;
    int updateRowsSize = 0;
    int deleteRowsSize = 0;
    int insertIndirectSize = 0;
    int updateIndirectSize = 0;
    int deleteIndirectSize = 0;
    // get the counts to right-size the buffers
    for (Operation operation : operations) {
      switch (operation.getChangeType()) {
        case INSERT:
          if (inserts == null) {
            inserts = new ArrayList<Operation>(operations.length / 3);
          }
          inserts.add(operation);
          insertRowsSize += operation.partialRowSize;
          insertIndirectSize += operation.stringsSize;
          break;
        case UPDATE:
          if (updates == null) {
            updates = new ArrayList<Operation>(operations.length / 3);
          }
          updates.add(operation);
          updateRowsSize += operation.partialRowSize;
          updateIndirectSize += operation.stringsSize;
          break;
        case DELETE:
          if (deletes == null) {
            deletes = new ArrayList<Operation>(operations.length / 3);
          }
          deletes.add(operation);
          deleteRowsSize += operation.partialRowSize;
          deleteIndirectSize += operation.stringsSize;
          break;
        default:
          throw new IllegalArgumentException("This operation has an unexpected type: "
              + operation);
      }
    }
    // builder.setSchema(ProtobufHelper.schemaToPb(schema);
    if (inserts != null) {
      // builder.setToInsertRows(createPartialRowsPB(inserts, insertRowsSize, insertIndirectSize);
    }
    if (updates != null) {
      // builder.setToUpdateRows(createPartialRowsPB(updates, updateRowsSize, updateIndirectSize);
    }
    if (deletes != null) {
      // builder.setToDeleteRows(createPartialRowsPB(delete, deleteRowsSize, deleteIndirectSize);
    }
    return requestBuilder;
  }

  static WireProtocol.PartialRowsPB createPartialRowsPB(List<Operation> operations,
                                                        int rowSize, int indirectSize) {
    WireProtocol.PartialRowsPB.Builder builder = WireProtocol.PartialRowsPB.newBuilder();
    ByteBuffer rows = ByteBuffer.allocate(rowSize).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer indirectData = indirectSize == 0 ?
        null : ByteBuffer.allocate(indirectSize).order(ByteOrder.LITTLE_ENDIAN);
    for (Operation operation : operations) {
      rows.put(toByteArray(operation.columnsBitSet));
      // TODO handle schemas with nulls
      int colIdx = 0;
      byte[] rowData = operation.rowAlloc;
      int stringsIndex = 0;
      int currentRowOffset = operation.offset;
      for (ColumnSchema col : operation.schema.getColumns()) {
        // Keys should always be specified, maybe check?
        if (operation.isSet(colIdx)) {
          if (col.getType() == Type.STRING) {
            Slice slice = operation.strings.get(stringsIndex);
            rows.putLong(indirectData.position());
            indirectData.put(slice.getRawArray(), slice.getRawOffset(), slice.length());
            rows.putLong(slice.length());
            stringsIndex++;
          } else {
            // This is for cols other than strings
            rows.put(rowData, currentRowOffset, col.getType().getSize());
          }
        }
        currentRowOffset += col.getType().getSize();
        colIdx++;
      }
    }
    assert !rows.hasRemaining();
    builder.setRows(ZeroCopyLiteralByteString.wrap(rows.array()));
    if (indirectData != null) {
      assert !indirectData.hasRemaining();
      builder.setIndirectData(ZeroCopyLiteralByteString.wrap(indirectData.array()));
    }
    return builder.build();
  }

  static int getSizeOfColumnBitSet(int count) {
    return (count + 7) / 8;
  }

  static byte[] toByteArray(BitSet bits) {
    byte[] bytes = new byte[getSizeOfColumnBitSet(bits.length())];
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i)) {
        bytes[i / 8] |= 1 << (i % 8);
      }
    }
    return bytes;
  }
}
