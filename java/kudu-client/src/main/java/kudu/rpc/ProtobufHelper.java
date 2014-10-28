// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.ColumnSchema;
import kudu.Common;
import kudu.Schema;
import kudu.Type;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ProtobufHelper {

  /**
   * Utility method to convert a Schema to its wire format.
   * @param schema Schema to convert
   * @return a list of ColumnSchemaPB
   */
  public static List<Common.ColumnSchemaPB> schemaToListPb(Schema schema) {
    ArrayList<Common.ColumnSchemaPB> columns =
        new ArrayList<Common.ColumnSchemaPB>(schema.getColumnCount());
    Common.ColumnSchemaPB.Builder schemaBuilder = Common.ColumnSchemaPB.newBuilder();
    for (ColumnSchema col : schema.getColumns()) {
      columns.add(columnToPb(schemaBuilder, col));
      schemaBuilder.clear();
    }
    return columns;
  }

  public static Common.SchemaPB schemaToPb(Schema schema) {
    Common.SchemaPB.Builder builder = Common.SchemaPB.newBuilder();
    builder.addAllColumns(schemaToListPb(schema));
    return builder.build();
  }

  public static Common.ColumnSchemaPB columnToPb(ColumnSchema column) {
    return columnToPb(Common.ColumnSchemaPB.newBuilder(), column);
  }

  public static Common.ColumnSchemaPB
  columnToPb(Common.ColumnSchemaPB.Builder schemaBuilder, ColumnSchema column) {
    schemaBuilder.setName(column.getName()).
        setType(column.getType().getDataType()).setIsKey(column.isKey()).setIsNullable(column
        .isNullable());
    if (column.getDefaultValue() != null) schemaBuilder.setReadDefaultValue
        (ZeroCopyLiteralByteString.wrap(objectToWireFormat(column, column.getDefaultValue())));
    return schemaBuilder.build();
  }

  public static Schema pbToSchema(Common.SchemaPB schema) {
    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(schema.getColumnsCount());
    for (Common.ColumnSchemaPB columnPb : schema.getColumnsList()) {
      Type type = Type.getTypeForDataType(columnPb.getType());
      Object defaultValue = columnPb.hasReadDefaultValue() ? byteStringToObject(type,
          columnPb.getReadDefaultValue()) : null;
      ColumnSchema column = new ColumnSchema(columnPb.getName(), type, columnPb.getIsKey(),
          columnPb.getIsNullable(), defaultValue);
      columns.add(column);
    }
    return new Schema(columns);
  }

  private static byte[] objectToWireFormat(ColumnSchema col, Object value) {
    switch (col.getType()) {
      case INT8:
        return new byte[] { ((Byte)value).byteValue() };
      case UINT8:
        return Bytes.fromUnsignedByte((Short) value);
      case INT16:
        return Bytes.fromShort((Short)value);
      case UINT16:
        return Bytes.fromUnsignedShort((Integer) value);
      case INT32:
        return Bytes.fromInt((Integer) value);
      case UINT32:
        return Bytes.fromUnsignedInt((Long) value);
      case INT64:
        return Bytes.fromLong((Long) value);
      case UINT64:
        return Bytes.fromUnsignedLong((BigInteger) value);
      case STRING:
        return ((String)value).getBytes(Charset.forName("UTF-8"));
      default:
        throw new IllegalArgumentException("The column " + col.getName() + " is of type " + col
            .getType() + " which is unknown");
    }
  }

  private static Object byteStringToObject(Type type, ByteString value) {
    byte[] buf = ZeroCopyLiteralByteString.zeroCopyGetBytes(value);
    switch (type) {
      case INT8:
        return Bytes.getByte(buf);
      case UINT8:
        return Bytes.getUnsignedByte(buf);
      case INT16:
        return Bytes.getShort(buf);
      case UINT16:
        return Bytes.getUnsignedShort(buf);
      case INT32:
        return Bytes.getInt(buf);
      case UINT32:
        return Bytes.getUnsignedInt(buf);
      case INT64:
        return Bytes.getLong(buf);
      case UINT64:
        return Bytes.getUnsignedLong(buf);
      case STRING:
        return new String(buf, Charset.forName("UTF-8"));
      default:
        throw new IllegalArgumentException("This type is unknown: " + type);
    }
  }
}
