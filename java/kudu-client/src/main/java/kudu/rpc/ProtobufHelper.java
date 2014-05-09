// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.ColumnSchema;
import kudu.Common;
import kudu.Schema;
import kudu.Type;

import java.util.ArrayList;
import java.util.List;

public class ProtobufHelper {

  public static String getShortTextFormat(Message m) {
    return "TODO: " + m.getClass().toString();
  }

  /**
   * TODO
   * @param schema
   * @return
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
      ColumnSchema column = new ColumnSchema(columnPb.getName(), Type.getTypeForDataType(columnPb
          .getType()), columnPb.getIsKey(), columnPb.getIsNullable(), null);
      columns.add(column);
    }
    return new Schema(columns);
  }

  private static byte[] objectToWireFormat(ColumnSchema col, Object value) {
    // TODO just like in Operation, we don't handle unsigned ints
    switch (col.getType()) {
      case INT8:
      case UINT8:
        return new byte[] { ((Byte)value).byteValue() };
      case INT16:
      case UINT16:
        return Bytes.fromShort((Short)value);
      case INT32:
      case UINT32:
        return Bytes.fromInt((Integer) value);
      case INT64:
      case UINT64:
        return Bytes.fromLong((Long) value);
      case STRING:
        return ((String)value).getBytes();
      default:
        throw new IllegalArgumentException("The column " + col.getName() + " is of type " + col
            .getType() + " which is unknown");
    }
  }
}
