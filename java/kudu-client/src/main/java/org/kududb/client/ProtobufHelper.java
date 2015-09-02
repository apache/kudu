// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import org.kududb.ColumnSchema;
import org.kududb.Common;
import org.kududb.Schema;
import org.kududb.Type;

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
    schemaBuilder
        .setName(column.getName())
        .setType(column.getType().getDataType())
        .setIsKey(column.isKey())
        .setIsNullable(column.isNullable())
        .setCfileBlockSize(column.getDesiredBlockSize());
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
      ColumnSchema column = new ColumnSchema.ColumnSchemaBuilder(columnPb.getName(), type)
          .key(columnPb.getIsKey())
          .nullable(columnPb.getIsNullable())
          .defaultValue(defaultValue)
          .build();
      columns.add(column);
    }
    return new Schema(columns);
  }

  private static byte[] objectToWireFormat(ColumnSchema col, Object value) {
    switch (col.getType()) {
      case BOOL:
        return Bytes.fromBoolean((Boolean) value);
      case INT8:
        return new byte[] { ((Byte)value).byteValue() };
      case INT16:
        return Bytes.fromShort((Short)value);
      case INT32:
        return Bytes.fromInt((Integer) value);
      case INT64:
      case TIMESTAMP:
        return Bytes.fromLong((Long) value);
      case STRING:
        return ((String)value).getBytes(Charset.forName("UTF-8"));
      case BINARY:
        return (byte[]) value;
      case FLOAT:
        return Bytes.fromFloat((Float)value);
      case DOUBLE:
        return Bytes.fromDouble((Double)value);
      default:
        throw new IllegalArgumentException("The column " + col.getName() + " is of type " + col
            .getType() + " which is unknown");
    }
  }

  private static Object byteStringToObject(Type type, ByteString value) {
    byte[] buf = ZeroCopyLiteralByteString.zeroCopyGetBytes(value);
    switch (type) {
      case BOOL:
        return Bytes.getBoolean(buf);
      case INT8:
        return Bytes.getByte(buf);
      case INT16:
        return Bytes.getShort(buf);
      case INT32:
        return Bytes.getInt(buf);
      case INT64:
      case TIMESTAMP:
        return Bytes.getLong(buf);
      case FLOAT:
        return Bytes.getFloat(buf);
      case DOUBLE:
        return Bytes.getDouble(buf);
      case STRING:
        return new String(buf, Charset.forName("UTF-8"));
      case BINARY:
        return buf;
      default:
        throw new IllegalArgumentException("This type is unknown: " + type);
    }
  }

  /**
   * Convert a {@link com.google.common.net.HostAndPort} to {@link org.kududb.Common.HostPortPB}
   * protobuf message for serialization.
   * @param hostAndPort The host and port object. Both host and port must be specified.
   * @return An initialized HostPortPB object.
   */
  public static Common.HostPortPB hostAndPortToPB(HostAndPort hostAndPort) {
    return Common.HostPortPB.newBuilder()
        .setHost(hostAndPort.getHostText())
        .setPort(hostAndPort.getPort())
        .build();
  }

  /**
   * Convert a {@link org.kududb.Common.HostPortPB} to {@link com.google.common.net.HostAndPort}.
   * @param hostPortPB The fully initialized HostPortPB object. Must have both host and port
   *                   specified.
   * @return An initialized initialized HostAndPort object.
   */
  public static HostAndPort hostAndPortFromPB(Common.HostPortPB hostPortPB) {
    return HostAndPort.fromParts(hostPortPB.getHost(), hostPortPB.getPort());
  }
}
