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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.DecimalUtil;

@InterfaceAudience.Private
public class ProtobufHelper {

  /**
   * The flags that are not included while serializing.
   */
  public enum SchemaPBConversionFlags {
    SCHEMA_PB_WITHOUT_COMMENT,
    SCHEMA_PB_WITHOUT_ID
  }

  /**
   * Utility method to convert a Schema to its wire format.
   * @param schema Schema to convert
   * @return a list of ColumnSchemaPB
   */
  public static List<Common.ColumnSchemaPB> schemaToListPb(Schema schema) {
    return schemaToListPb(schema, EnumSet.noneOf(SchemaPBConversionFlags.class));
  }

  public static List<Common.ColumnSchemaPB> schemaToListPb(Schema schema,
                                                           EnumSet<SchemaPBConversionFlags> flags) {
    ArrayList<Common.ColumnSchemaPB> columns = new ArrayList<>(schema.getColumnCount());
    Common.ColumnSchemaPB.Builder schemaBuilder = Common.ColumnSchemaPB.newBuilder();
    for (ColumnSchema col : schema.getColumns()) {
      int id = schema.hasColumnIds() ? schema.getColumnId(col.getName()) : -1;
      columns.add(columnToPb(schemaBuilder, id, col, flags));
      schemaBuilder.clear();
    }
    return columns;
  }

  public static Common.SchemaPB schemaToPb(Schema schema) {
    return schemaToPb(schema, EnumSet.noneOf(SchemaPBConversionFlags.class));
  }

  public static Common.SchemaPB schemaToPb(Schema schema,
                                           EnumSet<SchemaPBConversionFlags> flags) {
    Common.SchemaPB.Builder builder = Common.SchemaPB.newBuilder();
    builder.addAllColumns(schemaToListPb(schema, flags));
    return builder.build();
  }

  public static Common.ColumnSchemaPB columnToPb(ColumnSchema column) {
    return columnToPb(Common.ColumnSchemaPB.newBuilder(), -1, column);
  }

  public static Common.ColumnSchemaPB columnToPb(Common.ColumnSchemaPB.Builder schemaBuilder,
                                                 int colId,
                                                 ColumnSchema column) {
    return columnToPb(schemaBuilder,
                      colId,
                      column,
                      EnumSet.noneOf(SchemaPBConversionFlags.class));
  }

  public static Common.ColumnSchemaPB columnToPb(Common.ColumnSchemaPB.Builder schemaBuilder,
                                                 int colId,
                                                 ColumnSchema column,
                                                 EnumSet<SchemaPBConversionFlags> flags) {
    schemaBuilder
        .setName(column.getName())
        .setType(column.getWireType())
        .setIsKey(column.isKey())
        .setIsNullable(column.isNullable())
        .setCfileBlockSize(column.getDesiredBlockSize());

    if (!flags.contains(SchemaPBConversionFlags.SCHEMA_PB_WITHOUT_ID) && colId >= 0) {
      schemaBuilder.setId(colId);
    }
    if (column.getEncoding() != null) {
      schemaBuilder.setEncoding(column.getEncoding().getInternalPbType());
    }
    if (column.getCompressionAlgorithm() != null) {
      schemaBuilder.setCompression(column.getCompressionAlgorithm().getInternalPbType());
    }
    if (column.getDefaultValue() != null) {
      schemaBuilder.setReadDefaultValue(UnsafeByteOperations.unsafeWrap(
          objectToWireFormat(column, column.getDefaultValue())));
    }
    if(column.getTypeAttributes() != null) {
      schemaBuilder.setTypeAttributes(
          columnTypeAttributesToPb(Common.ColumnTypeAttributesPB.newBuilder(), column));
    }
    if (!flags.contains(SchemaPBConversionFlags.SCHEMA_PB_WITHOUT_COMMENT) &&
        !column.getComment().isEmpty()) {
      schemaBuilder.setComment(column.getComment());
    }
    return schemaBuilder.build();
  }

  public static Common.ColumnTypeAttributesPB columnTypeAttributesToPb(
      Common.ColumnTypeAttributesPB.Builder builder, ColumnSchema column) {
    ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
    if (typeAttributes.hasPrecision()) {
      builder.setPrecision(typeAttributes.getPrecision());
    }
    if (typeAttributes.hasScale()) {
      builder.setScale(typeAttributes.getScale());
    }
    return builder.build();
  }

  public static ColumnSchema pbToColumnSchema(Common.ColumnSchemaPB pb) {
    Type type = Type.getTypeForDataType(pb.getType());
    ColumnTypeAttributes typeAttributes = pb.hasTypeAttributes() ?
        pbToColumnTypeAttributes(pb.getTypeAttributes()) : null;
    Object defaultValue = pb.hasWriteDefaultValue() ?
        byteStringToObject(type, typeAttributes, pb.getWriteDefaultValue()) : null;
    ColumnSchema.Encoding encoding = ColumnSchema.Encoding.valueOf(pb.getEncoding().name());
    ColumnSchema.CompressionAlgorithm compressionAlgorithm =
        ColumnSchema.CompressionAlgorithm.valueOf(pb.getCompression().name());
    int desiredBlockSize = pb.getCfileBlockSize();
    return new ColumnSchema.ColumnSchemaBuilder(pb.getName(), type)
                           .key(pb.getIsKey())
                           .nullable(pb.getIsNullable())
                           .defaultValue(defaultValue)
                           .encoding(encoding)
                           .compressionAlgorithm(compressionAlgorithm)
                           .desiredBlockSize(desiredBlockSize)
                           .typeAttributes(typeAttributes)
                           .comment(pb.getComment())
                           .build();
  }

  public static ColumnTypeAttributes pbToColumnTypeAttributes(Common.ColumnTypeAttributesPB pb) {
    ColumnTypeAttributes.ColumnTypeAttributesBuilder builder =
        new ColumnTypeAttributes.ColumnTypeAttributesBuilder();
    if(pb.hasPrecision()) {
      builder.precision(pb.getPrecision());
    }
    if(pb.hasScale()) {
      builder.scale(pb.getScale());
    }
    return builder.build();
  }

  public static Schema pbToSchema(Common.SchemaPB schema) {
    List<ColumnSchema> columns = new ArrayList<>(schema.getColumnsCount());
    List<Integer> columnIds = new ArrayList<>(schema.getColumnsCount());
    for (Common.ColumnSchemaPB columnPb : schema.getColumnsList()) {
      columns.add(pbToColumnSchema(columnPb));
      int id = columnPb.getId();
      if (id < 0) {
        throw new IllegalArgumentException("Illegal column ID: " + id);
      }
      columnIds.add(id);
    }
    return new Schema(columns, columnIds);
  }

  /**
   * Factory method for creating a {@code PartitionSchema} from a protobuf message.
   *
   * @param pb the partition schema protobuf message
   * @return a partition instance
   */
  static PartitionSchema pbToPartitionSchema(Common.PartitionSchemaPB pb, Schema schema) {
    List<Integer> rangeColumns = pbToIds(pb.getRangeSchema().getColumnsList());
    PartitionSchema.RangeSchema rangeSchema = new PartitionSchema.RangeSchema(rangeColumns);

    ImmutableList.Builder<PartitionSchema.HashBucketSchema> hashSchemas = ImmutableList.builder();

    for (Common.PartitionSchemaPB.HashBucketSchemaPB hashBucketSchemaPB
        : pb.getHashBucketSchemasList()) {
      List<Integer> hashColumnIds = pbToIds(hashBucketSchemaPB.getColumnsList());

      PartitionSchema.HashBucketSchema hashSchema =
          new PartitionSchema.HashBucketSchema(hashColumnIds,
                                               hashBucketSchemaPB.getNumBuckets(),
                                               hashBucketSchemaPB.getSeed());

      hashSchemas.add(hashSchema);
    }

    return new PartitionSchema(rangeSchema, hashSchemas.build(), schema);
  }

  /**
   * Constructs a new {@code Partition} instance from the a protobuf message.
   * @param pb the protobuf message
   * @return the {@code Partition} corresponding to the message
   */
  static Partition pbToPartition(Common.PartitionPB pb) {
    return new Partition(pb.getPartitionKeyStart().toByteArray(),
                         pb.getPartitionKeyEnd().toByteArray(),
                         pb.getHashBucketsList());
  }

  /**
   * Deserializes a list of column identifier protobufs into a list of column IDs. This method
   * relies on the fact that the master will aways send a partition schema with column IDs, and not
   * column names (column names are only used when the client is sending the partition schema to
   * the master as part of the create table process).
   *
   * @param columnIdentifiers the column identifiers
   * @return the column IDs
   */
  private static List<Integer> pbToIds(
      List<Common.PartitionSchemaPB.ColumnIdentifierPB> columnIdentifiers) {
    ImmutableList.Builder<Integer> columnIds = ImmutableList.builder();
    for (Common.PartitionSchemaPB.ColumnIdentifierPB column : columnIdentifiers) {
      switch (column.getIdentifierCase()) {
        case ID:
          columnIds.add(column.getId());
          break;
        case NAME:
          throw new IllegalArgumentException(
              String.format("Expected column ID from master: %s", column));
        case IDENTIFIER_NOT_SET:
          throw new IllegalArgumentException("Unknown column: " + column);
        default:
          throw new IllegalArgumentException("Unknown identifier type!");
      }
    }
    return columnIds.build();
  }

  private static byte[] objectToWireFormat(ColumnSchema col, Object value) {
    switch (col.getType()) {
      case BOOL:
        return Bytes.fromBoolean((Boolean) value);
      case INT8:
        return new byte[] {(Byte) value};
      case INT16:
        return Bytes.fromShort((Short) value);
      case INT32:
        return Bytes.fromInt((Integer) value);
      case INT64:
      case UNIXTIME_MICROS:
        return Bytes.fromLong((Long) value);
      case STRING:
        return ((String) value).getBytes(UTF_8);
      case BINARY:
        return (byte[]) value;
      case FLOAT:
        return Bytes.fromFloat((Float) value);
      case DOUBLE:
        return Bytes.fromDouble((Double) value);
      case DECIMAL:
        return Bytes.fromBigDecimal((BigDecimal) value, col.getTypeAttributes().getPrecision());
      default:
        throw new IllegalArgumentException("The column " + col.getName() + " is of type " + col
            .getType() + " which is unknown");
    }
  }

  private static Object byteStringToObject(Type type, ColumnTypeAttributes typeAttributes,
                                           ByteString value) {
    ByteBuffer buf = value.asReadOnlyByteBuffer();
    buf.order(ByteOrder.LITTLE_ENDIAN);
    switch (type) {
      case BOOL:
        return buf.get() != 0;
      case INT8:
        return buf.get();
      case INT16:
        return buf.getShort();
      case INT32:
        return buf.getInt();
      case INT64:
      case UNIXTIME_MICROS:
        return buf.getLong();
      case FLOAT:
        return buf.getFloat();
      case DOUBLE:
        return buf.getDouble();
      case STRING:
        return value.toStringUtf8();
      case BINARY:
        return value.toByteArray();
      case DECIMAL:
        return Bytes.getDecimal(value.toByteArray(),
            typeAttributes.getPrecision(), typeAttributes.getScale());
      default:
        throw new IllegalArgumentException("This type is unknown: " + type);
    }
  }

  /**
   * Serializes an object based on its Java type. Used for Alter Column
   * operations where the column's type is not available. `value` must be
   * a Kudu-compatible type or else throws {@link IllegalArgumentException}.
   *
   * @param colName the name of the column (for the error message)
   * @param value the value to serialize
   * @return the serialized object
   */
  protected static ByteString objectToByteStringNoType(String colName, Object value) {
    byte[] bytes;
    if (value instanceof Boolean) {
      bytes = Bytes.fromBoolean((Boolean) value);
    } else if (value instanceof Byte) {
      bytes = new byte[] {(Byte) value};
    } else if (value instanceof Short) {
      bytes = Bytes.fromShort((Short) value);
    } else if (value instanceof Integer) {
      bytes = Bytes.fromInt((Integer) value);
    } else if (value instanceof Long) {
      bytes = Bytes.fromLong((Long) value);
    } else if (value instanceof String) {
      bytes = ((String) value).getBytes(UTF_8);
    } else if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else if (value instanceof ByteBuffer) {
      bytes = ((ByteBuffer) value).array();
    } else if (value instanceof Float) {
      bytes = Bytes.fromFloat((Float) value);
    } else if (value instanceof Double) {
      bytes = Bytes.fromDouble((Double) value);
    } else if (value instanceof BigDecimal) {
      bytes = Bytes.fromBigDecimal((BigDecimal) value, DecimalUtil.MAX_DECIMAL_PRECISION);
    } else {
      throw new IllegalArgumentException("The default value provided for " +
          "column " + colName + " is of class " + value.getClass().getName() +
          " which does not map to a supported Kudu type");
    }
    return UnsafeByteOperations.unsafeWrap(bytes);
  }

  /**
   * Convert a {@link HostAndPort} to
   *     {@link org.apache.kudu.Common.HostPortPB}
   * protobuf message for serialization.
   * @param hostAndPort The host and port object. Both host and port must be specified.
   * @return An initialized HostPortPB object.
   */
  public static Common.HostPortPB hostAndPortToPB(HostAndPort hostAndPort) {
    return Common.HostPortPB.newBuilder()
        .setHost(hostAndPort.getHost())
        .setPort(hostAndPort.getPort())
        .build();
  }

  /**
   * Convert a {@link org.apache.kudu.Common.HostPortPB} to
   *     {@link HostAndPort}.
   * @param hostPortPB The fully initialized HostPortPB object.
   *                   Must have both host and port specified.
   * @return An initialized initialized HostAndPort object.
   */
  public static HostAndPort hostAndPortFromPB(Common.HostPortPB hostPortPB) {
    return new HostAndPort(hostPortPB.getHost(), hostPortPB.getPort());
  }

  /**
   * Convert a list of HostPortPBs into a comma-separated string.
   */
  public static String hostPortPbListToString(List<Common.HostPortPB> pbs) {
    List<String> strs = new ArrayList<>(pbs.size());
    for (Common.HostPortPB pb : pbs) {
      strs.add(pb.getHost() + ":" + pb.getPort());
    }
    return Joiner.on(',').join(strs);
  }
}
