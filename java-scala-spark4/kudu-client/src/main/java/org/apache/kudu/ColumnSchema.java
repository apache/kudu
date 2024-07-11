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

package org.apache.kudu;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Common.EncodingType;
import org.apache.kudu.Compression.CompressionType;
import org.apache.kudu.util.CharUtil;

/**
 * Represents a Kudu Table column. Use {@link ColumnSchema.ColumnSchemaBuilder} in order to
 * create columns.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ColumnSchema {

  private final String name;
  private final Type type;
  private final boolean key;
  private final boolean keyUnique;
  private final boolean nullable;
  private final boolean immutable;
  private final boolean autoIncrementing;
  private final Object defaultValue;
  private final int desiredBlockSize;
  private final Encoding encoding;
  private final CompressionAlgorithm compressionAlgorithm;
  private final ColumnTypeAttributes typeAttributes;
  private final int typeSize;
  private final Common.DataType wireType;
  private final String comment;

  /**
   * Specifies the encoding of data for a column on disk.
   * Not all encodings are available for all data types.
   * Refer to the Kudu documentation for more information on each encoding.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum Encoding {
    UNKNOWN(EncodingType.UNKNOWN_ENCODING),
    AUTO_ENCODING(EncodingType.AUTO_ENCODING),
    PLAIN_ENCODING(EncodingType.PLAIN_ENCODING),
    PREFIX_ENCODING(EncodingType.PREFIX_ENCODING),
    GROUP_VARINT(EncodingType.GROUP_VARINT),
    RLE(EncodingType.RLE),
    DICT_ENCODING(EncodingType.DICT_ENCODING),
    BIT_SHUFFLE(EncodingType.BIT_SHUFFLE);

    final EncodingType internalPbType;

    Encoding(EncodingType internalPbType) {
      this.internalPbType = internalPbType;
    }

    @InterfaceAudience.Private
    public EncodingType getInternalPbType() {
      return internalPbType;
    }
  }

  /**
   * Specifies the compression algorithm of data for a column on disk.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum CompressionAlgorithm {
    UNKNOWN(CompressionType.UNKNOWN_COMPRESSION),
    DEFAULT_COMPRESSION(CompressionType.DEFAULT_COMPRESSION),
    NO_COMPRESSION(CompressionType.NO_COMPRESSION),
    SNAPPY(CompressionType.SNAPPY),
    LZ4(CompressionType.LZ4),
    ZLIB(CompressionType.ZLIB);

    final CompressionType internalPbType;

    CompressionAlgorithm(CompressionType internalPbType) {
      this.internalPbType = internalPbType;
    }

    @InterfaceAudience.Private
    public CompressionType getInternalPbType() {
      return internalPbType;
    }
  }

  private ColumnSchema(String name, Type type, boolean key, boolean keyUnique,
                       boolean nullable, boolean immutable, boolean autoIncrementing,
                       Object defaultValue, int desiredBlockSize, Encoding encoding,
                       CompressionAlgorithm compressionAlgorithm,
                       ColumnTypeAttributes typeAttributes, Common.DataType wireType,
                       String comment) {
    this.name = name;
    this.type = type;
    this.key = key;
    this.keyUnique = keyUnique;
    this.nullable = nullable;
    this.immutable = immutable;
    this.autoIncrementing = autoIncrementing;
    this.defaultValue = defaultValue;
    this.desiredBlockSize = desiredBlockSize;
    this.encoding = encoding;
    this.compressionAlgorithm = compressionAlgorithm;
    this.typeAttributes = typeAttributes;
    this.typeSize = type.getSize(typeAttributes);
    this.wireType = wireType;
    this.comment = comment;
  }

  /**
   * Get the column's Type
   * @return the type
   */
  public Type getType() {
    return type;
  }

  /**
   * Get the column's name
   * @return A string representation of the name
   */
  public String getName() {
    return name;
  }

  /**
   * Answers if the column part of the key
   * @return true if the column is part of the key, else false
   */
  public boolean isKey() {
    return key;
  }

  /**
   * Answers if the key is unique
   * @return true if the key is unique
   */
  public boolean isKeyUnique() {
    return keyUnique;
  }

  /**
   * Answers if the column can be set to null
   * @return true if it can be set to null, else false
   */
  public boolean isNullable() {
    return nullable;
  }

  /**
   * Answers if the column is immutable
   * @return true if it is immutable, else false
   */
  public boolean isImmutable() {
    return immutable;
  }

  /**
   * Answers if the column is auto-incrementing column
   * @return true if the column value is automatically assigned with incrementing value
   */
  public boolean isAutoIncrementing() {
    return autoIncrementing;
  }

  /**
   * The Java object representation of the default value that's read
   * @return the default read value
   */
  public Object getDefaultValue() {
    return defaultValue;
  }

  /**
   * Gets the desired block size for this column.
   * If no block size has been explicitly specified for this column,
   * returns 0 to indicate that the server-side default will be used.
   *
   * @return the block size, in bytes, or 0 if none has been configured.
   */
  public int getDesiredBlockSize() {
    return desiredBlockSize;
  }

  /**
   * Return the encoding of this column, or null if it is not known.
   */
  public Encoding getEncoding() {
    return encoding;
  }

  /**
   * Return the compression algorithm of this column, or null if it is not known.
   */
  public CompressionAlgorithm getCompressionAlgorithm() {
    return compressionAlgorithm;
  }

  /**
   * Return the column type attributes for the column, or null if it is not known.
   */
  public ColumnTypeAttributes getTypeAttributes() {
    return typeAttributes;
  }

  /**
   * Get the column's underlying DataType.
   */
  @InterfaceAudience.Private
  public Common.DataType getWireType() {
    return wireType;
  }

  /**
   * The size of this type in bytes on the wire.
   * @return A size
   */
  public int getTypeSize() {
    return typeSize;
  }

  /**
   * Return the comment for the column. An empty string means there is no comment.
   */
  public String getComment() {
    return comment;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnSchema)) {
      return false;
    }
    ColumnSchema that = (ColumnSchema) o;
    return Objects.equals(name, that.name) &&
        Objects.equals(type, that.type) &&
        Objects.equals(key, that.key) &&
        Objects.equals(keyUnique, that.keyUnique) &&
        Objects.equals(autoIncrementing, that.autoIncrementing) &&
        Objects.equals(typeAttributes, that.typeAttributes) &&
        Objects.equals(comment, that.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, key, typeAttributes, comment);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Column name: ");
    sb.append(name);
    sb.append(", type: ");
    sb.append(type.getName());
    if (typeAttributes != null) {
      sb.append(typeAttributes.toStringForType(type));
    }
    if (!comment.isEmpty()) {
      sb.append(", comment: ");
      sb.append(comment);
    }
    return sb.toString();
  }

  /**
   * Builder for ColumnSchema.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class ColumnSchemaBuilder {
    private static final List<Type> TYPES_WITH_ATTRIBUTES = Arrays.asList(Type.DECIMAL,
                                                                         Type.VARCHAR);
    private final String name;
    private final Type type;
    private boolean key = false;
    private boolean keyUnique = false;
    private boolean nullable = false;
    private boolean immutable = false;
    private Object defaultValue = null;
    private int desiredBlockSize = 0;
    private Encoding encoding = null;
    private CompressionAlgorithm compressionAlgorithm = null;
    private ColumnTypeAttributes typeAttributes = null;
    private Common.DataType wireType = null;
    private String comment = "";

    /**
     * Constructor for the required parameters.
     * @param name column's name
     * @param type column's type
     * @throws IllegalArgumentException if the column's name equals the reserved
     * auto-incrementing column name
     */
    public ColumnSchemaBuilder(String name, Type type) {
      if (name.equalsIgnoreCase(Schema.getAutoIncrementingColumnName())) {
        throw new IllegalArgumentException("Column name " +
            Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine");
      }
      this.name = name;
      this.type = type;
    }

    /**
     * Constructor to copy an existing columnSchema
     * @param that the columnSchema to copy
     */
    public ColumnSchemaBuilder(ColumnSchema that) {
      this.name = that.name;
      this.type = that.type;
      this.key = that.key;
      this.keyUnique = that.keyUnique;
      this.nullable = that.nullable;
      this.immutable = that.immutable;
      this.defaultValue = that.defaultValue;
      this.desiredBlockSize = that.desiredBlockSize;
      this.encoding = that.encoding;
      this.compressionAlgorithm = that.compressionAlgorithm;
      this.typeAttributes = that.typeAttributes;
      this.wireType = that.wireType;
      this.comment = that.comment;
    }

    /**
     * Sets if the column is part of the row key. False by default.
     * This function call overrides any previous key() and nonUniqueKey() call.
     * @param key a boolean that indicates if the column is part of the key
     * @return this instance
     */
    public ColumnSchemaBuilder key(boolean key) {
      this.key = key;
      this.keyUnique = key ? true : false;
      return this;
    }

    /**
     * Sets if the column is part of the row non unique key. False by default.
     * This function call overrides any previous key() and nonUniqueKey() call.
     * @param key a boolean that indicates if the column is a part of the non unique key
     * @return this instance
     */
    public ColumnSchemaBuilder nonUniqueKey(boolean key) {
      this.key = key;
      this.keyUnique = false;
      return this;
    }

    /**
     * Marks the column as allowing null values. False by default.
     * <p>
     * <strong>NOTE:</strong> the "not-nullable-by-default" behavior here differs from
     * the equivalent API in the Python and C++ clients. It also differs from the
     * standard behavior of SQL <code>CREATE TABLE</code> statements. It is
     * recommended to always specify nullability explicitly using this API
     * in order to avoid confusion.
     *
     * @param nullable a boolean that indicates if the column allows null values
     * @return this instance
     */
    public ColumnSchemaBuilder nullable(boolean nullable) {
      this.nullable = nullable;
      return this;
    }

    /**
     * Marks the column as immutable or not. False by default.
     *
     * @param immutable a boolean that indicates if the column is immutable
     * @return this instance
     */
    public ColumnSchemaBuilder immutable(boolean immutable) {
      this.immutable = immutable;
      return this;
    }

    /**
     * Sets the default value that will be read from the column. Null by default.
     * @param defaultValue a Java object representation of the default value that's read
     * @return this instance
     */
    public ColumnSchemaBuilder defaultValue(Object defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    /**
     * Set the desired block size for this column.
     *
     * This is the number of bytes of user data packed per block on disk, and
     * represents the unit of IO when reading this column. Larger values
     * may improve scan performance, particularly on spinning media. Smaller
     * values may improve random access performance, particularly for workloads
     * that have high cache hit rates or operate on fast storage such as SSD.
     *
     * Note that the block size specified here corresponds to uncompressed data.
     * The actual size of the unit read from disk may be smaller if
     * compression is enabled.
     *
     * It's recommended that this not be set any lower than 4096 (4KB) or higher
     * than 1048576 (1MB).
     * @param desiredBlockSize the desired block size, in bytes
     * @return this instance
     * <!-- TODO(KUDU-1107): move the above info to docs -->
     */
    public ColumnSchemaBuilder desiredBlockSize(int desiredBlockSize) {
      this.desiredBlockSize = desiredBlockSize;
      return this;
    }

    /**
     * Set the block encoding for this column. See the documentation for the list
     * of valid options.
     */
    public ColumnSchemaBuilder encoding(Encoding encoding) {
      this.encoding = encoding;
      return this;
    }

    /**
     * Set the compression algorithm for this column. See the documentation for the list
     * of valid options.
     */
    public ColumnSchemaBuilder compressionAlgorithm(CompressionAlgorithm compressionAlgorithm) {
      this.compressionAlgorithm = compressionAlgorithm;
      return this;
    }

    /**
     * Set the column type attributes for this column.
     */
    public ColumnSchemaBuilder typeAttributes(ColumnTypeAttributes typeAttributes) {
      if (typeAttributes != null && !TYPES_WITH_ATTRIBUTES.contains(type)) {
        throw new IllegalArgumentException(
            "ColumnTypeAttributes are not used on " + type + " columns");
      }
      this.typeAttributes = typeAttributes;
      return this;
    }

    /**
     * Allows an alternate {@link Common.DataType} to override the {@link Type}
     * when serializing the ColumnSchema on the wire.
     * This is useful for virtual columns specified by their type such as
     * {@link Common.DataType#IS_DELETED}.
     */
    @InterfaceAudience.Private
    public ColumnSchemaBuilder wireType(Common.DataType wireType) {
      this.wireType = wireType;
      return this;
    }

    /**
     * Set the comment for this column.
     */
    public ColumnSchemaBuilder comment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Builds a {@link ColumnSchema} using the passed parameters.
     * @return a new {@link ColumnSchema}
     */
    public ColumnSchema build() {
      // Set the wire type if it wasn't explicitly set.
      if (wireType == null) {
        this.wireType = type.getDataType(typeAttributes);
      }
      if (type == Type.VARCHAR) {
        if (typeAttributes == null || !typeAttributes.hasLength() ||
            typeAttributes.getLength() < CharUtil.MIN_VARCHAR_LENGTH ||
            typeAttributes.getLength() > CharUtil.MAX_VARCHAR_LENGTH) {
          throw new IllegalArgumentException(
            String.format("VARCHAR's length must be set and between %d and %d",
                          CharUtil.MIN_VARCHAR_LENGTH, CharUtil.MAX_VARCHAR_LENGTH));
        }
      }

      return new ColumnSchema(name, type, key, keyUnique, nullable, immutable,
                              /* autoIncrementing */false, defaultValue,
                              desiredBlockSize, encoding, compressionAlgorithm,
                              typeAttributes, wireType, comment);
    }
  }

  /**
   * Builder for ColumnSchema of the auto-incrementing column. It's used internally in Kudu
   * client library.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class AutoIncrementingColumnSchemaBuilder {
    private final String name;
    private final Type type;
    private int desiredBlockSize = 0;
    private Encoding encoding = null;
    private CompressionAlgorithm compressionAlgorithm = null;
    private Common.DataType wireType = null;
    private String comment = "";

    /**
     * Constructor with default parameter values for {@link ColumnSchema}.
     */
    public AutoIncrementingColumnSchemaBuilder() {
      this.name = Schema.getAutoIncrementingColumnName();
      this.type = Schema.getAutoIncrementingColumnType();
    }

    /**
     * Set the desired block size for this column.
     */
    public AutoIncrementingColumnSchemaBuilder desiredBlockSize(int desiredBlockSize) {
      this.desiredBlockSize = desiredBlockSize;
      return this;
    }

    /**
     * Set the block encoding for this column. This function should be called when
     * fetching column schema from Kudu server.
     */
    public AutoIncrementingColumnSchemaBuilder encoding(Encoding encoding) {
      this.encoding = encoding;
      return this;
    }

    /**
     * Set the compression algorithm for this column. This function should be called
     * when fetching column schema from Kudu server.
     */
    public AutoIncrementingColumnSchemaBuilder compressionAlgorithm(
        CompressionAlgorithm compressionAlgorithm) {
      this.compressionAlgorithm = compressionAlgorithm;
      return this;
    }

    /**
     * Allows an alternate {@link Common.DataType} to override the {@link Type}
     * when serializing the ColumnSchema on the wire.
     * This is useful for virtual columns specified by their type such as
     * {@link Common.DataType#IS_DELETED}.
     */
    @InterfaceAudience.Private
    public AutoIncrementingColumnSchemaBuilder wireType(Common.DataType wireType) {
      this.wireType = wireType;
      return this;
    }

    /**
     * Set the comment for this column.
     */
    public AutoIncrementingColumnSchemaBuilder comment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Builds a {@link ColumnSchema} for auto-incrementing column with passed parameters.
     * @return a new {@link ColumnSchema}
     */
    public ColumnSchema build() {
      // Set the wire type if it wasn't explicitly set.
      if (wireType == null) {
        this.wireType = type.getDataType(null);
      }
      return new ColumnSchema(name, type, /* key */true, /* keyUnique */false,
                              /* nullable */false, /* immutable */false,
                              /* autoIncrementing */true, /* defaultValue */null,
                              desiredBlockSize, encoding, compressionAlgorithm,
                              /* typeAttributes */null, wireType, comment);
    }
  }
}
