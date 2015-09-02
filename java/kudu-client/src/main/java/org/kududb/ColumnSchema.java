// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb;

/**
 * Represents a Kudu Table column. Use {@link ColumnSchema.ColumnSchemaBuilder} in order to
 * create columns.
 */
public class ColumnSchema {

  private final String name;
  private final Type type;
  private final boolean key;
  private final boolean nullable;
  private final Object defaultValue;
  private final int desiredBlockSize;

  private ColumnSchema(String name, Type type, boolean key, boolean nullable,
                       Object defaultValue, int desiredBlockSize) {
    this.name = name;
    this.type = type;
    this.key = key;
    this.nullable = nullable;
    this.defaultValue = defaultValue;
    this.desiredBlockSize = desiredBlockSize;
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
   * Answers if the column can be set to null
   * @return true if it can be set to null, else false
   */
  public boolean isNullable() {
    return nullable;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ColumnSchema that = (ColumnSchema) o;

    if (key != that.key) return false;
    if (!name.equals(that.name)) return false;
    if (!type.equals(that.type)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + (key ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Column name: " + name + ", type: " + type.getName();
  }

  /**
   * Builder for ColumnSchema.
   */
  public static class ColumnSchemaBuilder {
    private final String nestedName;
    private final Type nestedType;
    private boolean nestedKey = false;
    private boolean nestedNullable = false;
    private Object nestedDefaultValue = null;
    private int nestedBlockSize = 0;

    /**
     * Constructor for the required parameters.
     * @param name column's name
     * @param type column's type
     */
    public ColumnSchemaBuilder(String name, Type type) {
      this.nestedName = name;
      this.nestedType = type;
    }

    /**
     * Sets if the column is part of the row key. False by default.
     * @param key a boolean that indicates if the column is part of the key
     * @return this instance
     */
    public ColumnSchemaBuilder key(boolean key) {
      this.nestedKey = key;
      return this;
    }

    /**
     * Marks the column as allowing null values. False by default.
     * @param nullable a boolean that indicates if the column allows null values
     * @return this instance
     */
    public ColumnSchemaBuilder nullable(boolean nullable) {
      this.nestedNullable = nullable;
      return this;
    }

    /**
     * Sets the default value that will be read from the column. Null by default.
     * @param defaultValue a Java object representation of the default value that's read
     * @return this instance
     */
    public ColumnSchemaBuilder defaultValue(Object defaultValue) {
      this.nestedDefaultValue = defaultValue;
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
     * @param blockSize the desired block size, in bytes
     * @return this instance
     * <!-- TODO(KUDU-1107): move the above info to docs -->
     */
    public ColumnSchemaBuilder desiredBlockSize(int blockSize) {
      this.nestedBlockSize = blockSize;
      return this;
    }

    /**
     * Builds a {@link ColumnSchema} using the passed parameters.
     * @return a new {@link ColumnSchema}
     */
    public ColumnSchema build() {
      return new ColumnSchema(nestedName, nestedType,
                              nestedKey, nestedNullable, nestedDefaultValue,
                              nestedBlockSize);
    }
  }
}
