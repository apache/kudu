// Copyright (c) 2013, Cloudera, inc.
package kudu;

/**
 * Represents a Kudu Table column.
 */
public class ColumnSchema {

  private final String name;
  private final Type type;
  private final boolean key;

  /**
   * Constructor for a non-key column, requires a name and a type
   * @param name Column's name
   * @param type Column's type
   */
  public ColumnSchema(String name, Type type) {
    this(name, type, false);
  }

  /**
   * Constructor for any column, specify the name, type, and if it's a key
   * @param name Column's name
   * @param type Column's type
   * @param key If this column is part of the key
   */
  public ColumnSchema(String name, Type type, boolean key) {
    this.name = name;
    this.type = type;
    this.key = key;
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
}
