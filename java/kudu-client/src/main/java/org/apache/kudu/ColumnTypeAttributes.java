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

import java.util.Objects;

/**
 * Represents a Kudu Table column's type attributes.
 */
@org.apache.yetus.audience.InterfaceAudience.Public
@org.apache.yetus.audience.InterfaceStability.Evolving
public class ColumnTypeAttributes {

  private final boolean hasPrecision;
  private final int precision;

  private final boolean hasScale;
  private final int scale;

  private ColumnTypeAttributes(boolean hasPrecision, int precision,
                               boolean hasScale, int scale) {
    this.hasPrecision = hasPrecision;
    this.precision = precision;
    this.hasScale = hasScale;
    this.scale = scale;
  }

  /**
   * Returns true if the precision is set;
   */
  public boolean hasPrecision() {
    return hasPrecision;
  }

  /**
   * Return the precision;
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Returns true if the scale is set;
   */
  public boolean hasScale() {
    return hasScale;
  }

  /**
   * Return the scale;
   */
  public int getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnTypeAttributes)) {
      return false;
    }

    ColumnTypeAttributes that = (ColumnTypeAttributes) o;

    if (hasPrecision != that.hasPrecision) {
      return false;
    }
    if (precision != that.precision) {
      return false;
    }
    if (hasScale != that.hasScale) {
      return false;
    }
    if (scale != that.scale) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(hasPrecision, precision, hasScale, scale);
  }

  /**
   * Return a string representation appropriate for `type`.
   * This is meant to be postfixed to the name of a primitive type to describe
   * the full type, e.g. decimal(10, 4).
   * @param type the type.
   * @return a postfix string.
   */
  public String toStringForType(Type type) {
    if (type == Type.DECIMAL) {
      return "(" + precision + ", " + scale + ")";
    } else {
      return "";
    }
  }

  @Override
  public String toString() {
    return "hasPrecision: " + hasPrecision + ", precision: " + precision +
        ", hasScale: " + hasScale + ", scale: " + scale;
  }

  /**
   * Builder for ColumnTypeAttributes.
   */
  @org.apache.yetus.audience.InterfaceAudience.Public
  @org.apache.yetus.audience.InterfaceStability.Evolving
  public static class ColumnTypeAttributesBuilder {

    private boolean hasPrecision;
    private int precision;
    private boolean hasScale;
    private int scale;

    /**
     * Set the precision. Only used for Decimal columns.
     */
    public ColumnTypeAttributesBuilder precision(int precision) {
      this.hasPrecision = true;
      this.precision = precision;
      return this;
    }

    /**
     * Set the scale. Only used for Decimal columns.
     */
    public ColumnTypeAttributesBuilder scale(int scale) {
      this.hasScale = true;
      this.scale = scale;
      return this;
    }

    /**
     * Builds a {@link ColumnTypeAttributes} using the passed parameters.
     * @return a new {@link ColumnTypeAttributes}
     */
    public ColumnTypeAttributes build() {
      return new ColumnTypeAttributes(hasPrecision, precision, hasScale, scale);
    }
  }
}
