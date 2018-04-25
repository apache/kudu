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

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.DecimalUtil;

/**
 * A predicate which can be used to filter rows based on the value of a column.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduPredicate {

  /**
   * The predicate type.
   */
  @InterfaceAudience.Private
  enum PredicateType {
    /** A predicate which filters all rows. */
    NONE,
    /** A predicate which filters all rows not equal to a value. */
    EQUALITY,
    /** A predicate which filters all rows not in a range. */
    RANGE,
    /** A predicate which filters all null rows. */
    IS_NOT_NULL,
    /** A predicate which filters all non-null rows. */
    IS_NULL,
    /** A predicate which filters all rows not matching a list of values. */
    IN_LIST,
  }

  /**
   * The comparison operator of a predicate.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum ComparisonOp {
    GREATER,
    GREATER_EQUAL,
    EQUAL,
    LESS,
    LESS_EQUAL,
  }

  private final PredicateType type;
  private final ColumnSchema column;

  /**
   * The inclusive lower bound value if this is a Range predicate, or
   * the createEquality value if this is an Equality predicate.
   */
  private final byte[] lower;

  /** The exclusive upper bound value if this is a Range predicate. */
  private final byte[] upper;

  /** IN-list values. */
  private final byte[][] inListValues;

  /**
   * Creates a new {@code KuduPredicate} on a boolean column.
   * @param column the column schema
   * @param op the comparison operation
   * @param value the value to compare against
   */
  public static KuduPredicate newComparisonPredicate(ColumnSchema column,
                                                     ComparisonOp op,
                                                     boolean value) {
    checkColumn(column, Type.BOOL);
    // Create the comparison predicate. Range predicates on boolean values can
    // always be converted to either an equality, an IS NOT NULL (filtering only
    // null values), or NONE (filtering all values).
    switch (op) {
      case GREATER: {
        // b > true  -> b NONE
        // b > false -> b = true
        if (value) {
          return none(column);
        } else {
          return new KuduPredicate(PredicateType.EQUALITY, column, Bytes.fromBoolean(true), null);
        }
      }
      case GREATER_EQUAL: {
        // b >= true  -> b = true
        // b >= false -> b IS NOT NULL
        if (value) {
          return new KuduPredicate(PredicateType.EQUALITY, column, Bytes.fromBoolean(true), null);
        } else {
          return newIsNotNullPredicate(column);
        }
      }
      case EQUAL: return new KuduPredicate(PredicateType.EQUALITY, column,
                                           Bytes.fromBoolean(value), null);
      case LESS: {
        // b < true  -> b NONE
        // b < false -> b = true
        if (value) {
          return new KuduPredicate(PredicateType.EQUALITY, column, Bytes.fromBoolean(false), null);
        } else {
          return none(column);
        }
      }
      case LESS_EQUAL: {
        // b <= true  -> b IS NOT NULL
        // b <= false -> b = false
        if (value) {
          return newIsNotNullPredicate(column);
        } else {
          return new KuduPredicate(PredicateType.EQUALITY, column, Bytes.fromBoolean(false), null);
        }
      }
      default: throw new RuntimeException("unknown comparison op");
    }
  }

  /**
   * Creates a new comparison predicate on an integer or timestamp column.
   * @param column the column schema
   * @param op the comparison operation
   * @param value the value to compare against
   */
  public static KuduPredicate newComparisonPredicate(ColumnSchema column,
                                                     ComparisonOp op,
                                                     long value) {
    checkColumn(column, Type.INT8, Type.INT16, Type.INT32, Type.INT64, Type.UNIXTIME_MICROS);
    long minValue = minIntValue(column.getType());
    long maxValue = maxIntValue(column.getType());
    Preconditions.checkArgument(value <= maxValue && value >= minValue,
                                "integer value out of range for %s column: %s",
                                column.getType(), value);

    if (op == ComparisonOp.LESS_EQUAL) {
      if (value == maxValue) {
        // If the value can't be incremented because it is at the top end of the
        // range, then substitute the predicate with an IS NOT NULL predicate.
        // This has the same effect as an inclusive upper bound on the maximum
        // value. If the column is not nullable then the IS NOT NULL predicate
        // is ignored.
        return newIsNotNullPredicate(column);
      }
      value += 1;
      op = ComparisonOp.LESS;
    } else if (op == ComparisonOp.GREATER) {
      if (value == maxValue) {
        return none(column);
      }
      value += 1;
      op = ComparisonOp.GREATER_EQUAL;
    }

    byte[] bytes;
    switch (column.getType()) {
      case INT8: {
        bytes = new byte[] { (byte) value };
        break;
      }
      case INT16: {
        bytes = Bytes.fromShort((short) value);
        break;
      }
      case INT32: {
        bytes = Bytes.fromInt((int) value);
        break;
      }
      case INT64:
      case UNIXTIME_MICROS: {
        bytes = Bytes.fromLong(value);
        break;
      }
      default:
        throw new RuntimeException("already checked");
    }
    switch (op) {
      case GREATER_EQUAL:
        if (value == minValue) {
          return newIsNotNullPredicate(column);
        } else if (value == maxValue) {
          return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
        }
        return new KuduPredicate(PredicateType.RANGE, column, bytes, null);
      case EQUAL:
        return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
      case LESS:
        if (value == minValue) {
          return none(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, null, bytes);
      default:
        throw new RuntimeException("unknown comparison op");
    }
  }

  /**
   * Creates a new comparison predicate on a Decimal column.
   * @param column the column schema
   * @param op the comparison operation
   * @param value the value to compare against
   */
  public static KuduPredicate newComparisonPredicate(ColumnSchema column,
                                                     ComparisonOp op,
                                                     BigDecimal value) {
    checkColumn(column, Type.DECIMAL);
    ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
    int precision = typeAttributes.getPrecision();
    int scale = typeAttributes.getScale();

    // Coerce the value to have the same precision and scale
    value = DecimalUtil.coerce(value, precision, scale);

    BigDecimal minValue = DecimalUtil.minValue(precision, scale);
    BigDecimal maxValue = DecimalUtil.maxValue(precision, scale);
    Preconditions.checkArgument(value.compareTo(maxValue) <= 0 && value.compareTo(minValue) >= 0,
        "Decimal value out of range for %s column: %s",
        column.getType(), value);
    BigDecimal smallestValue = DecimalUtil.smallestValue(scale);

    if (op == ComparisonOp.LESS_EQUAL) {
      if (value.equals(maxValue)) {
        // If the value can't be incremented because it is at the top end of the
        // range, then substitute the predicate with an IS NOT NULL predicate.
        // This has the same effect as an inclusive upper bound on the maximum
        // value. If the column is not nullable then the IS NOT NULL predicate
        // is ignored.
        return newIsNotNullPredicate(column);
      }
      value = value.add(smallestValue);
      op = ComparisonOp.LESS;
    } else if (op == ComparisonOp.GREATER) {
      if (value.equals(maxValue)) {
        return none(column);
      }
      value = value.add(smallestValue);
      op = ComparisonOp.GREATER_EQUAL;
    }

    byte[] bytes = Bytes.fromBigDecimal(value, precision);

    switch (op) {
      case GREATER_EQUAL:
        if (value.equals(minValue)) {
          return newIsNotNullPredicate(column);
        } else if (value.equals(maxValue)) {
          return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
        }
        return new KuduPredicate(PredicateType.RANGE, column, bytes, null);
      case EQUAL:
        return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
      case LESS:
        if (value.equals(minValue)) {
          return none(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, null, bytes);
      default:
        throw new RuntimeException("unknown comparison op");
    }
  }

  /**
   * Creates a new comparison predicate on a float column.
   * @param column the column schema
   * @param op the comparison operation
   * @param value the value to compare against
   */
  public static KuduPredicate newComparisonPredicate(ColumnSchema column,
                                                     ComparisonOp op,
                                                     float value) {
    checkColumn(column, Type.FLOAT);
    if (op == ComparisonOp.LESS_EQUAL) {
      if (value == Float.POSITIVE_INFINITY) {
        return newIsNotNullPredicate(column);
      }
      value = Math.nextAfter(value, Float.POSITIVE_INFINITY);
      op = ComparisonOp.LESS;
    } else if (op == ComparisonOp.GREATER) {
      if (value == Float.POSITIVE_INFINITY) {
        return none(column);
      }
      value = Math.nextAfter(value, Float.POSITIVE_INFINITY);
      op = ComparisonOp.GREATER_EQUAL;
    }

    byte[] bytes = Bytes.fromFloat(value);
    switch (op) {
      case GREATER_EQUAL:
        if (value == Float.NEGATIVE_INFINITY) {
          return newIsNotNullPredicate(column);
        } else if (value == Float.POSITIVE_INFINITY) {
          return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
        }
        return new KuduPredicate(PredicateType.RANGE, column, bytes, null);
      case EQUAL:
        return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
      case LESS:
        if (value == Float.NEGATIVE_INFINITY) {
          return none(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, null, bytes);
      default:
        throw new RuntimeException("unknown comparison op");
    }
  }

  /**
   * Creates a new comparison predicate on a double column.
   * @param column the column schema
   * @param op the comparison operation
   * @param value the value to compare against
   */
  public static KuduPredicate newComparisonPredicate(ColumnSchema column,
                                                     ComparisonOp op,
                                                     double value) {
    checkColumn(column, Type.DOUBLE);
    if (op == ComparisonOp.LESS_EQUAL) {
      if (value == Double.POSITIVE_INFINITY) {
        return newIsNotNullPredicate(column);
      }
      value = Math.nextAfter(value, Double.POSITIVE_INFINITY);
      op = ComparisonOp.LESS;
    } else if (op == ComparisonOp.GREATER) {
      if (value == Double.POSITIVE_INFINITY) {
        return none(column);
      }
      value = Math.nextAfter(value, Double.POSITIVE_INFINITY);
      op = ComparisonOp.GREATER_EQUAL;
    }

    byte[] bytes = Bytes.fromDouble(value);
    switch (op) {
      case GREATER_EQUAL:
        if (value == Double.NEGATIVE_INFINITY) {
          return newIsNotNullPredicate(column);
        } else if (value == Double.POSITIVE_INFINITY) {
          return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
        }
        return new KuduPredicate(PredicateType.RANGE, column, bytes, null);
      case EQUAL:
        return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
      case LESS:
        if (value == Double.NEGATIVE_INFINITY) {
          return none(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, null, bytes);
      default:
        throw new RuntimeException("unknown comparison op");
    }
  }

  /**
   * Creates a new comparison predicate on a string column.
   * @param column the column schema
   * @param op the comparison operation
   * @param value the value to compare against
   */
  public static KuduPredicate newComparisonPredicate(ColumnSchema column,
                                                     ComparisonOp op,
                                                     String value) {
    checkColumn(column, Type.STRING);

    byte[] bytes = Bytes.fromString(value);
    if (op == ComparisonOp.LESS_EQUAL) {
      bytes = Arrays.copyOf(bytes, bytes.length + 1);
      op = ComparisonOp.LESS;
    } else if (op == ComparisonOp.GREATER) {
      bytes = Arrays.copyOf(bytes, bytes.length + 1);
      op = ComparisonOp.GREATER_EQUAL;
    }

    switch (op) {
      case GREATER_EQUAL:
        if (bytes.length == 0) {
          return newIsNotNullPredicate(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, bytes, null);
      case EQUAL:
        return new KuduPredicate(PredicateType.EQUALITY, column, bytes, null);
      case LESS:
        if (bytes.length == 0) {
          return none(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, null, bytes);
      default:
        throw new RuntimeException("unknown comparison op");
    }
  }

  /**
   * Creates a new comparison predicate on a binary column.
   * @param column the column schema
   * @param op the comparison operation
   * @param value the value to compare against
   */
  public static KuduPredicate newComparisonPredicate(ColumnSchema column,
                                                     ComparisonOp op,
                                                     byte[] value) {
    checkColumn(column, Type.BINARY);

    if (op == ComparisonOp.LESS_EQUAL) {
      value = Arrays.copyOf(value, value.length + 1);
      op = ComparisonOp.LESS;
    } else if (op == ComparisonOp.GREATER) {
      value = Arrays.copyOf(value, value.length + 1);
      op = ComparisonOp.GREATER_EQUAL;
    }

    switch (op) {
      case GREATER_EQUAL:
        if (value.length == 0) {
          return newIsNotNullPredicate(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, value, null);
      case EQUAL:
        return new KuduPredicate(PredicateType.EQUALITY, column, value, null);
      case LESS:
        if (value.length == 0) {
          return none(column);
        }
        return new KuduPredicate(PredicateType.RANGE, column, null, value);
      default:
        throw new RuntimeException("unknown comparison op");
    }
  }

  /**
   * Creates a new IN list predicate.
   *
   * The list must contain values of the correct type for the column.
   *
   * @param column the column that the predicate applies to
   * @param values list of values which the column values must match
   * @param <T> the type of values, must match the type of the column
   * @return an IN list predicate
   */
  public static <T> KuduPredicate newInListPredicate(final ColumnSchema column, List<T> values) {
    if (values.isEmpty()) {
      return none(column);
    }
    T t = values.get(0);

    SortedSet<byte[]> vals = new TreeSet<>(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] a, byte[] b) {
        return KuduPredicate.compare(column, a, b);
      }
    });

    if (t instanceof Boolean) {
      checkColumn(column, Type.BOOL);
      for (T value : values) {
        vals.add(Bytes.fromBoolean((Boolean) value));
      }
    } else if (t instanceof Byte) {
      checkColumn(column, Type.INT8);
      for (T value : values) {
        vals.add(new byte[] {(Byte) value});
      }
    } else if (t instanceof Short) {
      checkColumn(column, Type.INT16);
      for (T value : values) {
        vals.add(Bytes.fromShort((Short) value));
      }
    } else if (t instanceof Integer) {
      checkColumn(column, Type.INT32);
      for (T value : values) {
        vals.add(Bytes.fromInt((Integer) value));
      }
    } else if (t instanceof Long) {
      checkColumn(column, Type.INT64, Type.UNIXTIME_MICROS);
      for (T value : values) {
        vals.add(Bytes.fromLong((Long) value));
      }
    } else if (t instanceof Float) {
      checkColumn(column, Type.FLOAT);
      for (T value : values) {
        vals.add(Bytes.fromFloat((Float) value));
      }
    } else if (t instanceof Double) {
      checkColumn(column, Type.DOUBLE);
      for (T value : values) {
        vals.add(Bytes.fromDouble((Double) value));
      }
    } else if (t instanceof BigDecimal) {
        checkColumn(column, Type.DECIMAL);
        for (T value : values) {
          vals.add(Bytes.fromBigDecimal((BigDecimal) value,
              column.getTypeAttributes().getPrecision()));
        }
    } else if (t instanceof String) {
      checkColumn(column, Type.STRING);
      for (T value : values) {
        vals.add(Bytes.fromString((String) value));
      }
    } else if (t instanceof byte[]) {
      checkColumn(column, Type.BINARY);
      for (T value : values) {
        vals.add((byte[]) value);
      }
    } else {
      throw new IllegalArgumentException(String.format("illegal type for IN list values: %s",
                                                       t.getClass().getName()));
    }

    return buildInList(column, vals);
  }

  /**
   * Creates a new {@code IS NOT NULL} predicate.
   *
   * @param column the column that the predicate applies to
   * @return an {@code IS NOT NULL} predicate
   */
  public static KuduPredicate newIsNotNullPredicate(ColumnSchema column) {
    return new KuduPredicate(PredicateType.IS_NOT_NULL, column, null, null);
  }

  /**
   * Creates a new {@code IS NULL} predicate.
   *
   * @param column the column that the predicate applies to
   * @return an {@code IS NULL} predicate
   */
  public static KuduPredicate newIsNullPredicate(ColumnSchema column) {
    if (!column.isNullable()) {
      return none(column);
    }
    return new KuduPredicate(PredicateType.IS_NULL, column, null, null);
  }

  /**
   * @param type the predicate type
   * @param column the column to which the predicate applies
   * @param lower the lower bound serialized value if this is a Range predicate,
   *              or the equality value if this is an Equality predicate
   * @param upper the upper bound serialized value if this is an Equality predicate
   */
  @InterfaceAudience.LimitedPrivate("Test")
  KuduPredicate(PredicateType type, ColumnSchema column, byte[] lower, byte[] upper) {
    this.type = type;
    this.column = column;
    this.lower = lower;
    this.upper = upper;
    this.inListValues = null;
  }

  /**
   * Constructor for IN list predicate.
   * @param column the column to which the predicate applies
   * @param inListValues the encoded IN list values
   */
  private KuduPredicate(ColumnSchema column, byte[][] inListValues) {
    this.column = column;
    this.type = PredicateType.IN_LIST;
    this.lower = null;
    this.upper = null;
    this.inListValues = inListValues;
  }

  /**
   * Factory function for a {@code None} predicate.
   * @param column the column to which the predicate applies
   * @return a None predicate
   */
  @InterfaceAudience.LimitedPrivate("Test")
  static KuduPredicate none(ColumnSchema column) {
    return new KuduPredicate(PredicateType.NONE, column, null, null);
  }

  /**
   * @return the type of this predicate
   */
  PredicateType getType() {
    return type;
  }

  /**
   * Merges another {@code ColumnPredicate} into this one, returning a new
   * {@code ColumnPredicate} which matches the logical intersection ({@code AND})
   * of the input predicates.
   * @param other the predicate to merge with this predicate
   * @return a new predicate that is the logical intersection
   */
  KuduPredicate merge(KuduPredicate other) {
    Preconditions.checkArgument(column.equals(other.column),
                                "predicates from different columns may not be merged");

    // First, consider other.type == NONE, IS_NOT_NULL, or IS_NULL
    // NONE predicates dominate.
    if (other.type == PredicateType.NONE) {
      return other;
    }

    // NOT NULL is dominated by all other predicates,
    // except IS NULL, for which the merge is NONE.
    if (other.type == PredicateType.IS_NOT_NULL) {
      return type == PredicateType.IS_NULL ? none(column) : this;
    }

    // NULL merged with any predicate type besides itself is NONE.
    if (other.type == PredicateType.IS_NULL) {
      return type == PredicateType.IS_NULL ? this : none(column);
    }

    // Now other.type == EQUALITY, RANGE, or IN_LIST.
    switch (type) {
      case NONE: return this;
      case IS_NOT_NULL: return other;
      case IS_NULL: return none(column);
      case EQUALITY: {
        if (other.type == PredicateType.EQUALITY) {
          if (compare(column, lower, other.lower) != 0) {
            return none(this.column);
          } else {
            return this;
          }
        } else if (other.type == PredicateType.RANGE) {
          if (other.rangeContains(lower)) {
            return this;
          } else {
            return none(this.column);
          }
        } else {
          Preconditions.checkState(other.type == PredicateType.IN_LIST);
          return other.merge(this);
        }
      }
      case RANGE: {
        if (other.type == PredicateType.EQUALITY || other.type == PredicateType.IN_LIST) {
          return other.merge(this);
        } else {
          Preconditions.checkState(other.type == PredicateType.RANGE);
          byte[] newLower = other.lower == null ||
              (lower != null && compare(column, lower, other.lower) >= 0) ? lower : other.lower;
          byte[] newUpper = other.upper == null ||
              (upper != null && compare(column, upper, other.upper) <= 0) ? upper : other.upper;
          if (newLower != null && newUpper != null && compare(column, newLower, newUpper) >= 0) {
            return none(column);
          } else {
            if (newLower != null && newUpper != null && areConsecutive(newLower, newUpper)) {
              return new KuduPredicate(PredicateType.EQUALITY, column, newLower, null);
            } else {
              return new KuduPredicate(PredicateType.RANGE, column, newLower, newUpper);
            }
          }
        }
      }
      case IN_LIST: {
        if (other.type == PredicateType.EQUALITY) {
          if (this.inListContains(other.lower)) {
            return other;
          } else {
            return none(column);
          }
        } else if (other.type == PredicateType.RANGE) {
          List<byte[]> values = new ArrayList<>();
          for (byte[] value : inListValues) {
            if (other.rangeContains(value)) {
              values.add(value);
            }
          }
          return buildInList(column, values);
        } else {
          Preconditions.checkState(other.type == PredicateType.IN_LIST);
          List<byte[]> values = new ArrayList<>();
          for (byte[] value : inListValues) {
            if (other.inListContains(value)) {
              values.add(value);
            }
          }
          return buildInList(column, values);
        }
      }
      default: throw new IllegalStateException(String.format("unknown predicate type %s", this));
    }
  }

  /**
   * Builds an IN list predicate from a collection of raw values. The collection
   * must be sorted and deduplicated.
   *
   * @param column the column
   * @param values the IN list values
   * @return an IN list predicate
   */
  private static KuduPredicate buildInList(ColumnSchema column, Collection<byte[]> values) {
    // IN (true, false) predicates can be simplified to IS NOT NULL.
    if (column.getType().getDataType(column.getTypeAttributes()) ==
        Common.DataType.BOOL && values.size() > 1) {
      return newIsNotNullPredicate(column);
    }

    switch (values.size()) {
      case 0: return KuduPredicate.none(column);
      case 1: return new KuduPredicate(PredicateType.EQUALITY, column,
                                       values.iterator().next(), null);
      default: return new KuduPredicate(column, values.toArray(new byte[values.size()][]));
    }
  }

  /**
   * @param value the value to check for
   * @return {@code true} if this IN list predicate contains the value
   */
  boolean inListContains(byte[] value) {
    final Comparator<byte[]> comparator = new Comparator<byte[]>() {
      @Override
      public int compare(byte[] a, byte[] b) {
        return KuduPredicate.compare(column, a, b);
      }
    };
    return Arrays.binarySearch(inListValues, value, comparator) >= 0;
  }

  /**
   * @param value the value to check
   * @return {@code true} if this RANGE predicate contains the value
   */
  boolean rangeContains(byte[] value) {
    return (lower == null || compare(column, value, lower) >= 0) &&
           (upper == null || compare(column, value, upper) < 0);
  }

  /**
   * @return the schema of the predicate column
   */
  ColumnSchema getColumn() {
    return column;
  }

  /**
   * Convert the predicate to the protobuf representation.
   * @return the protobuf message for this predicate
   */
  @InterfaceAudience.Private
  public Common.ColumnPredicatePB toPB() {
    Common.ColumnPredicatePB.Builder builder = Common.ColumnPredicatePB.newBuilder();
    builder.setColumn(column.getName());

    switch (type) {
      case EQUALITY: {
        builder.getEqualityBuilder().setValue(ByteString.copyFrom(lower));
        break;
      }
      case RANGE: {
        Common.ColumnPredicatePB.Range.Builder b = builder.getRangeBuilder();
        if (lower != null) {
          b.setLower(ByteString.copyFrom(lower));
        }
        if (upper != null) {
          b.setUpper(ByteString.copyFrom(upper));
        }
        break;
      }
      case IS_NOT_NULL: {
        builder.setIsNotNull(builder.getIsNotNullBuilder());
        break;
      }
      case IS_NULL: {
        builder.setIsNull(builder.getIsNullBuilder());
        break;
      }
      case IN_LIST: {
        Common.ColumnPredicatePB.InList.Builder inListBuilder = builder.getInListBuilder();
        for (byte[] value : inListValues) {
          inListBuilder.addValues(ByteString.copyFrom(value));
        }
        break;
      }
      case NONE: throw new IllegalStateException(
          "can not convert None predicate to protobuf message");
      default: throw new IllegalArgumentException(
          String.format("unknown predicate type: %s", type));
    }
    return builder.build();
  }

  /**
   * Convert a column predicate protobuf message into a predicate.
   * @return a predicate
   */
  @InterfaceAudience.Private
  public static KuduPredicate fromPB(Schema schema, Common.ColumnPredicatePB pb) {
    final ColumnSchema column = schema.getColumn(pb.getColumn());
    switch (pb.getPredicateCase()) {
      case EQUALITY:
        return new KuduPredicate(PredicateType.EQUALITY, column,
                                 pb.getEquality().getValue().toByteArray(), null);
      case RANGE: {
        Common.ColumnPredicatePB.Range range = pb.getRange();
        return new KuduPredicate(PredicateType.RANGE, column,
                                 range.hasLower() ? range.getLower().toByteArray() : null,
                                 range.hasUpper() ? range.getUpper().toByteArray() : null);
      }
      case IS_NOT_NULL:
        return newIsNotNullPredicate(column);
      case IS_NULL:
        return newIsNullPredicate(column);
      case IN_LIST: {
        Common.ColumnPredicatePB.InList inList = pb.getInList();

        SortedSet<byte[]> values = new TreeSet<>(new Comparator<byte[]>() {
          @Override
          public int compare(byte[] a, byte[] b) {
            return KuduPredicate.compare(column, a, b);
          }
        });

        for (ByteString value : inList.getValuesList()) {
          values.add(value.toByteArray());
        }
        return buildInList(column, values);
      }
      default:
        throw new IllegalArgumentException("unknown predicate type");
    }
  }

  /**
   * Compares two bounds based on the type of the column.
   * @param column the column which the values belong to
   * @param a the first serialized value
   * @param b the second serialized value
   * @return the comparison of the serialized values based on the column type
   */
  private static int compare(ColumnSchema column, byte[] a, byte[] b) {
    switch (column.getType().getDataType(column.getTypeAttributes())) {
      case BOOL:
        return Boolean.compare(Bytes.getBoolean(a), Bytes.getBoolean(b));
      case INT8:
        return Byte.compare(Bytes.getByte(a), Bytes.getByte(b));
      case INT16:
        return Short.compare(Bytes.getShort(a), Bytes.getShort(b));
      case INT32:
      case DECIMAL32:
        return Integer.compare(Bytes.getInt(a), Bytes.getInt(b));
      case INT64:
      case UNIXTIME_MICROS:
      case DECIMAL64:
        return Long.compare(Bytes.getLong(a), Bytes.getLong(b));
      case FLOAT:
        return Float.compare(Bytes.getFloat(a), Bytes.getFloat(b));
      case DOUBLE:
        return Double.compare(Bytes.getDouble(a), Bytes.getDouble(b));
      case STRING:
      case BINARY:
        return UnsignedBytes.lexicographicalComparator().compare(a, b);
      case DECIMAL128:
        return Bytes.getBigInteger(a).compareTo(Bytes.getBigInteger(b));
      default:
        throw new IllegalStateException(String.format("unknown column type %s", column.getType()));
    }
  }

  /**
   * Returns true if increment(a) == b.
   * @param a the value which would be incremented
   * @param b the target value
   * @return true if increment(a) == b
   */
  private boolean areConsecutive(byte[] a, byte[] b) {
    switch (column.getType().getDataType(column.getTypeAttributes())) {
      case BOOL: return false;
      case INT8: {
        byte m = Bytes.getByte(a);
        byte n = Bytes.getByte(b);
        return m < n && m + 1 == n;
      }
      case INT16: {
        short m = Bytes.getShort(a);
        short n = Bytes.getShort(b);
        return m < n && m + 1 == n;
      }
      case INT32:
      case DECIMAL32:{
        int m = Bytes.getInt(a);
        int n = Bytes.getInt(b);
        return m < n && m + 1 == n;
      }
      case INT64:
      case UNIXTIME_MICROS:
      case DECIMAL64:  {
        long m = Bytes.getLong(a);
        long n = Bytes.getLong(b);
        return m < n && m + 1 == n;
      }
      case FLOAT: {
        float m = Bytes.getFloat(a);
        float n = Bytes.getFloat(b);
        return m < n && Math.nextAfter(m, Float.POSITIVE_INFINITY) == n;
      }
      case DOUBLE: {
        double m = Bytes.getDouble(a);
        double n = Bytes.getDouble(b);
        return m < n && Math.nextAfter(m, Double.POSITIVE_INFINITY) == n;
      }
      case STRING:
      case BINARY: {
        if (a.length + 1 != b.length || b[a.length] != 0) {
          return false;
        }
        for (int i = 0; i < a.length; i++) {
          if (a[i] != b[i]) {
            return false;
          }
        }
        return true;
      }
      case DECIMAL128: {
        BigInteger m = Bytes.getBigInteger(a);
        BigInteger n = Bytes.getBigInteger(b);
        return m.compareTo(n) < 0 && m.add(BigInteger.ONE).equals(n);
      }
      default:
        throw new IllegalStateException(String.format("unknown column type %s", column.getType()));
    }
  }

  /**
   * @return the encoded lower bound.
   */
  byte[] getLower() {
    return lower;
  }

  /**
   * @return the encoded upper bound.
   */
  byte[] getUpper() {
    return upper;
  }

  /**
   * @return the IN list values. Always kept sorted and de-duplicated.
   */
  byte[][] getInListValues() {
    return inListValues;
  }

  /**
   * Returns the maximum value for the integer type.
   * @param type an integer type
   * @return the maximum value
   */
  @InterfaceAudience.LimitedPrivate("Test")
  static long maxIntValue(Type type) {
    switch (type) {
      case INT8:
        return Byte.MAX_VALUE;
      case INT16:
        return Short.MAX_VALUE;
      case INT32:
        return Integer.MAX_VALUE;
      case UNIXTIME_MICROS:
      case INT64:
        return Long.MAX_VALUE;
      default:
        throw new IllegalArgumentException("type must be an integer type");
    }
  }

  /**
   * Returns the minimum value for the integer type.
   * @param type an integer type
   * @return the minimum value
   */
  @InterfaceAudience.LimitedPrivate("Test")
  static long minIntValue(Type type) {
    switch (type) {
      case INT8:
        return Byte.MIN_VALUE;
      case INT16:
        return Short.MIN_VALUE;
      case INT32:
        return Integer.MIN_VALUE;
      case UNIXTIME_MICROS:
      case INT64:
        return Long.MIN_VALUE;
      default:
        throw new IllegalArgumentException("type must be an integer type");
    }
  }

  /**
   * Checks that the column is one of the expected types.
   * @param column the column being checked
   * @param passedTypes the expected types (logical OR)
   */
  private static void checkColumn(ColumnSchema column, Type... passedTypes) {
    for (Type type : passedTypes) {
      if (column.getType().equals(type)) {
        return;
      }
    }
    throw new IllegalArgumentException(String.format("%s's type isn't %s, it's %s",
                                                     column.getName(), Arrays.toString(passedTypes),
                                                     column.getType().getName()));
  }

  /**
   * Returns the string value of serialized value according to the type of column.
   * @param value the value
   * @return the text representation of the value
   */
  private String valueToString(byte[] value) {
    switch (column.getType().getDataType(column.getTypeAttributes())) {
      case BOOL: return Boolean.toString(Bytes.getBoolean(value));
      case INT8: return Byte.toString(Bytes.getByte(value));
      case INT16: return Short.toString(Bytes.getShort(value));
      case INT32: return Integer.toString(Bytes.getInt(value));
      case INT64: return Long.toString(Bytes.getLong(value));
      case UNIXTIME_MICROS: return RowResult.timestampToString(Bytes.getLong(value));
      case FLOAT: return Float.toString(Bytes.getFloat(value));
      case DOUBLE: return Double.toString(Bytes.getDouble(value));
      case STRING: {
        String v = Bytes.getString(value);
        StringBuilder sb = new StringBuilder(2 + v.length());
        sb.append('"');
        sb.append(v);
        sb.append('"');
        return sb.toString();
      }
      case BINARY: return Bytes.hex(value);
      case DECIMAL32:
      case DECIMAL64:
      case DECIMAL128:
       ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
       return Bytes.getDecimal(value, typeAttributes.getPrecision(),
           typeAttributes.getScale()).toString();
      default:
        throw new IllegalStateException(String.format("unknown column type %s", column.getType()));
    }
  }

  @Override
  public String toString() {
    switch (type) {
      case EQUALITY: return String.format("`%s` = %s", column.getName(), valueToString(lower));
      case RANGE: {
        if (lower == null) {
          return String.format("`%s` < %s", column.getName(), valueToString(upper));
        } else if (upper == null) {
          return String.format("`%s` >= %s", column.getName(), valueToString(lower));
        } else {
          return String.format("`%s` >= %s AND `%s` < %s",
                               column.getName(), valueToString(lower),
                               column.getName(), valueToString(upper));
        }
      }
      case IN_LIST: {
        List<String> strings = new ArrayList<>(inListValues.length);
        for (byte[] value : inListValues) {
          strings.add(valueToString(value));
        }
        return String.format("`%s` IN (%s)", column.getName(), Joiner.on(", ").join(strings));
      }
      case IS_NOT_NULL: return String.format("`%s` IS NOT NULL", column.getName());
      case IS_NULL: return String.format("`%s` IS NULL", column.getName());
      case NONE: return String.format("`%s` NONE", column.getName());
      default: throw new IllegalArgumentException(String.format("unknown predicate type %s", type));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KuduPredicate that = (KuduPredicate) o;
    return type == that.type &&
        column.equals(that.column) &&
        Arrays.equals(lower, that.lower) &&
        Arrays.equals(upper, that.upper) &&
        Arrays.deepEquals(inListValues, that.inListValues);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, column, Arrays.hashCode(lower),
                            Arrays.hashCode(upper), Arrays.deepHashCode(inListValues));
  }
}
