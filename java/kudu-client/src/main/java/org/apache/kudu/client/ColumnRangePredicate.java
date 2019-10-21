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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.DecimalUtil;

/**
 * A range predicate on one of the columns in the underlying data.
 * Both boundaries are inclusive.
 * @deprecated use the {@link KuduPredicate} class instead.
 */
@InterfaceAudience.Public
@Deprecated
public class ColumnRangePredicate {

  private final Tserver.ColumnRangePredicatePB.Builder pb = Tserver.ColumnRangePredicatePB
      .newBuilder();
  private final ColumnSchema column;
  private byte[] lowerBound = null;
  private byte[] upperBound = null;

  /**
   * Create the predicate on the specified column
   * @param column
   */
  public ColumnRangePredicate(ColumnSchema column) {
    this.column = column;
    this.pb.setColumn(ProtobufHelper.columnToPb(column));
  }

  private void setLowerBoundInternal(byte[] value) {
    this.lowerBound = value;
    pb.setLowerBound(UnsafeByteOperations.unsafeWrap(this.lowerBound));
  }

  private void setUpperBoundInternal(byte[] value) {
    this.upperBound = value;
    pb.setInclusiveUpperBound(UnsafeByteOperations.unsafeWrap(this.upperBound));
  }

  /**
   * Convert a bound into a {@link KuduPredicate}.
   * @param column the column
   * @param op the bound comparison operator
   * @param bound the bound
   * @return the {@code KuduPredicate}
   */
  private static KuduPredicate toKuduPredicate(ColumnSchema column,
                                               KuduPredicate.ComparisonOp op,
                                               byte[] bound) {
    if (bound == null) {
      return null;
    }
    switch (column.getType().getDataType(column.getTypeAttributes())) {
      case BOOL:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getBoolean(bound));
      case INT8:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getByte(bound));
      case INT16:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getShort(bound));
      case INT32:
      case DATE:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getInt(bound));
      case INT64:
      case UNIXTIME_MICROS:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getLong(bound));
      case FLOAT:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getFloat(bound));
      case DOUBLE:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getDouble(bound));
      case VARCHAR:
      case STRING:
        return KuduPredicate.newComparisonPredicate(column, op, Bytes.getString(bound));
      case BINARY:
        return KuduPredicate.newComparisonPredicate(column, op, bound);
      case DECIMAL32:
      case DECIMAL64:
      case DECIMAL128:
        ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
        return KuduPredicate.newComparisonPredicate(column, op,
            Bytes.getDecimal(bound, typeAttributes.getPrecision(), typeAttributes.getScale()));
      default:
        throw new IllegalStateException(String.format("unknown column type %s", column.getType()));
    }
  }

  /**
   * Convert this column range predicate into a {@link KuduPredicate}.
   * @return the column predicate.
   */
  public KuduPredicate toKuduPredicate() {
    KuduPredicate lower =
        toKuduPredicate(column, KuduPredicate.ComparisonOp.GREATER_EQUAL, lowerBound);
    KuduPredicate upper =
        toKuduPredicate(column, KuduPredicate.ComparisonOp.LESS_EQUAL, upperBound);

    if (upper != null && lower != null) {
      return upper.merge(lower);
    } else if (upper != null) {
      return upper;
    } else {
      return lower;
    }
  }

  /**
   * Set a boolean for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(boolean lowerBound) {
    checkColumn(Type.BOOL);
    setLowerBoundInternal(Bytes.fromBoolean(lowerBound));
  }

  /**
   * Set a byte for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(byte lowerBound) {
    checkColumn(Type.INT8);
    setLowerBoundInternal(new byte[] {lowerBound});
  }

  /**
   * Set a short for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(short lowerBound) {
    checkColumn(Type.INT16);
    setLowerBoundInternal(Bytes.fromShort(lowerBound));
  }

  /**
   * Set an int for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(int lowerBound) {
    checkColumn(Type.INT32);
    setLowerBoundInternal(Bytes.fromInt(lowerBound));
  }

  /**
   * Set a long for the lower bound
   *
   * If 'lowerBound' is a timestamp see {@link PartialRow#addLong(String, long)} for the
   * format.
   *
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(long lowerBound) {
    checkColumn(Type.INT64, Type.UNIXTIME_MICROS);
    setLowerBoundInternal(Bytes.fromLong(lowerBound));
  }

  /**
   * Set a string for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(String lowerBound) {
    checkColumn(Type.STRING, Type.VARCHAR);
    setLowerBoundInternal(lowerBound.getBytes(UTF_8));
  }

  /**
   * Set a binary value for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(byte[] lowerBound) {
    checkColumn(Type.BINARY);
    setLowerBoundInternal(lowerBound);
  }

  /**
   * Set a float for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(float lowerBound) {
    checkColumn(Type.FLOAT);
    setLowerBoundInternal(Bytes.fromFloat(lowerBound));
  }

  /**
   * Set a double for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(double lowerBound) {
    checkColumn(Type.DOUBLE);
    setLowerBoundInternal(Bytes.fromDouble(lowerBound));
  }

  /**
   * Set a BigDecimal for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(BigDecimal lowerBound) {
    checkColumn(Type.DECIMAL);
    int precision = column.getTypeAttributes().getPrecision();
    int scale = column.getTypeAttributes().getScale();
    BigDecimal coercedVal = DecimalUtil.coerce(lowerBound, precision, scale);
    setLowerBoundInternal(Bytes.fromBigDecimal(coercedVal, precision));
  }

  /**
   * Set a boolean for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(boolean upperBound) {
    checkColumn(Type.BOOL);
    setUpperBoundInternal(Bytes.fromBoolean(upperBound));
  }

  /**
   * Set a byte for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(byte upperBound) {
    checkColumn(Type.INT8);
    setUpperBoundInternal(new byte[] {upperBound});
  }

  /**
   * Set a short for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(short upperBound) {
    checkColumn(Type.INT16);
    setUpperBoundInternal(Bytes.fromShort(upperBound));
  }

  /**
   * Set an int for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(int upperBound) {
    checkColumn(Type.INT32);
    setUpperBoundInternal(Bytes.fromInt(upperBound));
  }

  /**
   * Set a long for the upper bound
   *
   * If 'upperBound' is a timestamp see {@link PartialRow#addLong(String, long)} for the
   * format.
   *
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(long upperBound) {
    checkColumn(Type.INT64, Type.UNIXTIME_MICROS);
    setUpperBoundInternal(Bytes.fromLong(upperBound));
  }

  /**
   * Set a string for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(String upperBound) {
    checkColumn(Type.STRING, Type.VARCHAR);
    setUpperBoundInternal(upperBound.getBytes(UTF_8));
  }

  /**
   * Set a binary value for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(byte[] upperBound) {
    checkColumn(Type.BINARY);
    setUpperBoundInternal(upperBound);
  }

  /**
   * Set a float for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(float upperBound) {
    checkColumn(Type.FLOAT);
    setUpperBoundInternal(Bytes.fromFloat(upperBound));
  }

  /**
   * Set a double for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(double upperBound) {
    checkColumn(Type.DOUBLE);
    setUpperBoundInternal(Bytes.fromDouble(upperBound));
  }

  /**
   * Set a BigDecimal for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(BigDecimal upperBound) {
    checkColumn(Type.DECIMAL);
    int precision = column.getTypeAttributes().getPrecision();
    int scale = column.getTypeAttributes().getScale();
    BigDecimal coercedVal = DecimalUtil.coerce(upperBound, precision, scale);
    setUpperBoundInternal(Bytes.fromBigDecimal(coercedVal, precision));
  }

  /**
   * Get the column used by this predicate
   * @return the column
   */
  public ColumnSchema getColumn() {
    return column;
  }

  /**
   * Get the lower bound in its raw representation
   * @return lower bound as a byte array
   */
  public byte[] getLowerBound() {
    return lowerBound;
  }

  /**
   * Get the upper bound in its raw representation
   * @return upper bound as a byte array
   */
  public byte[] getUpperBound() {
    return upperBound;
  }

  /**
   * Converts a list of predicates into an opaque byte array. This is a convenience method for use
   * cases that require passing predicates as messages.
   * @param predicates a list of predicates
   * @return an opaque byte array, or null if the list was empty
   */
  public static byte[] toByteArray(List<ColumnRangePredicate> predicates) {
    if (predicates.isEmpty()) {
      return null;
    }

    Tserver.ColumnRangePredicateListPB.Builder predicateListBuilder =
        Tserver.ColumnRangePredicateListPB.newBuilder();

    for (ColumnRangePredicate crp : predicates) {
      predicateListBuilder.addRangePredicates(crp.getPb());
    }

    return predicateListBuilder.build().toByteArray();
  }

  /**
   * Converts a given byte array to a list of predicates in their pb format.
   * @param listBytes bytes obtained from {@link #toByteArray(List)}
   * @return a list of predicates
   * @throws IllegalArgumentException thrown when the passed bytes aren't valid
   */
  static List<Tserver.ColumnRangePredicatePB> fromByteArray(byte[] listBytes) {
    List<Tserver.ColumnRangePredicatePB> predicates = new ArrayList<>();
    if (listBytes == null || listBytes.length == 0) {
      return predicates;
    }
    Tserver.ColumnRangePredicateListPB list = ColumnRangePredicate.getPbFromBytes(listBytes);
    return list.getRangePredicatesList();
  }

  /**
   * Get the predicate in its protobuf form.
   * @return this predicate in protobuf
   */
  Tserver.ColumnRangePredicatePB getPb() {
    return pb.build();
  }

  /**
   * Creates a {@code ColumnRangePredicate} from a protobuf column range predicate message.
   * @param pb the protobuf message
   * @return a column range predicate
   */
  static ColumnRangePredicate fromPb(Tserver.ColumnRangePredicatePB pb) {
    ColumnRangePredicate pred =
        new ColumnRangePredicate(ProtobufHelper.pbToColumnSchema(pb.getColumn()));
    if (pb.hasLowerBound()) {
      pred.setLowerBoundInternal(pb.getLowerBound().toByteArray());
    }
    if (pb.hasInclusiveUpperBound()) {
      pred.setUpperBoundInternal(pb.getInclusiveUpperBound().toByteArray());
    }
    return pred;
  }

  /**
   * Convert a list of predicates given in bytes back to its pb format. It also hides the
   * InvalidProtocolBufferException.
   */
  private static Tserver.ColumnRangePredicateListPB getPbFromBytes(byte[] listBytes) {
    try {
      return Tserver.ColumnRangePredicateListPB.parseFrom(listBytes);
    } catch (InvalidProtocolBufferException e) {
      // We shade our pb dependency so we can't send out the exception above since other modules
      // won't know what to expect.
      // Intentionally not redacting the list to make this more useful.
      throw new IllegalArgumentException("Encountered an invalid column range predicate list: " +
          Bytes.pretty(listBytes), e);
    }
  }

  private void checkColumn(Type... passedTypes) {
    for (Type type : passedTypes) {
      if (this.column.getType().equals(type)) {
        return;
      }
    }
    throw new IllegalArgumentException(String.format("%s's type isn't %s, it's %s",
        column.getName(), Arrays.toString(passedTypes), column.getType().getName()));
  }
}
