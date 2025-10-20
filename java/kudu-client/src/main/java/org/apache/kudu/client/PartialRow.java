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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.DecimalUtil;
import org.apache.kudu.util.StringUtil;
import org.apache.kudu.util.TimestampUtil;

/**
 * Class used to represent parts of a row along with its schema.<p>
 *
 * Values can be replaced as often as needed, but once the enclosing {@link Operation} is applied
 * then they cannot be changed again. This means that a PartialRow cannot be reused.<p>
 *
 * Each PartialRow is backed by an byte array where all the cells (except strings and binary data)
 * are written. The others are kept in a List.<p>
 *
 * This class isn't thread-safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PartialRow {

  private final Schema schema;

  // Variable length data. If string, will be UTF-8 encoded. Elements of this list _must_ have a
  // mark that we can reset() to. Readers of these fields (encoders, etc) must call reset() before
  // attempting to read these values.
  private final List<ByteBuffer> varLengthData;
  private final byte[] rowAlloc;

  private final BitSet columnsBitSet;
  private final BitSet nullsBitSet;

  private boolean frozen = false;

  private static final Type ARRAY_TYPE = Type.NESTED;

  private byte[] getVarLenBytes(int columnIndex) {
    ByteBuffer dup = getVarLengthData(columnIndex);
    dup.reset();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return out;
  }

  /**
   * This is not a stable API, prefer using {@link Schema#newPartialRow()}
   * to create a new partial row.
   * @param schema the schema to use for this row
   */
  public PartialRow(Schema schema) {
    this.schema = schema;
    this.columnsBitSet = new BitSet(this.schema.getColumnCount());
    this.nullsBitSet = schema.hasNullableColumns() ?
        new BitSet(this.schema.getColumnCount()) : null;
    this.rowAlloc = new byte[schema.getRowSize()];
    // Pre-fill the array with nulls. We'll only replace cells that have varlen values.
    this.varLengthData = Arrays.asList(new ByteBuffer[this.schema.getColumnCount()]);
  }

  /**
   * Creates a new partial row by deep-copying the data-fields of the provided partial row.
   * @param row the partial row to copy
   */
  PartialRow(PartialRow row) {
    this.schema = row.schema;

    this.varLengthData = Lists.newArrayListWithCapacity(row.varLengthData.size());
    for (ByteBuffer data: row.varLengthData) {
      if (data == null) {
        this.varLengthData.add(null);
      } else {
        data.reset();
        // Deep copy the ByteBuffer.
        ByteBuffer clone = ByteBuffer.allocate(data.remaining());
        clone.put(data);
        clone.flip();

        clone.mark(); // We always expect a mark.
        this.varLengthData.add(clone);
      }
    }

    this.rowAlloc = row.rowAlloc.clone();
    this.columnsBitSet = (BitSet) row.columnsBitSet.clone();
    this.nullsBitSet = row.nullsBitSet == null ? null : (BitSet) row.nullsBitSet.clone();
  }

  /**
   * Add a boolean for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addBoolean(int columnIndex, boolean val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BOOL);
    rowAlloc[getPositionInRowAllocAndSetBitSet(columnIndex)] = (byte) (val ? 1 : 0);
  }

  /**
   * Add a boolean for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addBoolean(String columnName, boolean val) {
    addBoolean(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's boolean
   * @param columnName name of the column to get data for
   * @return a boolean
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public boolean getBoolean(String columnName) {
    return getBoolean(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's boolean
   * @param columnIndex Column index in the schema
   * @return a boolean
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public boolean getBoolean(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BOOL);
    checkValue(columnIndex);
    byte b = rowAlloc[schema.getColumnOffset(columnIndex)];
    return b == 1;
  }

  /**
   * Add a byte for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addByte(int columnIndex, byte val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT8);
    rowAlloc[getPositionInRowAllocAndSetBitSet(columnIndex)] = val;
  }

  /**
   * Add a byte for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addByte(String columnName, byte val) {
    addByte(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's byte
   * @param columnName name of the column to get data for
   * @return a byte
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public byte getByte(String columnName) {
    return getByte(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's byte
   * @param columnIndex Column index in the schema
   * @return a byte
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public byte getByte(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT8);
    checkValue(columnIndex);
    return rowAlloc[schema.getColumnOffset(columnIndex)];
  }

  /**
   * Add a short for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addShort(int columnIndex, short val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT16);
    Bytes.setShort(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add a short for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addShort(String columnName, short val) {
    addShort(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's short
   * @param columnName name of the column to get data for
   * @return a short
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public short getShort(String columnName) {
    return getShort(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's short
   * @param columnIndex Column index in the schema
   * @return a short
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public short getShort(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT16);
    checkValue(columnIndex);
    return Bytes.getShort(rowAlloc, schema.getColumnOffset(columnIndex));
  }

  /**
   * Add an int for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addInt(int columnIndex, int val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT32);
    Bytes.setInt(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an int for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addInt(String columnName, int val) {
    addInt(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's integer
   * @param columnName name of the column to get data for
   * @return an integer
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public int getInt(String columnName) {
    return getInt(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's integer
   * @param columnIndex Column index in the schema
   * @return an integer
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public int getInt(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT32);
    checkValue(columnIndex);
    return Bytes.getInt(rowAlloc, schema.getColumnOffset(columnIndex));
  }

  /**
   * Add an long for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addLong(int columnIndex, long val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT64, Type.UNIXTIME_MICROS);
    Bytes.setLong(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an long for the specified column.
   *
   * If this is a UNIXTIME_MICROS column, the long value provided should be the number of
   * microseconds between a given time and January 1, 1970 UTC.
   * For example, to encode the current time, use setLong(System.currentTimeMillis() * 1000);
   *
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addLong(String columnName, long val) {
    addLong(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's long
   *
   * If this is a UNIXTIME_MICROS column, the long value corresponds to a number of microseconds
   * since midnight, January 1, 1970 UTC.
   *
   * @param columnName name of the column to get data for
   * @return a long
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public long getLong(String columnName) {
    return getLong(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's long
   *
   * If this is a UNIXTIME_MICROS column, the long value corresponds to a number of microseconds
   * since midnight, January 1, 1970 UTC.
   *
   * @param columnIndex Column index in the schema
   * @return a long
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public long getLong(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT64, Type.UNIXTIME_MICROS);
    checkColumnExists(schema.getColumnByIndex(columnIndex));
    checkValue(columnIndex);
    return Bytes.getLong(rowAlloc, schema.getColumnOffset(columnIndex));
  }

  /**
   * Add an float for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addFloat(int columnIndex, float val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.FLOAT);
    Bytes.setFloat(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an float for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addFloat(String columnName, float val) {
    addFloat(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's float
   * @param columnName name of the column to get data for
   * @return a float
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public float getFloat(String columnName) {
    return getFloat(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's float
   * @param columnIndex Column index in the schema
   * @return a float
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public float getFloat(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.FLOAT);
    checkValue(columnIndex);
    return Bytes.getFloat(rowAlloc, schema.getColumnOffset(columnIndex));
  }

  /**
   * Add an double for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addDouble(int columnIndex, double val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.DOUBLE);
    Bytes.setDouble(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an double for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addDouble(String columnName, double val) {
    addDouble(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's double
   * @param columnName name of the column to get data for
   * @return a double
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public double getDouble(String columnName) {
    return getDouble(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's double
   * @param columnIndex Column index in the schema
   * @return a double
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public double getDouble(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.DOUBLE);
    checkValue(columnIndex);
    return Bytes.getDouble(rowAlloc, schema.getColumnOffset(columnIndex));
  }

  /**
   * Add a Decimal for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addDecimal(int columnIndex, BigDecimal val) {
    checkNotFrozen();
    ColumnSchema column = schema.getColumnByIndex(columnIndex);
    ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
    checkColumn(column, Type.DECIMAL);
    BigDecimal coercedVal = DecimalUtil.coerce(val,typeAttributes.getPrecision(),
        typeAttributes.getScale());
    Bytes.setBigDecimal(rowAlloc, coercedVal, typeAttributes.getPrecision(),
        getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add a Decimal for the specified column.
   *
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addDecimal(String columnName, BigDecimal val) {
    addDecimal(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's BigDecimal
   *
   * @param columnName name of the column to get data for
   * @return a BigDecimal
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public BigDecimal getDecimal(String columnName) {
    return getDecimal(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's Decimal.
   *
   * @param columnIndex Column index in the schema
   * @return a BigDecimal
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public BigDecimal getDecimal(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.DECIMAL);
    checkColumnExists(schema.getColumnByIndex(columnIndex));
    checkValue(columnIndex);
    ColumnSchema column = schema.getColumnByIndex(columnIndex);
    ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
    return Bytes.getDecimal(rowAlloc, schema.getColumnOffset(columnIndex),
        typeAttributes.getPrecision(), typeAttributes.getScale());
  }

  /**
   * Add a Timestamp for the specified column.
   *
   * Note: Timestamp instances with nanosecond precision are truncated to microseconds.
   *
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addTimestamp(int columnIndex, Timestamp val) {
    checkNotFrozen();
    ColumnSchema column = schema.getColumnByIndex(columnIndex);
    checkColumn(column, Type.UNIXTIME_MICROS);
    long micros = TimestampUtil.timestampToMicros(val);
    Bytes.setLong(rowAlloc, micros, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add a Timestamp for the specified column.
   *
   * Note: Timestamp instances with nanosecond precision are truncated to microseconds.
   *
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addTimestamp(String columnName, Timestamp val) {
    addTimestamp(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's Timestamp.
   *
   * @param columnName name of the column to get data for
   * @return a Timestamp
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public Timestamp getTimestamp(String columnName) {
    return getTimestamp(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's Timestamp.
   *
   * @param columnIndex Column index in the schema
   * @return a Timestamp
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public Timestamp getTimestamp(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.UNIXTIME_MICROS);
    checkColumnExists(schema.getColumnByIndex(columnIndex));
    checkValue(columnIndex);
    long micros = Bytes.getLong(rowAlloc, schema.getColumnOffset(columnIndex));
    return TimestampUtil.microsToTimestamp(micros);
  }

  /**
   * Add a sql.Date for the specified column.
   *
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addDate(int columnIndex, Date val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.DATE);
    int days = DateUtil.sqlDateToEpochDays(val);
    Bytes.setInt(rowAlloc, days, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add a Date for the specified column.
   *
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addDate(String columnName, Date val) {
    addDate(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's Date.
   *
   * @param columnName name of the column to get data for
   * @return a Date
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public Date getDate(String columnName) {
    return getDate(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's Date.
   *
   * @param columnIndex Column index in the schema
   * @return a Date
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public Date getDate(int columnIndex) {
    checkColumnExists(schema.getColumnByIndex(columnIndex));
    checkColumn(schema.getColumnByIndex(columnIndex), Type.DATE);
    checkValue(columnIndex);
    int days = Bytes.getInt(rowAlloc, schema.getColumnOffset(columnIndex));
    return DateUtil.epochDaysToSqlDate(days);
  }

  /**
   * Add a String for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addString(int columnIndex, String val) {
    addStringUtf8(columnIndex, Bytes.fromString(val));
  }

  /**
   * Add a String for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addString(String columnName, String val) {
    addStringUtf8(columnName, Bytes.fromString(val));
  }

  /**
   * Add a VARCHAR for the specified column.
   *
   * Truncates val to the length of the column in characters.
   *
   * @param columnIndex Index of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist, is the wrong type
   *         or the string is not UTF-8
   * @throws IllegalStateException if the row was already applied
   */
  public void addVarchar(int columnIndex, String val) {
    ColumnSchema column = schema.getColumnByIndex(columnIndex);
    checkColumn(column, Type.VARCHAR);
    checkNotFrozen();
    int length = column.getTypeAttributes().getLength();
    if (length < val.length()) {
      val = val.substring(0, length);
    }
    byte[] bytes = Bytes.fromString(val);
    addVarLengthData(columnIndex, bytes);
  }

  /**
   * Add a VARCHAR for the specified column.
   *
   * Truncates val to the length of the column in characters.
   *
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist, is the wrong type
   *         or the string is not UTF-8
   * @throws IllegalStateException if the row was already applied
   */
  public void addVarchar(String columnName, String val) {
    addVarchar(schema.getColumnIndex(columnName), val);
  }

  /**
   * Get the specified column's string.
   * @param columnName name of the column to get data for
   * @return a string
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public String getString(String columnName) {
    return getString(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's string.
   * @param columnIndex Column index in the schema
   * @return a string
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public String getString(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.STRING);
    checkValue(columnIndex);
    return new String(getVarLengthData(columnIndex).array(), StandardCharsets.UTF_8);
  }

  /**
   * Get the specified column's VARCHAR.
   * @param columnName Name of the column to get the data for
   * @return a VARCHAR
   * @throws IllegalArgumentException if the column is null, is unset,
   *         or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public String getVarchar(String columnName) {
    return getVarchar(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's VARCHAR.
   * @param columnIndex Column index in the schema
   * @return a VARCHAR
   * @throws IllegalArgumentException if the column is null, is unset,
   *         or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public String getVarchar(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.VARCHAR);
    checkValue(columnIndex);
    return new String(getVarLengthData(columnIndex).array(), StandardCharsets.UTF_8);
  }

  /**
   * Add a String for the specified value, encoded as UTF8.
   * Note that the provided value must not be mutated after this.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addStringUtf8(int columnIndex, byte[] val) {
    // TODO: use Utf8.isWellFormed from Guava 16 to verify that.
    // the user isn't putting in any garbage data.
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.STRING);
    addVarLengthData(columnIndex, val);
  }

  /**
   * Add a String for the specified value, encoded as UTF8.
   * Note that the provided value must not be mutated after this.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   *
   */
  public void addStringUtf8(String columnName, byte[] val) {
    addStringUtf8(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add binary data with the specified value.
   * Note that the provided value must not be mutated after this.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addBinary(int columnIndex, byte[] val) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BINARY);
    addVarLengthData(columnIndex, val);
  }

  /**
   * Add binary data with the specified value, from the current ByteBuffer's position to its limit.
   * This method duplicates the ByteBuffer but doesn't copy the data. This means that the wrapped
   * data must not be mutated after this.
   * @param columnIndex the column's index in the schema
   * @param value byte buffer to get the value from
   * @throws IllegalArgumentException if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addBinary(int columnIndex, ByteBuffer value) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BINARY);
    addVarLengthData(columnIndex, value);
  }

  /**
   * Add binary data with the specified value.
   * Note that the provided value must not be mutated after this.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addBinary(String columnName, byte[] val) {
    addBinary(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add binary data with the specified value, from the current ByteBuffer's position to its limit.
   * This method duplicates the ByteBuffer but doesn't copy the data. This means that the wrapped
   * data must not be mutated after this.
   * @param columnName Name of the column
   * @param value byte buffer to get the value from
   * @throws IllegalArgumentException if the column doesn't exist
   * or if the value doesn't match the column's type
   * @throws IllegalStateException if the row was already applied
   */
  public void addBinary(String columnName, ByteBuffer value) {
    addBinary(schema.getColumnIndex(columnName), value);
  }

  /**
   * Get a copy of the specified column's binary data.
   * @param columnName name of the column to get data for
   * @return a byte[] with the binary data.
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public byte[] getBinaryCopy(String columnName) {
    return getBinaryCopy(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get a copy of the specified column's binary data.
   * @param columnIndex Column index in the schema
   * @return a byte[] with the binary data.
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public byte[] getBinaryCopy(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BINARY);
    checkValue(columnIndex);
    byte[] data = getVarLengthData(columnIndex).array();
    byte[] ret = new byte[data.length];
    System.arraycopy(data, 0, ret, 0, data.length);
    return ret;
  }

  /**
   * Get the specified column's binary data.
   *
   * This doesn't copy the data and instead returns a ByteBuffer that wraps it.
   *
   * @param columnName name of the column to get data for
   * @return a ByteBuffer with the binary data.
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public ByteBuffer getBinary(String columnName) {
    return getBinary(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's binary data.
   *
   * This doesn't copy the data and instead returns a ByteBuffer that wraps it.
   *
   * @param columnIndex Column index in the schema
   * @return a ByteBuffer with the binary data.
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public ByteBuffer getBinary(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BINARY);
    checkValue(columnIndex);
    return getVarLengthData(columnIndex);
  }

  /**
   * Returns the logical contents of an array column as a boxed Java array.
   *
   * <p>This mirrors {@link RowResult#getArrayData(int)} but for PartialRow.</p>
   *
   * @param columnIndex index of the array column
   * @return a boxed Java array (e.g., {@code Integer[]}, {@code String[]}, {@code BigDecimal[]})
   *         or {@code null} if the column is unset or null
   * @throws IllegalArgumentException if the column is not an array type
   */
  @Nullable
  public final Object getArrayData(int columnIndex) {
    ColumnSchema col = schema.getColumnByIndex(columnIndex);
    checkColumn(col, Type.NESTED);

    if (col.getNestedTypeDescriptor() == null || !col.getNestedTypeDescriptor().isArray()) {
      throw new IllegalArgumentException(String.format(
          "Column %s is NESTED but not an array descriptor", col.getName()));
    }
    if (isNull(columnIndex) || !isSet(columnIndex)) {
      return null;
    }
    ArrayCellView view = getArray(columnIndex);
    return ArrayCellViewHelper.toJavaArray(view, col);
  }

  public final Object getArrayData(String columnName) {
    return getArrayData(schema.getColumnIndex(columnName));
  }

  /**
   * Normalize a validity array for object arrays where elements may be null.
   * For primitive arrays (e.g. boolean[]), callers can pass validity directly.
   *
   * Behavior:
   * - If {@code validity == null}, infer validity by marking each non-null element {@code true}.
   * - If {@code validity.length == values.length}, validate length and mask out any null elements
   *   (i.e., null values are always marked invalid regardless of the validity bit).
   * - If {@code validity.length == 0}, treat it as an optimization meaning "all elements valid",
   *   but only if all {@code values[]} are non-null; otherwise throw
   *   {@link IllegalArgumentException}.
   *
   * This ensures consistency with serialization rules, where an empty validity vector
   * is accepted only when all elements are valid (non-null).
   */

  private static <T> boolean[] normalizeValidity(T[] values, boolean[] validity) {
    if (values == null) {
      return null;
    }

    // explicit non-empty validity vector
    if (validity != null && validity.length != 0) {
      if (validity.length != values.length) {
        throw new IllegalArgumentException(
            "validity length mismatch: " + validity.length + " vs " + values.length);
      }
      boolean[] corrected = new boolean[values.length];
      for (int i = 0; i < values.length; i++) {
        corrected[i] = (values[i] != null) && validity[i];
      }
      return corrected;
    }

    // empty validity vector => all valid, must have no nulls
    if (validity != null && validity.length == 0) {
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) {
          throw new IllegalArgumentException(
              String.format("Empty validity vector provided, but values[%d] is null", i));
        }
      }
      boolean[] allValid = new boolean[values.length];
      java.util.Arrays.fill(allValid, true);
      return allValid;
    }

    // validity == null => infer validity from non-nulls
    boolean[] inferred = new boolean[values.length];
    for (int i = 0; i < values.length; i++) {
      inferred[i] = (values[i] != null);
    }
    return inferred;
  }

  private void checkValidityLength(int valuesLen, boolean[] validity) {
    if (validity != null && validity.length != 0 && validity.length != valuesLen) {
      throw new IllegalArgumentException(
          "validity length mismatch: " + validity.length + " vs " + valuesLen);
    }
  }


  /**
   * Adds array-typed column values to this row and defines how validity (nullability)
   * is interpreted for array elements.
   *
   * <p>Each {@code addArray*()} method writes a complete array cell into the row.
   * The caller may optionally provide a {@code validity} vector to control which
   * elements are null or valid:
   * <ul>
   *   <li>If {@code validity == null}, validity is inferred from whether each
   *       array element is {@code null}.</li>
   *   <li>If {@code validity.length == 0}, it signals that all elements are valid;
   *       this is only allowed when the array contains no nulls.</li>
   *   <li>If {@code validity.length > 0}, it must match the array length; each
   *       element is valid only if both the value is non-null and the corresponding
   *       validity bit is {@code true}.</li>
   * </ul>
   *
   * <p>For primitive arrays (which cannot contain nulls), the validity vector
   * is ignored, and the entire array is treated as valid. Empty or all-true
   * validity masks are omitted from serialization for efficiency.
   */

  public void addArrayInt8(int columnIndex, byte[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }
    checkValidityLength(values.length, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeInt8(values, validity));
  }

  public void addArrayInt8(String columnName, byte[] values, boolean[] validity) {
    addArrayInt8(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayInt8(int columnIndex, byte[] values) {
    addArrayInt8(columnIndex, values, null);
  }

  public void addArrayInt8(String columnName, byte[] values) {
    addArrayInt8(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayInt16(int columnIndex, short[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }
    checkValidityLength(values.length, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeInt16(values, validity));
  }

  public void addArrayInt16(String columnName, short[] values, boolean[] validity) {
    addArrayInt16(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayInt16(int columnIndex, short[] values) {
    addArrayInt16(columnIndex, values, null);
  }

  public void addArrayInt16(String columnName, short[] values) {
    addArrayInt16(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayInt32(int columnIndex, int[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }
    checkValidityLength(values.length, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeInt32(values, validity));
  }

  public void addArrayInt32(String columnName, int[] values, boolean[] validity) {
    addArrayInt32(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayInt32(int columnIndex, int[] values) {
    addArrayInt32(columnIndex, values, null);
  }

  public void addArrayInt32(String columnName, int[] values) {
    addArrayInt32(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayInt64(int columnIndex, long[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }
    checkValidityLength(values.length, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeInt64(values, validity));
  }

  public void addArrayInt64(String columnName, long[] values, boolean[] validity) {
    addArrayInt64(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayInt64(int columnIndex, long[] values) {
    addArrayInt64(columnIndex, values, null);
  }

  public void addArrayInt64(String columnName, long[] values) {
    addArrayInt64(schema.getColumnIndex(columnName), values, null);
  }


  public void addArrayFloat(int columnIndex, float[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }
    checkValidityLength(values.length, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeFloat(values, validity));
  }

  public void addArrayFloat(String columnName, float[] values, boolean[] validity) {
    addArrayFloat(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayFloat(int columnIndex, float[] values) {
    addArrayFloat(columnIndex, values, null);
  }

  public void addArrayFloat(String columnName, float[] values) {
    addArrayFloat(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayDouble(int columnIndex, double[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }
    checkValidityLength(values.length, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeDouble(values, validity));
  }

  public void addArrayDouble(String columnName, double[] values, boolean[] validity) {
    addArrayDouble(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayDouble(int columnIndex, double[] values) {
    addArrayDouble(columnIndex, values, null);
  }

  public void addArrayDouble(String columnName, double[] values) {
    addArrayDouble(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayString(int columnIndex, String[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }

    boolean[] valids = normalizeValidity(values, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeString(values, valids));
  }


  public void addArrayString(String columnName, String[] values, boolean[] validity) {
    addArrayString(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayString(int columnIndex, String[] values) {
    addArrayString(columnIndex, values, null);
  }

  public void addArrayString(String columnName, String[] values) {
    addArrayString(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayBinary(int columnIndex, byte[][] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }

    boolean[] valids = normalizeValidity(values, validity);
    addVarLengthData(columnIndex, Array1dSerdes.serializeBinary(values, valids));
  }

  public void addArrayBinary(String columnName, byte[][] values, boolean[] validity) {
    addArrayBinary(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayBinary(int columnIndex, byte[][] values) {
    addArrayBinary(columnIndex, values, null);
  }

  public void addArrayBinary(String columnName, byte[][] values) {
    addArrayBinary(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayBool(int columnIndex, boolean[] values, boolean[] validity) {
    checkNotFrozen();
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);

    if (values == null) {
      setNull(columnIndex);
      return;
    }
    checkValidityLength(values.length, validity);
    byte[] asBytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      asBytes[i] = (byte)(values[i] ? 1 : 0);
    }

    addVarLengthData(columnIndex, Array1dSerdes.serializeUInt8(asBytes, validity));
  }

  public void addArrayBool(String columnName, boolean[] values, boolean[] validity) {
    addArrayBool(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayBool(int columnIndex, boolean[] values) {
    addArrayBool(columnIndex, values, null);
  }

  public void addArrayBool(String columnName, boolean[] values) {
    addArrayBool(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayTimestamp(int columnIndex, Timestamp[] values) {
    addArrayTimestampInternal(columnIndex, values, null);
  }

  public void addArrayTimestamp(String columnName, Timestamp[] values) {
    addArrayTimestamp(schema.getColumnIndex(columnName), values);
  }

  public void addArrayTimestamp(int columnIndex,
                                Timestamp[] values,
                                boolean[] validity) {
    addArrayTimestampInternal(columnIndex, values, validity);
  }

  public void addArrayTimestamp(String columnName,
                                Timestamp[] values,
                                boolean[] validity) {
    addArrayTimestamp(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayDate(int columnIndex, Date[] values) {
    addArrayDateInternal(columnIndex, values, null);
  }

  public void addArrayDate(String columnName, Date[] values) {
    addArrayDate(schema.getColumnIndex(columnName), values);
  }

  public void addArrayDate(int columnIndex, Date[] values, boolean[] validity) {
    addArrayDateInternal(columnIndex, values, validity);
  }

  public void addArrayDate(String columnName, Date[] values, boolean[] validity) {
    addArrayDate(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayVarchar(int columnIndex, String[] values) {
    addArrayVarcharInternal(columnIndex, values, null);
  }

  public void addArrayVarchar(String columnName, String[] values) {
    addArrayVarchar(schema.getColumnIndex(columnName), values, null);
  }

  public void addArrayVarchar(int columnIndex, String[] values, boolean[] validity) {
    addArrayVarcharInternal(columnIndex, values, validity);
  }

  public void addArrayVarchar(String columnName, String[] values, boolean[] validity) {
    addArrayVarchar(schema.getColumnIndex(columnName), values, validity);
  }

  public void addArrayDecimal(int columnIndex, BigDecimal[] values) {
    addArrayDecimalInternal(columnIndex, values, null);
  }

  public void addArrayDecimal(String columnName, BigDecimal[] values) {
    addArrayDecimal(schema.getColumnIndex(columnName), values);
  }

  public void addArrayDecimal(int columnIndex,
                              BigDecimal[] values,
                              boolean[] validity) {
    addArrayDecimalInternal(columnIndex, values, validity);
  }

  public void addArrayDecimal(String columnName,
                              BigDecimal[] values,
                              boolean[] validity) {
    addArrayDecimal(schema.getColumnIndex(columnName), values, validity);
  }

  private ArrayCellView getArray(int columnIndex) {
    checkColumn(schema.getColumnByIndex(columnIndex), ARRAY_TYPE);
    checkValue(columnIndex);
    return new ArrayCellView(getVarLenBytes(columnIndex));
  }

  private void addArrayTimestampInternal(int columnIndex,
                                         Timestamp[] values,
                                         boolean[] validity) {
    checkNotFrozen();

    if (values == null) {
      setNull(columnIndex);
      return;
    }

    boolean[] valids = normalizeValidity(values, validity);
    long[] micros = new long[values.length];

    for (int i = 0; i < values.length; i++) {
      if (valids[i]) {
        micros[i] = TimestampUtil.timestampToMicros(values[i]);
      }
    }

    addArrayInt64(columnIndex, micros, valids);
  }

  private void addArrayVarcharInternal(int columnIndex,
                                       String[] values,
                                       boolean[] validity) {
    checkNotFrozen();
    ColumnSchema col = schema.getColumnByIndex(columnIndex);
    int maxLen = col.getTypeAttributes().getLength();

    if (values == null) {
      setNull(columnIndex);
      return;
    }

    boolean[] valids = normalizeValidity(values, validity);
    String[] truncated = new String[values.length];

    for (int i = 0; i < values.length; i++) {
      if (valids[i]) {
        String s = values[i];
        truncated[i] = (s.length() > maxLen) ? s.substring(0, maxLen) : s;
      } else {
        truncated[i] = ""; // content unused when validity[i] is false
      }
    }

    addVarLengthData(columnIndex, Array1dSerdes.serializeString(truncated, valids));
  }

  private void addArrayDateInternal(int columnIndex, Date[] values, boolean[] validity) {
    checkNotFrozen();

    if (values == null) {
      setNull(columnIndex);
      return;
    }

    boolean[] valids = normalizeValidity(values, validity);
    int[] days = new int[values.length];

    for (int i = 0; i < values.length; i++) {
      if (valids[i]) {
        days[i] = DateUtil.sqlDateToEpochDays(values[i]);
      }
    }

    addArrayInt32(columnIndex, days, valids);
  }

  private void addArrayDecimalInternal(int columnIndex,
                                       BigDecimal[] values,
                                       boolean[] validity) {
    checkNotFrozen();
    ColumnSchema col = schema.getColumnByIndex(columnIndex);
    ColumnTypeAttributes attrs = col.getTypeAttributes();
    int precision = attrs.getPrecision();
    int scale = attrs.getScale();

    if (values == null) {
      setNull(columnIndex);
      return;
    }

    boolean[] valids = normalizeValidity(values, validity);

    if (precision <= 9) {
      int[] unscaled = new int[values.length];
      for (int i = 0; i < values.length; i++) {
        if (valids[i]) {
          BigDecimal bd = values[i].setScale(scale, RoundingMode.UNNECESSARY);
          unscaled[i] = bd.unscaledValue().intValueExact();
        }
      }
      addVarLengthData(columnIndex, Array1dSerdes.serializeInt32(unscaled, valids));

    } else if (precision <= 18) {
      long[] unscaled = new long[values.length];
      for (int i = 0; i < values.length; i++) {
        if (valids[i]) {
          BigDecimal bd = values[i].setScale(scale, RoundingMode.UNNECESSARY);
          unscaled[i] = bd.unscaledValue().longValueExact();
        }
      }
      addVarLengthData(columnIndex, Array1dSerdes.serializeInt64(unscaled, valids));

    } else {
      throw new IllegalStateException("DECIMAL128 arrays not supported yet");
    }
  }

  private void addVarLengthData(int columnIndex, byte[] val) {
    addVarLengthData(columnIndex, ByteBuffer.wrap(val));
  }

  private void addVarLengthData(int columnIndex, ByteBuffer val) {
    // A duplicate will copy all the original's metadata but still point to the same content.
    ByteBuffer duplicate = val.duplicate();
    // Mark the current position so we can reset to it.
    duplicate.mark();

    varLengthData.set(columnIndex, duplicate);
    // Set the usage bit but we don't care where it is.
    getPositionInRowAllocAndSetBitSet(columnIndex);
    // We don't set anything in row alloc, it will be managed at encoding time.
  }

  /**
   * Get the list variable length data cells that were added to this row.
   * @return a list of binary data, may be empty
   */
  List<ByteBuffer> getVarLengthData() {
    return varLengthData;
  }

  private ByteBuffer getVarLengthData(int columnIndex) {
    return varLengthData.get(columnIndex).duplicate();
  }

  /**
   * Set the specified column to null
   * @param columnIndex the column's index in the schema
   * @throws IllegalArgumentException if the column doesn't exist or cannot be set to null
   * @throws IllegalStateException if the row was already applied
   */
  public void setNull(int columnIndex) {
    setNull(this.schema.getColumnByIndex(columnIndex));
  }

  /**
   * Set the specified column to null
   * @param columnName Name of the column
   * @throws IllegalArgumentException if the column doesn't exist or cannot be set to null
   * @throws IllegalStateException if the row was already applied
   */
  public void setNull(String columnName) {
    setNull(this.schema.getColumn(columnName));
  }

  private void setNull(ColumnSchema column) {
    assert nullsBitSet != null;
    checkNotFrozen();
    checkColumnExists(column);
    if (!column.isNullable()) {
      throw new IllegalArgumentException(column.getName() + " cannot be set to null");
    }
    int idx = schema.getColumns().indexOf(column);
    columnsBitSet.set(idx);
    nullsBitSet.set(idx);
  }

  /**
   * Get if the specified column is NULL
   * @param columnName name of the column in the schema
   * @return true if the column cell is null and the column is nullable,
   * false otherwise
   * @throws IllegalArgumentException if the column doesn't exist
   */
  public boolean isNull(String columnName) {
    return isNull(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get if the specified column is NULL
   * @param columnIndex Column index in the schema
   * @return true if the column cell is null and the column is nullable,
   * false otherwise
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public boolean isNull(int columnIndex) {
    checkColumnExists(schema.getColumnByIndex(columnIndex));
    return schema.getColumnByIndex(columnIndex).isNullable() && isSetToNull(columnIndex);
  }

  /**
   * Add the specified column's value as an Object.
   *
   * This method is useful when you don't care about autoboxing
   * and your existing type handling logic is based on Java types.
   *
   * The accepted Object type is based on the column's {@link Type}:
   *  Type.BOOL -> java.lang.Boolean
   *  Type.INT8 -> java.lang.Byte
   *  Type.INT16 -> java.lang.Short
   *  Type.INT32 -> java.lang.Integer
   *  Type.INT64 -> java.lang.Long
   *  Type.UNIXTIME_MICROS -> java.sql.Timestamp or java.lang.Long
   *  Type.FLOAT -> java.lang.Float
   *  Type.DOUBLE -> java.lang.Double
   *  Type.STRING -> java.lang.String
   *  Type.VARCHAR -> java.lang.String
   *  Type.BINARY -> byte[] or java.lang.ByteBuffer
   *  Type.DECIMAL -> java.math.BigDecimal
   *
   * @param columnName name of the column in the schema
   * @param val the value to add as an Object
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public void addObject(String columnName, Object val) {
    addObject(this.schema.getColumnIndex(columnName), val);
  }

  /**
   * Add the specified column's value as an Object.
   *
   * This method is useful when you don't care about autoboxing
   * and your existing type handling logic is based on Java types.
   *
   * The accepted Object type is based on the column's {@link Type}:
   *  Type.BOOL -> java.lang.Boolean
   *  Type.INT8 -> java.lang.Byte
   *  Type.INT16 -> java.lang.Short
   *  Type.INT32 -> java.lang.Integer
   *  Type.INT64 -> java.lang.Long
   *  Type.UNIXTIME_MICROS -> java.sql.Timestamp or java.lang.Long
   *  Type.FLOAT -> java.lang.Float
   *  Type.DOUBLE -> java.lang.Double
   *  Type.STRING -> java.lang.String
   *  Type.VARCHAR -> java.lang.String
   *  Type.BINARY -> byte[] or java.lang.ByteBuffer
   *  Type.DECIMAL -> java.math.BigDecimal
   *  Type.DATE -> java.sql.Date
   *  Type.NESTED (array)    -> one of:
   *     - Primitive arrays: byte[], short[], int[], long[], float[], double[], boolean[]
   *     - Object arrays: java.lang.String[], java.sql.Date[], java.sql.Timestamp[],
   *                      java.math.BigDecimal[], byte[][]
   *     - Already-wrapped: {@link ArrayCellView} (serialized form)
   *
   * For arrays, {@code validity} is always passed as {@code null} from here:
   *  - Primitive arrays cannot contain null elements, so all entries are valid.
   *  - Object arrays may contain nulls; the corresponding addArray* methods
   *    internally reconstruct validity masks via {@code normalizeValidity()}.
   * This keeps {@code addObject()} simple while ensuring correct null handling.
   *
   * Array serializers also treat {@code validity.length == 0}
   * the same as {@code null}, meaning "all elements valid". This allows callers
   * to omit the validity vector for fully non-null data.
   *
   * @param columnIndex column index in the schema
   * @param val the value to add as an Object
   * @throws IllegalStateException if the row was already applied
   * @throws IndexOutOfBoundsException if the column doesn't exist
   * @throws IllegalArgumentException if the value type is incompatible
   */
  public void addObject(int columnIndex, Object val) {
    checkNotFrozen();
    ColumnSchema col = schema.getColumnByIndex(columnIndex);
    checkColumnExists(col);
    try {
      if (val == null) {
        setNull(columnIndex);
        return;
      }
      switch (col.getType()) {
        case BOOL:
          addBoolean(columnIndex, (Boolean) val);
          break;
        case INT8:
          addByte(columnIndex, (Byte) val);
          break;
        case INT16:
          addShort(columnIndex, (Short) val);
          break;
        case INT32:
          addInt(columnIndex, (Integer) val);
          break;
        case INT64:
          addLong(columnIndex, (Long) val);
          break;
        case UNIXTIME_MICROS:
          if (val instanceof Timestamp) {
            addTimestamp(columnIndex, (Timestamp) val);
          } else {
            addLong(columnIndex, (Long) val);
          }
          break;
        case FLOAT:
          addFloat(columnIndex, (Float) val);
          break;
        case DOUBLE:
          addDouble(columnIndex, (Double) val);
          break;
        case STRING:
          addString(columnIndex, (String) val);
          break;
        case VARCHAR:
          addVarchar(columnIndex, (String) val);
          break;
        case DATE:
          addDate(columnIndex, (Date) val);
          break;
        case BINARY:
          if (val instanceof byte[]) {
            addBinary(columnIndex, (byte[]) val);
          } else {
            addBinary(columnIndex, (ByteBuffer) val);
          }
          break;
        case DECIMAL:
          addDecimal(columnIndex, (BigDecimal) val);
          break;
        case /*ARRAY*/ NESTED: {
          // Note: we always pass `validity = null` here.
          //   - For primitive arrays: elements cannot be null. Hence, all entries valid.
          //   - For object arrays: the addArray* methods call normalizeValidity()
          //     to infer nulls from the values[]. So validity is reconstructed.
          // This keeps addObject() simple while ensuring nulls are handled correctly.
          if (val instanceof byte[]) {
            addArrayInt8(columnIndex, (byte[]) val, null);
            break;
          }
          if (val instanceof short[]) {
            addArrayInt16(columnIndex, (short[]) val, null);
            break;
          }
          if (val instanceof int[]) {
            addArrayInt32(columnIndex, (int[]) val, null);
            break;
          }
          if (val instanceof long[]) {
            addArrayInt64(columnIndex, (long[]) val, null);
            break;
          }
          if (val instanceof boolean[]) {
            addArrayBool(columnIndex, (boolean[]) val, null);
            break;
          }
          if (val instanceof float[]) {
            addArrayFloat(columnIndex, (float[]) val, null);
            break;
          }
          if (val instanceof double[]) {
            addArrayDouble(columnIndex, (double[]) val, null);
            break;
          }
          if (val instanceof Timestamp[]) {
            addArrayTimestamp(columnIndex, (Timestamp[]) val);
            break;
          }
          if (val instanceof String[]) {
            if (col.getNestedTypeDescriptor().getArrayDescriptor().getElemType() == Type.VARCHAR) {
              addArrayVarchar(columnIndex, (String[]) val, null);
            } else {
              addArrayString(columnIndex, (String[]) val, null);
            }
            break;
          }
          if (val instanceof Date[]) {
            addArrayDate(columnIndex, (Date[]) val);
            break;
          }
          if (val instanceof byte[][]) {
            addArrayBinary(columnIndex, (byte[][]) val, null);
            break;
          }
          if (val instanceof BigDecimal[]) {
            addArrayDecimal(columnIndex, (BigDecimal[]) val, null);
            break;
          }
          // Allow pre-built serialized FlatBuffer payloads.
          if (val instanceof ByteBuffer) {
            addVarLengthData(columnIndex, (ByteBuffer) val);
            break;
          }
          if (val instanceof ArrayCellView) {
            addVarLengthData(columnIndex, ((ArrayCellView) val).toBytes());
            break;
          }

          throw new IllegalArgumentException(
              "Unsupported object type for array column " + col.getName());
        }
        default:
          throw new IllegalArgumentException("Unsupported column type: " + col.getType());
      }
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Value type does not match column type " + col.getType() +
              " for column " + col.getName());
    }
  }

  /**
   * Get the specified column's value as an Object.
   *
   * This method is useful when you don't care about autoboxing
   * and your existing type handling logic is based on Java types.
   *
   * The Object type is based on the column's {@link Type}:
   *  Type.BOOL -> java.lang.Boolean
   *  Type.INT8 -> java.lang.Byte
   *  Type.INT16 -> java.lang.Short
   *  Type.INT32 -> java.lang.Integer
   *  Type.INT64 -> java.lang.Long
   *  Type.UNIXTIME_MICROS -> java.sql.Timestamp
   *  Type.FLOAT -> java.lang.Float
   *  Type.DOUBLE -> java.lang.Double
   *  Type.STRING -> java.lang.String
   *  Type.VARCHAR -> java.lang.String
   *  Type.BINARY -> byte[]
   *  Type.DECIMAL -> java.math.BigDecimal
   *  Type.DATE -> java.sql.Date
   *
   * @param columnName name of the column in the schema
   * @return the column's value as an Object, null if the column value is null or unset
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public Object getObject(String columnName) {
    return getObject(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's value as an Object.
   *
   * This method is useful when you don't care about autoboxing
   * and your existing type handling logic is based on Java types.
   *
   * The Object type is based on the column's {@link Type}:
   *  Type.BOOL -> java.lang.Boolean
   *  Type.INT8 -> java.lang.Byte
   *  Type.INT16 -> java.lang.Short
   *  Type.INT32 -> java.lang.Integer
   *  Type.INT64 -> java.lang.Long
   *  Type.UNIXTIME_MICROS -> java.sql.Timestamp
   *  Type.FLOAT -> java.lang.Float
   *  Type.DOUBLE -> java.lang.Double
   *  Type.STRING -> java.lang.String
   *  Type.VARCHAR -> java.lang.String
   *  Type.BINARY -> byte[]
   *  Type.DECIMAL -> java.math.BigDecimal
   *
   * @param columnIndex Column index in the schema
   * @return the column's value as an Object, null if the column value is null or unset
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public Object getObject(int columnIndex) {
    checkColumnExists(schema.getColumnByIndex(columnIndex));
    if (isNull(columnIndex) || !isSet(columnIndex)) {
      return null;
    }
    Type type = schema.getColumnByIndex(columnIndex).getType();
    switch (type) {
      case BOOL: return getBoolean(columnIndex);
      case INT8: return getByte(columnIndex);
      case INT16: return getShort(columnIndex);
      case INT32: return getInt(columnIndex);
      case INT64: return getLong(columnIndex);
      case DATE: return getDate(columnIndex);
      case UNIXTIME_MICROS: return getTimestamp(columnIndex);
      case FLOAT: return getFloat(columnIndex);
      case DOUBLE: return getDouble(columnIndex);
      case VARCHAR: return getVarchar(columnIndex);
      case STRING: return getString(columnIndex);
      case BINARY: return getBinaryCopy(columnIndex);
      case DECIMAL: return getDecimal(columnIndex);
      case NESTED: return getArray(columnIndex);
      default: throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  /**
   * Verifies if the column exists and belongs to one of the specified types
   * @param column column the user wants to set
   * @param types types we expect
   * @throws IllegalArgumentException if the column or type was invalid
   */
  private void checkColumn(ColumnSchema column, Type... types) {
    checkColumnExists(column);
    for (Type type : types) {
      if (column.getType().equals(type)) {
        return;
      }
    }
    throw new IllegalArgumentException(String.format("%s isn't %s, it's %s", column.getName(),
        Arrays.toString(types), column.getType().getName()));
  }

  /**
   * @param column column the user wants to set
   * @throws IllegalArgumentException if the column doesn't exist
   */
  private void checkColumnExists(ColumnSchema column) {
    if (column == null) {
      throw new IllegalArgumentException("Column name isn't present in the table's schema");
    }
  }

  /**
   * @param columnIndex Column index in the schema
   * @throws IllegalArgumentException if the column is unset or null
   */
  private void checkValue(int columnIndex) {
    if (!isSet(columnIndex)) {
      throw new IllegalArgumentException("Column value is not set");
    }

    if (isNull(columnIndex)) {
      throw new IllegalArgumentException("Column value is null");
    }
  }

  /**
   * @throws IllegalStateException if the row was already applied
   */
  private void checkNotFrozen() {
    if (frozen) {
      throw new IllegalStateException("This row was already applied and cannot be modified.");
    }
  }

  /**
   * Sets the column bit set for the column index, and returns the column's offset.
   * @param columnIndex the index of the column to get the position for and mark as set
   * @return the offset in rowAlloc for the column
   */
  private int getPositionInRowAllocAndSetBitSet(int columnIndex) {
    columnsBitSet.set(columnIndex);
    return schema.getColumnOffset(columnIndex);
  }

  /**
   * Get if the specified column has been set
   * @param columnName name of the column in the schema
   * @return true if the column has been set
   * @throws IllegalArgumentException if the column doesn't exist
   */
  public boolean isSet(String columnName) {
    return isSet(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get if the specified column has been set
   * @param columnIndex Column index in the schema
   * @return true if the column has been set
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public boolean isSet(int columnIndex) {
    checkColumnExists(schema.getColumnByIndex(columnIndex));
    return this.columnsBitSet.get(columnIndex);
  }

  /**
   * Tells if the specified column was set to null by the user
   * @param column column's index in the schema
   * @return true if it was set, else false
   */
  boolean isSetToNull(int column) {
    if (this.nullsBitSet == null) {
      return false;
    }
    return this.nullsBitSet.get(column);
  }

  /**
   * Returns the encoded primary key of the row.
   * @return a byte array containing an encoded primary key
   */
  public byte[] encodePrimaryKey() {
    return KeyEncoder.encodePrimaryKey(this);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    int numCols = schema.getColumnCount();
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    boolean first = true;
    for (int idx = 0; idx < numCols; ++idx) {
      if (!columnsBitSet.get(idx)) {
        continue;
      }

      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }

      ColumnSchema col = schema.getColumnByIndex(idx);
      sb.append(col.getType().getName());
      if (col.getTypeAttributes() != null) {
        sb.append(col.getTypeAttributes().toStringForType(col.getType()));
      }
      sb.append(' ');
      sb.append(col.getName());
      sb.append('=');

      appendCellValueDebugString(idx, sb);
    }
    sb.append(')');
    return sb.toString();
  }

  /**
   * Transforms the row key into a string representation where each column is in the format:
   * "type col_name=value".
   * @return a string representation of the operation's row key
   */
  public String stringifyRowKey() {
    int numRowKeys = schema.getPrimaryKeyColumnCount();
    List<Integer> idxs = new ArrayList<>(numRowKeys);
    for (int i = 0; i < numRowKeys; i++) {
      idxs.add(i);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("(");
    appendDebugString(idxs, sb);
    sb.append(")");
    return sb.toString();
  }

  /**
   * Appends a debug string for the provided columns in the row.
   *
   * @param idxs the column indexes
   * @param sb the string builder to append to
   */
  void appendDebugString(List<Integer> idxs, StringBuilder sb) {
    boolean first = true;
    for (int idx : idxs) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }

      ColumnSchema col = schema.getColumnByIndex(idx);

      sb.append(col.getType().getName());
      sb.append(' ');
      sb.append(col.getName());
      sb.append('=');

      appendCellValueDebugString(idx, sb);
    }
  }

  /**
   * Appends a short debug string for the provided columns in the row.
   *
   * @param idxs the column indexes
   * @param sb the string builder to append to
   */
  void appendShortDebugString(List<Integer> idxs, StringBuilder sb) {
    boolean first = true;
    for (int idx : idxs) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      appendCellValueDebugString(idx, sb);
    }
  }

  /**
   * Appends a debug string for the provided cell value in the row.
   *
   * @param idx the column index
   * @param sb the string builder to append to
   */
  void appendCellValueDebugString(Integer idx, StringBuilder sb) {
    ColumnSchema col = schema.getColumnByIndex(idx);
    Preconditions.checkState(columnsBitSet.get(idx), "Column %s is not set", col.getName());

    // Handle nulls
    if (nullsBitSet != null && nullsBitSet.get(idx)) {
      if (col.isArray()) {
        sb.append(col.getType().getName())
                .append("[] ")
                .append(col.getName())
                .append("=NULL");
      } else {
        sb.append("NULL");
      }
      return;
    }

    // Handle arrays
    if (col.isArray()) {
      ByteBuffer buf = getVarLengthData(idx);
      if (buf == null || !buf.hasRemaining()) {
        sb.append(col.getType().getName())
                .append("[] ")
                .append(col.getName())
                .append("=NULL");
        return;
      }

      ByteBuffer dup = buf.duplicate();
      byte[] raw = new byte[dup.remaining()];
      dup.get(raw);

      try {
        ArrayCellView view = new ArrayCellView(raw);
        sb.append(col.getType().getName())
                .append("[] ")
                .append(col.getName())
                .append("=")
                .append(view);
      } catch (RuntimeException e) {
        sb.append("<invalid array data>");
      }
      return;
    }
    switch (col.getType()) {
      case BOOL:
        sb.append(Bytes.getBoolean(rowAlloc, schema.getColumnOffset(idx)));
        return;
      case INT8:
        sb.append(Bytes.getByte(rowAlloc, schema.getColumnOffset(idx)));
        return;
      case INT16:
        sb.append(Bytes.getShort(rowAlloc, schema.getColumnOffset(idx)));
        return;
      case INT32:
        sb.append(Bytes.getInt(rowAlloc, schema.getColumnOffset(idx)));
        return;
      case INT64:
        sb.append(Bytes.getLong(rowAlloc, schema.getColumnOffset(idx)));
        return;
      case DATE:
        sb.append(DateUtil.epochDaysToDateString(
            Bytes.getInt(rowAlloc, schema.getColumnOffset(idx))));
        return;
      case UNIXTIME_MICROS:
        sb.append(TimestampUtil.timestampToString(
            Bytes.getLong(rowAlloc, schema.getColumnOffset(idx))));
        return;
      case FLOAT:
        sb.append(Bytes.getFloat(rowAlloc, schema.getColumnOffset(idx)));
        return;
      case DOUBLE:
        sb.append(Bytes.getDouble(rowAlloc, schema.getColumnOffset(idx)));
        return;
      case DECIMAL:
        ColumnTypeAttributes typeAttributes = col.getTypeAttributes();
        sb.append(Bytes.getDecimal(rowAlloc, schema.getColumnOffset(idx),
            typeAttributes.getPrecision(), typeAttributes.getScale()));
        return;
      case VARCHAR:
      case BINARY:
      case STRING:
        ByteBuffer value = getVarLengthData().get(idx).duplicate();
        value.reset(); // Make sure we start at the beginning.
        byte[] data = new byte[value.limit() - value.position()];
        value.get(data);
        if (col.getType() == Type.STRING || col.getType() == Type.VARCHAR) {
          sb.append('"');
          StringUtil.appendEscapedSQLString(Bytes.getString(data), sb);
          sb.append('"');
        } else {
          sb.append(Bytes.pretty(data));
        }
        return;
      case /*ARRAY*/ NESTED: {
        ArrayCellView view = getArray(idx);
        sb.append("ARRAY[len=").append(view.length()).append("]");
        try {
          Object arr = view.toJavaArray();
          String s;
          if (arr instanceof Object[]) {
            Object[] objs = (Object[]) arr;
            if (objs instanceof byte[][]) {
              // Pretty-print nested binary arrays
              s = java.util.Arrays.deepToString(objs);
            } else {
              // Strings, Dates, Timestamps, BigDecimals, etc.
              s = java.util.Arrays.toString(objs);
            }
          } else if (arr instanceof int[]) {
            s = java.util.Arrays.toString((int[]) arr);
          } else if (arr instanceof long[]) {
            s = java.util.Arrays.toString((long[]) arr);
          } else if (arr instanceof short[]) {
            s = java.util.Arrays.toString((short[]) arr);
          } else if (arr instanceof byte[]) {
            s = java.util.Arrays.toString((byte[]) arr);
          } else if (arr instanceof float[]) {
            s = java.util.Arrays.toString((float[]) arr);
          } else if (arr instanceof double[]) {
            s = java.util.Arrays.toString((double[]) arr);
          } else if (arr instanceof boolean[]) {
            s = java.util.Arrays.toString((boolean[]) arr);
          } else {
            // Fallback for unexpected array types
            s = arr.toString();
          }
          sb.append(s);
        } catch (RuntimeException ignore) {
          sb.append("<invalid array>");
        }
        return;
      }
      default:
        throw new RuntimeException("unreachable");
    }
  }

  /**
   * Sets the column to the minimum possible value for the column's type.
   * @param index the index of the column to set to the minimum
   */
  void setMin(int index) {
    ColumnSchema column = schema.getColumnByIndex(index);
    Type type = column.getType();
    switch (type) {
      case BOOL:
        addBoolean(index, false);
        break;
      case INT8:
        addByte(index, Byte.MIN_VALUE);
        break;
      case INT16:
        addShort(index, Short.MIN_VALUE);
        break;
      case INT32:
        addInt(index, Integer.MIN_VALUE);
        break;
      case DATE:
        addDate(index, DateUtil.epochDaysToSqlDate(DateUtil.MIN_DATE_VALUE));
        break;
      case INT64:
      case UNIXTIME_MICROS:
        addLong(index, Long.MIN_VALUE);
        break;
      case FLOAT:
        addFloat(index, -Float.MAX_VALUE);
        break;
      case DOUBLE:
        addDouble(index, -Double.MAX_VALUE);
        break;
      case DECIMAL:
        ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
        addDecimal(index,
            DecimalUtil.minValue(typeAttributes.getPrecision(), typeAttributes.getScale()));
        break;
      case STRING:
        addStringUtf8(index, AsyncKuduClient.EMPTY_ARRAY);
        break;
      case BINARY:
        addBinary(index, AsyncKuduClient.EMPTY_ARRAY);
        break;
      case VARCHAR:
        addVarchar(index, "");
        break;
      default:
        throw new RuntimeException("unreachable");
    }
  }

  /**
   * Sets the column to the provided raw value.
   * @param index the index of the column to set
   * @param value the raw value
   */
  void setRaw(int index, byte[] value) {
    ColumnSchema column = schema.getColumnByIndex(index);
    Type type = column.getType();
    switch (type) {
      case BOOL:
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case DATE:
      case UNIXTIME_MICROS:
      case FLOAT:
      case DOUBLE:
      case DECIMAL: {
        Preconditions.checkArgument(value.length == column.getTypeSize());
        System.arraycopy(value, 0, rowAlloc,
            getPositionInRowAllocAndSetBitSet(index), value.length);
        break;
      }
      case VARCHAR:
      case STRING:
      case BINARY:
      case /*ARRAY*/ NESTED: {
        addVarLengthData(index, value);
        break;
      }
      default:
        throw new RuntimeException("unreachable");
    }
  }

  /**
   * Increments the column at the given index, returning {@code false} if the
   * value is already the maximum.
   *
   * @param index the column index to increment
   * @return {@code true} if the column is successfully incremented, or {@code false} if
   *         it is already the maximum value
   */
  @SuppressWarnings("BigDecimalEquals")
  boolean incrementColumn(int index) {
    ColumnSchema column = schema.getColumnByIndex(index);
    Type type = column.getType();
    Preconditions.checkState(isSet(index));
    int offset = schema.getColumnOffset(index);
    switch (type) {
      case BOOL: {
        boolean isFalse = rowAlloc[offset] == 0;
        rowAlloc[offset] = 1;
        return isFalse;
      }
      case INT8: {
        byte existing = rowAlloc[offset];
        if (existing == Byte.MAX_VALUE) {
          return false;
        }
        rowAlloc[offset] = (byte) (existing + 1);
        return true;
      }
      case INT16: {
        short existing = Bytes.getShort(rowAlloc, offset);
        if (existing == Short.MAX_VALUE) {
          return false;
        }
        Bytes.setShort(rowAlloc, (short) (existing + 1), offset);
        return true;
      }
      case INT32: {
        int existing = Bytes.getInt(rowAlloc, offset);
        if (existing == Integer.MAX_VALUE) {
          return false;
        }
        Bytes.setInt(rowAlloc, existing + 1, offset);
        return true;
      }
      case DATE: {
        int existing = Bytes.getInt(rowAlloc, offset);
        if (existing == DateUtil.MAX_DATE_VALUE) {
          return false;
        }
        Bytes.setInt(rowAlloc, existing + 1, offset);
        return true;
      }
      case INT64:
      case UNIXTIME_MICROS: {
        long existing = Bytes.getLong(rowAlloc, offset);
        if (existing == Long.MAX_VALUE) {
          return false;
        }
        Bytes.setLong(rowAlloc, existing + 1, offset);
        return true;
      }
      case FLOAT: {
        float existing = Bytes.getFloat(rowAlloc, offset);
        float incremented = Math.nextAfter(existing, Float.POSITIVE_INFINITY);
        if (existing == incremented) {
          return false;
        }
        Bytes.setFloat(rowAlloc, incremented, offset);
        return true;
      }
      case DOUBLE: {
        double existing = Bytes.getDouble(rowAlloc, offset);
        double incremented = Math.nextAfter(existing, Double.POSITIVE_INFINITY);
        if (existing == incremented) {
          return false;
        }
        Bytes.setDouble(rowAlloc, incremented, offset);
        return true;
      }
      case DECIMAL: {
        int precision = column.getTypeAttributes().getPrecision();
        int scale = column.getTypeAttributes().getScale();
        BigDecimal existing = Bytes.getDecimal(rowAlloc, offset, precision, scale);
        BigDecimal max = DecimalUtil.maxValue(precision, scale);
        if (existing.equals(max)) {
          return false;
        }
        BigDecimal smallest = DecimalUtil.smallestValue(scale);
        Bytes.setBigDecimal(rowAlloc, existing.add(smallest), precision, offset);
        return true;
      }
      case VARCHAR:
      case STRING:
      case BINARY: {
        ByteBuffer data = varLengthData.get(index);
        data.reset();
        int len = data.limit() - data.position();
        byte[] incremented = new byte[len + 1];
        System.arraycopy(data.array(), data.arrayOffset() + data.position(), incremented, 0, len);
        addVarLengthData(index, incremented);
        return true;
      }
      default:
        throw new RuntimeException("unreachable");
    }
  }

  /**
   * Returns {@code true} if the upper row is equal to the incremented lower
   * row. Neither row is modified.
   * @param lower the lower row
   * @param upper the upper, possibly incremented, row
   * @param indexes the columns in key order
   * @return whether the upper row is equal to the incremented lower row
   */
  static boolean isIncremented(PartialRow lower, PartialRow upper, List<Integer> indexes) {
    boolean equals = false;
    ListIterator<Integer> iter = indexes.listIterator(indexes.size());
    while (iter.hasPrevious()) {
      int index = iter.previous();
      if (equals) {
        if (isCellEqual(lower, upper, index)) {
          continue;
        }
        return false;
      }

      if (!lower.isSet(index) && !upper.isSet(index)) {
        continue;
      }
      if (!isCellIncremented(lower, upper, index)) {
        return false;
      }
      equals = true;
    }
    return equals;
  }

  /**
   * Checks if the specified cell is equal in both rows.
   * @param a a row
   * @param b a row
   * @param index the column index
   * @return {@code true} if the cell values for the given column are equal
   */
  @SuppressWarnings("BigDecimalEquals")
  private static boolean isCellEqual(PartialRow a, PartialRow b, int index) {
    // These checks are perhaps overly restrictive, but right now we only use
    // this method for checking fully-set keys.
    Preconditions.checkArgument(a.getSchema().equals(b.getSchema()));
    Preconditions.checkArgument(a.getSchema().getColumnByIndex(index).isKey());
    Preconditions.checkArgument(a.isSet(index));
    Preconditions.checkArgument(b.isSet(index));

    ColumnSchema column = a.getSchema().getColumnByIndex(index);
    Type type = column.getType();
    int offset = a.getSchema().getColumnOffset(index);

    switch (type) {
      case BOOL:
      case INT8:
        return a.rowAlloc[offset] == b.rowAlloc[offset];
      case INT16:
        return Bytes.getShort(a.rowAlloc, offset) == Bytes.getShort(b.rowAlloc, offset);
      case DATE:
      case INT32:
        return Bytes.getInt(a.rowAlloc, offset) == Bytes.getInt(b.rowAlloc, offset);
      case INT64:
      case UNIXTIME_MICROS:
        return Bytes.getLong(a.rowAlloc, offset) == Bytes.getLong(b.rowAlloc, offset);
      case FLOAT:
        return Bytes.getFloat(a.rowAlloc, offset) == Bytes.getFloat(b.rowAlloc, offset);
      case DOUBLE:
        return Bytes.getDouble(a.rowAlloc, offset) == Bytes.getDouble(b.rowAlloc, offset);
      case DECIMAL:
        ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
        int precision = typeAttributes.getPrecision();
        int scale = typeAttributes.getScale();
        return Bytes.getDecimal(a.rowAlloc, offset, precision, scale)
            .equals(Bytes.getDecimal(b.rowAlloc, offset, precision, scale));
      case VARCHAR:
      case STRING:
      case BINARY: {
        ByteBuffer dataA = a.varLengthData.get(index).duplicate();
        ByteBuffer dataB = b.varLengthData.get(index).duplicate();
        dataA.reset();
        dataB.reset();
        int lenA = dataA.limit() - dataA.position();
        int lenB = dataB.limit() - dataB.position();

        if (lenA != lenB) {
          return false;
        }
        for (int i = 0; i < lenA; i++) {
          if (dataA.get(dataA.position() + i) != dataB.get(dataB.position() + i)) {
            return false;
          }
        }
        return true;
      }
      default:
        throw new RuntimeException("unreachable");
    }
  }

  /**
   * Checks if the specified cell is in the upper row is an incremented version
   * of the cell in the lower row.
   * @param lower the lower row
   * @param upper the possibly incremented upper row
   * @param index the index of the column to check
   * @return {@code true} if the column cell value in the upper row is equal to
   *         the value in the lower row, incremented by one.
   */
  @SuppressWarnings("BigDecimalEquals")
  private static boolean isCellIncremented(PartialRow lower, PartialRow upper, int index) {
    // These checks are perhaps overly restrictive, but right now we only use
    // this method for checking fully-set keys.
    Preconditions.checkArgument(lower.getSchema().equals(upper.getSchema()));
    Preconditions.checkArgument(lower.getSchema().getColumnByIndex(index).isKey());
    Preconditions.checkArgument(lower.isSet(index));
    Preconditions.checkArgument(upper.isSet(index));

    ColumnSchema column = lower.getSchema().getColumnByIndex(index);
    Type type = column.getType();
    int offset = lower.getSchema().getColumnOffset(index);

    switch (type) {
      case BOOL:
        return lower.rowAlloc[offset] + 1 == upper.rowAlloc[offset];
      case INT8: {
        byte val = lower.rowAlloc[offset];
        return val != Byte.MAX_VALUE && val + 1 == upper.rowAlloc[offset];
      }
      case INT16: {
        short val = Bytes.getShort(lower.rowAlloc, offset);
        return val != Short.MAX_VALUE && val + 1 == Bytes.getShort(upper.rowAlloc, offset);
      }
      case INT32: {
        int val = Bytes.getInt(lower.rowAlloc, offset);
        return val != Integer.MAX_VALUE && val + 1 == Bytes.getInt(upper.rowAlloc, offset);
      }
      case DATE: {
        int val = Bytes.getInt(lower.rowAlloc, offset);
        return val != DateUtil.MAX_DATE_VALUE && val + 1 == Bytes.getInt(upper.rowAlloc, offset);
      }
      case INT64:
      case UNIXTIME_MICROS: {
        long val = Bytes.getLong(lower.rowAlloc, offset);
        return val != Long.MAX_VALUE && val + 1 == Bytes.getLong(upper.rowAlloc, offset);
      }
      case FLOAT: {
        float val = Bytes.getFloat(lower.rowAlloc, offset);
        return val != Float.POSITIVE_INFINITY &&
               Math.nextAfter(val, Float.POSITIVE_INFINITY) ==
                   Bytes.getFloat(upper.rowAlloc, offset);
      }
      case DOUBLE: {
        double val = Bytes.getDouble(lower.rowAlloc, offset);
        return val != Double.POSITIVE_INFINITY &&
               Math.nextAfter(val, Double.POSITIVE_INFINITY) ==
                   Bytes.getDouble(upper.rowAlloc, offset);
      }
      case DECIMAL: {
        ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
        int precision = typeAttributes.getPrecision();
        int scale = typeAttributes.getScale();
        BigDecimal val = Bytes.getDecimal(lower.rowAlloc, offset, precision, scale);
        BigDecimal smallestVal = DecimalUtil.smallestValue(scale);
        return val.add(smallestVal).equals(
                Bytes.getDecimal(upper.rowAlloc, offset, precision, scale));
      }
      case VARCHAR:
      case STRING:
      case BINARY: {
        // Check that b is 1 byte bigger than a, the extra byte is 0, and the other bytes are equal.
        ByteBuffer dataA = lower.varLengthData.get(index).duplicate();
        ByteBuffer dataB = upper.varLengthData.get(index).duplicate();
        dataA.reset();
        dataB.reset();
        int lenA = dataA.limit() - dataA.position();
        int lenB = dataB.limit() - dataB.position();

        if (lenA == Integer.MAX_VALUE ||
            lenA + 1 != lenB ||
            dataB.get(dataB.limit() - 1) != 0) {
          return false;
        }

        for (int i = 0; i < lenA; i++) {
          if (dataA.get(dataA.position() + i) != dataB.get(dataB.position() + i)) {
            return false;
          }
        }
        return true;
      }
      default:
        throw new RuntimeException("unreachable");
    }
  }

  /**
   * Get the schema used for this row.
   * @return a schema that came from KuduTable
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get the byte array that contains all the data added to this partial row. Variable length data
   * is contained separately, see {@link #getVarLengthData()}. In their place you'll find their
   * index in that list and their size.
   * @return a byte array containing the data for this row, except strings
   */
  byte[] getRowAlloc() {
    return rowAlloc;
  }

  /**
   * Get the bit set that indicates which columns were set.
   * @return a bit set for columns with data
   */
  BitSet getColumnsBitSet() {
    return columnsBitSet;
  }

  /**
   * Get the bit set for the columns that were specifically set to null
   * @return a bit set for null columns
   */
  BitSet getNullsBitSet() {
    return nullsBitSet;
  }

  /**
   * Prevents this PartialRow from being modified again. Can be called multiple times.
   */
  void freeze() {
    this.frozen = true;
  }

  /**
   * @return in memory size of this row.
   * <p>
   * Note: the size here is not accurate, as we do not count all the fields, but it is
   * enough for most scenarios.
   */
  long size() {
    long size = (long) rowAlloc.length + columnsBitSet.size() / Byte.SIZE;
    if (nullsBitSet != null) {
      size += nullsBitSet.size() / Byte.SIZE;
    }
    for (ByteBuffer bb : varLengthData) {
      if (bb != null) {
        size += bb.capacity();
      }
    }
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartialRow)) {
      return false;
    }
    PartialRow that = (PartialRow) o;

    return Objects.equals(schema, that.schema) &&
    Objects.equals(columnsBitSet, that.columnsBitSet) &&
    Objects.equals(nullsBitSet, that.nullsBitSet) &&
    varLengthData.equals(that.varLengthData) &&
    Arrays.equals(rowAlloc, that.rowAlloc);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(schema, columnsBitSet, nullsBitSet);

    // Hash values for each set column using getObject()
    for (int i = 0; i < schema.getColumnCount(); i++) {
      if (!columnsBitSet.get(i)) {
        continue;
      }

      Object value = getObject(i);
      // Special handling for byte arrays, which don't hash content by default
      if (value instanceof byte[]) {
        result = 31 * result + Arrays.hashCode((byte[]) value);
      } else {
        result = 31 * result + Objects.hashCode(value);
      }
    }

    return result;
  }


}
