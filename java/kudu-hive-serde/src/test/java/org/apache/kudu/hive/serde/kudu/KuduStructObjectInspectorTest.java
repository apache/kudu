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

package org.apache.kudu.hive.serde.kudu;

import static java.math.BigDecimal.ROUND_UP;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.ColumnTypeAttributes.ColumnTypeAttributesBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.hive.serde.HiveKuduSerDe;
import org.apache.kudu.hive.serde.KuduStructObjectInspector;
import org.apache.kudu.hive.serde.PartialRowWritable;
import org.junit.Before;
import org.junit.Test;

public class KuduStructObjectInspectorTest {

  private KuduStructObjectInspector kuduOi;
  private StandardStructObjectInspector standardOI;
  private Schema kuduSchema;
  private int size;
  // 10/31/2018 @ 9:21am (UTC)
  private long longTimestampMicros = TimeUnit.MILLISECONDS.toMicros(1540977683123L);
  private Timestamp timestamp = new Timestamp(1540977683123L);
  private TimestampWritable timestampWritable = new TimestampWritable(timestamp);

  private BigDecimal piDecimal = new BigDecimal(Math.PI).round(new MathContext(10)).setScale(8, ROUND_UP);
  private HiveDecimalWritable hivePiDecimal = new HiveDecimalWritable(HiveDecimal.create(piDecimal));

  @Before
  public void setUp() throws Exception {
    ColumnSchema key = new ColumnSchemaBuilder("id", Type.INT32).build();
    ColumnSchema value = new ColumnSchemaBuilder("value", Type.STRING).build();
    ColumnSchema last_changed = new ColumnSchemaBuilder("last_changed", Type.UNIXTIME_MICROS).build();
    ColumnTypeAttributes decimalAttr = new ColumnTypeAttributesBuilder().precision(10).scale(8).build();
    ColumnSchema pidecimal = new ColumnSchemaBuilder("pidecimal", Type.DECIMAL).typeAttributes(decimalAttr).build();
    List<ColumnSchema> columnSchemas = Arrays.asList(key, value, last_changed, pidecimal);
    List<Integer> columnIds = Arrays.asList(0, 1, 2, 3);
    List<String> columnNames = columnSchemas.stream().map(ColumnSchema::getName).collect(Collectors.toList());
    List<ObjectInspector> columnOIs = Arrays.asList(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);

    size = columnSchemas.size();
    kuduSchema = new Schema(columnSchemas, columnIds);
    kuduOi = new KuduStructObjectInspector(kuduSchema);
    standardOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
  }

  private PartialRow getRow() {

    PartialRow partialRow = new PartialRow(kuduSchema);
    partialRow.addInt(0, 5);
    partialRow.addString(1, "testvalue");

    partialRow.addLong(2, longTimestampMicros);
    partialRow.addDecimal(3, piDecimal);
    return partialRow;
  }

  @Test
  public void testOI() {
    assertEquals("struct<id:int,value:string,last_changed:timestamp,pidecimal:decimal(10,8)>", kuduOi.toString());
    PartialRow partialRow = getRow();
    List<Object> out = kuduOi.getStructFieldsDataAsList(partialRow);
    assertEquals(size, out.size());
    assertEquals(5, out.get(0));
    assertEquals("testvalue", out.get(1));
    assertEquals(timestampWritable, out.get(2));
    assertEquals(hivePiDecimal, out.get(3));
    for (StructField structField: kuduOi.getAllStructFieldRefs()) {
      if (structField.getFieldName().equals("id")) {
        kuduOi.setStructFieldData(partialRow, structField, 4);
      }

      if (structField.getFieldName().equals("value")) {
        kuduOi.setStructFieldData(partialRow, structField, "anothertestvalue");
      }
    }
    List<Object> outAgain = kuduOi.getStructFieldsDataAsList(partialRow);
    assertEquals(size, outAgain.size());
    assertEquals(4, outAgain.get(0));
    assertEquals("anothertestvalue", outAgain.get(1));
  }

  @Test
  public void testSerde() throws SerDeException {
    HiveKuduSerDe serde = new HiveKuduSerDe(kuduSchema);
    Object data = standardOI.create();
    for (StructField field: standardOI.getAllStructFieldRefs()) {

      if (field.getFieldName().equals("id")) {
        standardOI.setStructFieldData(data, field, 3);
      }

      if (field.getFieldName().equals("value")) {
        standardOI.setStructFieldData(data, field, "standardoivalue");
      }

      if (field.getFieldName().equals("last_changed")) {
        standardOI.setStructFieldData(data, field, timestamp);
      }

      if (field.getFieldName().equals("pidecimal")) {
        standardOI.setStructFieldData(data, field, hivePiDecimal);
      }
    }
    PartialRowWritable result = serde.serialize(data, standardOI);
    List<Object> outAgain = kuduOi.getStructFieldsDataAsList(result.getRow());
    assertEquals(size, outAgain.size());
    assertEquals(3, outAgain.get(0));
    assertEquals("standardoivalue", outAgain.get(1));
    assertEquals(timestampWritable, outAgain.get(2));
    assertEquals(longTimestampMicros, result.getRow().getLong(2));
    assertEquals(hivePiDecimal, outAgain.get(3));

    PartialRow row = (PartialRow) serde.deserialize(result);
    List<Object> outYetAgain = kuduOi.getStructFieldsDataAsList(row);
    assertEquals(size, outYetAgain.size());
    assertEquals(3, outYetAgain.get(0));
    assertEquals("standardoivalue", outYetAgain.get(1));
    assertEquals(timestampWritable, outYetAgain.get(2));
    assertEquals(longTimestampMicros, row.getLong(2));
    assertEquals(hivePiDecimal, outYetAgain.get(3));

  }
}