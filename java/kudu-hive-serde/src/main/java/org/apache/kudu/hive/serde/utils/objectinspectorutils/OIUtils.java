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

package org.apache.kudu.hive.serde.utils.objectinspectorutils;

import java.sql.Timestamp;
import java.util.Map;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

public class OIUtils {

  public static Object copyToOtherObjectInspector(Object sourceData,
      ObjectInspector sourceOi, ObjectInspector targetOi) {
    Object result;

    if (sourceData == null) {
      return null;
    }

    switch (sourceOi.getCategory()) {
      case LIST:
        ListObjectInspector listObjectInspector = (ListObjectInspector) sourceOi;

        // array<string> to struct<value:array<string>>
        if (!(targetOi instanceof SettableListObjectInspector) && targetOi instanceof SettableStructObjectInspector) {
          // ok, we are trying to convert a list to a struct
          SettableStructObjectInspector structTargetOi = (SettableStructObjectInspector) targetOi;

          // make sure that our struct only has one field
          if (structTargetOi.getAllStructFieldRefs().size() == 1) {
            // get the field inside our struct
            StructField targetStructField = structTargetOi.getAllStructFieldRefs().get(0);

            // this field should now be the List we want to get at
            if (targetStructField.getFieldObjectInspector() instanceof SettableListObjectInspector) {
              SettableListObjectInspector targetListOI =
                  (SettableListObjectInspector) targetStructField.getFieldObjectInspector();

              // convert the input object to the target list
              Object inner = copyToOtherObjectInspector(sourceData, listObjectInspector, targetListOI);

              // wrap the converted object in our struct
              result = structTargetOi.create();
              result = structTargetOi.setStructFieldData(result, targetStructField, inner);
              return result;
            }
          }

        }

        SettableListObjectInspector settableListObjectInspector = (SettableListObjectInspector) targetOi;

        result = settableListObjectInspector.create(listObjectInspector
            .getListLength(sourceData));

        for (int i = 0; i < listObjectInspector.getListLength(sourceData); i++) {
          Object objectListElement = listObjectInspector.getListElement(
              sourceData, i);

          Object newElement = copyToOtherObjectInspector(
              objectListElement,
              listObjectInspector.getListElementObjectInspector(),
              settableListObjectInspector
                  .getListElementObjectInspector());
          settableListObjectInspector.set(result, i, newElement);
        }
        return result;

      case MAP:
        MapObjectInspector mapObjectInspector = (MapObjectInspector) sourceOi;
        SettableMapObjectInspector settableMapObjectInspector = (SettableMapObjectInspector) targetOi;

        result = settableMapObjectInspector.create();

        for (Map.Entry<?, ?> entry : mapObjectInspector.getMap(sourceData)
            .entrySet()) {
          Object newKeyElement = copyToOtherObjectInspector(
              entry.getKey(),
              mapObjectInspector.getMapKeyObjectInspector(),
              settableMapObjectInspector.getMapKeyObjectInspector());
          Object newValueElement = copyToOtherObjectInspector(
              entry.getValue(),
              mapObjectInspector.getMapValueObjectInspector(),
              settableMapObjectInspector.getMapValueObjectInspector());
          settableMapObjectInspector.put(result, newKeyElement,
              newValueElement);
        }
        return result;

      case STRUCT:
        StructObjectInspector structObjectInspector = (StructObjectInspector) sourceOi;
        SettableStructObjectInspector settableStructObjectInspector = (SettableStructObjectInspector) targetOi;

        result = settableStructObjectInspector.create();

        for (StructField targetStructField : settableStructObjectInspector
            .getAllStructFieldRefs()) {
          StructField sourceStructField = structObjectInspector
              .getStructFieldRef(targetStructField.getFieldName());
          if (sourceStructField != null) {

            Object newElement = copyToOtherObjectInspector(
                structObjectInspector.getStructFieldData(
                    sourceData, sourceStructField),
                sourceStructField.getFieldObjectInspector(),
                targetStructField.getFieldObjectInspector());

            result = settableStructObjectInspector.setStructFieldData(
                result, targetStructField, newElement);
          } else {
            throw new IllegalArgumentException("Error, Field "
                + targetStructField.getFieldName()
                + " was not found in the source inspector");
          }
        }

        return result;

      case UNION:
        throw new IllegalArgumentException("Not implemented");

      case PRIMITIVE:
        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) sourceOi;

        switch (primitiveObjectInspector.getPrimitiveCategory()) {
          case BINARY:
            SettableBinaryObjectInspector settableBinaryObjectInspector = (SettableBinaryObjectInspector) targetOi;
            byte[] bytes = ((BinaryObjectInspector) primitiveObjectInspector)
                .getPrimitiveJavaObject(sourceData);
            return settableBinaryObjectInspector.create(bytes);

          case BOOLEAN:
            SettableBooleanObjectInspector settableBooleanObjectInspector = (SettableBooleanObjectInspector) targetOi;
            boolean bool = ((BooleanObjectInspector) primitiveObjectInspector)
                .get(sourceData);
            return settableBooleanObjectInspector.create(bool);

          case BYTE:
            SettableByteObjectInspector settableByteObjectInspector = (SettableByteObjectInspector) targetOi;
            byte byteObj = ((ByteObjectInspector) primitiveObjectInspector)
                .get(sourceData);
            return settableByteObjectInspector.create(byteObj);

          case VARCHAR:
          case CHAR:
          case STRING:
            SettableStringObjectInspector settableStringObjectInspector = (SettableStringObjectInspector) targetOi;
            String stringObj = ((StringObjectInspector) primitiveObjectInspector)
                .getPrimitiveJavaObject(sourceData);
            return settableStringObjectInspector.create(stringObj);

          case DATE:
            SettableDateObjectInspector settableDateObjectInspector = (SettableDateObjectInspector) targetOi;
            java.sql.Date dateObj = ((DateObjectInspector) primitiveObjectInspector)
                .getPrimitiveJavaObject(sourceData);
            return settableDateObjectInspector.create(dateObj);

          case DECIMAL:
            SettableHiveDecimalObjectInspector settableHiveDecimalObjectInspector = (SettableHiveDecimalObjectInspector) targetOi;
            HiveDecimal decimal = ((HiveDecimalObjectInspector) primitiveObjectInspector)
                .getPrimitiveJavaObject(sourceData);
            return settableHiveDecimalObjectInspector.create(decimal);
          case FLOAT:
            SettableFloatObjectInspector settableFloatObjectInspector = (SettableFloatObjectInspector) targetOi;
            float floatObj = ((FloatObjectInspector) primitiveObjectInspector)
                .get(sourceData);
            return settableFloatObjectInspector.create(floatObj);

          case DOUBLE:
            SettableDoubleObjectInspector settableDoubleObjectInspector = (SettableDoubleObjectInspector) targetOi;
            double doubleObj = ((DoubleObjectInspector) primitiveObjectInspector)
                .get(sourceData);
            return settableDoubleObjectInspector.create(doubleObj);

          case INT:
            SettableIntObjectInspector settableIntObjectInspector = (SettableIntObjectInspector) targetOi;
            int intObj = ((IntObjectInspector) primitiveObjectInspector)
                .get(sourceData);
            return settableIntObjectInspector.create(intObj);

          case LONG:
            SettableLongObjectInspector settableLongObjectInspector = (SettableLongObjectInspector) targetOi;
            long longObj = ((LongObjectInspector) primitiveObjectInspector)
                .get(sourceData);
            return settableLongObjectInspector.create(longObj);

          case SHORT:
            SettableShortObjectInspector settableShortObjectInspector = (SettableShortObjectInspector) targetOi;
            short shortObj = ((ShortObjectInspector) primitiveObjectInspector)
                .get(sourceData);
            return settableShortObjectInspector.create(shortObj);

          case TIMESTAMP:
            SettableTimestampObjectInspector settableTimestampObjectInspector = (SettableTimestampObjectInspector) targetOi;
            Timestamp timestampObj = ((TimestampObjectInspector) primitiveObjectInspector)
                .getPrimitiveJavaObject(sourceData);
            return settableTimestampObjectInspector.create(timestampObj);

          case UNKNOWN:
          case VOID:
          default:
            throw new IllegalArgumentException("Unknown primitive type");
        }

      default:
        throw new IllegalArgumentException("Unknown message type");
    }
  }

}
