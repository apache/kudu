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

package org.apache.kudu.hive.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidClassException;
import java.security.AccessController;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

/**
 * Created by bimal on 4/12/16.
 */
@SuppressWarnings("WeakerAccess")
public class HiveKuduBridgeUtils {


  private static final Log LOG = LogFactory.getLog(HiveKuduBridgeUtils.class);
  private static final Text KUDU_TOKEN_KIND = new Text("kudu-authn-data");

  public static Type hiveTypeToKuduType(String hiveType) throws SerDeException {
    switch (hiveType.toLowerCase(Locale.US)) {
      case serdeConstants.STRING_TYPE_NAME:
      case serdeConstants.VARCHAR_TYPE_NAME:
      case serdeConstants.CHAR_TYPE_NAME:
        return Type.STRING;
      case serdeConstants.TINYINT_TYPE_NAME:
        return Type.INT8;
      case serdeConstants.SMALLINT_TYPE_NAME:
        return Type.INT16;
      case serdeConstants.INT_TYPE_NAME:
        return Type.INT32;
      case serdeConstants.BIGINT_TYPE_NAME:
        return Type.INT64;
      case serdeConstants.FLOAT_TYPE_NAME:
        return Type.FLOAT;
      case serdeConstants.DOUBLE_TYPE_NAME:
        return Type.DOUBLE;
      case serdeConstants.TIMESTAMP_TYPE_NAME:
        return Type.UNIXTIME_MICROS;
      case serdeConstants.BOOLEAN_TYPE_NAME:
        return Type.BOOL;
      case serdeConstants.BINARY_TYPE_NAME:
        return Type.BINARY;
      default: {
        if (hiveType.toLowerCase(Locale.US).startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
          return Type.DECIMAL;
        } else {
          throw new SerDeException("Unrecognized column type: " + hiveType + " not supported in Kudu");
        }
      }
    }
  }

  public static ObjectInspector getObjectInspector(ColumnSchema kuduColumn) throws SerDeException {
    switch (kuduColumn.getType()) {
      case STRING:
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
      case FLOAT:
        return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
      case DOUBLE:
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
      case BOOL:
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
      case INT8:
        return PrimitiveObjectInspectorFactory.javaByteObjectInspector;
      case INT16:
        return PrimitiveObjectInspectorFactory.javaShortObjectInspector;
      case INT32:
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
      case INT64:
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
      case UNIXTIME_MICROS:
        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
      case BINARY:
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
      case DECIMAL: {
        PrimitiveTypeInfo typeInfo = new DecimalTypeInfo(kuduColumn.getTypeAttributes().getPrecision(),
            kuduColumn.getTypeAttributes().getScale());
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
      }
      default:
        throw new SerDeException("Cannot find getObjectInspector for: "
            + kuduColumn.getType());
    }
  }

  public static Object readObject(DataInput in, Type kuduType)
      throws IOException {
    switch (kuduType) {
      case STRING:
        return in.readUTF();
      case FLOAT:
        return in.readFloat();
      case DOUBLE:
        return in.readDouble();
      case BOOL:
        return in.readBoolean();
      case INT8:
        return in.readByte();
      case INT16:
        return in.readShort();
      case INT32:
        return in.readInt();
      case INT64:
        return in.readLong();
      case UNIXTIME_MICROS: {
        TimestampWritable timestampWritable = new TimestampWritable();
        timestampWritable.readFields(in);
        return timestampWritable;
      }
      case DECIMAL:
        HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable();
        hiveDecimalWritable.readFields(in);
        return hiveDecimalWritable;
      case BINARY: {
        int size = in.readInt();
        byte[] b = new byte[size];
        in.readFully(b);
        return b;
      }
      default:
        throw new IOException("Cannot read Object for type: " + kuduType.name());
    }
  }

  public static void writeObject(Object obj, Type kuduType, DataOutput out)
      throws IOException {
    switch (kuduType) {
      case STRING: {
        String s = obj.toString();
        out.writeUTF(s);
        break;
      }
      case FLOAT: {
        Float f = (Float) obj;
        out.writeFloat(f);
        break;
      }
      case DOUBLE: {
        Double d = (Double) obj;
        out.writeDouble(d);
        break;
      }
      case BOOL: {
        Boolean b = (Boolean) obj;
        out.writeBoolean(b);
        break;
      }
      case INT8: {
        Byte b = (Byte) obj;
        out.writeByte(b.intValue());
        break;
      }
      case INT16: {
        Short s = (Short) obj;
        out.writeShort(s);
        break;
      }
      case INT32: {
        Integer i = (Integer) obj;
        out.writeInt(i);
        break;
      }
      case INT64: {
        Long l = (Long) obj;
        out.writeLong(l);
        break;
      }
      case UNIXTIME_MICROS: {
        TimestampWritable time = (TimestampWritable) obj;
        time.write(out);
        break;
      }
      case DECIMAL: {
        HiveDecimalWritable decimal = (HiveDecimalWritable) obj;
        decimal.write(out);
        break;
      }
      case BINARY: {
        byte[] b = (byte[]) obj;
        out.writeInt(b.length);
        out.write(b);
        break;
      }
      default:
        throw new IOException("Cannot write Object '"
            + obj.getClass().getSimpleName() + "' as type: " + kuduType.name());
    }
  }


  public static void setPartialRowValue(PartialRow partialRow, ColumnSchema schema, Object value)
      throws IOException {
    if (value == null) {
      partialRow.setNull(schema.getName());
      return;
    }
    Type type = schema.getType();
    switch (type) {
      case STRING: {
        partialRow.addString(schema.getName(), (String) value);
        break;
      }
      case FLOAT: {
        partialRow.addFloat(schema.getName(), (Float) value);
        break;
      }
      case DOUBLE: {
        partialRow.addDouble(schema.getName(), (Double) value);
        break;
      }
      case BOOL: {
        partialRow.addBoolean(schema.getName(), (Boolean) value);
        break;
      }
      case INT8: {
        partialRow.addByte(schema.getName(), (Byte) value);
        break;
      }
      case INT16: {
        partialRow.addShort(schema.getName(), (Short) value);
        break;
      }
      case INT32: {
        partialRow.addInt(schema.getName(), (Integer) value);
        break;
      }
      case INT64: {
        partialRow.addLong(schema.getName(), (Long) value);
        break;
      }
      case UNIXTIME_MICROS: {
        partialRow.addLong(schema.getName(), TimestampConverter.toKudu((TimestampWritable) value));
        break;
      }
      case DECIMAL: {
        partialRow.addDecimal(schema.getName(), ((HiveDecimalWritable) value).getHiveDecimal().bigDecimalValue());
        break;
      }
      case BINARY: {
        partialRow.addBinary(schema.getName(), (byte[]) value);
        break;
      }
      default:
        throw new IOException("Cannot write DataInput as type: " + type.name());
    }
  }

  public static Object getRowValue(PartialRow partialRow, ColumnSchema schema) throws IOException {
    if (partialRow.isNull(schema.getName())) {
      return null;
    }
    Type type = schema.getType();
    switch (type) {
      case STRING: {
        return partialRow.getString(schema.getName());
      }
      case FLOAT: {
        return partialRow.getFloat(schema.getName());
      }
      case DOUBLE: {
        return partialRow.getDouble(schema.getName());
      }
      case BOOL: {
        return partialRow.getBoolean(schema.getName());
      }
      case INT8: {
        return partialRow.getByte(schema.getName());
      }
      case INT16: {
        return partialRow.getShort(schema.getName());
      }
      case INT32: {
        return partialRow.getInt(schema.getName());
      }
      case INT64: {
        return partialRow.getLong(schema.getName());
      }
      case UNIXTIME_MICROS: {
        return TimestampConverter.toHive(partialRow.getLong(schema.getName()));
      }
      case DECIMAL: {
        return new HiveDecimalWritable(HiveDecimal.create(partialRow.getDecimal(schema.getName())));
      }
      case BINARY: {
        return partialRow.getBinary(schema.getName());
      }
      default:
        throw new IOException("Cannot write DataInput as type: " + type.name());
    }
  }


  private static Object getRowValue(RowResult rowResult, ColumnSchema schema) throws IOException {
    if (rowResult.isNull(schema.getName())) {
      return null;
    }
    Type type = schema.getType();
    switch (type) {
      case STRING: {
        return rowResult.getString(schema.getName());
      }
      case FLOAT: {
        return rowResult.getFloat(schema.getName());
      }
      case DOUBLE: {
        return rowResult.getDouble(schema.getName());
      }
      case BOOL: {
        return rowResult.getBoolean(schema.getName());
      }
      case INT8: {
        return rowResult.getByte(schema.getName());
      }
      case INT16: {
        return rowResult.getShort(schema.getName());
      }
      case INT32: {
        return rowResult.getInt(schema.getName());
      }
      case INT64: {
        return rowResult.getLong(schema.getName());
      }
      case UNIXTIME_MICROS: {
        return TimestampConverter.toHive(rowResult.getLong(schema.getName()));
      }
      case DECIMAL: {
        return new HiveDecimalWritable(HiveDecimal.create(rowResult.getDecimal(schema.getName())));
      }
      case BINARY: {
        return rowResult.getBinary(schema.getName());
      }
      default:
        throw new IOException("Cannot read RowResult as type: " + type.name());
    }
  }


  public static void copyPartialRowToPartialRow(PartialRow in, PartialRow out, Schema schema) throws IOException {

    for (ColumnSchema columnSchema : schema.getColumns()) {
      Object value = getRowValue(in, columnSchema);
      setPartialRowValue(out, columnSchema, value);
    }
  }

  public static void copyRowResultToPartialRow(RowResult record, PartialRow out, Schema schema) throws IOException {
    for (ColumnSchema columnSchema : schema.getColumns()) {
      Object value = getRowValue(record, columnSchema);
      setPartialRowValue(out, columnSchema, value);
    }
  }


  public static KuduClient getKuduClient(Configuration conf) {

    String masterAddresses = conf.get(HiveKuduConstants.MASTER_ADDRESSES_KEY);
    return getKuduClient(conf, masterAddresses);
  }

  public static KuduClient getKuduClient(@Nullable Configuration conf, Properties props) {

    String masterAddresses = props.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME);
    return getKuduClient(conf, masterAddresses);
  }

  public static KuduClient getKuduClient(@Nullable Configuration conf, String masterAddresses) {

    long operationTimeoutMs = AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;

    if (conf != null) {
      operationTimeoutMs = conf.getLong(HiveKuduConstants.OPERATION_TIMEOUT_MS_KEY,
          AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
    }

    KuduClient client = new KuduClient.KuduClientBuilder(masterAddresses)
        .defaultOperationTimeoutMs(operationTimeoutMs)
        .build();
    importCredentialsFromCurrentSubject(client);
    return client;
  }

  public static ComparisonOp hiveToKuduComparisonOp(ExprNodeGenericFuncDesc comparisonOp) throws InvalidClassException {
    if (comparisonOp.getGenericUDF() instanceof GenericUDFOPEqual) {
      return ComparisonOp.EQUAL;
    }
    if (comparisonOp.getGenericUDF() instanceof GenericUDFOPGreaterThan) {
      return ComparisonOp.GREATER;
    }

    if (comparisonOp.getGenericUDF() instanceof GenericUDFOPEqualOrGreaterThan) {
      return ComparisonOp.GREATER_EQUAL;
    }

    if (comparisonOp.getGenericUDF() instanceof GenericUDFOPLessThan) {
      return ComparisonOp.LESS;
    }

    if (comparisonOp.getGenericUDF() instanceof GenericUDFOPEqualOrLessThan) {
      return ComparisonOp.LESS_EQUAL;
    }

    throw new InvalidClassException(comparisonOp.getGenericUDF().getUdfName() + " is not a supported Comparison");
  }


  public static void importCredentialsFromCurrentSubject(KuduClient client) {
    Subject subj = Subject.getSubject(AccessController.getContext());
    if (subj == null) {
      return;
    }
    Text service = new Text(client.getMasterAddressesAsString());
    // Find the Hadoop credentials stored within the JAAS subject.
    Set<Credentials> credSet = subj.getPrivateCredentials(Credentials.class);
    for (Credentials creds : credSet) {
      for (Token<?> tok : creds.getAllTokens()) {
        if (!tok.getKind().equals(KUDU_TOKEN_KIND)) {
          continue;
        }
        // Only import credentials relevant to the service corresponding to
        // 'client'. This is necessary if we want to support a job which
        // reads from one cluster and writes to another.
        if (!tok.getService().equals(service)) {
          LOG.debug("Not importing credentials for service " + service +
              "(expecting service " + service + ")");
          continue;
        }
        LOG.debug("Importing credentials for service " + service);
        client.importAuthenticationCredentials(tok.getPassword());
        return;
      }
    }
  }
}
