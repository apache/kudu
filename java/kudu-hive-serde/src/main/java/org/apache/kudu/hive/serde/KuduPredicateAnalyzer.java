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

import com.google.common.base.Splitter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.ColumnTypeAttributes.ColumnTypeAttributesBuilder;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class KuduPredicateAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(KuduPredicateAnalyzer.class);

  public static DecomposedPredicate decomposePredicate(JobConf jobConf, ExprNodeDesc predicate) {

    IndexPredicateAnalyzer analyzer = IndexPredicateAnalyzer.createAnalyzer(false);
    analyzer.clearAllowedColumnNames();
    Iterable<String> keyColumns = Splitter.onPattern("\\s*,\\s*").split(jobConf.get(HiveKuduConstants.KEY_COLUMNS));
    for (String keyColName : keyColumns) {

      analyzer.allowColumnName(keyColName);
    }
    List<IndexSearchCondition> conditions = new ArrayList<>();

    ExprNodeGenericFuncDesc residualPredicate =
        (ExprNodeGenericFuncDesc) analyzer.analyzePredicate(predicate, conditions);
    List<KuduPredicate> predicates = new ArrayList<>();
    try {
      for (IndexSearchCondition indexSearchCondition : conditions) {

        Type kuduType = HiveKuduBridgeUtils
            .hiveTypeToKuduType(indexSearchCondition.getColumnDesc().getTypeInfo().getTypeName());
        ColumnSchemaBuilder columnSchemaBuilder = new ColumnSchemaBuilder(
            indexSearchCondition.getColumnDesc().getColumn(), kuduType)
            .key(true)
            .nullable(false);

        if (kuduType.equals(Type.DECIMAL)) {
          DecimalTypeInfo typeInfo = (DecimalTypeInfo) indexSearchCondition.getColumnDesc().getTypeInfo();
          int precision = typeInfo.getPrecision();
          int scale = typeInfo.getScale();
          ColumnTypeAttributes decimalAttr = new ColumnTypeAttributesBuilder().precision(precision).scale(scale)
              .build();
          columnSchemaBuilder.typeAttributes(decimalAttr);
        }

        ColumnSchema columnSchema = columnSchemaBuilder.build();
        ComparisonOp comparisonOp = HiveKuduBridgeUtils
            .hiveToKuduComparisonOp(indexSearchCondition.getOriginalExpr());
        Object value = indexSearchCondition.getConstantDesc().getValue();
        KuduPredicate kuduPredicate = getPredicateForType(columnSchema, comparisonOp, value);
        predicates.add(kuduPredicate);
      }
      jobConf.set(HiveKuduConstants.ENCODED_PREDICATES_KEY, base64EncodePredicates(predicates));

    } catch (SerDeException | IOException e) {
      LOG.warn("Exception when decomposing predicate ", e);
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(conditions);
    decomposedPredicate.residualPredicate = residualPredicate;

    return decomposedPredicate;
  }

  private static String base64EncodePredicates(List<KuduPredicate> predicates) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (KuduPredicate predicate : predicates) {
      predicate.toPB().writeDelimitedTo(byteArrayOutputStream);
    }
    return Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
  }

  private static KuduPredicate getPredicateForType(ColumnSchema columnSchema, ComparisonOp comparisonOp, Object value)
      throws ClassCastException {
    KuduPredicate kuduPredicate;
    switch (columnSchema.getType()) {
      case BOOL:
        kuduPredicate = KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (boolean) value);
        break;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case UNIXTIME_MICROS:
        kuduPredicate = KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (long) value);
        break;
      case FLOAT:
        kuduPredicate = KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (float) value);
        break;
      case DOUBLE:
        kuduPredicate = KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (double) value);
        break;
      case STRING:
        kuduPredicate = KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (String) value);
        break;
      case BINARY:
        kuduPredicate = KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (byte[]) value);
        break;
      case DECIMAL:
        kuduPredicate = KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (BigDecimal) value);
        break;
      default:
        throw new ClassCastException("failed to find Predicate for " + columnSchema.getType());

    }
    return kuduPredicate;
  }

}
