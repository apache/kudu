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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.Rule;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.test.junit.RetryRule;

public class TestColumnRangePredicate {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testRawLists() {
    ColumnSchema col1 = new ColumnSchema.ColumnSchemaBuilder("col1", Type.INT32).build();
    ColumnSchema col2 = new ColumnSchema.ColumnSchemaBuilder("col2", Type.STRING).build();

    ColumnSchema col3 = new ColumnSchema.ColumnSchemaBuilder("col3", Type.DECIMAL)
        .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
            .precision(6).scale(2).build()
        ).build();

    ColumnRangePredicate pred1 = new ColumnRangePredicate(col1);
    pred1.setLowerBound(1);

    ColumnRangePredicate pred2 = new ColumnRangePredicate(col1);
    pred2.setUpperBound(2);

    ColumnRangePredicate pred3 = new ColumnRangePredicate(col2);
    pred3.setLowerBound("aaa");
    pred3.setUpperBound("bbb");

    ColumnRangePredicate pred4 = new ColumnRangePredicate(col3);
    pred4.setLowerBound(BigDecimal.valueOf(12345, 2));

    List<ColumnRangePredicate> preds = Lists.newArrayList(pred1, pred2, pred3, pred4);

    byte[] rawPreds = ColumnRangePredicate.toByteArray(preds);

    List<Tserver.ColumnRangePredicatePB> decodedPreds = null;
    try {
      decodedPreds = ColumnRangePredicate.fromByteArray(rawPreds);
    } catch (IllegalArgumentException e) {
      fail("Couldn't decode: " + e.getMessage());
    }

    assertEquals(4, decodedPreds.size());

    assertEquals(col1.getName(), decodedPreds.get(0).getColumn().getName());
    assertEquals(1, Bytes.getInt(decodedPreds.get(0).getLowerBound().toByteArray()));
    assertFalse(decodedPreds.get(0).hasInclusiveUpperBound());

    assertEquals(col1.getName(), decodedPreds.get(1).getColumn().getName());
    assertEquals(2, Bytes.getInt(decodedPreds.get(1).getInclusiveUpperBound().toByteArray()));
    assertFalse(decodedPreds.get(1).hasLowerBound());

    assertEquals(col2.getName(), decodedPreds.get(2).getColumn().getName());
    assertEquals("aaa", Bytes.getString(decodedPreds.get(2).getLowerBound().toByteArray()));
    assertEquals("bbb", Bytes.getString(decodedPreds.get(2).getInclusiveUpperBound().toByteArray()));

    assertEquals(col3.getName(), decodedPreds.get(3).getColumn().getName());
    assertEquals(12345, Bytes.getInt(decodedPreds.get(3).getLowerBound().toByteArray()));
    assertFalse(decodedPreds.get(0).hasInclusiveUpperBound());
  }
}
