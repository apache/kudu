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

import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.createFourTabletsTableWithNineRows;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.util.DateUtil;

public class TestRowErrors {

  private static final Schema basicSchema = getBasicSchema();

  private KuduTable table;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Test(timeout = 100000)
  public void singleTabletTest() throws Exception {
    String tableName = TestRowErrors.class.getName() + "-" + System.currentTimeMillis();
    harness.getClient().createTable(tableName, basicSchema, getBasicCreateTableOptions());
    table = harness.getClient().openTable(tableName);
    AsyncKuduSession session = harness.getAsyncClient().newSession();

    // Insert 3 rows to play with.
    for (int i = 0; i < 3; i++) {
      session.apply(createInsert(i)).join(DEFAULT_SLEEP);
    }

    // Try a single dupe row insert with AUTO_FLUSH_SYNC.
    Insert dupeForZero = createInsert(0);
    OperationResponse resp = session.apply(dupeForZero).join(DEFAULT_SLEEP);
    assertTrue(resp.hasRowError());
    assertTrue(resp.getRowError().getOperation() == dupeForZero);

    // Now try inserting two dupes and one good row, make sure we get only two errors back.
    dupeForZero = createInsert(0);
    Insert dupeForTwo = createInsert(2);
    session.setFlushMode(AsyncKuduSession.FlushMode.MANUAL_FLUSH);
    session.apply(dupeForZero);
    session.apply(dupeForTwo);
    session.apply(createInsert(4));

    List<OperationResponse> responses = session.flush().join(DEFAULT_SLEEP);
    List<RowError> errors = OperationResponse.collectErrors(responses);
    assertEquals(2, errors.size());
    assertTrue(errors.get(0).getOperation() == dupeForZero);
    assertTrue(errors.get(1).getOperation() == dupeForTwo);
  }

  /**
   * Test collecting errors from multiple tablets.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void multiTabletTest() throws Exception {
    String tableName = TestRowErrors.class.getName() + "-" + System.currentTimeMillis();
    createFourTabletsTableWithNineRows(harness.getAsyncClient(), tableName, DEFAULT_SLEEP);
    table = harness.getClient().openTable(tableName);
    KuduSession session = harness.getClient().newSession();
    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);

    final int dupRows = 3;
    session.apply(createInsert(12));
    session.apply(createInsert(22));
    session.apply(createInsert(32));

    session.flush();

    RowErrorsAndOverflowStatus reos = session.getPendingErrors();
    assertEquals(dupRows, reos.getRowErrors().length);
    assertEquals(0, session.countPendingErrors());
  }

  @Test(timeout = 100000)
  public void readableRowErrorTest() throws Exception {
    KuduSession session = harness.getClient().newSession();
    Map<Type, String> dataByType = new HashMap<>();
    dataByType.put(Type.INT32, "10000");
    dataByType.put(Type.DATE, "1970-01-01");
    dataByType.put(Type.STRING, "fun with ütf");
    dataByType.put(Type.BINARY, "[0, 1, 2, 3, 4]");
    int anotherColData = 101;
    Type[] types = new Type[] {Type.INT32, Type.DATE, Type.STRING, Type.BINARY};
    for (Type dataType : types) {
      flushDifferentTypeData(dataType, dataByType, anotherColData, session);
      for (RowError re : session.getPendingErrors().getRowErrors()) {
        String cmpStr = String.format("Row error for row=(%s c0=%s, int32 c1=%d)",
                        dataType.getName(), dataByType.get(dataType), anotherColData);
        if (dataType == Type.STRING || dataType == Type.BINARY) {
          cmpStr = String.format("Row error for row=(%s c0=\"%s\", int32 c1=%d)",
                   dataType.getName(), dataByType.get(dataType), anotherColData);
        }
        assertTrue(re.toString().contains(cmpStr));
      }
    }
  }

  private void flushDifferentTypeData(Type dataType, Map<Type, String> dataByType,
                                      int anotherColData, KuduSession session)
                                      throws Exception {
    String tableName = TestRowErrors.class.getName() + "-" + System.currentTimeMillis();
    CreateTableOptions createOptions = new CreateTableOptions()
        .addHashPartitions(ImmutableList.of("c0"), 2, 0);
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", dataType)
                                .nullable(false)
                                .key(true)
                                .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
                                .nullable(false)
                                .build());
    Schema schema = new Schema(columns);

    KuduClient client = harness.getClient();
    client.createTable(tableName, schema, createOptions);
    table = client.openTable(tableName);

    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    Update update = table.newUpdate();
    PartialRow row = update.getRow();
    switch (dataType) {
      // Type.INT32.
      case INT32:
        row.addInt("c0", Integer.parseInt(dataByType.get(dataType)));
        break;
      // Type.DATE.
      case DATE:
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd" );
        java.util.Date d1 = sdf.parse(dataByType.get(dataType));
        java.sql.Date d2 = new java.sql.Date(d1.getTime());
        row.addDate("c0",  d2);
        break;
      // Type.STRING.
      case STRING:
        row.addString("c0", dataByType.get(dataType));
        break;
      // Type.BINARY.
      case BINARY:
        row.addBinary("c0", dataByType.get(dataType).getBytes("UTF-8"));
        break;
      default:
        return;
    }
    row.addInt("c1", anotherColData);
    session.apply(update);
    session.flush();
  }

  private Insert createInsert(int key) {
    return createBasicSchemaInsert(table, key);
  }
}
