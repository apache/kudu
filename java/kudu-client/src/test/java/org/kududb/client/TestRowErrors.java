// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestRowErrors extends BaseKuduTest {

  // Generate a unique table name
  private static final String TABLE_NAME =
      TestRowErrors.class.getName() + "-" + System.currentTimeMillis();

  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    createTable(TABLE_NAME, basicSchema, new CreateTableBuilder());

    table = openTable(TABLE_NAME);
  }


  @Test(timeout = 100000)
  public void test() throws Exception {
    AsyncKuduSession session = client.newSession();

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

  private Insert createInsert(int key) {
    return createBasicSchemaInsert(table, key);
  }
}
