//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
    createTable(TABLE_NAME, basicSchema, new CreateTableOptions());

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
