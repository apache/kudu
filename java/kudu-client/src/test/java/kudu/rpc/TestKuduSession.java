// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import kudu.Schema;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * This class can either start its own cluster or rely on an existing one.
 * By default it assumes that the master is at localhost:64000.
 * The cluster's configuration flags is found at flagsPath as defined in the pom file.
 * Set startCluster to true in order have the test start the cluster for you.
 * All those properties are set via surefire's systemPropertyVariables, meaning this:
 * $ mvn test -DstartCluster=false
 * will use an existing cluster at default address found above.
 *
 * The test creates a table with a unique(ish) name which it deletes at the end.
 */
public class TestKuduSession extends BaseKuduTest {
  // Generate a unique table name
  private final static String tableName =
      TestKuduSession.class.getName()+"-"+System.currentTimeMillis();

  private static Schema schema = getBasicSchema();
  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    createTable(tableName, schema, new CreateTableBuilder());

    table = client.openTable(tableName, schema);
  }



  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test(timeout = 100000)
  public void test() throws Exception {

    KuduSession session = client.newSession(); // using the default, auto-sync
    assertFalse("There shouldn't be any data in the beginning", exists(0));
    Deferred<Object> d = session.apply(createInsert(0));
    d.join();
    assertTrue(exists(0));

    for (int i = 1; i < 10; i++) {
      d = session.apply(createInsert(i));
    }
    d.join();

    assertEquals(10, countInRange(0, 10));

    session.setFlushMode(KuduSession.FlushMode.MANUAL_FLUSH);
    session.setMutationBufferSpace(10);

    d = session.apply(createInsert(10));

    try {
      session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    } catch (IllegalArgumentException ex) {
      /* expected, flush mode remains manual */
    }

    assertFalse(exists(10));

    for (int i = 11; i < 20; i++) {
      d = session.apply(createInsert(i));
    }

    assertEquals(0, countInRange(10, 20));
    try {
      session.apply(createInsert(20));
    } catch (NonRecoverableException ex) {
      /* expected, buffer would be too big */
    }
    assertEquals(0, countInRange(10, 20)); // the buffer should still be full

    session.flush();
    d.join(); // d is from the last good insert eg 20
    assertEquals(10, countInRange(10, 20)); // now everything should be there

    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);

    d = session.apply(createInsert(20));
    Thread.sleep(50); // waiting a minimal amount of time to make sure the interval is in effect
    assertFalse(exists(20));
    // Add 10 items, the last one will stay in the buffer
    for (int i = 21; i < 30; i++) {
      d = session.apply(createInsert(i));
    }
    Deferred<Object> buffered = session.apply(createInsert(30));
    long now = System.currentTimeMillis();
    d.join();
    // auto flush will force flush if the buffer is full as it should be now
    // so we check that we didn't wait the full interval
    long elapsed = System.currentTimeMillis() - now;
    assertTrue(elapsed < 950);
    assertEquals(10, countInRange(20, 31));
    buffered.join();
    assertEquals(11, countInRange(20, 31));

    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    Update update = createUpdate(30);
    update.addInt(schema.getColumn(2).getName(), 999);
    update.addString(schema.getColumn(3).getName(), "updated data");
    d = session.apply(update);
    d.addErrback(defaultErrorCB);
    d.join();
    assertEquals(31, countInRange(0, 31));

    Delete del = createDelete(30);
    d = session.apply(del);
    d.addErrback(defaultErrorCB);
    d.join();
    assertEquals(30, countInRange(0, 31));

    session.setFlushMode(KuduSession.FlushMode.MANUAL_FLUSH);
    session.setMutationBufferSpace(35);
    for (int i = 0; i < 20; i++) {
      buffered = session.apply(createDelete(i));
    }
    assertEquals(30, countInRange(0, 31));
    session.flush();
    buffered.join();
    assertEquals(10, countInRange(0, 31));

    for (int i = 30; i < 40; i++) {
      session.apply(createInsert(i));
    }

    for (int i = 20; i < 30; i++) {
      buffered = session.apply(createDelete(i));
    }

    assertEquals(10, countInRange(0, 40));
    session.flush();
    buffered.join(2000);
    assertEquals(10, countInRange(0, 40));

    // TODO add a test for manual flush against multiple tablets once we can pre-split on ints

    // Test nulls
    // add 10 rows with the nullable column set to null
    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    for (int i = 40; i < 50; i++) {
      session.apply(createInsertWithNull(i)).join();
    }

    // now scan those rows and make sure the column is null
    assertEquals(10, countNullColumns(40, 50));

    // Test Alter
    // Add a col
    AlterTableBuilder atb = new AlterTableBuilder();
    atb.addColumn("testaddint", Type.INT32, 4);
    submitAlterAndCheck(atb);

    // rename that col
    atb = new AlterTableBuilder();
    atb.renameColumn("testaddint", "newtestaddint");
    submitAlterAndCheck(atb);

    // delete it
    atb = new AlterTableBuilder();
    atb.dropColumn("newtestaddint");
    submitAlterAndCheck(atb);

    String newTableName = tableName+"new";

    // rename our table
    atb = new AlterTableBuilder();
    atb.renameTable(newTableName);
    submitAlterAndCheck(atb, tableName, newTableName);

    // rename it back
    atb = new AlterTableBuilder();
    atb.renameTable(tableName);
    submitAlterAndCheck(atb, newTableName, tableName);

    // try adding two columns, where one is nullable
    atb = new AlterTableBuilder();
    atb.addColumn("testaddmulticolnotnull", Type.INT32, 4);
    atb.addNullableColumn("testaddmulticolnull", Type.STRING);
    submitAlterAndCheck(atb);
  }

  /**
   * Helper method to submit an Alter and wait for it to happen, using the default table name
   */
  public static void submitAlterAndCheck(AlterTableBuilder atb) throws Exception {
    submitAlterAndCheck(atb, tableName, tableName);
  }

  public static void submitAlterAndCheck(AlterTableBuilder atb,
                                         String tableToAlter, String tableToCheck) throws
      Exception {
    Deferred<Object> alterDeffered = client.alterTable(tableToAlter, atb);
    alterDeffered.join(DEFAULT_SLEEP);
    boolean done  = client.syncWaitOnAlterCompletion(tableToCheck, DEFAULT_SLEEP);
    assertTrue(done);
  }

  public static Insert createInsert(int key) {
    Insert insert = table.newInsert();
    insert.addInt(schema.getColumn(0).getName(), key);
    insert.addInt(schema.getColumn(1).getName(), 2);
    insert.addInt(schema.getColumn(2).getName(), 3);
    insert.addString(schema.getColumn(3).getName(), "a string");
    return insert;
  }

  public static Insert createInsertWithNull(int key) {
    Insert insert = table.newInsert();
    insert.addInt(schema.getColumn(0).getName(), key);
    insert.addInt(schema.getColumn(1).getName(), 2);
    insert.addInt(schema.getColumn(2).getName(), 3);
    insert.setNull(schema.getColumn(3).getName());
    return insert;
  }

  public static Update createUpdate(int key) {

    Update update = table.newUpdate();
    update.addInt(schema.getColumn(0).getName(), key);
    return update;
  }

  public static Delete createDelete(int key) {
    Delete delete = table.newDelete();
    delete.addInt(schema.getColumn(0).getName(), key);
    return delete;
  }

  public static boolean exists(final int key) throws Exception {

    KuduScanner scanner = getScanner(key, key);
    final AtomicBoolean exists = new AtomicBoolean(false);

    Callback<Object, KuduScanner.RowResultIterator> cb =
        new Callback<Object, KuduScanner.RowResultIterator>() {
      @Override
      public Object call(KuduScanner.RowResultIterator arg) throws Exception {
        if (arg == null) return null;
        RowResult row;
        while (arg.hasNext()) {
          row = arg.next();
          if (row.getInt(0) == key) {
            exists.set(true);
            break;
          }
        }
        return null;
      }
    };

    while (scanner.hasMoreRows()) {
      Deferred<KuduScanner.RowResultIterator> data = scanner.nextRows();
      data.addCallbacks(cb, defaultErrorCB);
      data.join();
      if (exists.get()) {
        break;
      }
    }

    Deferred<KuduScanner.RowResultIterator> closer = scanner.close();
    closer.join();
    return exists.get();
  }

  public static int countNullColumns(final int startKey, final int endKey) throws Exception {

    KuduScanner scanner = getScanner(startKey, endKey);
    final AtomicInteger ai = new AtomicInteger();

    Callback<Object, KuduScanner.RowResultIterator> cb =
        new Callback<Object, KuduScanner.RowResultIterator>() {
          @Override
          public Object call(KuduScanner.RowResultIterator arg) throws Exception {
            if (arg == null) return null;
            RowResult row;
            while (arg.hasNext()) {
              row = arg.next();
              if (row.isNull(3)) {
                ai.incrementAndGet();
              }
            }
            return null;
          }
        };

    while (scanner.hasMoreRows()) {
      Deferred<KuduScanner.RowResultIterator> data = scanner.nextRows();
      data.addCallbacks(cb, defaultErrorCB);
      data.join();
    }

    Deferred<KuduScanner.RowResultIterator> closer = scanner.close();
    closer.join();
    return ai.get();
  }

  public static int countInRange(final int startOrder, final int endOrder) throws Exception {

    KuduScanner scanner = getScanner(startOrder, endOrder);
    return countRowsInScan(scanner);
  }

  private static KuduScanner getScanner(int start, int end) {
    KuduScanner scanner = client.newScanner(table, schema);
    ColumnRangePredicate predicate = new ColumnRangePredicate(schema.getColumn(0));
    predicate.setLowerBound(start);
    predicate.setUpperBound(end);
    scanner.addColumnRangePredicate(predicate);
    return scanner;
  }
}
