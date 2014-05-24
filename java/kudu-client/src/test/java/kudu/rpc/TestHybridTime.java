// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.stumbleupon.async.Deferred;
import kudu.ColumnSchema;
import kudu.Schema;
import kudu.tserver.Tserver;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static kudu.Type.STRING;
import static kudu.rpc.ExternalConsistencyMode.CLIENT_PROPAGATED;
import static kudu.util.HybridTimeUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This only tests client propagation since it's the only thing that is client-specific.
 * All the work for commit wait is done and tested on the server-side.
 */
public class TestHybridTime extends BaseKuduTest {

  // Generate a unique table name
  protected static final String TABLE_NAME =
    TestHybridTime.class.getName() + "-" + System.currentTimeMillis();

  protected static Schema schema = getSchema();
  protected static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    // create a 4-tablets table for scanning
    CreateTableBuilder builder = new CreateTableBuilder();
    KeyBuilder keyBuilder = new KeyBuilder(schema);
    builder.addSplitKey(keyBuilder.addString("1"));
    builder.addSplitKey(keyBuilder.addString("2"));
    builder.addSplitKey(keyBuilder.addString("3"));
    createTable(TABLE_NAME, schema, builder);

    table = openTable(TABLE_NAME);
  }

  private static Schema getSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(1);
    columns.add(new ColumnSchema("key", STRING, true));
    return new Schema(columns);
  }

  /**
   * We write to all tablets. We increment the timestamp we get back from the first write
   * by some amount. The remaining writes should force an update to the server's clock and
   * only increment the logical values.
   *
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void test() throws Exception {
    KuduSession session = client.newSession();
    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    session.setExternalConsistencyMode(CLIENT_PROPAGATED);
    long[] clockValues;
    long previousLogicalValue = 0;
    long previousPhysicalValue = 0;

    // Test timestamp propagation with single operations
    String[] keys = new String[] {"1", "2", "3"};
    for (int i = 0; i < keys.length; i++) {
      Insert insert = table.newInsert();
      insert.addString(schema.getColumn(0).getName(), keys[i]);
      Deferred<Object> d = session.apply(insert);
      Tserver.WriteResponsePB response = (Tserver.WriteResponsePB) d.join(DEFAULT_SLEEP);
      assertTrue(response.hasWriteTimestamp());
      clockValues = HTTimestampToPhysicalAndLogical(response.getWriteTimestamp());
      LOG.debug("Clock value after write[" + i + "]: " + new Date(clockValues[0] / 1000).toString()
        + " Logical value: " + clockValues[1]);
      // on the very first write we update the clock into the future
      // so that remaining writes only update logical values
      if (i == 0) {
        assertEquals(clockValues[1], 0);
        long toUpdateTs = clockValues[0] + 5000000;
        previousPhysicalValue = toUpdateTs;
        // After the first write we fake-update the clock into the future. Following writes
        // should force the servers to update their clocks to this value.
        client.updateLastPropagatedTimestamp(
          clockTimestampToHTTimestamp(toUpdateTs, TimeUnit.MICROSECONDS));
      } else {
        assertEquals(clockValues[0], previousPhysicalValue);
        assertTrue(clockValues[1] > previousLogicalValue);
        previousLogicalValue = clockValues[1];
      }
    }

    // Test timestamp propagation with Batches
    session.setFlushMode(KuduSession.FlushMode.MANUAL_FLUSH);
    keys = new String[] {"11", "22", "33"};
    for (int i = 0; i < keys.length; i++) {
      Insert insert = table.newInsert();
      insert.addString(schema.getColumn(0).getName(), keys[i]);
      session.apply(insert);
      Deferred<Object> d = session.flush();
      Object responseObj = d.join(DEFAULT_SLEEP);
      assertTrue("Response was not of the expected type: " + responseObj.getClass().getName(),
        responseObj instanceof ArrayList);
      List<Tserver.WriteResponsePB> responses = (List<Tserver.WriteResponsePB>) responseObj;
      assertEquals("Response was not of the expected size: " + responses.size(),
        1, responses.size());

      Tserver.WriteResponsePB response = responses.get(0);
      assertTrue(response.hasWriteTimestamp());
      clockValues = HTTimestampToPhysicalAndLogical(response.getWriteTimestamp());
      LOG.debug("Clock value after write[" + i + "]: " + new Date(clockValues[0] / 1000).toString()
        + " Logical value: " + clockValues[1]);
      assertEquals(clockValues[0], previousPhysicalValue);
      assertTrue(clockValues[1] > previousLogicalValue);
      previousLogicalValue = clockValues[1];
    }

    // Scan all rows with READ_LATEST (the default) we should get 6 rows back
    KuduScanner scanner = client.newScanner(table, schema);
    assertEquals(6, countRowsInScan(scanner));

    // Now scan at multiple instances with READ_AT_SNAPSHOT we should get different
    // counts depending on the scan timestamp.
    long snapTime = physicalAndLogicalToHTTimestamp(previousPhysicalValue, 0);
    assertEquals(1, scanAtSnapshot(snapTime));
    snapTime = physicalAndLogicalToHTTimestamp(previousPhysicalValue, 4);
    assertEquals(3, scanAtSnapshot(snapTime));
    // Our last snap time needs to one one into the future w.r.t. the last write's timestamp
    // for us to be able to get all rows, but the snap timestamp can't be bigger than the prop.
    // timestamp so we increase both.
    client.updateLastPropagatedTimestamp(client.getLastPropagatedTimestamp() + 1);
    snapTime = physicalAndLogicalToHTTimestamp(previousPhysicalValue, previousLogicalValue + 1);
    assertEquals(6, scanAtSnapshot(snapTime));
  }

  private int scanAtSnapshot(long time) throws Exception {
    KuduScanner scanner = client.newSnapshotScanner(table, schema);
    scanner.setSnapshotTimestamp(time);
    return countRowsInScan(scanner);
  }
}
