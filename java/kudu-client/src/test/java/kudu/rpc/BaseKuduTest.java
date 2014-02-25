// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.ColumnSchema;
import kudu.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static kudu.Type.INT32;
import static kudu.Type.STRING;
import static org.junit.Assert.fail;

public class BaseKuduTest {

  public static final Logger LOG = LoggerFactory.getLogger(BaseKuduTest.class);

  private final static String MASTER_ADDRESS = "masterAddress";
  private final static String MASTER_PORT = "masterPort";
  private final static String FLAGS_PATH = "flagsPath";
  private final static String START_CLUSTER = "startCluster";
  private static boolean startCluster;
  static String masterAddress;
  static int masterPort;

  static final int DEFAULT_SLEEP = 10000;
  static Process master;
  static Process tabletServer;
  static KuduClient client;

  private static List<String> tableNames = new ArrayList<String>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // the following props are set via kudu-client's pom
    masterAddress = System.getProperty(MASTER_ADDRESS);
    masterPort = Integer.parseInt(System.getProperty(MASTER_PORT));
    String flagsPath = System.getProperty(FLAGS_PATH);
    startCluster = Boolean.parseBoolean(System.getProperty(START_CLUSTER));

    if (startCluster) {
      String[] masterCmdLine = {"kudu-master", "--flagfile=" + flagsPath};
      String[] tsCmdLine = {"kudu-tablet_server", masterCmdLine[1]};

      master = Runtime.getRuntime().exec(masterCmdLine);
      Thread.sleep(300);
      tabletServer = Runtime.getRuntime().exec(tsCmdLine);
      // TODO lower than 1000 and we burp a too many retries, fix
      Thread.sleep(1000);
    }

    client = new KuduClient(masterAddress, masterPort);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      for (String tableName : tableNames) {
        final AtomicBoolean gotError = new AtomicBoolean(false);
        Deferred<Object> d = client.deleteTable(tableName);
        d.addErrback(new Callback<Object, Object>() {
          @Override
          public Object call(Object arg) throws Exception {
            LOG.warn("tearDown errback " + arg);
            gotError.set(true);
            return null;
          }
        });
        d.join(DEFAULT_SLEEP);
        if (gotError.get()) {
          fail("Couldn't delete a table");
        }
      }
    } finally {
      client.shutdown();
      if (startCluster) {
        master.destroy();
        tabletServer.destroy();
      }
    }
  }

  static void createTable(String tableName, Schema schema, CreateTableBuilder builder) {
    Deferred<Object> d = client.createTable(tableName, schema, builder);
    final AtomicBoolean gotError = new AtomicBoolean(false);
    d.addErrback(new Callback<Object, Object>() {
      @Override
      public Object call(Object arg) throws Exception {
        gotError.set(true);
        return null;
      }
    });
    try {
      d.join(DEFAULT_SLEEP);
    } catch (Exception e) {
      fail("Timed out");
    }
    if (gotError.get()) {
      fail("Got error during table creation, is the Kudu master running at " +
          masterAddress + ":" + masterPort + "?");
    }
    tableNames.add(tableName);
  }

  static int countRowsInScan(KuduScanner scanner) throws Exception{
    final AtomicInteger counter = new AtomicInteger();

    Callback<Object, KuduScanner.RowResultIterator> cb = new Callback<Object, KuduScanner.RowResultIterator>() {
      @Override
      public Object call(KuduScanner.RowResultIterator arg) throws Exception {
        if (arg == null) return null;
        RowResult row;
        while (arg.hasNext()) {
          row = arg.next();
          counter.incrementAndGet();
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
    closer.addCallbacks(cb, defaultErrorCB);
    closer.join();
    return counter.get();
  }

  public static Schema getBasicSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(4);
    columns.add(new ColumnSchema("key", INT32, true));
    columns.add(new ColumnSchema("column1_i", INT32));
    columns.add(new ColumnSchema("column2_i", INT32));
    columns.add(new ColumnSchema("column3_s", STRING));
    return new Schema(columns);
  }

  static Callback<Object, Object> defaultErrorCB = new Callback<Object, Object>() {
    @Override
    public Object call(Object arg) throws Exception {
      if (arg == null) return null;
      if (arg instanceof Exception) {
        LOG.warn("Got exception", (Exception) arg);
      } else {
        LOG.warn("Got an error response back " + arg);
      }
      return null;
    }
  };

}
