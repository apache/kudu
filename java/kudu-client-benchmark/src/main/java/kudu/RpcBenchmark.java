// Copyright (c) 2013, Cloudera, inc.
package kudu;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.rpc.Insert;
import kudu.rpc.KuduClient;
import kudu.rpc.KuduScanner;
import kudu.rpc.KuduSession;
import kudu.rpc.KuduTable;
import kudu.rpc.RowResult;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class currently just contains code to drive load, nothing fancy
 * TODO make it do benchmarks
 */
public class RpcBenchmark {

  private static final long DEFAULT_SLEEP = 5000;

  private static void test(String address, int port) throws Exception {
    KuduClient client = null;
    try {
      client = new KuduClient(address, port);
      Schema schema = Tpch1Schema.getTpch1Schema();
      String tableName = "tpch1";
      Deferred<Object> create = client.createTable(tableName, schema);
      create.join(DEFAULT_SLEEP);
      KuduTable table = (KuduTable)client.openTable(tableName).join(DEFAULT_SLEEP);
      Deferred<Object> dd = client.ping();
      dd.join(DEFAULT_SLEEP);

      final AtomicLong counter = new AtomicLong();
      Callback<Object, KuduScanner.RowResultIterator> cb = new Callback<Object, KuduScanner.RowResultIterator>() {
        @Override
        public Object call(KuduScanner.RowResultIterator arg) throws Exception {
          if (arg == null) return null;
          //System.out.println("Got me some data! " + arg.getNumRows());
          counter.addAndGet(arg.getNumRows());
          //Thread.sleep(13);
          RowResult row;
          while (arg.hasNext()) {
            row = arg.next();
            //System.out.println(row.toStringLongFormat());
          }
          return null;
        }
      };

      int count = 100000;
      final CountDownLatch latch = new CountDownLatch(count);
      Callback<Object, Object> insertCb = new Callback<Object, Object>() {

        @Override
        public Object call(Object arg) throws Exception {
          if (arg instanceof Exception) {
            System.out.println(((Exception)arg).getMessage());
          }
          latch.countDown();
          return null;
        }
      };

      KuduSession session = client.newSession(); // defaults to auto flush sync
      session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
      Deferred<Object> lastD = null;
      long time = System.currentTimeMillis();
      for (int i = count * 0; i < count * 30; i++) {
        Insert insert = table.newInsert();
        insert.addInt(schema.getColumn(0).getName(), i);
        insert.addInt(schema.getColumn(1).getName(), 2);
        insert.addInt(schema.getColumn(2).getName(), 3);
        insert.addInt(schema.getColumn(3).getName(), 4);
        insert.addInt(schema.getColumn(4).getName(), 5);
        insert.addInt(schema.getColumn(5).getName(), 6);
        insert.addInt(schema.getColumn(6).getName(), 7);
        insert.addInt(schema.getColumn(7).getName(), 8);

        insert.addString(schema.getColumn(8).getName(), "l_returnflag");
        insert.addString(schema.getColumn(9).getName(), "l_linestatus");
        insert.addString(schema.getColumn(10).getName(), "l_shipdate");
        insert.addString(schema.getColumn(11).getName(), "l_commitdate");
        insert.addString(schema.getColumn(12).getName(), "l_receiptdate");
        insert.addString(schema.getColumn(13).getName(), "l_shipinstruct");
        insert.addString(schema.getColumn(14).getName(), "l_shipmode");
        insert.addString(schema.getColumn(15).getName(), "l_comment");

        Deferred<Object> insertD = session.apply(insert);
        insertD.addCallbacks(insertCb, insertCb);
        lastD = insertD;
      }
      session.flush();
      for (int i = 0; i < 24; i++) {
        boolean completed = latch.await(DEFAULT_SLEEP, TimeUnit.MILLISECONDS);
        if (latch.getCount() != 0) {
          System.out.println("Not all rows came back, remaining " + latch.getCount());
        }
        if (!completed && i == 23) {
          System.exit(1);
        } else if (completed) break;
      }
      System.out.println("Inserted in " + (System.currentTimeMillis() - time));
      lastD.join();
      session.close();

      // Scan the whole table x times
      /*for (int i = 0; i < 1; i++) {
        time = System.currentTimeMillis();

        counter.set(0);

        KuduScanner scanner = client.newScanner(table);
        scanner.setPrefetching(i % 2 == 0);
        scanner.setSchema(schema);
        //scanner.setLimit(10);
        while (scanner.hasMoreRows()) {
          Deferred<KuduScanner.RowResultIterator> data = scanner.nextRows();
          data.addCallback(cb);
          data.join(2000);
        }

        Deferred<KuduScanner.RowResultIterator> closer = scanner.close();
        closer.addCallback(cb);
        closer.join(2000);
        System.out.println("Scanned " + counter.get() + " in " + (System.currentTimeMillis() - time));
      }*/
    } finally {
      client.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    String address = "127.0.0.1";
    int port = 7051;
    if (args.length == 2) {
      address = args[0];
      port = Integer.parseInt(args[1]);
    } else if (args.length != 0) {
      System.out.println("Usage: RpcTest [master_address master_rpc_port]");
    }
    test(address, port);
    System.exit(0);
  }
}
