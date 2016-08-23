package org.kududb.examples.loadgen;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class InsertLoadgen {

  private static class RandomDataGenerator {
    private final Random rng;
    private final int index;
    private final Type type;

    /**
     * Instantiate a random data generator for a specific field.
     * @param index The numerical index of the column in the row schema
     * @param type The type of the data at index {@code index}
     */
    public RandomDataGenerator(int index, Type type) {
      this.rng = new Random();
      this.index = index;
      this.type = type;
    }

    /**
     * Add random data to the given row for the column at index {@code index}
     * of type {@code type}
     * @param row The row to add the field to
     */
    void generateColumnData(PartialRow row) {
      switch (type) {
        case INT8:
          row.addByte(index, (byte) rng.nextInt(Byte.MAX_VALUE));
          return;
        case INT16:
          row.addShort(index, (short)rng.nextInt(Short.MAX_VALUE));
          return;
        case INT32:
          row.addInt(index, rng.nextInt(Integer.MAX_VALUE));
          return;
        case INT64:
        case TIMESTAMP:
          row.addLong(index, rng.nextLong());
          return;
        case BINARY:
          byte bytes[] = new byte[16];
          rng.nextBytes(bytes);
          row.addBinary(index, bytes);
          return;
        case STRING:
          row.addString(index, UUID.randomUUID().toString());
          return;
        case BOOL:
          row.addBoolean(index, rng.nextBoolean());
          return;
        case FLOAT:
          row.addFloat(index, rng.nextFloat());
          return;
        case DOUBLE:
          row.addDouble(index, rng.nextDouble());
          return;
        default:
          throw new UnsupportedOperationException("Unknown type " + type);
      }
    }
  }

  public static void runLoad(String masterHost, String tableName) {
    KuduClient client = new KuduClient.KuduClientBuilder(masterHost).build();

    try {
      KuduTable table = client.openTable(tableName);
      Schema schema = table.getSchema();
      List<RandomDataGenerator> generators = new ArrayList<>(schema.getColumnCount());
      for (int i = 0; i < schema.getColumnCount(); i++) {
        generators.add(new RandomDataGenerator(i, schema.getColumnByIndex(i).getType()));
      }

      KuduSession session = client.newSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      while (true) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        for (int i = 0; i < schema.getColumnCount(); i++) {
          generators.get(i).generateColumnData(row);
        }
        session.apply(insert);
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: InsertLoadgen kudu_master_host kudu_table num_threads");
      System.exit(1);
    }

    final String masterHost = args[0];
    final String tableName = args[1];
    int numThreads = Integer.parseInt(args[2]);

    final CountDownLatch latch = new CountDownLatch(numThreads);

    List<Thread> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      threads.add(new Thread(new Runnable() {
        public void run() {
          runLoad(masterHost, tableName);
          latch.countDown();
        }
      }));
      threads.get(i).start();
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
