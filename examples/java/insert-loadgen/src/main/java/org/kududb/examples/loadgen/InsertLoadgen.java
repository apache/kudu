package org.kududb.examples.loadgen;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;

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
        case UNIXTIME_MICROS:
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

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: InsertLoadgen kudu_master_host kudu_table");
      System.exit(1);
    }

    String masterHost = args[0];
    String tableName = args[1];

    try (KuduClient client = new KuduClient.KuduClientBuilder(masterHost).build()) {
      KuduTable table = client.openTable(tableName);
      Schema schema = table.getSchema();
      List<RandomDataGenerator> generators = new ArrayList<>(schema.getColumnCount());
      for (int i = 0; i < schema.getColumnCount(); i++) {
        generators.add(new RandomDataGenerator(i, schema.getColumnByIndex(i).getType()));
      }

      KuduSession session = client.newSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      for (int insertCount = 0; ; insertCount++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        for (int i = 0; i < schema.getColumnCount(); i++) {
          generators.get(i).generateColumnData(row);
        }
        session.apply(insert);

        if (insertCount % 1000 == 0 && session.countPendingErrors() > 0) {
          throw new RuntimeException(session.getPendingErrors().getRowErrors()[0].toString());
        }
      }
    }
  }
}
