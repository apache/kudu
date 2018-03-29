package org.apache.kudu.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration.FlushMode;


public class KuduCollectlExample {
  private static final int GRAPHITE_PORT = 2003;
  private static final String TABLE_NAME = "metrics";
  private static final String ID_TABLE_NAME = "metric_ids";

  private static final String KUDU_MASTER =
      System.getProperty("kuduMasters", "localhost:7051");

  private KuduClient client;
  private KuduTable table;
  private KuduTable idTable;

  private Set<String> existingMetrics = Collections.newSetFromMap(
    new ConcurrentHashMap<String, Boolean>());

  public static void main(String[] args) throws Exception {
    new KuduCollectlExample().run();
  }

  KuduCollectlExample() {
    this.client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
  }
  
  public void run() throws Exception {
    createTableIfNecessary();
    createIdTableIfNecessary();
    this.table = client.openTable(TABLE_NAME);
    this.idTable = client.openTable(ID_TABLE_NAME);
    try (ServerSocket listener = new ServerSocket(GRAPHITE_PORT)) {
      while (true) {
        Socket s = listener.accept();
        new HandlerThread(s).start();
      }
    }
  }
  
  private void createTableIfNecessary() throws Exception {
    if (client.tableExists(TABLE_NAME)) {
      return;
    }
    
    List<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchemaBuilder("host", Type.STRING).key(true).encoding(
        ColumnSchema.Encoding.DICT_ENCODING).build());
    cols.add(new ColumnSchemaBuilder("metric", Type.STRING).key(true).encoding(
        ColumnSchema.Encoding.DICT_ENCODING).build());
    cols.add(new ColumnSchemaBuilder("timestamp", Type.INT32).key(true).encoding(
        ColumnSchema.Encoding.BIT_SHUFFLE).build());
    cols.add(new ColumnSchemaBuilder("value", Type.DOUBLE)
        .encoding(ColumnSchema.Encoding.BIT_SHUFFLE).build());

    // Need to set this up since we're not pre-partitioning.
    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("host");
    rangeKeys.add("metric");
    rangeKeys.add("timestamp");

    client.createTable(TABLE_NAME, new Schema(cols),
                       new CreateTableOptions().setRangePartitionColumns(rangeKeys));
  }
  
  private void createIdTableIfNecessary() throws Exception {
    if (client.tableExists(ID_TABLE_NAME)) {
      return;
    }
    
    ArrayList<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchemaBuilder("host", Type.STRING).key(true).build());
    cols.add(new ColumnSchemaBuilder("metric", Type.STRING).key(true).build());

    // Need to set this up since we're not pre-partitioning.
    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("host");
    rangeKeys.add("metric");

    client.createTable(ID_TABLE_NAME, new Schema(cols),
                       new CreateTableOptions().setRangePartitionColumns(rangeKeys));
  }

  class HandlerThread extends Thread {
    private Socket socket;
    private KuduSession session;

    HandlerThread(Socket s) {
      this.socket = s;
      this.session = client.newSession();
      // TODO: AUTO_FLUSH_BACKGROUND would be better for this kind of usecase, but
      // it seems like it's buffering data too long, and only flushing based on size.
      // Perhaps we should support a time-based buffering as well?
      session.setFlushMode(FlushMode.MANUAL_FLUSH);
      
      // Increase the number of mutations that we can buffer
      session.setMutationBufferSpace(10000);
    }
    
    @Override
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        System.err.println("exception handling connection from " + socket);
        e.printStackTrace();
      }
    }

    private void insertIdIfNecessary(String host, String metric) throws Exception {
      String id = host + "/" + metric;
      if (existingMetrics.contains(id)) {
        return;
      }
      Insert ins = idTable.newInsert();
      ins.getRow().addString("host", host);
      ins.getRow().addString("metric", metric);
      session.apply(ins);
      session.flush();
      // TODO: error handling!
      //System.err.println("registered new metric " + id);
      existingMetrics.add(id);
    }
    
    private void doRun() throws Exception {
      BufferedReader br = new BufferedReader(new InputStreamReader(
          socket.getInputStream()));
      socket = null;
      
      // Read lines from collectd. Each line should look like:
      // hostname.example.com/.cpuload.avg1 2.27 1435788059
      String input;
      while ((input = br.readLine()) != null) { 
        String[] fields = input.split(" ");
        if (fields.length != 3) {
          throw new Exception("Invalid input: " + input);
        }
        String[] hostAndMetric = fields[0].split("/.");
        if (hostAndMetric.length != 2) {
          System.err.println("bad line: " + input);
          throw new Exception("expected /. delimiter between host and metric name. " +
              "Did you run collectl with --export=collectl,<hostname>,p=/ ?");
        }
        String host = hostAndMetric[0];
        String metric = hostAndMetric[1];
        insertIdIfNecessary(host, metric);
        double val = Double.parseDouble(fields[1]);        
        int ts = Integer.parseInt(fields[2]);
        
        Insert insert = table.newInsert();
        insert.getRow().addString("host", hostAndMetric[0]);
        insert.getRow().addString("metric", hostAndMetric[1]);
        insert.getRow().addInt("timestamp", ts);
        insert.getRow().addDouble("value", val);
        session.apply(insert);
        
        // If there's more data to read, don't flush yet -- better to accumulate
        // a larger batch.
        if (!br.ready()) {
          List<OperationResponse> responses = session.flush();
          for (OperationResponse r : responses) {
            if (r.hasRowError()) {
              RowError e = r.getRowError();
              // TODO: the client should offer an enum for different row errors, instead
              // of string comparison!
              if ("ALREADY_PRESENT".equals(e.getStatus())) {
                continue;
              }
              System.err.println("Error inserting " + e.getOperation().toString()
                  + ": " + e.toString());
            }
          }
        }
      }
    }
  }
}
