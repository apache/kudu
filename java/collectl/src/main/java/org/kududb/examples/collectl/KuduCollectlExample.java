package org.kududb.examples.collectl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.kududb.ColumnSchema;
import org.kududb.ColumnSchema.ColumnSchemaBuilder;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.BatchResponse;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.RowError;
import org.kududb.client.SessionConfiguration.FlushMode;


public class KuduCollectlExample {
  private static final int GRAPHITE_PORT = 2003;
  private static final String TABLE_NAME = "metrics2";
  private static final String ID_TABLE_NAME = "metric_ids";

  private KuduClient client;
  private KuduTable table;
  private KuduTable idTable;

  private Set<String> existingMetrics = Collections.newSetFromMap(
    new ConcurrentHashMap<String, Boolean>());

  public static void main(String[] args) throws Exception {
    new KuduCollectlExample().run();
  }

  KuduCollectlExample() {
    this.client = new KuduClient.KuduClientBuilder("127.0.0.1").build();
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
    
    ArrayList<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchemaBuilder("host", Type.STRING).key(true).build());
    cols.add(new ColumnSchemaBuilder("metric", Type.STRING).key(true).build());
    cols.add(new ColumnSchemaBuilder("timestamp", Type.INT32).key(true).build());
    cols.add(new ColumnSchemaBuilder("value", Type.DOUBLE).build());

    client.createTable(TABLE_NAME, new Schema(cols));
  }
  
  private void createIdTableIfNecessary() throws Exception {
    if (client.tableExists(ID_TABLE_NAME)) {
      return;
    }
    
    ArrayList<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchemaBuilder("host", Type.STRING).key(true).build());
    cols.add(new ColumnSchemaBuilder("metric", Type.STRING).key(true).build());

    client.createTable(ID_TABLE_NAME, new Schema(cols));
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
      ins.addString("host", host);
      ins.addString("metric", metric);
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
        insert.addString("host", hostAndMetric[0]);
        insert.addString("metric", hostAndMetric[1]);
        insert.addInt("timestamp", ts);
        insert.addDouble("value", val);
        session.apply(insert);
        
        // If there's more data to read, don't flush yet -- better to accumulate
        // a larger batch.
        if (!br.ready()) {
          ArrayList<BatchResponse> responses = session.flush();
          // TODO: the client should not group BatchResponse by tablet, since tablets
          // are an implementation detail.
          for (BatchResponse r : responses) {
            for (RowError e : r.getRowErrors()) {
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
