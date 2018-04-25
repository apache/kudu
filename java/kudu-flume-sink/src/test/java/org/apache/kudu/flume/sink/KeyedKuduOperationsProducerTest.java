/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.kudu.flume.sink;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.MASTER_ADDRESSES;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.PRODUCER;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.PRODUCER_PREFIX;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.TABLE_NAME;
import static org.apache.kudu.flume.sink.SimpleKeyedKuduOperationsProducer.KEY_COLUMN_DEFAULT;
import static org.apache.kudu.flume.sink.SimpleKeyedKuduOperationsProducer.OPERATION_PROP;
import static org.apache.kudu.flume.sink.SimpleKeyedKuduOperationsProducer.PAYLOAD_COLUMN_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.BaseKuduTest;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;

public class KeyedKuduOperationsProducerTest extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(KeyedKuduOperationsProducerTest.class);

  private KuduTable createNewTable(String tableName) throws Exception {
    LOG.info("Creating new table...");

    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(
        new ColumnSchema.ColumnSchemaBuilder(KEY_COLUMN_DEFAULT, Type.STRING)
            .key(true).build());
    columns.add(
        new ColumnSchema.ColumnSchemaBuilder(PAYLOAD_COLUMN_DEFAULT, Type.BINARY)
            .key(false).build());
    CreateTableOptions createOptions =
      new CreateTableOptions().setRangePartitionColumns(ImmutableList.of(KEY_COLUMN_DEFAULT))
                              .setNumReplicas(1);
    KuduTable table = createTable(tableName, new Schema(columns), createOptions);

    LOG.info("Created new table.");

    return table;
  }

  @Test
  public void testEmptyChannelWithInsert() throws Exception {
    testEvents(0, "insert");
  }

  @Test
  public void testOneEventWithInsert() throws Exception {
    testEvents(1, "insert");
  }

  @Test
  public void testThreeEventsWithInsert() throws Exception {
    testEvents(3, "insert");
  }

  @Test
  public void testEmptyChannelWithUpsert() throws Exception {
    testEvents(0, "upsert");
  }

  @Test
  public void testOneEventWithUpsert() throws Exception {
    testEvents(1, "upsert");
  }

  @Test
  public void testThreeEventsWithUpsert() throws Exception {
    testEvents(3, "upsert");
  }

  @Test
  public void testDuplicateRowsWithUpsert() throws Exception {
    LOG.info("Testing events with upsert...");

    KuduTable table = createNewTable("testDupUpsertEvents");
    String tableName = table.getName();
    Context ctx = new Context(ImmutableMap.of(PRODUCER_PREFIX + OPERATION_PROP, "upsert"));
    KuduSink sink = createSink(tableName, ctx);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    Transaction tx = channel.getTransaction();
    tx.begin();

    int numRows = 3;
    for (int i = 0; i < numRows; i++) {
      Event e = EventBuilder.withBody(String.format("payload body %s", i), UTF_8);
      e.setHeaders(ImmutableMap.of(KEY_COLUMN_DEFAULT, String.format("key %s", i)));
      channel.put(e);
    }

    tx.commit();
    tx.close();

    Sink.Status status = sink.process();
    assertTrue("incorrect status for non-empty channel", status != Sink.Status.BACKOFF);

    List<String> rows = scanTableToStrings(table);
    assertEquals(numRows + " row(s) expected", numRows, rows.size());

    for (int i = 0; i < numRows; i++) {
      assertTrue("incorrect payload", rows.get(i).contains("payload body " + i));
    }

    Transaction utx = channel.getTransaction();
    utx.begin();

    Event dup = EventBuilder.withBody("payload body upserted".getBytes(UTF_8));
    dup.setHeaders(ImmutableMap.of("key", String.format("key %s", 0)));
    channel.put(dup);

    utx.commit();
    utx.close();

    Sink.Status upStatus = sink.process();
    assertTrue("incorrect status for non-empty channel", upStatus != Sink.Status.BACKOFF);

    List<String> upRows = scanTableToStrings(table);
    assertEquals(numRows + " row(s) expected", numRows, upRows.size());

    assertTrue("incorrect payload", upRows.get(0).contains("payload body upserted"));
    for (int i = 1; i < numRows; i++) {
      assertTrue("incorrect payload", upRows.get(i).contains("payload body " + i));
    }

    LOG.info("Testing events with upsert finished successfully.");
  }

  private void testEvents(int eventCount, String operation) throws Exception {
    LOG.info("Testing {} events...", eventCount);

    KuduTable table = createNewTable("test" + eventCount + "events" + operation);
    String tableName = table.getName();
    Context ctx = new Context(ImmutableMap.of(PRODUCER_PREFIX + OPERATION_PROP, operation));
    KuduSink sink = createSink(tableName, ctx);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    Transaction tx = channel.getTransaction();
    tx.begin();

    for (int i = 0; i < eventCount; i++) {
      Event e = EventBuilder.withBody(String.format("payload body %s", i)
          .getBytes(UTF_8));
      e.setHeaders(ImmutableMap.of("key", String.format("key %s", i)));
      channel.put(e);
    }

    tx.commit();
    tx.close();

    Sink.Status status = sink.process();
    if (eventCount == 0) {
      assertTrue("incorrect status for empty channel", status == Sink.Status.BACKOFF);
    } else {
      assertTrue("incorrect status for non-empty channel", status != Sink.Status.BACKOFF);
    }

    List<String> rows = scanTableToStrings(table);
    assertEquals(eventCount + " row(s) expected", eventCount, rows.size());

    for (int i = 0; i < eventCount; i++) {
      assertTrue("incorrect payload", rows.get(i).contains("payload body " + i));
    }

    LOG.info("Testing {} events finished successfully.", eventCount);
  }

  private KuduSink createSink(String tableName, Context ctx) {
    LOG.info("Creating Kudu sink for '{}' table...", tableName);

    KuduSink sink = new KuduSink(syncClient);
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(TABLE_NAME, tableName);
    parameters.put(MASTER_ADDRESSES, getMasterAddresses());
    parameters.put(PRODUCER, SimpleKeyedKuduOperationsProducer.class.getName());
    Context context = new Context(parameters);
    context.putAll(ctx.getParameters());
    Configurables.configure(sink, context);

    LOG.info("Created Kudu sink for '{}' table.", tableName);

    return sink;
  }
}
