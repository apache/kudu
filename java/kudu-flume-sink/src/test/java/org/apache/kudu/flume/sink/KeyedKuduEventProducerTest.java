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

import com.google.common.base.Charsets;
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
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.BaseKuduTest;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyedKuduEventProducerTest extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(KeyedKuduEventProducerTest.class);

  private KuduTable createNewTable(String tableName) throws Exception {
    LOG.info("Creating new table...");

    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("payload", Type.BINARY).key(false).build());
    CreateTableOptions createOptions =
      new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"))
                              .setNumReplicas(1);
    KuduTable table = createTable(tableName, new Schema(columns), createOptions);

    LOG.info("Created new table.");

    return table;
  }

  @Test
  public void testEmptyChannelWithInsert() throws Exception {
    testEvents(0, "false");
  }

  @Test
  public void testOneEventWithInsert() throws Exception {
    testEvents(1, "false");
  }

  @Test
  public void testThreeEventsWithInsert() throws Exception {
    testEvents(3, "false");
  }

  @Test
  public void testEmptyChannelWithUpsert() throws Exception {
    testEvents(0, "true");
  }

  @Test
  public void testOneEventWithUpsert() throws Exception {
    testEvents(1, "true");
  }

  @Test
  public void testThreeEventsWithUpsert() throws Exception {
    testEvents(3, "true");
  }

  @Test
  public void testDuplicateRowsWithUpsert() throws Exception {
    LOG.info("Testing events with upsert...");

    KuduTable table = createNewTable("testDupUpsertEvents");
    String tableName = table.getName();
    Context ctx = new Context(ImmutableMap.of("producer.upsert", "true"));
    KuduSink sink = createSink(tableName, ctx);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    Transaction tx = channel.getTransaction();
    tx.begin();

    int numRows = 3;
    for (int i = 0; i < numRows; i++) {
      Event e = EventBuilder.withBody(String.format("payload body %s", i), Charsets.UTF_8);
      e.setHeaders(ImmutableMap.of("key", String.format("key %s", i)));
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

    Event dup = EventBuilder.withBody("payload body upserted".getBytes());
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

  private void testEvents(int eventCount, String upsert) throws Exception {
    LOG.info("Testing {} events...", eventCount);

    KuduTable table = createNewTable("test" + eventCount + "eventsUp" + upsert);
    String tableName = table.getName();
    Context ctx = new Context(ImmutableMap.of("producer.upsert", upsert));
    KuduSink sink = createSink(tableName, ctx);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    Transaction tx = channel.getTransaction();
    tx.begin();

    for (int i = 0; i < eventCount; i++) {
      Event e = EventBuilder.withBody(String.format("payload body %s", i).getBytes());
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
    parameters.put(KuduSinkConfigurationConstants.TABLE_NAME, tableName);
    parameters.put(KuduSinkConfigurationConstants.MASTER_ADDRESSES, getMasterAddresses());
    parameters.put(KuduSinkConfigurationConstants.PRODUCER, "org.apache.kudu.flume.sink.SimpleKeyedKuduEventProducer");
    Context context = new Context(parameters);
    context.putAll(ctx.getParameters());
    Configurables.configure(sink, context);

    LOG.info("Created Kudu sink for '{}' table.", tableName);

    return sink;
  }
}
