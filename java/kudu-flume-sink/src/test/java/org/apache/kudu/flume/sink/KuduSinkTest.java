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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.BaseKuduTest;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;

public class KuduSinkTest extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSinkTest.class);

  private KuduTable createNewTable(String tableName) throws Exception {
    LOG.info("Creating new table...");

    ArrayList<ColumnSchema> columns = new ArrayList<>(1);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("payload", Type.BINARY).key(true).build());
    CreateTableOptions createOptions =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("payload"))
                                .setNumReplicas(1);
    KuduTable table = createTable(tableName, new Schema(columns), createOptions);

    LOG.info("Created new table.");

    return table;
  }

  @Test
  public void testMandatoryParameters() {
    LOG.info("Testing mandatory parameters...");

    KuduSink sink = new KuduSink(syncClient);

    HashMap<String, String> parameters = new HashMap<>();
    Context context = new Context(parameters);
    try {
      Configurables.configure(sink, context);
      Assert.fail("Should have failed due to missing properties");
    } catch (NullPointerException npe) {
        //good
    }

    parameters.put(KuduSinkConfigurationConstants.TABLE_NAME, "tableName");
    context = new Context(parameters);
    try {
      Configurables.configure(sink, context);
      Assert.fail("Should have failed due to missing properties");
    } catch (NullPointerException npe) {
        //good
    }

    LOG.info("Testing mandatory parameters finished successfully.");
  }

  @Test(expected = FlumeException.class)
  public void testMissingTable() throws Exception {
    LOG.info("Testing missing table...");

    KuduSink sink = createSink("missingTable");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    LOG.info("Testing missing table finished successfully.");
  }

  @Test
  public void testEmptyChannelWithDefaults() throws Exception {
    testEventsWithDefaults(0);
  }

  @Test
  public void testOneEventWithDefaults() throws Exception {
    testEventsWithDefaults(1);
  }

  @Test
  public void testThreeEventsWithDefaults() throws Exception {
    testEventsWithDefaults(3);
  }

  @Test
  public void testDuplicateRowsWithDuplicatesIgnored() throws Exception {
    doTestDuplicateRows(true);
  }

  @Test
  public void testDuplicateRowsWithDuplicatesNotIgnored() throws Exception {
    doTestDuplicateRows(false);
  }

  private void doTestDuplicateRows(boolean ignoreDuplicateRows) throws Exception {
    KuduTable table = createNewTable("testDuplicateRows" + ignoreDuplicateRows);
    String tableName = table.getName();
    Context sinkContext = new Context();
    sinkContext.put(KuduSinkConfigurationConstants.IGNORE_DUPLICATE_ROWS,
                    Boolean.toString(ignoreDuplicateRows));
    KuduSink sink = createSink(tableName, sinkContext);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    Transaction tx = channel.getTransaction();
    tx.begin();

    for (int i = 0; i < 2; i++) {
      Event e = EventBuilder.withBody("key-0", Charsets.UTF_8); // Duplicate keys.
      channel.put(e);
    }

    tx.commit();
    tx.close();

    try {
      Sink.Status status = sink.process();
      if (!ignoreDuplicateRows) {
        fail("Incorrectly ignored duplicate rows!");
      }
      assertTrue("incorrect status for empty channel", status == Sink.Status.READY);
    } catch (EventDeliveryException e) {
      if (ignoreDuplicateRows) {
        throw new AssertionError("Failed to ignore duplicate rows!", e);
      } else {
        LOG.info("Correctly did not ignore duplicate rows", e);
        return;
      }
    }

    // We only get here if the process() succeeded.
    try {
      List<String> rows = scanTableToStrings(table);
      assertEquals("1 row expected", 1, rows.size());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LOG.info("Testing duplicate events finished successfully.");
  }

  private void testEventsWithDefaults(int eventCount) throws Exception {
    LOG.info("Testing {} events...", eventCount);

    KuduTable table = createNewTable("test" + eventCount + "events");
    String tableName = table.getName();
    KuduSink sink = createSink(tableName);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    Transaction tx = channel.getTransaction();
    tx.begin();

    for (int i = 0; i < eventCount; i++) {
      Event e = EventBuilder.withBody(String.format("payload body %s", i).getBytes());
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

  private KuduSink createSink(String tableName) {
    return createSink(tableName, new Context());
  }

  private KuduSink createSink(String tableName, Context ctx) {
    LOG.info("Creating Kudu sink for '{}' table...", tableName);

    KuduSink sink = new KuduSink(syncClient);
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(KuduSinkConfigurationConstants.TABLE_NAME, tableName);
    parameters.put(KuduSinkConfigurationConstants.MASTER_ADDRESSES, getMasterAddresses());
    Context context = new Context(parameters);
    context.putAll(ctx.getParameters());
    Configurables.configure(sink, context);

    LOG.info("Created Kudu sink for '{}' table.", tableName);

    return sink;
  }
}
