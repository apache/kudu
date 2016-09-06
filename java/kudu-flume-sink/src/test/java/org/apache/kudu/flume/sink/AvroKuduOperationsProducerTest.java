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

import static org.apache.kudu.flume.sink.AvroKuduOperationsProducer.SCHEMA_LITERAL_HEADER;
import static org.apache.kudu.flume.sink.AvroKuduOperationsProducer.SCHEMA_PROP;
import static org.apache.kudu.flume.sink.AvroKuduOperationsProducer.SCHEMA_URL_HEADER;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.MASTER_ADDRESSES;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.PRODUCER;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.PRODUCER_PREFIX;
import static org.apache.kudu.flume.sink.KuduSinkConfigurationConstants.TABLE_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
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
import org.junit.BeforeClass;
import org.junit.Test;

public class AvroKuduOperationsProducerTest extends BaseKuduTest {
  private static final String schemaPath = "src/test/avro/testAvroKuduOperationsProducer.avsc";
  private static String schemaLiteral;

  enum SchemaLocation {
    GLOBAL, URL, LITERAL
  }

  @BeforeClass
  public static void setupAvroSchemaBeforeClass() {
    try {
      schemaLiteral = Files.toString(new File(schemaPath), Charsets.UTF_8);
    } catch (IOException e) {
      throw new FlumeException("Unable to read schema file!", e);
    }
  }

  @Test
  public void testEmptyChannel() throws Exception {
    testEvents(0, SchemaLocation.GLOBAL);
  }

  @Test
  public void testOneEvent() throws Exception {
    testEvents(1, SchemaLocation.GLOBAL);
  }

  @Test
  public void testThreeEvents() throws Exception {
    testEvents(3, SchemaLocation.GLOBAL);
  }

  @Test
  public void testThreeEventsSchemaURLInEvent() throws Exception {
    testEvents(3, SchemaLocation.URL);
  }

  @Test
  public void testThreeEventsSchemaLiteralInEvent() throws Exception {
    testEvents(3, SchemaLocation.LITERAL);
  }

  private void testEvents(int eventCount, SchemaLocation schemaLocation)
      throws Exception {
    KuduTable table = createNewTable(
        String.format("test%sevents%s", eventCount, schemaLocation));
    String tableName = table.getName();
    String schemaURI = new File(schemaPath).getAbsoluteFile().toURI().toString();
    Context ctx = schemaLocation != SchemaLocation.GLOBAL ? new Context()
        : new Context(ImmutableMap.of(PRODUCER_PREFIX + SCHEMA_PROP, schemaURI));
    KuduSink sink = createSink(tableName, ctx);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();

    Transaction tx = channel.getTransaction();
    tx.begin();
    writeEventsToChannel(channel, eventCount, schemaLocation);
    tx.commit();
    tx.close();

    Sink.Status status = sink.process();
    if (eventCount == 0) {
      assertEquals("incorrect status for empty channel", status, Sink.Status.BACKOFF);
    } else {
      assertEquals("incorrect status for non-empty channel", status, Sink.Status.READY);
    }

    List<String> answers = makeAnswers(eventCount);
    List<String> rows = scanTableToStrings(table);
    assertEquals("wrong number of rows inserted", answers.size(), rows.size());
    assertArrayEquals("wrong rows inserted", answers.toArray(), rows.toArray());
  }

  private KuduTable createNewTable(String tableName) throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(5);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("longField", Type.INT64).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("doubleField", Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("nullableField", Type.STRING).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("stringField", Type.STRING).build());
    CreateTableOptions createOptions =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"))
            .setNumReplicas(1);
    return createTable(tableName, new Schema(columns), createOptions);
  }

  private KuduSink createSink(String tableName, Context ctx) {
    KuduSink sink = new KuduSink(syncClient);
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(TABLE_NAME, tableName);
    parameters.put(MASTER_ADDRESSES, getMasterAddresses());
    parameters.put(PRODUCER, AvroKuduOperationsProducer.class.getName());
    Context context = new Context(parameters);
    context.putAll(ctx.getParameters());
    Configurables.configure(sink, context);

    return sink;
  }

  private void writeEventsToChannel(Channel channel, int eventCount,
                                    SchemaLocation schemaLocation) throws Exception {
    for (int i = 0; i < eventCount; i++) {
      AvroKuduOperationsProducerTestRecord record = new AvroKuduOperationsProducerTestRecord();
      record.setKey(10 * i);
      record.setLongField(2L * i);
      record.setDoubleField(2.71828 * i);
      record.setNullableField(i % 2 == 0 ? null : "taco");
      record.setStringField(String.format("hello %d", i));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<AvroKuduOperationsProducerTestRecord> writer =
          new SpecificDatumWriter<>(AvroKuduOperationsProducerTestRecord.class);
      writer.write(record, encoder);
      encoder.flush();
      Event e = EventBuilder.withBody(out.toByteArray());
      if (schemaLocation == SchemaLocation.URL) {
        String schemaURI = new File(schemaPath).getAbsoluteFile().toURI().toString();
        e.setHeaders(ImmutableMap.of(SCHEMA_URL_HEADER, schemaURI));
      } else if (schemaLocation == SchemaLocation.LITERAL) {
        e.setHeaders(ImmutableMap.of(SCHEMA_LITERAL_HEADER, schemaLiteral));
      }
      channel.put(e);
    }
  }

  private List<String> makeAnswers(int eventCount) {
    List<String> answers = Lists.newArrayList();
    for (int i = 0; i < eventCount; i++) {
      answers.add(String.format(
          "INT32 key=%s, INT64 longField=%s, DOUBLE doubleField=%s, " +
              "STRING nullableField=%s, STRING stringField=hello %s",
          10 * i,
          2 * i,
          2.71828 * i,
          i % 2 == 0 ? "NULL" : "taco",
          i));
    }
    Collections.sort(answers);
    return answers;
  }
}
