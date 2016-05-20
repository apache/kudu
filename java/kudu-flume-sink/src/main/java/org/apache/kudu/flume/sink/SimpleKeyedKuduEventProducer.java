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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.util.Collections;
import java.util.List;

/**
 * <p>A simple serializer that generates one {@link Insert} or {@link Upsert} per {@link Event} by writing the event
 * body into a BINARY column. The pair (key column name, key column value) should be a header in the {@link Event};
 * the column name is configurable but the column type must be STRING. Multiple key columns are not supported.</p>
 *
 * <p><strong>Simple Keyed Kudu Event Producer configuration parameters</strong></p>
 *
 * <table cellpadding=3 cellspacing=0 border=1>
 * <tr><th>Property Name</th><th>Default</th><th>Required?</th><th>Description</th></tr>
 * <tr><td>producer.payloadColumn</td><td>payload</td><td>No</td><td>The name of the BINARY column to write the Flume event body to.</td></tr>
 * <tr><td>producer.keyColumn</td><td>key</td><td>No</td><td>The name of the STRING key column of the target Kudu table.</td></tr>
 * <tr><td>producer.upsert</td><td>false</td><td>No</td><td>Whether to insert or upsert events.</td></tr>
 * </table>
 */
public class SimpleKeyedKuduEventProducer implements KuduEventProducer {
  private byte[] payload;
  private String key;
  private KuduTable table;
  private String payloadColumn;
  private String keyColumn;
  private boolean upsert;

  public SimpleKeyedKuduEventProducer(){
  }

  @Override
  public void configure(Context context) {
    payloadColumn = context.getString("payloadColumn","payload");
    keyColumn = context.getString("keyColumn", "key");
    upsert = context.getBoolean("upsert", false);
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  @Override
  public void initialize(Event event, KuduTable table) {
    this.payload = event.getBody();
    this.key = event.getHeaders().get(keyColumn);
    this.table = table;
  }

  @Override
  public List<Operation> getOperations() throws FlumeException {
    try {
      Operation op = (upsert) ? table.newUpsert() : table.newInsert();
      PartialRow row = op.getRow();
      row.addString(keyColumn, key);
      row.addBinary(payloadColumn, payload);

      return Collections.singletonList(op);
    } catch (Exception e){
      throw new FlumeException("Failed to create Kudu Operation object!", e);
    }
  }

  @Override
  public void close() {
  }
}

