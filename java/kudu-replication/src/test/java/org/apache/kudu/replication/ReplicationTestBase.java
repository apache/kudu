// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication;

import static org.apache.kudu.test.ClientTestUtil.getAllTypesCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getPartialRowWithAllTypes;
import static org.apache.kudu.test.ClientTestUtil.getSchemaWithAllTypes;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.junit.Before;
import org.junit.Rule;

import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.test.KuduTestHarness;

public class ReplicationTestBase {
  protected static final String TABLE_NAME = "replication_test_table";

  @Rule
  public final KuduTestHarness sourceHarness = new KuduTestHarness();
  @Rule
  public final KuduTestHarness sinkHarness = new KuduTestHarness();

  protected KuduClient sourceClient;
  protected KuduClient sinkClient;
  protected ReplicationEnvProvider envProvider;

  @Before
  public void setupClientsAndEnvProvider() {
    this.sourceClient = sourceHarness.getClient();
    this.sinkClient = sinkHarness.getClient();
    this.envProvider = new ReplicationEnvProvider(
            createDefaultJobConfig(),
            createDefaultReaderConfig(),
            createDefaultWriterConfig());
  }


  protected ReplicationJobConfig createDefaultJobConfig() {
    ReplicationJobConfig jobConfig = ReplicationJobConfig.builder()
            .setSourceMasterAddresses(sourceHarness.getMasterAddressesAsString())
            .setSinkMasterAddresses(sinkHarness.getMasterAddressesAsString())
            .setTableName(TABLE_NAME)
            .setDiscoveryIntervalSeconds(2)
            .build();
    return jobConfig;
  }

  protected KuduReaderConfig createDefaultReaderConfig() {
    KuduReaderConfig readerConfig = KuduReaderConfig.Builder
            .setMasters(sourceHarness.getMasterAddressesAsString())
            .build();
    return readerConfig;
  }

  protected KuduWriterConfig createDefaultWriterConfig() {
    KuduWriterConfig writerConfig = KuduWriterConfig.Builder
            .setMasters(sinkHarness.getMasterAddressesAsString())
            .build();
    return writerConfig;
  }

  protected void createAllTypesTable(KuduClient client) throws Exception {
    Schema schema = getSchemaWithAllTypes();
    CreateTableOptions options = getAllTypesCreateTableOptions();
    client.createTable(TABLE_NAME, schema, options);
  }

  protected void insertRowsIntoAllTypesTable(
          KuduClient client, int startKey, int count) throws Exception {
    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();
    for (int i = 0; i < count; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      getPartialRowWithAllTypes(row, (byte) (startKey + i));
      session.apply(insert);
    }
    session.flush();
    session.close();
  }

  protected void verifySourceAndSinkRowsEqual(int expectedRowCount) throws Exception {
    Map<Byte, RowResult> sourceMap = buildRowMapByInt8Key(sourceClient);
    Map<Byte, RowResult> sinkMap = buildRowMapByInt8Key(sinkClient);

    for (int i = 0; i < expectedRowCount; i++) {
      byte key = (byte) i;

      RowResult sourceRow = sourceMap.get(key);
      RowResult sinkRow = sinkMap.get(key);

      if (sourceRow == null || sinkRow == null) {
        throw new AssertionError("Missing row with int8 = " + key +
                "\nSource has: " + (sourceRow != null) +
                "\nSink has: " + (sinkRow != null));
      }

      compareRowResults(sourceRow, sinkRow, key);
    }
  }

  private Map<Byte, RowResult> buildRowMapByInt8Key(KuduClient client) throws Exception {
    Map<Byte, RowResult> map = new HashMap<>();
    KuduTable table = client.openTable(TABLE_NAME);
    KuduScanner scanner = client.newScannerBuilder(table).build();

    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      while (it.hasNext()) {
        RowResult row = it.next();
        byte key = row.getByte("int8");
        map.put(key, row);
      }
    }

    return map;
  }

  private void compareRowResults(RowResult a, RowResult b, byte key) {
    Schema schema = a.getSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      String colName = schema.getColumnByIndex(i).getName();

      Object valA = a.isNull(i) ? null : a.getObject(i);
      Object valB = b.isNull(i) ? null : b.getObject(i);

      if (!java.util.Objects.deepEquals(valA, valB)) {
        throw new AssertionError("Mismatch in column '" + colName + "' for int8 = " + key +
                "\nSource: " + valA +
                "\nSink:   " + valB);
      }
    }
  }
}
