// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.kudu.mapreduce;

import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

public class ITKuduTableOutputFormat {

  private static final String TABLE_NAME =
      ITKuduTableOutputFormat.class.getName() + "-" + System.currentTimeMillis();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Test
  @MasterServerConfig(flags = { "--rpc_max_message_size=3860733440" })
  @TabletServerConfig(flags = { "--rpc_max_message_size=3860733440" })
  public void test() throws Exception {
    harness.getClient().createTable(TABLE_NAME, getBasicSchema(), getBasicCreateTableOptions());

    KuduTableOutputFormat output = new KuduTableOutputFormat();
    Configuration conf = new Configuration();
    conf.set(KuduTableOutputFormat.MASTER_ADDRESSES_KEY, harness.getMasterAddressesAsString());
    conf.set(KuduTableOutputFormat.OUTPUT_TABLE_KEY, TABLE_NAME);
    output.setConf(conf);

    String multitonKey = conf.get(KuduTableOutputFormat.MULTITON_KEY);
    KuduTable table = KuduTableOutputFormat.getKuduTable(multitonKey);
    assertNotNull(table);

    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt(0, 1);
    row.addInt(1, 2);
    row.addInt(2, 3);
    row.addString(3, "a string");
    row.addBoolean(4, true);

    RecordWriter<NullWritable, Operation> rw = output.getRecordWriter(null);
    rw.write(NullWritable.get(), insert);
    rw.close(null);
    AsyncKuduScanner.AsyncKuduScannerBuilder builder = harness.getAsyncClient().newScannerBuilder(table);
    assertEquals(1, countRowsInScan(builder.build()));
  }

  @Test
  @MasterServerConfig(flags = { "--rpc_max_message_size=3860733440" })
  @TabletServerConfig(flags = { "--rpc_max_message_size=3860733440" })
  public void testWriteException() throws Exception {
    harness.getClient().createTable(TABLE_NAME, getBasicSchema(), getBasicTableOptionsWithNonCoveredRange());

    KuduTableOutputFormat output = new KuduTableOutputFormat();
    Configuration conf = new Configuration();
    conf.set(KuduTableOutputFormat.MASTER_ADDRESSES_KEY, harness.getMasterAddressesAsString());
    conf.set(KuduTableOutputFormat.OUTPUT_TABLE_KEY, TABLE_NAME);
    output.setConf(conf);

    String multitonKey = conf.get(KuduTableOutputFormat.MULTITON_KEY);
    KuduTable table = KuduTableOutputFormat.getKuduTable(multitonKey);
    assertNotNull(table);


    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt(0, 201); // outside of range partition
    row.addInt(1, 2);
    row.addInt(2, 3);
    row.addString(3, "a string");
    row.addBoolean(4, true);

    boolean threwException = false;
    RecordWriter<NullWritable, Operation> rw = output.getRecordWriter(null);
    try {
      rw.write(NullWritable.get(), insert);
    } catch (IOException e) {
      if (e.getMessage().contains("Row error for primary key=")) {
        threwException = true;
      }
    }
    assertTrue("writing a PK outside of range partition is supposed to throw an Exception", threwException);
    rw.close(null);
  }
}
