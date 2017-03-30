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
package org.apache.kudu.client;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests which provoke RPC queue overflow errors on the server side
 * to ensure that we properly handle them in the client.
 */
public class TestHandleTooBusy extends BaseKuduTest {
  private static final String TABLE_NAME = "TestHandleTooBusy";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    miniClusterBuilder
      // Short queue to provoke overflow.
      .addMasterFlag("--rpc_service_queue_length=1")
      // Low number of service threads, so things stay in the queue.
      .addMasterFlag("--rpc_num_service_threads=3")
      // inject latency so lookups process slowly.
      .addMasterFlag("--master_inject_latency_on_tablet_lookups_ms=100");

    BaseKuduTest.setUpBeforeClass();
  }

  /**
   * Provoke overflows in the master RPC queue while connecting to the master
   * and performing location lookups.
   */
  @Test(timeout=60000)
  public void testMasterLookupOverflow() throws Exception {
    createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    ExecutorService exec = Executors.newCachedThreadPool();
    List<Future<Void>> futures = Lists.newArrayList();
    for (int thr = 0; thr < 10; thr++) {
      futures.add(exec.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          for (int i = 0; i < 5; i++) {
            try (KuduClient c = new KuduClient.KuduClientBuilder(miniCluster.getMasterAddresses())
                .build()) {
              KuduTable table = c.openTable(TABLE_NAME);
              for (int j = 0; j < 5; j++) {
                KuduScanToken.KuduScanTokenBuilder scanBuilder = c.newScanTokenBuilder(table);
                scanBuilder.build();
                c.asyncClient.emptyTabletsCacheForTable(table.getTableId());
              }
            }
          }
          return null;
        }
      }));
    }
    for (Future<Void> f : futures) {
      f.get();
    }
  }

}
