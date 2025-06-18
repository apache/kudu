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

import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.noop.NoopCounter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.KuduTestHarness;

public class TestKuduMetrics {

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Test
  public void testDisabledMetrics() throws Exception {
    KuduMetrics.setEnabled(false);

    // Creating a meter results in a no-op meter that always returns 0.
    Counter foo =
        KuduMetrics.counter(KuduMetrics.RPC_RESPONSE_METRIC, KuduMetrics.CLIENT_ID_TAG, "foo");
    Assert.assertTrue(foo instanceof NoopCounter);
    foo.increment();
    Assert.assertEquals(0, (int) foo.count());

    // The registry doesn't have any meters.
    Assert.assertEquals(0, KuduMetrics.numMetrics());
  }

  @Test
  public void testClientIdFilter() throws Exception {
    KuduClient c1 = new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build();
    KuduClient c2 = new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build();

    c1.createTable("c1-table", getBasicSchema(), getBasicCreateTableOptions());
    c2.createTable("c2-table", getBasicSchema(), getBasicCreateTableOptions());

    String c1Id = c1.getClientId();
    String c2Id = c1.getClientId();

    int totalNumMetrics = KuduMetrics.numMetrics();
    int c1NumMetrics = KuduMetrics.numMetrics(KuduMetrics.CLIENT_ID_TAG, c1Id);
    int c2NumMetrics = KuduMetrics.numMetrics(KuduMetrics.CLIENT_ID_TAG, c2Id);

    KuduMetrics.logMetrics();  // Log the metric values to help debug failures.
    Assert.assertEquals(totalNumMetrics, c1NumMetrics + c2NumMetrics);

    // Disable the metrics and validate they are cleared.
    KuduMetrics.setEnabled(false);
    Assert.assertEquals(0, KuduMetrics.numMetrics());
    // Re-enable and verify they remain cleared.
    KuduMetrics.setEnabled(true);
    Assert.assertEquals(0, KuduMetrics.numMetrics());
  }
}
