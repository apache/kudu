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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.kudu.test.junit.RetryRule;

public class TestTableLocationsCache {
  private TableLocationsCache cache = new TableLocationsCache();

  @Rule
  public RetryRule retryRule = new RetryRule();

  /**
   * Prevent time from advancing during the test by mocking the time.
   */
  @Before
  @SuppressWarnings("unstable")
  public void mockTime() {
    TableLocationsCache.ticker = Mockito.mock(Ticker.class);
  }

  @After
  @SuppressWarnings("unstable")
  public void unmockTime() {
    TableLocationsCache.ticker = Ticker.systemTicker();
  }

  @Test
  public void testToString() {
    RemoteTablet tablet = TestRemoteTablet.getTablet(0, 1, -1);
    List<RemoteTablet> tablets = ImmutableList.of(tablet);
    cache.cacheTabletLocations(tablets,
        tablet.getPartition().getPartitionKeyStart(),
        1, // requested batch size,
        100); // ttl
    // Mock as if the time increased by 10ms (the ticker is in nanoseconds).
    // This will result in a remaining TTL of 90.
    Mockito.when(TableLocationsCache.ticker.read()).thenReturn(10 * 1000000L);
    assertEquals("[Tablet{lowerBoundPartitionKey=0x, upperBoundPartitionKey=0x, " +
                 "ttl=90, tablet=" + tablet.toString() + "}]",
                 cache.toString());
  }

  /**
   * Test to reproduce the issue in KUDU-3273.
   *
   * For TableLocationsCache, if get(key) is called immediately after
   * cacheTabletLocations(key), it should not return null.
   * But if partitions change and a call to cacheTabletLocations(otherKey)
   * occurs between calls to cacheTableLocations(key) and get(key),
   * the latter might return null.
   */
  @Test
  public void testGet() {
    byte[] keyB = "b".getBytes(StandardCharsets.UTF_8);
    List<RemoteTablet> tablets = ImmutableList.of(
            TestRemoteTablet.getTablet(0, 1, -1, keyB, AsyncKuduClient.EMPTY_ARRAY));
    cache.cacheTabletLocations(tablets, AsyncKuduClient.EMPTY_ARRAY, 1, 100);
    assertNotNull(cache.get(AsyncKuduClient.EMPTY_ARRAY));

    byte[] keyA = "a".getBytes(StandardCharsets.UTF_8);
    tablets = ImmutableList.of(
            TestRemoteTablet.getTablet(0, 1, -1, keyA, AsyncKuduClient.EMPTY_ARRAY));
    cache.cacheTabletLocations(tablets, keyA, 1, 100);

    assertNull(cache.get(AsyncKuduClient.EMPTY_ARRAY));

    cache.cacheTabletLocations(tablets, AsyncKuduClient.EMPTY_ARRAY, 1, 100);
    assertNotNull(cache.get(AsyncKuduClient.EMPTY_ARRAY));
  }

  // Test for checking client will not get expired tablet locations.
  @Test(timeout = 30000)
  public void testTTLForTableLocationsCache() {
    final int TTL_MS = 100;
    byte[] partitionKey = "start_key".getBytes(StandardCharsets.UTF_8);
    List<RemoteTablet> tablets = ImmutableList.of(
        TestRemoteTablet.getTablet(0, 1, -1, partitionKey, AsyncKuduClient.EMPTY_ARRAY));
    cache.cacheTabletLocations(tablets, partitionKey, 1, TTL_MS);
    assertNotNull(cache.get(partitionKey));
    // Mock as if the time increased by 100ms.
    Mockito.when(TableLocationsCache.ticker.read()).thenReturn(TTL_MS * 1000000L);
    assertNull(cache.get(partitionKey));
  }

  // Test for checking client will not get expired master locations.
  @Test(timeout = 30000)
  public void testTTLForMasterLocationsCache() {
    final int TTL_MS = 100;
    List<RemoteTablet> masterTablets =
        ImmutableList.of(TestRemoteTablet.getTablet(0, 1, -1));
    cache.cacheTabletLocations(masterTablets, null, 1, TTL_MS);
    assertNotNull(cache.get(null));
    // Mock as if the time increased by 100ms.
    Mockito.when(TableLocationsCache.ticker.read()).thenReturn(TTL_MS * 1000000L);
    assertNull(cache.get(null));
  }
}
