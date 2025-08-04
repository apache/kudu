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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestReplicationMetrics extends ReplicationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationMetrics.class);
  private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

  @ClassRule
  public static final MiniClusterWithClientResource flinkCluster =
          new MiniClusterWithClientResource(
                  new MiniClusterResourceConfiguration.Builder()
                          .setNumberSlotsPerTaskManager(1)
                          .setNumberTaskManagers(1)
                          .setConfiguration(reporter.addToConfiguration(new Configuration()))
                          .build());

  private Long waitForTimestampAdvancement(
          JobID jobId, Long previousTimestamp, long timeoutMillis) throws Exception {
    long startTime = System.currentTimeMillis();
    long endTime = startTime + timeoutMillis;

    while (System.currentTimeMillis() < endTime) {
      Long currentTimestamp = getCurrentMetricsTimestamp(jobId);

      if (currentTimestamp != null && currentTimestamp > previousTimestamp) {
        long waitedMs = System.currentTimeMillis() - startTime;
        LOG.info("Timestamp advanced from {} to {} (waited {}ms)",
                 previousTimestamp, currentTimestamp, waitedMs);
        return currentTimestamp;
      }
      Thread.sleep(500);
    }

    // Timeout reached
    Long finalTimestamp = getCurrentMetricsTimestamp(jobId);
    throw new AssertionError(String.format(
            "Timeout waiting for timestamp advancement. Previous: %s, Current: %s, Waited: %dms",
            previousTimestamp, finalTimestamp, timeoutMillis));
  }

  private Long getCurrentMetricsTimestamp(JobID jobId) throws Exception {
    Map<String, Metric> metrics = reporter.getMetricsByIdentifiers(jobId);
    Optional<Gauge<Long>> lastEndTimestamp = findMetric(metrics, "lastEndTimestamp");

    return lastEndTimestamp.map(Gauge::getValue).orElse(null);
  }

  @SuppressWarnings("unchecked")
  private <T> Optional<Gauge<T>> findMetric(Map<String, Metric> metrics, String nameSubstring) {
    return metrics.entrySet().stream()
            .filter(entry -> entry.getKey().contains(nameSubstring))
            .map(Map.Entry::getValue)
            .filter(metric -> metric instanceof Gauge)
            .map(metric -> (Gauge<T>) metric)
            .findFirst();
  }

  @After
  public void cleanup() {
    flinkCluster.cancelAllJobs();
  }

  @Test(timeout = 100000)
  public void testMetricsPresence() throws Exception {
    createAllTypesTable(sourceClient);
    createAllTypesTable(sinkClient);

    insertRowsIntoAllTypesTable(sourceClient, 0, 10);
    JobID jobId = envProvider.getEnv().executeAsync().getJobID();
    waitForTimestampAdvancement(jobId, 0L, 15000);

    Map<String, Metric> metrics = reporter.getMetricsByIdentifiers(jobId);

    // Verify all expected metrics are present
    Optional<Gauge<Long>> lastEndTimestamp = findMetric(metrics, "lastEndTimestamp");
    Optional<Gauge<Integer>> pendingCount = findMetric(metrics, "pendingCount");
    Optional<Gauge<Integer>> unassignedCount = findMetric(metrics, "unassignedCount");

    assertTrue("lastEndTimestamp metric should be present", lastEndTimestamp.isPresent());
    assertTrue("pendingCount metric should be present", pendingCount.isPresent());
    assertTrue("unassignedCount metric should be present", unassignedCount.isPresent());

    // Basic sanity checks
    assertNotNull("lastEndTimestamp should have a value", lastEndTimestamp.get().getValue());
    assertNotNull("pendingCount should have a value", pendingCount.get().getValue());
    assertNotNull("unassignedCount should have a value", unassignedCount.get().getValue());
  }

  @Test(timeout = 100000)
  public void testTimestampProgression() throws Exception {
    createAllTypesTable(sourceClient);
    createAllTypesTable(sinkClient);

    // Insert some data to process
    insertRowsIntoAllTypesTable(sourceClient, 0, 10);
    JobID jobId = envProvider.getEnv().executeAsync().getJobID();

    // Wait for initial timestamp to be set
    Long ts1 = waitForTimestampAdvancement(jobId, 0L, 15000);
    assertNotNull("Initial timestamp should be set", ts1);
    assertTrue("Initial timestamp should be positive", ts1 > 0);

    // Insert more data and wait for timestamp progression
    insertRowsIntoAllTypesTable(sourceClient, 10, 10);
    Long ts2 = waitForTimestampAdvancement(jobId, ts1, 10000);

    // Insert more data and wait for another progression
    insertRowsIntoAllTypesTable(sourceClient, 20, 10);
    Long ts3 = waitForTimestampAdvancement(jobId, ts2, 10000);

    // Assert monotonic increase
    assertTrue("Timestamp should advance: ts2 > ts1", ts2 > ts1);
    assertTrue("Timestamp should advance: ts3 > ts2", ts3 > ts2);
  }
}
