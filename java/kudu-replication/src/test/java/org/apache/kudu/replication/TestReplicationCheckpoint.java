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

import static org.apache.kudu.test.ClientTestUtil.countRowsInTable;
import static org.apache.kudu.test.junit.AssertHelpers.assertEventuallyTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.KuduTable;

/**
 * The replication job uses at-least-once semantics with UPSERT operations for idempotency.
 * This means row count assertions ALONE are insufficient to verify checkpoint restoration,
 * because UPSERT makes a full re-scan indistinguishable from proper checkpoint restoration:
 * - Checkpoint working: Restores state -> incremental replication -> correct row count
 * - Checkpoint broken: Starts fresh -> full re-scan -> UPSERT deduplicates -> correct row count
 *
 * Both scenarios produce the same final row count! Therefore, we use a two-step verification:
 * - Positive case (testStopAndRestartFromCheckpoint): Restarts from valid checkpoint.
 *   We verify that (1) the job reaches RUNNING state (proves checkpoint was loaded and
 *   restoration succeeded), and (2) all data is eventually replicated correctly (proves
 *   functional correctness). The combination proves checkpoint restoration is actually
 *   working, not just that we got the right final data by accident.
 *
 * - Negative case (testRestartWithInvalidCheckpointFails): Deliberately provides an
 *   invalid checkpoint path. We expect the job to FAIL during startup (before reaching
 *   RUNNING state), proving that our test setup actually depends on checkpoint restoration
 *   working and isn't just passing due to idempotent re-replication.
 */
public class TestReplicationCheckpoint extends ReplicationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationCheckpoint.class);

  @ClassRule
  public static final MiniClusterWithClientResource flinkCluster =
          new MiniClusterWithClientResource(
                  new MiniClusterResourceConfiguration.Builder()
                          .setNumberSlotsPerTaskManager(2)
                          .setNumberTaskManagers(1)
                          .setConfiguration(new Configuration())
                          .build());

  private ClusterClient<?> clusterClient;

  @After
  public void cleanup() {
    flinkCluster.cancelAllJobs();
  }

  @Before
  public void setUp() {
    clusterClient = flinkCluster.getClusterClient();
  }

  @Test(timeout = 180000)
  public void testStopAndRestartFromCheckpoint() throws Exception {
    createAllTypesTable(sourceClient);
    createAllTypesTable(sinkClient);

    ReplicationEnvProvider envProvider = new ReplicationEnvProvider(
        createDefaultJobConfig(), createDefaultReaderConfig(), createDefaultWriterConfig(), true);

    insertRowsIntoAllTypesTable(sourceClient, 0, 10);

    JobClient jobClient = envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);

    // 30s = 15 discovery cycles @ 2s - sufficient for 10 rows finishing ideally in one cycle.
    assertEventuallyTrue("Initial 10 rows should be replicated",
        () -> countRowsInTable(sinkTable) == 10, 30000);

    // 10s = 20 checkpoint cycles @ 500ms - finds checkpoint quickly
    waitForCheckpointCompletion(checkpointDir, jobClient.getJobID(), 10000);

    clusterClient.cancel(jobClient.getJobID()).get();
    waitForJobTermination(jobClient.getJobID(), clusterClient, 15000);

    insertRowsIntoAllTypesTable(sourceClient, 10, 10);

    Path latestCheckpoint = findLatestCheckpoint(checkpointDir, jobClient.getJobID());
    LOG.info("Found latest checkpoint: {}", latestCheckpoint);

    ReplicationEnvProvider restartedEnvProvider = new ReplicationEnvProvider(
        createDefaultJobConfig(), createDefaultReaderConfig(), createDefaultWriterConfig(), true);

    JobGraph jobGraph = restartedEnvProvider.getEnv().getStreamGraph().getJobGraph();
    jobGraph.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath(latestCheckpoint.toString(), false));

    JobID restartedJobId = clusterClient.submitJob(jobGraph).get();
    LOG.info("Restarted job with ID: {} from checkpoint: {}", restartedJobId, latestCheckpoint);

    // Verify job reaches RUNNING state (proves checkpoint restoration succeeded)
    // Job transitions: INITIALIZING -> CREATED -> RUNNING
    // 20s timeout - job startup should be quick (couple seconds)
    JobStatus restartedJobStatus = clusterClient.getJobStatus(restartedJobId).get();
    long statusTimeoutMs = 20000;
    long statusStartTime = System.currentTimeMillis();
    while (restartedJobStatus != JobStatus.RUNNING &&
            !restartedJobStatus.isGloballyTerminalState() &&
            System.currentTimeMillis() - statusStartTime < statusTimeoutMs) {
      Thread.sleep(500);
      restartedJobStatus = clusterClient.getJobStatus(restartedJobId).get();
      LOG.debug("Job status: {}", restartedJobStatus);
    }

    assertTrue(
        "Job should reach RUNNING state after checkpoint restoration, got: " + restartedJobStatus,
        restartedJobStatus == JobStatus.RUNNING);

    assertEventuallyTrue("All 20 rows should be replicated after restart",
        () -> countRowsInTable(sinkTable) == 20, 30000);

    clusterClient.cancel(restartedJobId).get();
  }

  @Test(timeout = 180000)
  public void testRestartWithInvalidCheckpointFails() throws Exception {
    createAllTypesTable(sourceClient);
    createAllTypesTable(sinkClient);

    ReplicationEnvProvider envProvider = new ReplicationEnvProvider(
        createDefaultJobConfig(), createDefaultReaderConfig(), createDefaultWriterConfig(), true);

    insertRowsIntoAllTypesTable(sourceClient, 0, 10);

    JobClient jobClient = envProvider.getEnv().executeAsync();
    JobID jobId = jobClient.getJobID();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);

    // 30s = 15 discovery cycles @ 2s - sufficient for 10 rows finishing ideally in one cycle.
    assertEventuallyTrue("Initial 10 rows should be replicated",
        () -> countRowsInTable(sinkTable) == 10, 30000);

    // 10s = 20 checkpoint cycles @ 500ms
    waitForCheckpointCompletion(checkpointDir, jobId, 10000);

    clusterClient.cancel(jobId).get();
    waitForJobTermination(jobId, clusterClient, 15000);  // Cancellation is fast

    ReplicationEnvProvider restartedEnvProvider = new ReplicationEnvProvider(
        createDefaultJobConfig(), createDefaultReaderConfig(), createDefaultWriterConfig(), true);

    JobGraph jobGraph = restartedEnvProvider.getEnv().getStreamGraph().getJobGraph();
    String invalidCheckpointPath = checkpointDir.resolve("invalid-checkpoint-path").toString();
    jobGraph.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath(invalidCheckpointPath, false));

    JobID restartedJobId = clusterClient.submitJob(jobGraph).get();

    // 30s timeout - job should fail immediately on invalid checkpoint
    JobStatus finalStatus = clusterClient.getJobStatus(restartedJobId).get();
    long timeoutMs = 30000;
    long startTime = System.currentTimeMillis();
    while (!finalStatus.isGloballyTerminalState() &&
            System.currentTimeMillis() - startTime < timeoutMs) {
      Thread.sleep(500);
      finalStatus = clusterClient.getJobStatus(restartedJobId).get();
    }

    LOG.info("Job final status with invalid checkpoint: {}", finalStatus);
    assertEquals("Job should FAIL when given invalid checkpoint path",
        JobStatus.FAILED, finalStatus);
  }
}

