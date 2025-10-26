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
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
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

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;

/**
 * Stress test for checkpoint race condition (FLINK-38575) fixes in wrapped source connector.
 *
 * This test is designed to aggressively trigger the checkpoint race condition by:
 * - Creating 32 tablets (8 × 4 hash partitions) for maximum split count
 * - Inserting 100 rows into EACH partition per iteration (3200 rows/iteration, 32 splits)
 * - Using very aggressive checkpointing (1 second interval vs typical 30-60s)
 * - Configuring small split sizes (1KB) to generate more split events
 * - Performing 10 crash/restore cycles with hard cancels
 * - Comparing source vs sink row counts after each iteration
 *
 * Uses production UPSERT semantics (via CustomReplicationOperationMapper).
 * This allows the test to focus on detecting MISSING data (the actual symptom
 * of the race condition) by comparing source and sink row counts.
 *
 * Expected Behavior:
 * - Without fix: Test fails within 5-10 iterations with sink rows < source rows
 * - With fix: Source and sink remain identical across all 10 iterations
 */
public class TestReplicationCheckpointStress extends ReplicationTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReplicationCheckpointStress.class);

  private static final int NUM_ITERATIONS = 10;
  private static final int NUM_PARTITIONS = 32;
  private static final int ROWS_PER_PARTITION = 100;
  private static final int ROWS_PER_ITERATION = NUM_PARTITIONS * ROWS_PER_PARTITION;
  private static final int CHECKPOINT_INTERVAL_MS = 1000;

  @ClassRule
  public static final MiniClusterWithClientResource flinkCluster =
          new MiniClusterWithClientResource(
                  new MiniClusterResourceConfiguration.Builder()
                          .setNumberSlotsPerTaskManager(8)
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

  @Override
  protected ReplicationJobConfig.Builder createDefaultJobConfigBuilder() {
    return super.createDefaultJobConfigBuilder()
            .setCheckpointingIntervalMillis(CHECKPOINT_INTERVAL_MS);
  }

  @Override
  protected KuduReaderConfig createDefaultReaderConfig() {
    return KuduReaderConfig.Builder
            .setMasters(sourceHarness.getMasterAddressesAsString())
            .setSplitSizeBytes(1024)         // 1KB - tiny splits to force one per tablet
            .setBatchSizeBytes(65536)        // 64KB - higher throughput per split
            .build();
  }

  /**
   * Creates a table with aggressive partitioning to generate many splits.
   * Uses hash partitioning on two columns: 8 buckets × 4 buckets = 32 tablets.
   * This means every scan (full or incremental) generates 32 split events.
   */
  private void createStressTestTable() throws Exception {
    Schema schema = new Schema(Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64)
            .key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("partition_key", Type.INT32)
            .key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("iteration", Type.INT32)
            .build(),
        new ColumnSchema.ColumnSchemaBuilder("data", Type.STRING)
            .build(),
        new ColumnSchema.ColumnSchemaBuilder("insert_time", Type.UNIXTIME_MICROS)
            .build()
    ));

    CreateTableOptions options = new CreateTableOptions()
        .addHashPartitions(Arrays.asList("id"), 8)
        .addHashPartitions(Arrays.asList("partition_key"), 4)
        .setNumReplicas(1);

    sourceClient.createTable(TABLE_NAME, schema, options);
    sinkClient.createTable(TABLE_NAME, schema, options);
  }

  private void insertStressTestData(int iteration) throws Exception {
    KuduTable table = sourceClient.openTable(TABLE_NAME);
    KuduSession session = sourceClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    long baseId = (long) iteration * ROWS_PER_ITERATION;
    int rowIndex = 0;

    for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
      for (int i = 0; i < ROWS_PER_PARTITION; i++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("id", baseId + rowIndex);
        row.addInt("partition_key", partition);
        row.addInt("iteration", iteration);
        row.addString("data", String.format("stress_iter%d_part%d_row%d",
                                            iteration, partition, i));
        row.addLong("insert_time", System.currentTimeMillis() * 1000);
        session.apply(insert);
        rowIndex++;
      }
    }

    session.flush();
    session.close();
  }


  @Test(timeout = 600000)
  public void testCheckpointRestartStressWithManySmallSplits() throws Exception {
    createStressTestTable();

    ReplicationJobConfig config = createDefaultJobConfig();
    KuduReaderConfig readerConfig = createDefaultReaderConfig();
    KuduWriterConfig writerConfig = createDefaultWriterConfig();

    KuduTable sourceTable = sourceClient.openTable(TABLE_NAME);
    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);

    List<KuduScanToken> tokens = sourceClient.newScanTokenBuilder(sourceTable).build();
    assertEquals("Should have 32 tablets (8x4 hash partitions)", NUM_PARTITIONS, tokens.size());

    JobID currentJobId = null;
    int expectedTotalRows = 0;

    for (int iteration = 0; iteration < NUM_ITERATIONS; iteration++) {
      LOG.info("Stress iteration {}/{}", iteration + 1, NUM_ITERATIONS);

      // Step 1: Insert new batch of data
      insertStressTestData(iteration);
      expectedTotalRows += ROWS_PER_ITERATION;

      // Step 2: Start or restore job
      if (iteration == 0) {
        ReplicationEnvProvider envProvider = new ReplicationEnvProvider(
                config, readerConfig, writerConfig);
        currentJobId = envProvider.getEnv().executeAsync().getJobID();
      } else {
        Path checkpoint = findLatestCheckpoint(checkpointDir, currentJobId);

        ReplicationEnvProvider envProvider = new ReplicationEnvProvider(
                config, readerConfig, writerConfig);
        JobGraph jobGraph = envProvider.getEnv().getStreamGraph().getJobGraph();
        jobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(checkpoint.toString(), false));

        currentJobId = clusterClient.submitJob(jobGraph).get();
      }

      // Step 3: Wait for job to start processing, then wait for checkpoint
      // For stress testing, we don't need all data replicated - just need the job
      // to be actively processing so checkpoints trigger. Wait for a small amount
      // of data to appear (proof job is running), then wait for checkpoint.
      final int minRowsForCheckpoint = Math.min(ROWS_PER_PARTITION, expectedTotalRows);
      assertEventuallyTrue(
          String.format("[%d] Wait for job to start processing (>= %d rows)",
              iteration, minRowsForCheckpoint),
          () -> countRowsInTable(sinkTable) >= minRowsForCheckpoint,
          30000);

      // Now wait for at least ONE checkpoint to complete for this specific job.
      // This ensures we have a restore point while still crashing mid-replication.
      waitForCheckpointCompletion(checkpointDir, currentJobId, 15000);

      // Step 4: Hard cancel immediately after checkpoint (mid-replication crash)
      clusterClient.cancel(currentJobId).get();
      waitForJobTermination(currentJobId, clusterClient, 15000);
    }

    LOG.info("All {} crash cycles complete - starting final recovery", NUM_ITERATIONS);

    // Final restore: Let the job recover and complete all pending replication
    Path finalCheckpoint = findLatestCheckpoint(checkpointDir, currentJobId);
    ReplicationEnvProvider finalEnvProvider = new ReplicationEnvProvider(
            config, readerConfig, writerConfig);
    JobGraph finalJobGraph = finalEnvProvider.getEnv().getStreamGraph().getJobGraph();
    finalJobGraph.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath(finalCheckpoint.toString(), false));

    final JobID finalJobId = clusterClient.submitJob(finalJobGraph).get();

    // Wait for all data to eventually replicate (with UPSERT semantics handling duplicates)
    final int finalExpectedRows = expectedTotalRows;
    assertEventuallyTrue(
        String.format("All %d rows should eventually replicate after recovery",
            finalExpectedRows),
        () -> countRowsInTable(sinkTable) >= finalExpectedRows,
        180000);

    // Final verification: source vs sink must match perfectly
    long finalSourceCount = countRowsInTable(sourceTable);
    long finalSinkCount = countRowsInTable(sinkTable);

    assertEquals("Source rows must match expected after " + NUM_ITERATIONS + " iterations",
        expectedTotalRows, finalSourceCount);
    assertEquals(
        "Sink must match source (no data loss despite " + NUM_ITERATIONS + " hard crashes)",
        finalSourceCount, finalSinkCount);

    clusterClient.cancel(finalJobId).get();
  }
}

