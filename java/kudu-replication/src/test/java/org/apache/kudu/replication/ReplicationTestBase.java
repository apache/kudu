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

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  protected static final Logger LOG = LoggerFactory.getLogger(ReplicationTestBase.class);
  protected static final String TABLE_NAME = "replication_test_table";
  protected static final int DEFAULT_DISCOVERY_INTERVAL_SECONDS = 2;
  protected static final int DEFAULT_CHECKPOINTING_INTERVAL_MILLIS = 500;
  protected static final String CHECKPOINT_DIR_PREFIX = "chk-";
  protected static final String CHECKPOINT_METADATA_FILE = "_metadata";

  @Rule
  public final KuduTestHarness sourceHarness = new KuduTestHarness();
  @Rule
  public final KuduTestHarness sinkHarness = new KuduTestHarness();
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  protected KuduClient sourceClient;
  protected KuduClient sinkClient;
  protected ReplicationEnvProvider envProvider;
  protected Path checkpointDir;

  @Before
  public void setupClientsAndEnvProvider() throws Exception {
    this.sourceClient = sourceHarness.getClient();
    this.sinkClient = sinkHarness.getClient();
    this.checkpointDir = tempFolder.newFolder("checkpoints").toPath();
    this.envProvider = new ReplicationEnvProvider(
            createDefaultJobConfig(),
            createDefaultReaderConfig(),
            createDefaultWriterConfig());
  }


  protected ReplicationJobConfig.Builder createDefaultJobConfigBuilder() {
    return ReplicationJobConfig.builder()
            .setSourceMasterAddresses(sourceHarness.getMasterAddressesAsString())
            .setSinkMasterAddresses(sinkHarness.getMasterAddressesAsString())
            .setTableName(TABLE_NAME)
            .setDiscoveryIntervalSeconds(DEFAULT_DISCOVERY_INTERVAL_SECONDS)
            .setCheckpointingIntervalMillis(DEFAULT_CHECKPOINTING_INTERVAL_MILLIS)
            .setCheckpointsDirectory(checkpointDir.toUri().toString());
  }

  protected ReplicationJobConfig createDefaultJobConfig() {
    return createDefaultJobConfigBuilder().build();
  }

  protected KuduReaderConfig createDefaultReaderConfig() {
    return KuduReaderConfig.Builder
            .setMasters(sourceHarness.getMasterAddressesAsString())
            .build();
  }

  protected KuduWriterConfig createDefaultWriterConfig() {
    return KuduWriterConfig.Builder
            .setMasters(sinkHarness.getMasterAddressesAsString())
            .build();
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

  protected Path findLatestCheckpoint(Path checkpointDir, JobID jobId) {
    File jobCheckpointDir = checkpointDir.resolve(jobId.toString()).toFile();
    if (!jobCheckpointDir.exists() || !jobCheckpointDir.isDirectory()) {
      throw new AssertionError("Job checkpoint directory not found: " + jobCheckpointDir);
    }

    File[] checkpoints = jobCheckpointDir.listFiles(
        f -> f.isDirectory() && f.getName().startsWith(CHECKPOINT_DIR_PREFIX));

    if (checkpoints == null || checkpoints.length == 0) {
      throw new AssertionError("No checkpoints found in " + jobCheckpointDir);
    }

    List<File> complete = Arrays.stream(checkpoints)
        .filter(dir -> new File(dir, CHECKPOINT_METADATA_FILE).isFile())
        .sorted(Comparator.comparing(File::getName).reversed())
        .collect(Collectors.toList());

    if (complete.isEmpty()) {
      throw new AssertionError("No complete checkpoints (with " + CHECKPOINT_METADATA_FILE +
          ") found in " + jobCheckpointDir);
    }

    return complete.get(0).toPath();
  }

  protected void waitForCheckpointCompletion(Path checkpointDir, JobID jobId, long timeoutMillis)
      throws Exception {
    if (timeoutMillis < DEFAULT_CHECKPOINTING_INTERVAL_MILLIS) {
      throw new IllegalArgumentException(
          "Timeout (" + timeoutMillis + "ms) must be at least " +
          DEFAULT_CHECKPOINTING_INTERVAL_MILLIS + "ms");
    }
    long startTime = System.currentTimeMillis();
    long endTime = startTime + timeoutMillis;

    Path jobCheckpointDir = checkpointDir.resolve(jobId.toString());

    while (System.currentTimeMillis() < endTime) {
      if (hasCompleteCheckpoint(jobCheckpointDir.toFile())) {
        LOG.debug("Checkpoint detected for job {} in {}", jobId, jobCheckpointDir);
        return;
      }
      Thread.sleep(DEFAULT_CHECKPOINTING_INTERVAL_MILLIS);
    }

    throw new AssertionError(
        "No complete checkpoint found for job " + jobId + " in directory " + jobCheckpointDir +
            " after " + timeoutMillis + "ms");
  }

  protected boolean hasCompleteCheckpoint(File dir) {
    if (dir == null || !dir.exists() || !dir.isDirectory()) {
      return false;
    }

    File[] files = dir.listFiles();
    if (files == null) {
      return false;
    }

    for (File file : files) {
      if (file.getName().startsWith(CHECKPOINT_DIR_PREFIX) && file.isDirectory()) {
        File metadata = new File(file, CHECKPOINT_METADATA_FILE);
        if (metadata.exists() && metadata.isFile()) {
          return true;
        }
      }
      if (file.isDirectory() && !file.getName().equals("shared") &&
              !file.getName().equals("taskowned")) {
        if (hasCompleteCheckpoint(file)) {
          return true;
        }
      }
    }
    return false;
  }

  protected void waitForJobTermination(JobID jobId, ClusterClient<?> client, long timeoutMillis)
      throws Exception {
    if (timeoutMillis < 100) {
      throw new IllegalArgumentException(
          "Timeout (" + timeoutMillis + "ms) must be at least 100ms");
    }
    long startTime = System.currentTimeMillis();
    long endTime = startTime + timeoutMillis;

    while (System.currentTimeMillis() < endTime) {
      JobStatus status = client.getJobStatus(jobId).get();
      if (status.isGloballyTerminalState()) {
        LOG.debug("Job {} terminated with status: {}", jobId, status);
        return;
      }
      Thread.sleep(100);
    }

    throw new AssertionError("Job " + jobId + " did not terminate within " +
        timeoutMillis + "ms");
  }
}
