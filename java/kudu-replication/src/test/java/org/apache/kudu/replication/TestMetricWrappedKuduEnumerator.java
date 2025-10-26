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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumerator;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.kudu.source.split.SplitFinishedEvent;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.replication.wrappedsource.MetricWrappedKuduEnumerator;
import org.apache.kudu.replication.wrappedsource.ReflectionSecurityUtils;

/**
 * Unit tests for {@link MetricWrappedKuduEnumerator} checkpoint race condition fix (FLINK-38575).
 *
 * Tests that splits which finish between checkpoint snapshot and checkpoint completion
 * are properly retained in checkpoint state to prevent data loss.
 */
public class TestMetricWrappedKuduEnumerator {
  private static final Logger LOG = LoggerFactory.getLogger(TestMetricWrappedKuduEnumerator.class);

  /**
   * Tests the core race condition fix:
   * 1. Split finishes (SplitFinishedEvent arrives)
   * 2. Checkpoint snapshot is taken (should include finished split in pending)
   * 3. Job crashes before notifyCheckpointComplete
   * 4. On restore, finished split must be replayed (at-least-once semantics)
   *
   * Without the fix, the split would be removed from pending immediately on finish,
   * causing it to be missing from the checkpoint snapshot, leading to data loss.
   */
  @Test
  public void testSplitNotLostWhenFinishedBeforeCheckpointComplete() throws Exception {
    String splitId;
    KuduSourceEnumeratorState checkpointAfterComplete;
    try (MetricWrappedKuduEnumerator enumerator = createTestEnumerator()) {

      KuduSourceSplit testSplit = createTestSplit("tablet-1");

      enumerator.addReader(0);

      enumerator.handleSplitRequest(0, "localhost");
      List<KuduSourceSplit> assignedSplits = Collections.singletonList(testSplit);
      enumerator.addSplitsBack(assignedSplits, 0);

      SourceEvent splitFinishedEvent = createSplitFinishedEvent(assignedSplits);
      enumerator.handleSourceEvent(0, splitFinishedEvent);

      KuduSourceEnumeratorState checkpointBeforeComplete = enumerator.snapshotState(1L);

      List<KuduSourceSplit> pendingSplitsInCheckpoint =
              getPendingSplitsFromState(checkpointBeforeComplete);

      splitId = testSplit.splitId();
      assertTrue(
              "Finished split must be present in checkpoint before notifyCheckpointComplete",
              pendingSplitsInCheckpoint.stream()
                      .anyMatch(split -> split.splitId().equals(splitId)));

      enumerator.notifyCheckpointComplete(1L);

      checkpointAfterComplete = enumerator.snapshotState(2L);
    }
    List<KuduSourceSplit> pendingSplitsAfterCleanup =
        getPendingSplitsFromState(checkpointAfterComplete);

    assertTrue(
        "Finished split should be removed from pending after notifyCheckpointComplete",
        pendingSplitsAfterCleanup.stream()
            .noneMatch(split -> split.splitId().equals(splitId)));
  }

  /**
   * Tests that multiple splits finishing between checkpoints are all retained correctly.
   */
  @Test
  public void testMultipleSplitsRetainedBetweenCheckpoints() throws Exception {
    String split1Id;
    String split2Id;
    KuduSourceEnumeratorState checkpoint3;
    try (MetricWrappedKuduEnumerator enumerator = createTestEnumerator()) {

      KuduSourceSplit split1 = createTestSplit("tablet-1");

      enumerator.addReader(0);

      enumerator.addSplitsBack(Collections.singletonList(split1), 0);
      enumerator.handleSplitRequest(0, "localhost");
      enumerator.handleSourceEvent(0, createSplitFinishedEvent(Collections.singletonList(split1)));

      KuduSourceEnumeratorState checkpoint1 = enumerator.snapshotState(1L);
      List<KuduSourceSplit> pendingAfterCheckpoint1 = getPendingSplitsFromState(checkpoint1);

      split1Id = split1.splitId();
      assertTrue(
              "Split1 should be retained in pending after finishing but before checkpoint complete",
              pendingAfterCheckpoint1.stream().anyMatch(s -> s.splitId().equals(split1Id)));

      KuduSourceSplit split2 = createTestSplit("tablet-2");
      enumerator.addSplitsBack(Collections.singletonList(split2), 0);
      enumerator.handleSplitRequest(0, "localhost");
      enumerator.handleSourceEvent(0, createSplitFinishedEvent(Collections.singletonList(split2)));

      KuduSourceEnumeratorState checkpoint2 = enumerator.snapshotState(2L);
      List<KuduSourceSplit> pendingAfterCheckpoint2 = getPendingSplitsFromState(checkpoint2);

      split2Id = split2.splitId();
      assertTrue("Both finished splits should be in pending before any checkpoint completes",
              pendingAfterCheckpoint2.stream().anyMatch(s -> s.splitId().equals(split1Id)));
      assertTrue("Both finished splits should be in pending before any checkpoint completes",
              pendingAfterCheckpoint2.stream().anyMatch(s -> s.splitId().equals(split2Id)));

      enumerator.notifyCheckpointComplete(1L);

      checkpoint3 = enumerator.snapshotState(3L);
    }
    List<KuduSourceSplit> pendingAfterCleanup = getPendingSplitsFromState(checkpoint3);

    assertTrue("Both splits should be removed after checkpoint completes",
        pendingAfterCleanup.stream().noneMatch(s -> s.splitId().equals(split1Id)));
    assertTrue("Both splits should be removed after checkpoint completes",
        pendingAfterCleanup.stream().noneMatch(s -> s.splitId().equals(split2Id)));
  }

  /**
   * Tests that the PLAIN KuduSourceEnumerator has the race condition.
   * This test documents the bug in the upstream Flink Kudu connector and acts as a canary:
   * if this test starts FAILING, it means Flink has fixed FLINK-38575 upstream, and we can
   * potentially remove our wrapper.
   */
  @Test
  public void testPlainEnumeratorLosesSplitDueToRaceCondition() throws Exception {
    String splitId;
    KuduSourceEnumeratorState checkpointState;
    try (KuduSourceEnumerator plainEnumerator = createPlainEnumerator()) {

      KuduSourceSplit testSplit = createTestSplit("tablet-1");
      splitId = testSplit.splitId();

      plainEnumerator.addReader(0);
      plainEnumerator.handleSplitRequest(0, "localhost");
      List<KuduSourceSplit> assignedSplits = Collections.singletonList(testSplit);
      plainEnumerator.addSplitsBack(assignedSplits, 0);

      SourceEvent splitFinishedEvent = createSplitFinishedEvent(assignedSplits);
      plainEnumerator.handleSourceEvent(0, splitFinishedEvent);

      checkpointState = plainEnumerator.snapshotState(1L);
    }
    List<KuduSourceSplit> pendingSplitsInCheckpoint = getPendingSplitsFromState(checkpointState);

    boolean splitPresentInCheckpoint = pendingSplitsInCheckpoint.stream()
        .anyMatch(split -> split.splitId().equals(splitId));

    if (!splitPresentInCheckpoint) {
      LOG.info("EXPECTED: Plain enumerator exhibits race condition - " +
          "split lost between finish and checkpoint (FLINK-38575 still present in connector)");
    } else {
      throw new AssertionError(
          "Plain KuduSourceEnumerator unexpectedly retained the split in checkpoint! " +
          "This suggests FLINK-38575 has been fixed upstream. " +
          "Consider removing MetricWrappedKuduEnumerator if no longer needed.");
    }
  }

  @SuppressWarnings("unchecked")
  private MetricWrappedKuduEnumerator createTestEnumerator() {
    KuduTableInfo mockTableInfo = mock(KuduTableInfo.class);
    when(mockTableInfo.getName()).thenReturn("test_table");

    SplitEnumeratorMetricGroup mockMetricGroup = mock(SplitEnumeratorMetricGroup.class);
    when(mockMetricGroup.gauge(org.mockito.ArgumentMatchers.anyString(),
        org.mockito.ArgumentMatchers.any())).thenReturn(null);

    SplitEnumeratorContext<KuduSourceSplit> mockContext = mock(SplitEnumeratorContext.class);
    when(mockContext.currentParallelism()).thenReturn(1);
    when(mockContext.registeredReaders()).thenReturn(Collections.emptyMap());
    when(mockContext.metricGroup()).thenReturn(mockMetricGroup);

    KuduReaderConfig readerConfig = KuduReaderConfig.Builder
        .setMasters("localhost:7051")
        .build();

    return new MetricWrappedKuduEnumerator(
        mockTableInfo,
        readerConfig,
        org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED,
        Duration.ofSeconds(10),
        mockContext,
        null
    );
  }

  @SuppressWarnings("unchecked")
  private KuduSourceEnumerator createPlainEnumerator() {
    KuduTableInfo mockTableInfo = mock(KuduTableInfo.class);
    when(mockTableInfo.getName()).thenReturn("test_table");

    KuduReaderConfig readerConfig = KuduReaderConfig.Builder
        .setMasters("localhost:7051")
        .build();

    SplitEnumeratorContext<KuduSourceSplit> mockContext = mock(SplitEnumeratorContext.class);
    when(mockContext.currentParallelism()).thenReturn(1);
    when(mockContext.registeredReaders()).thenReturn(Collections.emptyMap());

    return new KuduSourceEnumerator(
        mockTableInfo,
        readerConfig,
        org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED,
        Duration.ofSeconds(10),
        mockContext
    );
  }

  private KuduSourceSplit createTestSplit(String splitId) {
    byte[] serializedToken = splitId.getBytes(StandardCharsets.UTF_8);
    return new KuduSourceSplit(serializedToken);
  }

  private SourceEvent createSplitFinishedEvent(List<KuduSourceSplit> finishedSplits) {
    return new SplitFinishedEvent(finishedSplits);
  }

  private List<KuduSourceSplit> getPendingSplitsFromState(KuduSourceEnumeratorState state) {
    return ReflectionSecurityUtils.getPrivateFieldValue(state, "pending");
  }
}


