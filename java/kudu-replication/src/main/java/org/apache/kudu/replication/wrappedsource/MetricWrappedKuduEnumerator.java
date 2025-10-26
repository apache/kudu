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

package org.apache.kudu.replication.wrappedsource;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumerator;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.util.HybridTimeUtil;

/**
 * A wrapper around KuduSourceEnumerator that adds metrics support until metrics are implemented
 * in the upstream Flink Kudu connector (FLINK-38187).
 * Uses the delegate pattern - all SplitEnumerator methods are delegated to the wrapped
 * KuduSourceEnumerator, while additionally exposing internal enumerator fields as metrics
 * via reflection to provide visibility into split assignment and timestamp tracking.
 *
 * Checkpoint Race Condition (FLINK-38575) Fix:
 * This wrapper also implements critical fixes to prevent data loss during checkpointing.
 * Without these fixes, records can be lost if the job crashes between split completion and
 * checkpoint completion. The fixes ensure at-least-once delivery semantics by:
 * 1. Deferring split removal from pending until checkpoint completes
 * 2. Re-enqueueing restored pending splits on restore to guarantee reprocessing
 *
 * TODO(mgreber): remove this file once FLINK-38187 and FLINK-38575 are resolved.
 *
 */
public class MetricWrappedKuduEnumerator implements SplitEnumerator<KuduSourceSplit,
                                                                    KuduSourceEnumeratorState> {

  private static final Logger LOG = LoggerFactory.getLogger(MetricWrappedKuduEnumerator.class);

  private final KuduSourceEnumerator delegate;
  private final SplitEnumeratorContext<KuduSourceSplit> context;

  private final Field lastEndTimestampField;
  private final Field pendingField;
  private final Field unassignedField;
  // Lazily initialized (not final)
  private Field finishedSplitsField;

  // Buffer finished splits until checkpoint completes
  private final Set<String> splitsPendingRemoval = new HashSet<>();

  public MetricWrappedKuduEnumerator(
          KuduTableInfo tableInfo,
          KuduReaderConfig readerConfig,
          Boundedness boundedness,
          Duration discoveryInterval,
          SplitEnumeratorContext<KuduSourceSplit> context,
          @Nullable KuduSourceEnumeratorState restoredState
  ) {
    this.context = context;

    if (restoredState != null) {
      this.delegate = new KuduSourceEnumerator(
              tableInfo, readerConfig, boundedness, discoveryInterval, context, restoredState);
    } else {
      this.delegate = new KuduSourceEnumerator(
              tableInfo, readerConfig, boundedness, discoveryInterval, context);
    }

    // Initialize final fields using privileged access to reflection operations
    this.lastEndTimestampField =
        ReflectionSecurityUtils.getAccessibleField(delegate, "lastEndTimestamp");
    this.pendingField = ReflectionSecurityUtils.getAccessibleField(delegate, "pending");
    this.unassignedField = ReflectionSecurityUtils.getAccessibleField(delegate, "unassigned");

    if (restoredState != null) {
      // Re-enqueue restored pending splits to guarantee reprocessing after failures.
      //
      // Since we use StatelessKuduReaderWrapper, readers don't checkpoint their splits.
      // All split lifecycle management flows through the enumerator, which simplifies
      // restoration and prevents duplicate split assignment issues.
      //
      // Restored pending contains two types of splits:
      // 1. True pending: splits actively being read when checkpoint was taken.
      //    Without reader state, these must be re-assigned from enumerator.
      //
      // 2. Buffered pending: splits finished by reader but kept in pending by our
      //    race fix (handleSourceEvent intercepts SplitFinishedEvent and defers
      //    removal until checkpoint completes). When checkpoint snapshot is taken,
      //    these buffered splits are captured in the snapshot, but their records
      //    may still be in-flight in the pipeline and not yet committed to Kudu.
      //    Re-enqueueing ensures these splits are replayed on restore, preventing
      //    data loss if the job crashes before checkpoint completes.
      //
      // Re-enqueueing all pending to unassigned ensures both types are replayed,
      // maintaining at-least-once semantics with idempotent sink operations.
      //
      // Note: With StatelessKuduReaderWrapper, readers start empty on restore, so all
      // splits flow through enumerator. We clear pending after re-enqueueing to unblock
      // timestamp advancement immediately. When splits finish, the buffering logic in
      // handleSourceEvent will re-add them to pending until checkpoint completes.
      List<KuduSourceSplit> pending = getPendingSplits();
      List<KuduSourceSplit> unassigned = getUnassignedSplits();
      if (!pending.isEmpty()) {
        LOG.info("Re-enqueueing {} restored pending splits to unassigned for reprocessing",
                 pending.size());
        unassigned.addAll(pending);
        pending.clear();
      }
    }
  }



  @Override
  // Safe casts: lambdas return correct types for Gauge<Long> and Gauge<Integer>
  public void start() {
    context.metricGroup().gauge("lastEndTimestamp", (Gauge<Long>) this::getLastEndTimestamp);
    context.metricGroup().gauge("pendingCount", (Gauge<Integer>) this::getPendingCount);
    context.metricGroup().gauge("unassignedCount", (Gauge<Integer>) this::getUnassignedCount);
    context.metricGroup().gauge("pendingRemovalCount",
            (Gauge<Integer>) this::getPendingRemovalCount);
    delegate.start();
  }


  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    delegate.handleSplitRequest(subtaskId, requesterHostname);
  }

  @Override
  public void addSplitsBack(List<KuduSourceSplit> splits, int subtaskId) {
    delegate.addSplitsBack(splits, subtaskId);
  }

  @Override
  public void addReader(int subtaskId) {
    delegate.addReader(subtaskId);
  }

  @Override
  public KuduSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
    return delegate.snapshotState(checkpointId);
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } catch (Exception e) {
      throw new RuntimeException("Error closing KuduSplitEnumerator", e);
    }
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    // Intercept SplitFinishedEvent and defer split removal until checkpoint completes
    // Check event type by class name to avoid depending on connector's internal event classes
    if (sourceEvent.getClass().getSimpleName().equals("SplitFinishedEvent")) {
      // Perform all reflection operations first, before any state mutation.
      // This ensures atomicity: if reflection fails (throws RuntimeException), no state has
      // been modified.
      List<KuduSourceSplit> finishedSplits;
      List<KuduSourceSplit> pending;

      try {
        finishedSplits = getFinishedSplits(sourceEvent);
        pending = getPendingSplits();
      } catch (RuntimeException e) {
        throw new RuntimeException(
            "Failed to access internal Flink connector fields via reflection in " +
            "wrapped enumerator.", e);
      }

      // Delegate the event to the underlying enumerator so it can schedule additional work
      // immediately. The delegate will remove finished splits from 'pending', which is the
      // normal behavior we need to intercept and fix.
      delegate.handleSourceEvent(subtaskId, sourceEvent);

      // After delegate processed the event, re-add finished splits back to 'pending'.
      // This preserves at-least-once semantics without blocking further split assignment.
      // The splits will remain in 'pending' until notifyCheckpointComplete() is called,
      // ensuring they're recoverable if the job crashes before checkpoint completion.
      for (KuduSourceSplit split : finishedSplits) {
        String splitId = split.splitId();
        splitsPendingRemoval.add(splitId);

        boolean wasInPending = pending.stream().anyMatch(s -> s.splitId().equals(splitId));
        if (!wasInPending) {
          pending.add(split);
        }

        LOG.debug("Split {} finished by subtask {}; deferring removal (restored in pending)",
                  splitId, subtaskId);
      }
      return;
    }

    // For all other events, delegate normally
    delegate.handleSourceEvent(subtaskId, sourceEvent);
  }

  private long getLastEndTimestamp() {
    long hybridTime = ReflectionSecurityUtils.getLongFieldValue(lastEndTimestampField, delegate);
    long[] parsed = HybridTimeUtil.HTTimestampToPhysicalAndLogical(hybridTime);
    long epochMicros = parsed[0];
    return epochMicros / 1_000;
  }

  private int getPendingCount() {
    List<KuduSourceSplit> pending = ReflectionSecurityUtils.getFieldValue(pendingField, delegate);
    return pending.size();
  }

  private int getUnassignedCount() {
    List<KuduSourceSplit> unassigned =
        ReflectionSecurityUtils.getFieldValue(unassignedField, delegate);
    return unassigned.size();
  }

  private int getPendingRemovalCount() {
    return splitsPendingRemoval.size();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // Only after checkpoint completes do we remove finished splits from pending
    // This ensures in-flight/buffered records are recoverable on restore
    if (!splitsPendingRemoval.isEmpty()) {
      List<KuduSourceSplit> pending = getPendingSplits();

      int removedCount = 0;
      for (String splitId : splitsPendingRemoval) {
        boolean removedFromPending = pending.removeIf(split -> split.splitId().equals(splitId));

        if (removedFromPending) {
          removedCount++;
          LOG.debug("Checkpoint {} complete: removed split {} from pending",
                    checkpointId, splitId);
        }
      }

      LOG.info("Checkpoint {} complete: removed {} finished splits from enumerator state",
               checkpointId, removedCount);
      splitsPendingRemoval.clear();
    }

    delegate.notifyCheckpointComplete(checkpointId);
  }

  private List<KuduSourceSplit> getPendingSplits() {
    return ReflectionSecurityUtils.getFieldValue(pendingField, delegate);
  }

  private List<KuduSourceSplit> getUnassignedSplits() {
    return ReflectionSecurityUtils.getFieldValue(unassignedField, delegate);
  }

  private List<KuduSourceSplit> getFinishedSplits(SourceEvent sourceEvent) {
    // Lazy initialization: finishedSplits is a field of sourceEvent, not our delegate,
    // so we can only cache the Field accessor once we receive the first event instance.
    if (finishedSplitsField == null) {
      finishedSplitsField = ReflectionSecurityUtils.getAccessibleField(
          sourceEvent, "finishedSplits");
    }
    return ReflectionSecurityUtils.getFieldValue(finishedSplitsField, sourceEvent);
  }
}

