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
import java.util.List;
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

import org.apache.kudu.util.HybridTimeUtil;

/**
 * A wrapper around KuduSourceEnumerator that adds metrics support until metrics are implemented
 * in the upstream Flink Kudu connector (FLINK-38187).
 * Uses the delegate pattern - all SplitEnumerator methods are delegated to the wrapped
 * KuduSourceEnumerator, while additionally exposing internal enumerator fields as metrics
 * via reflection to provide visibility into split assignment and timestamp tracking.
 * TODO(mgreber): remove this file once FLINK-38187 is resolved.
 */
public class MetricWrappedKuduEnumerator implements SplitEnumerator<KuduSourceSplit,
                                                                    KuduSourceEnumeratorState> {

  private final KuduSourceEnumerator delegate;
  private final SplitEnumeratorContext<KuduSourceSplit> context;

  private final Field lastEndTimestampField;
  private final Field pendingField;
  private final Field unassignedField;

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

  }



  @Override
  // Safe casts: lambdas return correct types for Gauge<Long> and Gauge<Integer>
  @SuppressWarnings("unchecked")
  public void start() {
    context.metricGroup().gauge("lastEndTimestamp", (Gauge<Long>) this::getLastEndTimestamp);
    context.metricGroup().gauge("pendingCount", (Gauge<Integer>) this::getPendingCount);
    context.metricGroup().gauge("unassignedCount", (Gauge<Integer>) this::getUnassignedCount);
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
}

